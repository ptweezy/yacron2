//! The scheduler: the heart of yacron2.
//!
//! A single task owns all mutable state (the loaded jobs, the set of running
//! instances, and per-job retry state) and drives everything through one
//! `select!` loop. Job executions, retry timers, and the web API all
//! communicate with it via an unbounded message channel, so there are no locks
//! on the shared state — it is an actor.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDateTime, Timelike, Utc};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::config::{parse_config, parse_config_string, Config, Job, Schedule, WebConfig};
use crate::error::ConfigResult;
use crate::job::{self, JobOutcome};
use crate::report;
use crate::web::WebServer;

/// Messages delivered to the scheduler task.
pub(crate) enum SchedulerMsg {
    JobStarted {
        id: u64,
        name: String,
        pid: Option<u32>,
    },
    JobFinished {
        id: u64,
        name: String,
        outcome: Box<JobOutcome>,
    },
    RetryDue {
        name: String,
    },
    Web(WebCommand),
}

/// Commands issued by the web API, each carrying a reply channel.
pub(crate) enum WebCommand {
    Status(oneshot::Sender<Vec<JobStatus>>),
    StartJob {
        name: String,
        reply: oneshot::Sender<StartJobResult>,
    },
}

/// Result of a web-triggered job start.
pub(crate) enum StartJobResult {
    Started,
    NotFound,
    Disabled,
}

/// A snapshot of one job's status, for `GET /status`.
pub(crate) enum JobStatus {
    Running {
        name: String,
        pids: Vec<u32>,
    },
    Disabled {
        name: String,
    },
    Scheduled {
        name: String,
        scheduled_in: ScheduledIn,
    },
}

pub(crate) enum ScheduledIn {
    Seconds(f64),
    Reboot,
}

/// A currently-running instance of a job.
struct RunningHandle {
    id: u64,
    pid: Option<u32>,
    cancel: watch::Sender<bool>,
    replaced: bool,
    config: Arc<Job>,
}

/// Per-job exponential-backoff retry bookkeeping.
struct RetryState {
    delay: f64,
    multiplier: f64,
    max_delay: f64,
    count: i64,
    cancelled: bool,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl RetryState {
    fn new(initial_delay: f64, multiplier: f64, max_delay: f64) -> RetryState {
        RetryState {
            delay: initial_delay,
            multiplier,
            max_delay,
            count: 0,
            cancelled: false,
            task: None,
        }
    }

    fn next_delay(&mut self) -> f64 {
        let delay = self.delay;
        self.delay = (self.delay * self.multiplier).min(self.max_delay);
        self.count += 1;
        delay
    }
}

/// The scheduler.
pub struct Cron {
    config_arg: Option<String>,
    jobs: Vec<Arc<Job>>,
    by_name: HashMap<String, usize>,
    running: HashMap<String, Vec<RunningHandle>>,
    retry: HashMap<String, RetryState>,
    next_id: u64,
    reports: JoinSet<()>,
    web: Option<WebServer>,
    web_config: Option<WebConfig>,
    logging_warned: bool,
    stop: bool,
    msg_tx: mpsc::UnboundedSender<SchedulerMsg>,
    msg_rx: mpsc::UnboundedReceiver<SchedulerMsg>,
}

impl Cron {
    fn empty(config_arg: Option<String>) -> Cron {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        Cron {
            config_arg,
            jobs: Vec::new(),
            by_name: HashMap::new(),
            running: HashMap::new(),
            retry: HashMap::new(),
            next_id: 0,
            reports: JoinSet::new(),
            web: None,
            web_config: None,
            logging_warned: false,
            stop: false,
            msg_tx,
            msg_rx,
        }
    }

    /// Construct a scheduler from a config file/directory, loading it once so
    /// configuration errors surface immediately (used for `--validate-config`).
    pub fn new(config_arg: String) -> ConfigResult<Cron> {
        let mut cron = Cron::empty(Some(config_arg));
        cron.reload()?;
        Ok(cron)
    }

    /// Construct a scheduler from an inline YAML string (for tests). The jobs
    /// are fixed; the run loop does not reload them.
    #[allow(dead_code)]
    pub fn from_yaml(yaml: &str) -> ConfigResult<Cron> {
        let config = parse_config_string(yaml, "")?;
        let mut cron = Cron::empty(None);
        cron.set_jobs(config.jobs);
        Ok(cron)
    }

    fn set_jobs(&mut self, jobs: Vec<Job>) {
        self.jobs = jobs.into_iter().map(Arc::new).collect();
        self.by_name = self
            .jobs
            .iter()
            .enumerate()
            .map(|(i, j)| (j.name.clone(), i))
            .collect();
    }

    /// Reload configuration from disk (no-op for inline configs), returning the
    /// web/logging config that came with it.
    fn reload(&mut self) -> ConfigResult<ReloadOutcome> {
        match &self.config_arg {
            Some(arg) => {
                let config: Config = parse_config(arg)?;
                let web = config.web.clone();
                let logging = config.logging.is_some();
                self.set_jobs(config.jobs);
                Ok(ReloadOutcome {
                    reloaded: true,
                    web,
                    has_logging: logging,
                })
            }
            None => Ok(ReloadOutcome {
                reloaded: false,
                web: None,
                has_logging: false,
            }),
        }
    }

    /// Run the scheduler until `shutdown` fires.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let mut startup = true;

        while !self.stop {
            match self.reload() {
                Ok(outcome) => {
                    if outcome.reloaded {
                        if let Err(err) = self.start_stop_web(outcome.web.clone()).await {
                            error!("web: {err}");
                        }
                    }
                    if outcome.has_logging && !self.logging_warned {
                        self.logging_warned = true;
                        warn!(
                            "a 'logging:' config section is present but is not \
                             applied by the Rust port; use --log-level / \
                             RUST_LOG instead"
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "Error in configuration file(s), so not updating any \
                         of the config.:\n{err}"
                    );
                }
            }

            self.spawn_due_jobs(startup).await;
            startup = false;

            // Sleep until the top of the next minute, servicing messages in the
            // meantime.
            let sleep = tokio::time::sleep(duration_to_next_minute());
            tokio::pin!(sleep);
            loop {
                self.reap_reports();
                tokio::select! {
                    _ = &mut sleep => break,
                    changed = shutdown.changed() => {
                        if changed.is_ok() && *shutdown.borrow() {
                            self.stop = true;
                            break;
                        }
                    }
                    Some(msg) = self.msg_rx.recv() => {
                        self.handle_message(msg).await;
                    }
                }
            }
        }

        self.shutdown().await;
    }

    async fn shutdown(&mut self) {
        info!("Shutting down (after currently running jobs finish)...");
        let names: Vec<String> = self.retry.keys().cloned().collect();
        for name in names {
            self.cancel_job_retries(&name);
        }

        // Wait for running jobs to finish, processing their completion events
        // (failures are not reported during shutdown; successes still are).
        while !self.running.is_empty() {
            match self.msg_rx.recv().await {
                Some(msg) => self.handle_message(msg).await,
                None => break,
            }
        }

        // Give in-flight reports a brief chance to flush.
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            while self.reports.join_next().await.is_some() {}
        })
        .await;

        if self.web.is_some() {
            info!("Stopping http server");
            let _ = self.start_stop_web(None).await;
        }
    }

    fn reap_reports(&mut self) {
        while self.reports.try_join_next().is_some() {}
    }

    async fn handle_message(&mut self, msg: SchedulerMsg) {
        match msg {
            SchedulerMsg::JobStarted { id, name, pid } => {
                if let Some(list) = self.running.get_mut(&name) {
                    if let Some(handle) = list.iter_mut().find(|h| h.id == id) {
                        handle.pid = pid;
                    }
                }
            }
            SchedulerMsg::JobFinished { id, name, outcome } => {
                self.handle_finished(id, &name, *outcome).await;
            }
            SchedulerMsg::RetryDue { name } => self.on_retry_due(name).await,
            SchedulerMsg::Web(cmd) => self.handle_web(cmd).await,
        }
    }

    // -- scheduling ---------------------------------------------------------

    async fn spawn_due_jobs(&mut self, startup: bool) {
        let due: Vec<Arc<Job>> = self
            .jobs
            .iter()
            .filter(|job| job_should_run(startup, job))
            .cloned()
            .collect();
        for job in due {
            self.launch_scheduled_job(job).await;
        }
    }

    async fn launch_scheduled_job(&mut self, job: Arc<Job>) {
        self.cancel_job_retries(&job.name);
        // A non-zero maximumRetries (including -1 = forever) arms retry state.
        if job.retry.maximum_retries != 0 {
            self.retry.insert(
                job.name.clone(),
                RetryState::new(
                    job.retry.initial_delay,
                    job.retry.backoff_multiplier,
                    job.retry.maximum_delay,
                ),
            );
        }
        self.maybe_launch_job(job).await;
    }

    async fn maybe_launch_job(&mut self, job: Arc<Job>) {
        if self
            .running
            .get(&job.name)
            .is_some_and(|list| !list.is_empty())
        {
            warn!(
                "Job {}: still running and concurrencyPolicy is {}",
                job.name, job.concurrency_policy
            );
            use crate::config::ConcurrencyPolicy::*;
            match job.concurrency_policy {
                Allow => {}
                Forbid => return,
                Replace => {
                    if let Some(list) = self.running.get_mut(&job.name) {
                        for handle in list.iter_mut() {
                            // Mark before cancelling so the completion is
                            // treated as a replacement, not a failure.
                            handle.replaced = true;
                            let _ = handle.cancel.send(true);
                        }
                    }
                }
            }
        }

        info!("Starting job {}", job.name);
        let id = self.next_id;
        self.next_id += 1;
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        self.running
            .entry(job.name.clone())
            .or_default()
            .push(RunningHandle {
                id,
                pid: None,
                cancel: cancel_tx,
                replaced: false,
                config: job.clone(),
            });

        let msg_tx = self.msg_tx.clone();
        let cfg = job.clone();
        tokio::spawn(async move {
            let start_tx = msg_tx.clone();
            let name = cfg.name.clone();
            let outcome = job::execute(&cfg, &mut cancel_rx, |pid| {
                let _ = start_tx.send(SchedulerMsg::JobStarted {
                    id,
                    name: name.clone(),
                    pid,
                });
            })
            .await;
            let _ = msg_tx.send(SchedulerMsg::JobFinished {
                id,
                name: cfg.name.clone(),
                outcome: Box::new(outcome),
            });
        });
        info!("Job {} spawned", job.name);
    }

    async fn on_retry_due(&mut self, name: String) {
        match self.by_name.get(&name) {
            Some(&idx) => {
                let job = self.jobs[idx].clone();
                self.maybe_launch_job(job).await;
            }
            None => {
                warn!(
                    "Cron job {name} was scheduled for retry, but disappeared \
                     from the configuration"
                );
                self.retry.remove(&name);
            }
        }
    }

    // -- completion ---------------------------------------------------------

    async fn handle_finished(&mut self, id: u64, name: &str, outcome: JobOutcome) {
        let handle = self.remove_running(id, name);
        let Some(handle) = handle else { return };

        if handle.replaced {
            info!("Job {name} was replaced by a newer instance");
            return;
        }

        let cfg = handle.config;
        let fail_reason = outcome.fail_reason(&cfg);
        info!(
            "Job {name} exit code {:?}; has stdout: {}, has stderr: {}; \
             fail_reason: {:?}",
            outcome.retcode,
            outcome.stdout.as_deref().is_some_and(|s| !s.is_empty()),
            outcome.stderr.as_deref().is_some_and(|s| !s.is_empty()),
            fail_reason,
        );

        if fail_reason.is_some() {
            self.handle_failure(cfg, outcome).await;
        } else {
            self.handle_success(cfg, outcome).await;
        }
    }

    fn remove_running(&mut self, id: u64, name: &str) -> Option<RunningHandle> {
        let list = self.running.get_mut(name)?;
        let pos = list.iter().position(|h| h.id == id)?;
        let handle = list.remove(pos);
        if list.is_empty() {
            self.running.remove(name);
        }
        Some(handle)
    }

    async fn handle_failure(&mut self, cfg: Arc<Job>, outcome: JobOutcome) {
        if self.stop {
            return;
        }
        if let Some(out) = outcome.stdout.as_deref().filter(|s| !s.is_empty()) {
            info!("Job {} STDOUT:\n{}", cfg.name, out.trim_end());
        }
        if let Some(err) = outcome.stderr.as_deref().filter(|s| !s.is_empty()) {
            info!("Job {} STDERR:\n{}", cfg.name, err.trim_end());
        }

        let permanent = self.decide_retry(&cfg);
        self.spawn_report(cfg, outcome, ReportKind::Failure { permanent });
    }

    /// Decide whether this failure is permanent (and, if not, schedule the next
    /// retry). Returns `true` when no more retries remain.
    fn decide_retry(&mut self, cfg: &Job) -> bool {
        let name = &cfg.name;
        let max = cfg.retry.maximum_retries;

        let state = match self.retry.get_mut(name) {
            None => return true,
            Some(state) if state.cancelled => return true,
            Some(state) => state,
        };

        // Cancel any already-pending retry timer.
        if let Some(task) = state.task.take() {
            task.abort();
        }

        if state.count >= max && max != -1 {
            self.cancel_job_retries(name);
            return true;
        }

        let delay = state.next_delay();
        let count = state.count;
        let tx = self.msg_tx.clone();
        let job_name = name.clone();
        let task = tokio::spawn(async move {
            info!(
                "Cron job {job_name} scheduled to be retried (#{count}) in \
                 {delay:.1} seconds"
            );
            tokio::time::sleep(Duration::from_secs_f64(delay.max(0.0))).await;
            let _ = tx.send(SchedulerMsg::RetryDue { name: job_name });
        });
        if let Some(state) = self.retry.get_mut(name) {
            state.task = Some(task);
        }
        false
    }

    async fn handle_success(&mut self, cfg: Arc<Job>, outcome: JobOutcome) {
        self.cancel_job_retries(&cfg.name);
        self.spawn_report(cfg, outcome, ReportKind::Success);
    }

    fn cancel_job_retries(&mut self, name: &str) {
        if let Some(mut state) = self.retry.remove(name) {
            state.cancelled = true;
            if let Some(task) = state.task.take() {
                task.abort();
            }
        }
    }

    fn spawn_report(&mut self, cfg: Arc<Job>, outcome: JobOutcome, kind: ReportKind) {
        self.reports.spawn(async move {
            match kind {
                ReportKind::Success => {
                    info!("Cron job {}: reporting success", cfg.name);
                    report::run_reports(&cfg.on_success_report, true, &cfg, &outcome).await;
                }
                ReportKind::Failure { permanent } => {
                    info!("Cron job {}: reporting failure", cfg.name);
                    report::run_reports(&cfg.on_failure_report, false, &cfg, &outcome).await;
                    if permanent {
                        info!("Cron job {}: reporting permanent failure", cfg.name);
                        report::run_reports(
                            &cfg.on_permanent_failure_report,
                            false,
                            &cfg,
                            &outcome,
                        )
                        .await;
                    }
                }
            }
        });
    }

    // -- web ----------------------------------------------------------------

    async fn handle_web(&mut self, cmd: WebCommand) {
        match cmd {
            WebCommand::Status(reply) => {
                let _ = reply.send(self.build_status());
            }
            WebCommand::StartJob { name, reply } => {
                let result = match self.by_name.get(&name) {
                    None => StartJobResult::NotFound,
                    Some(&idx) => {
                        let job = self.jobs[idx].clone();
                        if !job.enabled {
                            StartJobResult::Disabled
                        } else {
                            self.maybe_launch_job(job).await;
                            StartJobResult::Started
                        }
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    fn build_status(&self) -> Vec<JobStatus> {
        self.jobs
            .iter()
            .map(|job| {
                let running = self.running.get(&job.name).filter(|list| !list.is_empty());
                if let Some(list) = running {
                    JobStatus::Running {
                        name: job.name.clone(),
                        pids: list.iter().filter_map(|h| h.pid).collect(),
                    }
                } else if !job.enabled {
                    JobStatus::Disabled {
                        name: job.name.clone(),
                    }
                } else {
                    let scheduled_in = match &job.schedule {
                        Schedule::Reboot => ScheduledIn::Reboot,
                        Schedule::Cron(expr) => {
                            let now = truncate_to_minute(job.timezone.now());
                            let secs = expr
                                .next_after(now)
                                .map(|next| (next - now).num_seconds() as f64)
                                .unwrap_or(0.0);
                            ScheduledIn::Seconds(secs)
                        }
                    };
                    JobStatus::Scheduled {
                        name: job.name.clone(),
                        scheduled_in,
                    }
                }
            })
            .collect()
    }

    async fn start_stop_web(&mut self, web_config: Option<WebConfig>) -> ConfigResult<()> {
        let changed = self.web_config != web_config;
        if self.web.is_some() && (web_config.is_none() || changed) {
            info!("Stopping http server");
            if let Some(server) = self.web.take() {
                server.stop().await;
            }
            self.web_config = None;
        }

        if let Some(config) = &web_config {
            if !config.listen.is_empty() && self.web.is_none() {
                let server = WebServer::start(config, self.msg_tx.clone()).await?;
                self.web = Some(server);
                self.web_config = web_config;
            }
        }
        Ok(())
    }
}

struct ReloadOutcome {
    reloaded: bool,
    web: Option<WebConfig>,
    has_logging: bool,
}

enum ReportKind {
    Success,
    Failure { permanent: bool },
}

/// Decide whether a job is due to run right now.
fn job_should_run(startup: bool, job: &Job) -> bool {
    if !job.enabled {
        debug!("Job {} is disabled in the config", job.name);
        return false;
    }
    if startup {
        return matches!(job.schedule, Schedule::Reboot);
    }
    match &job.schedule {
        Schedule::Reboot => false,
        Schedule::Cron(expr) => {
            let now = truncate_to_minute(job.timezone.now());
            expr.matches(&now)
        }
    }
}

fn truncate_to_minute(dt: NaiveDateTime) -> NaiveDateTime {
    dt.with_second(0)
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt)
}

/// Seconds until the top of the next minute (UTC), matching the original
/// minute-aligned wakeup.
fn duration_to_next_minute() -> Duration {
    let now = Utc::now();
    let secs = now.second() as f64 + now.nanosecond() as f64 / 1e9;
    Duration::from_secs_f64((60.0 - secs).max(0.001))
}

/// Human-readable "in N seconds/minutes/hours/days", ported verbatim.
pub(crate) fn natural_time(seconds: f64) -> String {
    let plural = |n: i64| if n >= 2 { "s" } else { "" };
    if seconds < 120.0 {
        let n = seconds as i64;
        return format!("in {n} second{}", plural(n));
    }
    let minutes = seconds / 60.0;
    if minutes < 120.0 {
        let n = minutes as i64;
        return format!("in {n} minute{}", plural(n));
    }
    let hours = minutes / 60.0;
    if hours < 48.0 {
        let n = hours as i64;
        return format!("in {n} hour{}", plural(n));
    }
    let days = hours / 24.0;
    let n = days as i64;
    format!("in {n} day{}", plural(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn natural_time_matches() {
        assert_eq!(natural_time(10.0), "in 10 seconds");
        assert_eq!(natural_time(305.0), "in 5 minutes");
        assert_eq!(natural_time(5000.0), "in 83 minutes");
        assert_eq!(natural_time(50000.0), "in 13 hours");
        assert_eq!(natural_time(500000.0), "in 5 days");
    }

    #[cfg(unix)]
    fn sleeper_cron(policy: &str) -> Cron {
        Cron::from_yaml(&format!(
            "jobs:\n  - name: test\n    command: sleep 30\n    \
             schedule: \"@reboot\"\n    concurrencyPolicy: {policy}\n"
        ))
        .unwrap()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrency_allow_runs_both() {
        let mut cron = sleeper_cron("Allow");
        let job = cron.jobs[0].clone();
        cron.maybe_launch_job(job.clone()).await;
        cron.maybe_launch_job(job).await;
        assert_eq!(cron.running["test"].len(), 2);
        assert!(cron.running["test"].iter().all(|h| !h.replaced));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrency_forbid_skips_second() {
        let mut cron = sleeper_cron("Forbid");
        let job = cron.jobs[0].clone();
        cron.maybe_launch_job(job.clone()).await;
        cron.maybe_launch_job(job).await;
        assert_eq!(cron.running["test"].len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrency_replace_marks_old() {
        let mut cron = sleeper_cron("Replace");
        let job = cron.jobs[0].clone();
        cron.maybe_launch_job(job.clone()).await;
        cron.maybe_launch_job(job).await;
        let list = &cron.running["test"];
        assert_eq!(list.len(), 2);
        assert!(list[0].replaced);
        assert!(!list[1].replaced);
    }
}
