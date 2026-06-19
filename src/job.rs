//! Running a single cron job: building its command line, capturing and
//! prefixing its output (with the top/bottom save-limit behaviour), enforcing
//! execution and kill timeouts, and (on Unix) dropping privileges.

use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{Command as JobCommand, EnvVar, Job};
use crate::statsd::StatsdWriter;
use crate::template;

/// The sentinel exit code used when a job is killed for exceeding its
/// `executionTimeout` (matches the original yacron2).
pub const TIMEOUT_RETCODE: i32 = -100;

/// The exit code reported when a job's command could not be launched at all.
pub const SPAWN_FAILED_RETCODE: i32 = 127;

/// An immutable snapshot of a finished job run.
#[derive(Debug, Clone, Default)]
pub struct JobOutcome {
    pub retcode: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub stdout_discarded: usize,
    pub stderr_discarded: usize,
    /// The environment overrides applied to the child (for report templates),
    /// or `None` when the job inherited yacron2's environment unchanged.
    pub env: Option<Vec<EnvVar>>,
}

impl JobOutcome {
    /// Why this run is considered a failure, or `None` if it succeeded. The
    /// predicate order matches the original `RunningJob.fail_reason`.
    pub fn fail_reason(&self, cfg: &Job) -> Option<String> {
        let fw = &cfg.fails_when;
        if fw.always {
            return Some("failsWhen=always".to_string());
        }
        if fw.nonzero_return && self.retcode != Some(0) {
            let code = match self.retcode {
                Some(c) => c.to_string(),
                None => "None".to_string(),
            };
            return Some(format!("failsWhen=nonzeroReturn and retcode={code}"));
        }
        if fw.produces_stdout
            && (self.stdout.as_deref().is_some_and(|s| !s.is_empty()) || self.stdout_discarded > 0)
        {
            return Some("failsWhen=producesStdout and stdout is not empty".to_string());
        }
        if fw.produces_stderr
            && (self.stderr.as_deref().is_some_and(|s| !s.is_empty()) || self.stderr_discarded > 0)
        {
            return Some("failsWhen=producesStderr and stderr is not empty".to_string());
        }
        None
    }

    pub fn failed(&self, cfg: &Job) -> bool {
        self.fail_reason(cfg).is_some()
    }
}

/// Run a job to completion, returning a snapshot of its result.
///
/// `on_start` is invoked once with the child PID (or `None` if the command
/// could not be launched) so the scheduler can record it for `GET /status`.
/// `cancel` lets the scheduler terminate the run early (for
/// `concurrencyPolicy: Replace`).
pub async fn execute(
    cfg: &Job,
    cancel: &mut watch::Receiver<bool>,
    on_start: impl FnOnce(Option<u32>),
) -> JobOutcome {
    let env_overrides = if cfg.environment.is_empty() {
        None
    } else {
        Some(cfg.environment.clone())
    };

    let mut command = build_command(cfg);
    if cfg.capture_stdout {
        command.stdout(Stdio::piped());
    }
    if cfg.capture_stderr {
        command.stderr(Stdio::piped());
    }
    command.kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            warn!("Error launching subprocess of job {}: {err}", cfg.name);
            on_start(None);
            // Treated as a normal failure with the conventional 127, not a bug.
            return JobOutcome {
                retcode: Some(SPAWN_FAILED_RETCODE),
                env: env_overrides,
                ..Default::default()
            };
        }
    };

    on_start(child.id());

    let mut statsd = cfg.statsd.as_ref().map(StatsdWriter::new);
    if let Some(writer) = statsd.as_mut() {
        if let Err(err) = writer.job_started().await {
            warn!(
                "Job {}: failed to send statsd start metric: {err}",
                cfg.name
            );
        }
    }

    let stdout_reader = child
        .stdout
        .take()
        .map(|stream| spawn_reader(cfg, stream, StreamKind::Stdout));
    let stderr_reader = child
        .stderr
        .take()
        .map(|stream| spawn_reader(cfg, stream, StreamKind::Stderr));

    let retcode = wait_for_exit(cfg, &mut child, cancel).await;

    let (stdout, stdout_discarded) = join_reader(stdout_reader).await;
    let (stderr, stderr_discarded) = join_reader(stderr_reader).await;

    let outcome = JobOutcome {
        retcode,
        stdout,
        stderr,
        stdout_discarded,
        stderr_discarded,
        env: env_overrides,
    };

    if let Some(writer) = statsd.as_mut() {
        if let Err(err) = writer.job_stopped(outcome.failed(cfg)).await {
            warn!("Job {}: failed to send statsd stop metric: {err}", cfg.name);
        }
    }

    outcome
}

/// Construct the (yet-unspawned) command for a job, applying the shell/argv
/// rules, environment overrides, and (on Unix) the privilege-drop hook.
fn build_command(cfg: &Job) -> Command {
    let (program, args) = resolve_argv(&cfg.command, &cfg.shell);

    let mut std_cmd = std::process::Command::new(program);
    std_cmd.args(args);

    // The child always inherits yacron2's environment; configured variables are
    // overlaid on top (never a clean slate), matching the original.
    for var in &cfg.environment {
        std_cmd.env(&var.key, &var.value);
    }

    install_privilege_drop(cfg, &mut std_cmd);

    Command::from(std_cmd)
}

/// Resolve a job command into `(program, args)`.
pub(crate) fn resolve_argv(command: &JobCommand, shell: &str) -> (String, Vec<String>) {
    match command {
        JobCommand::Argv(argv) => {
            let mut it = argv.iter();
            let program = it.next().cloned().unwrap_or_default();
            (program, it.cloned().collect())
        }
        JobCommand::Line(line) => {
            if !shell.is_empty() {
                (shell.to_string(), vec!["-c".to_string(), line.clone()])
            } else {
                default_shell_argv(line)
            }
        }
    }
}

#[cfg(unix)]
fn default_shell_argv(line: &str) -> (String, Vec<String>) {
    (
        "/bin/sh".to_string(),
        vec!["-c".to_string(), line.to_string()],
    )
}

#[cfg(not(unix))]
fn default_shell_argv(line: &str) -> (String, Vec<String>) {
    ("cmd".to_string(), vec!["/C".to_string(), line.to_string()])
}

#[cfg(unix)]
fn install_privilege_drop(cfg: &Job, cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;

    if cfg.uid.is_none() && cfg.gid.is_none() {
        return;
    }
    let uid = cfg.uid;
    let gid = cfg.gid;
    let username = cfg.username.clone();
    // SAFETY: the closure runs in the forked child before exec and only calls
    // async-signal-safe-enough setgroups/setgid/setuid via nix.
    unsafe {
        cmd.pre_exec(move || demote(uid, gid, username.as_deref()));
    }
}

#[cfg(not(unix))]
fn install_privilege_drop(_cfg: &Job, _cmd: &mut std::process::Command) {
    // user/group are rejected at config-parse time on non-Unix platforms.
}

/// Drop privileges in the child. Order matters: supplementary groups must be
/// set/cleared *before* setuid, then the primary gid, then the uid — otherwise
/// the child keeps root's supplementary groups (the classic setuid pitfall).
#[cfg(unix)]
fn demote(uid: Option<u32>, gid: Option<u32>, username: Option<&str>) -> std::io::Result<()> {
    use nix::unistd::{setgid, setgroups, setuid, Gid, Uid};
    use std::ffi::CString;

    let io = |e: nix::Error| std::io::Error::from(e);

    match (username, gid) {
        (Some(name), Some(gid)) => {
            let cname =
                CString::new(name).map_err(|_| std::io::Error::other("invalid username"))?;
            nix::unistd::initgroups(&cname, Gid::from_raw(gid)).map_err(io)?;
        }
        _ => {
            setgroups(&[]).map_err(io)?;
        }
    }
    if let Some(gid) = gid {
        setgid(Gid::from_raw(gid)).map_err(io)?;
    }
    if let Some(uid) = uid {
        setuid(Uid::from_raw(uid)).map_err(io)?;
    }
    Ok(())
}

/// Await the child's exit, honouring `executionTimeout` and scheduler-driven
/// cancellation.
async fn wait_for_exit(
    cfg: &Job,
    child: &mut Child,
    cancel: &mut watch::Receiver<bool>,
) -> Option<i32> {
    let deadline = cfg
        .execution_timeout
        .map(|t| Instant::now() + Duration::from_secs_f64(t));

    loop {
        if let Some(deadline) = deadline {
            tokio::select! {
                status = child.wait() => return status.ok().and_then(status_to_code),
                _ = tokio::time::sleep_until(deadline.into()) => {
                    info!(
                        "Job {} exceeded its executionTimeout of {:.1} seconds, \
                         cancelling it...",
                        cfg.name,
                        cfg.execution_timeout.unwrap_or(0.0)
                    );
                    terminate(cfg, child).await;
                    return Some(TIMEOUT_RETCODE);
                }
                changed = cancel.changed() => {
                    if changed.is_ok() && *cancel.borrow() {
                        return terminate(cfg, child).await;
                    }
                }
            }
        } else {
            tokio::select! {
                status = child.wait() => return status.ok().and_then(status_to_code),
                changed = cancel.changed() => {
                    if changed.is_ok() && *cancel.borrow() {
                        return terminate(cfg, child).await;
                    }
                }
            }
        }
    }
}

/// Gracefully terminate a child (SIGTERM), waiting up to `killTimeout` before
/// forcefully killing it (SIGKILL). Returns the child's final exit code.
async fn terminate(cfg: &Job, child: &mut Child) -> Option<i32> {
    send_terminate(child);

    let kill_timeout = Duration::from_secs_f64(cfg.kill_timeout.max(0.0));
    match tokio::time::timeout(kill_timeout, child.wait()).await {
        Ok(Ok(status)) => status_to_code(status),
        Ok(Err(_)) => None,
        Err(_) => {
            warn!(
                "Job {} did not gracefully terminate after {:.1} seconds, \
                 killing it...",
                cfg.name, cfg.kill_timeout
            );
            let _ = child.kill().await;
            child.wait().await.ok().and_then(status_to_code)
        }
    }
}

/// Send the graceful-termination signal (SIGTERM on Unix; on other platforms,
/// the only available stop is a hard kill).
#[cfg(unix)]
fn send_terminate(child: &Child) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    if let Some(pid) = child.id() {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
}

#[cfg(not(unix))]
fn send_terminate(child: &mut Child) {
    let _ = child.start_kill();
}

fn status_to_code(status: std::process::ExitStatus) -> Option<i32> {
    if let Some(code) = status.code() {
        return Some(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        return status.signal().map(|sig| -sig);
    }
    #[allow(unreachable_code)]
    None
}

// ---------------------------------------------------------------------------
// Output capture
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum StreamKind {
    Stdout,
    Stderr,
}

impl StreamKind {
    fn name(self) -> &'static str {
        match self {
            StreamKind::Stdout => "stdout",
            StreamKind::Stderr => "stderr",
        }
    }
}

/// One serialized lock so concurrent readers never interleave mid-line on the
/// shared console.
fn console_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn spawn_reader<R>(cfg: &Job, stream: R, kind: StreamKind) -> JoinHandle<(String, usize)>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let prefix = template::format_stream_prefix(&cfg.stream_prefix, &cfg.name, kind.name());
    let save_limit = cfg.save_limit.max(0) as usize;
    let max_line_length = cfg.max_line_length.max(1) as usize;
    let job_name = cfg.name.clone();
    tokio::spawn(async move {
        read_stream(
            stream,
            &prefix,
            save_limit,
            max_line_length,
            kind,
            &job_name,
        )
        .await
    })
}

async fn join_reader(handle: Option<JoinHandle<(String, usize)>>) -> (Option<String>, usize) {
    match handle {
        Some(handle) => match handle.await {
            Ok((output, discarded)) => (Some(output), discarded),
            Err(_) => (Some(String::new()), 0),
        },
        None => (None, 0),
    }
}

/// Accumulates saved output: the first `save_limit/2` lines verbatim, then a
/// sliding window of the last `save_limit - save_limit/2`, counting how many
/// middle lines were dropped.
struct Saver {
    save_limit: usize,
    limit_top: usize,
    limit_bottom: usize,
    top: Vec<String>,
    bottom: VecDeque<String>,
    discarded: usize,
}

impl Saver {
    fn new(save_limit: usize) -> Saver {
        let limit_top = save_limit / 2;
        Saver {
            save_limit,
            limit_top,
            limit_bottom: save_limit - limit_top,
            top: Vec::new(),
            bottom: VecDeque::new(),
            discarded: 0,
        }
    }

    fn push(&mut self, line: String) {
        if self.save_limit == 0 {
            self.discarded += 1;
            return;
        }
        if self.top.len() < self.limit_top {
            self.top.push(line);
        } else if self.limit_bottom == 0 {
            self.discarded += 1;
        } else {
            if self.bottom.len() == self.limit_bottom {
                self.bottom.pop_front();
                self.discarded += 1;
            }
            self.bottom.push_back(line);
        }
    }

    fn finish(self) -> (String, usize) {
        let mut output = String::new();
        for line in &self.top {
            output.push_str(line);
        }
        if !self.bottom.is_empty() {
            if self.discarded > 0 {
                output.push_str(&format!(
                    "   [.... {} lines discarded ...]\n",
                    self.discarded
                ));
            }
            for line in &self.bottom {
                output.push_str(line);
            }
        }
        (output, self.discarded)
    }
}

async fn read_stream<R>(
    stream: R,
    prefix: &str,
    save_limit: usize,
    max_line_length: usize,
    kind: StreamKind,
    job_name: &str,
) -> (String, usize)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(stream);
    let mut saver = Saver::new(save_limit);

    loop {
        match read_capped_line(&mut reader, max_line_length).await {
            Ok(LineRead::Line(bytes)) => {
                let line = String::from_utf8_lossy(&bytes).into_owned();
                emit(prefix, &line, kind).await;
                saver.push(line);
            }
            Ok(LineRead::Overlong) => {
                warn!("job {job_name}: ignored a very long line");
            }
            Ok(LineRead::Eof) => break,
            Err(_) => break,
        }
    }

    saver.finish()
}

enum LineRead {
    Line(Vec<u8>),
    Overlong,
    Eof,
}

/// Read a single newline-terminated line, capping memory at `max` bytes. A line
/// longer than `max` is consumed and dropped (signalled as `Overlong`).
async fn read_capped_line<R>(reader: &mut BufReader<R>, max: usize) -> std::io::Result<LineRead>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line: Vec<u8> = Vec::new();
    let mut overlong = false;

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if line.is_empty() && !overlong {
                LineRead::Eof
            } else if overlong {
                LineRead::Overlong
            } else {
                LineRead::Line(line)
            });
        }

        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let consume = pos + 1;
            if !overlong {
                line.extend_from_slice(&available[..consume]);
                if line.len() > max {
                    overlong = true;
                }
            }
            reader.consume(consume);
            return Ok(if overlong {
                LineRead::Overlong
            } else {
                LineRead::Line(line)
            });
        }

        let len = available.len();
        if !overlong {
            line.extend_from_slice(available);
            if line.len() > max {
                overlong = true;
                line = Vec::new();
            }
        }
        reader.consume(len);
    }
}

async fn emit(prefix: &str, line: &str, kind: StreamKind) {
    let _guard = console_lock().lock().await;
    let payload = format!("{prefix}{line}");
    match kind {
        StreamKind::Stdout => {
            let mut out = tokio::io::stdout();
            let _ = out.write_all(payload.as_bytes()).await;
            let _ = out.flush().await;
        }
        StreamKind::Stderr => {
            let mut err = tokio::io::stderr();
            let _ = err.write_all(payload.as_bytes()).await;
            let _ = err.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn saver_run(save_limit: usize, lines: &[&str]) -> (String, usize) {
        let mut saver = Saver::new(save_limit);
        for line in lines {
            saver.push((*line).to_string());
        }
        saver.finish()
    }

    #[test]
    fn save_limit_keeps_all_when_under() {
        let (out, disc) = saver_run(10, &["line1\n", "line2\n", "line3\n", "line4\n"]);
        assert_eq!(out, "line1\nline2\nline3\nline4\n");
        assert_eq!(disc, 0);
    }

    #[test]
    fn save_limit_one_keeps_last() {
        let (out, disc) = saver_run(1, &["line1\n", "line2\n", "line3\n", "line4\n"]);
        assert_eq!(out, "   [.... 3 lines discarded ...]\nline4\n");
        assert_eq!(disc, 3);
    }

    #[test]
    fn save_limit_two_keeps_first_and_last() {
        let (out, disc) = saver_run(2, &["line1\n", "line2\n", "line3\n", "line4\n"]);
        assert_eq!(out, "line1\n   [.... 2 lines discarded ...]\nline4\n");
        assert_eq!(disc, 2);
    }

    #[test]
    fn save_limit_zero_discards_all() {
        let (out, disc) = saver_run(0, &["a\n", "b\n"]);
        assert_eq!(out, "");
        assert_eq!(disc, 2);
    }

    #[cfg(unix)]
    mod process {
        use super::super::*;
        use crate::config::parse_config_string;

        fn job(yaml: &str) -> crate::config::Job {
            parse_config_string(yaml, "")
                .unwrap()
                .jobs
                .into_iter()
                .next()
                .unwrap()
        }

        async fn run(job: &crate::config::Job) -> JobOutcome {
            let (_tx, mut rx) = watch::channel(false);
            execute(job, &mut rx, |_pid| {}).await
        }

        #[tokio::test]
        async fn success_captures_stdout() {
            let job = job("jobs:\n  - name: t\n    command: echo hello\n    \
                 schedule: \"* * * * *\"\n    captureStdout: true\n");
            let outcome = run(&job).await;
            assert_eq!(outcome.retcode, Some(0));
            assert_eq!(outcome.stdout.as_deref(), Some("hello\n"));
            assert!(!outcome.failed(&job));
        }

        #[tokio::test]
        async fn nonzero_exit_is_failure() {
            let job = job(
                "jobs:\n  - name: t\n    command: |\n      echo oops 1>&2\n      \
                 exit 3\n    schedule: \"* * * * *\"\n    captureStderr: true\n",
            );
            let outcome = run(&job).await;
            assert_eq!(outcome.retcode, Some(3));
            assert_eq!(outcome.stderr.as_deref(), Some("oops\n"));
            assert!(outcome.failed(&job));
        }

        #[tokio::test]
        async fn execution_timeout_kills_job() {
            let job = job(
                "jobs:\n  - name: t\n    command: |\n      echo start\n      \
                 sleep 5\n    schedule: \"* * * * *\"\n    captureStdout: true\n    \
                 executionTimeout: 0.3\n    killTimeout: 0.3\n",
            );
            let outcome = run(&job).await;
            assert_eq!(outcome.retcode, Some(TIMEOUT_RETCODE));
            assert_eq!(outcome.stdout.as_deref(), Some("start\n"));
        }

        #[tokio::test]
        async fn missing_command_reports_127() {
            let job = job("jobs:\n  - name: t\n    command:\n      - \
                 /this/does/not/exist\n    schedule: \"* * * * *\"\n");
            let outcome = run(&job).await;
            assert_eq!(outcome.retcode, Some(SPAWN_FAILED_RETCODE));
            assert!(outcome.failed(&job));
        }

        #[tokio::test]
        async fn environment_is_passed_to_child() {
            let job = job(
                "jobs:\n  - name: t\n    command: echo $YACRON2_TEST_VAR\n    \
                 schedule: \"* * * * *\"\n    captureStdout: true\n    \
                 environment:\n      - key: YACRON2_TEST_VAR\n        \
                 value: hithere\n",
            );
            let outcome = run(&job).await;
            assert_eq!(outcome.stdout.as_deref(), Some("hithere\n"));
        }
    }
}
