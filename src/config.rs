//! Configuration loading, merging, and validation.
//!
//! The shape mirrors the original Python yacron2: a built-in default config is
//! deep-merged with an optional `defaults:` block and then with each job. The
//! merge is performed on the raw YAML tree (reproducing the original
//! `mergedicts` rules exactly), after which the fully-merged value is
//! deserialized into strict, typed structs — `#[serde(deny_unknown_fields)]`
//! rejects typo'd keys the same way strictyaml's schema did.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{Local, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_yaml::{Mapping, Value};

use crate::error::{cfg_err, ConfigError, ConfigResult};
pub use crate::schedule::Schedule;

mod envfile;
mod scalar;

pub use scalar::ScalarString;

// ---------------------------------------------------------------------------
// Resolved domain types (shared between deserialization and runtime)
// ---------------------------------------------------------------------------

/// How a job reacts to a previous instance still running at trigger time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub enum ConcurrencyPolicy {
    /// Run another instance concurrently (default).
    Allow,
    /// Skip this trigger while an instance is running.
    Forbid,
    /// Cancel the running instance and start a fresh one.
    Replace,
}

impl std::fmt::Display for ConcurrencyPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ConcurrencyPolicy::Allow => "Allow",
            ConcurrencyPolicy::Forbid => "Forbid",
            ConcurrencyPolicy::Replace => "Replace",
        })
    }
}

/// A command, either run through a shell (string) or executed directly (list).
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Command {
    /// A shell command line.
    Line(String),
    /// An explicit argv vector, executed without a shell.
    Argv(Vec<String>),
}

impl Command {
    /// A flat string rendering (argv joined by spaces), for templates/logging.
    pub fn display(&self) -> String {
        match self {
            Command::Line(s) => s.clone(),
            Command::Argv(v) => v.join(" "),
        }
    }
}

/// The `failsWhen` predicate set.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailsWhen {
    #[serde(rename = "producesStdout")]
    pub produces_stdout: bool,
    #[serde(rename = "producesStderr")]
    pub produces_stderr: bool,
    #[serde(rename = "nonzeroReturn")]
    pub nonzero_return: bool,
    pub always: bool,
}

/// Exponential-backoff retry parameters.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    #[serde(rename = "maximumRetries")]
    pub maximum_retries: i64,
    #[serde(rename = "initialDelay")]
    pub initial_delay: f64,
    #[serde(rename = "maximumDelay")]
    pub maximum_delay: f64,
    #[serde(rename = "backoffMultiplier")]
    pub backoff_multiplier: f64,
}

/// A secret resolvable from an inline value, a file, or an environment var.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default, rename = "fromFile")]
    pub from_file: Option<String>,
    #[serde(default, rename = "fromEnvVar")]
    pub from_env_var: Option<String>,
}

impl SecretRef {
    /// Resolve the secret. Returns `Ok(None)` when nothing is configured, and
    /// `Err` only for an unreadable `fromFile`.
    pub fn resolve(&self) -> std::io::Result<Option<String>> {
        if let Some(v) = self.value.as_ref().filter(|v| !v.is_empty()) {
            return Ok(Some(v.clone()));
        }
        if let Some(path) = &self.from_file {
            let contents = std::fs::read_to_string(path)?;
            return Ok(Some(contents.trim().to_string()));
        }
        if let Some(var) = &self.from_env_var {
            return Ok(std::env::var(var).ok().filter(|v| !v.is_empty()));
        }
        Ok(None)
    }
}

/// A template-able extra attribute value for Sentry.
#[allow(dead_code)] // parsed for config fidelity; Sentry reporting is stubbed
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ExtraValue {
    Bool(bool),
    Int(i64),
    Str(String),
}

/// Sentry reporter configuration (parsed for fidelity; reporting is a stub).
#[allow(dead_code)] // most fields are only consumed once Sentry is implemented
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SentryReport {
    pub dsn: SecretRef,
    pub fingerprint: Vec<String>,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub extra: Option<BTreeMap<String, ExtraValue>>,
    pub body: String,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default, rename = "maxStringLength")]
    pub max_string_length: Option<i64>,
}

/// Email reporter configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailReport {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default, rename = "smtpHost")]
    pub smtp_host: Option<String>,
    #[serde(rename = "smtpPort")]
    pub smtp_port: u16,
    pub subject: String,
    pub body: String,
    #[serde(default)]
    pub username: Option<String>,
    pub password: SecretRef,
    pub tls: bool,
    pub starttls: bool,
    pub validate_certs: bool,
    pub html: bool,
}

/// Shell-command reporter configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellReport {
    pub shell: String,
    #[serde(default)]
    pub command: Option<Command>,
}

/// The trio of reporters attached to a job outcome.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub sentry: SentryReport,
    pub mail: MailReport,
    pub shell: ShellReport,
}

/// Statsd metric sink configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statsd {
    pub prefix: String,
    pub host: String,
    pub port: u16,
}

/// A single environment variable binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvVar {
    pub key: String,
    pub value: String,
}

/// A resolved scheduling timezone.
#[derive(Clone, Debug)]
pub enum Timezone {
    /// Coordinated Universal Time.
    Utc,
    /// A named IANA timezone (database bundled via `chrono-tz`).
    Named(chrono_tz::Tz),
    /// The host's local timezone.
    Local,
}

impl Timezone {
    /// The current wall-clock time in this timezone, as a naive datetime.
    pub fn now(&self) -> NaiveDateTime {
        match self {
            Timezone::Utc => Utc::now().naive_utc(),
            Timezone::Named(tz) => Utc::now().with_timezone(tz).naive_local(),
            Timezone::Local => Local::now().naive_local(),
        }
    }
}

// ---------------------------------------------------------------------------
// Raw deserialization helpers (serde structs feeding the merged YAML value)
// ---------------------------------------------------------------------------

/// A user or group, identified by name or numeric id.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum IdSpec {
    Id(u32),
    Name(String),
}

/// Schedule as written in YAML: a crontab string or a structured object.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ScheduleSpec {
    Line(String),
    Fields(ScheduleFields),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleFields {
    minute: Option<ScalarString>,
    hour: Option<ScalarString>,
    #[serde(rename = "dayOfMonth")]
    day_of_month: Option<ScalarString>,
    month: Option<ScalarString>,
    year: Option<ScalarString>,
    #[serde(rename = "dayOfWeek")]
    day_of_week: Option<ScalarString>,
}

impl ScheduleSpec {
    fn resolve(self) -> ConfigResult<Schedule> {
        match self {
            ScheduleSpec::Line(s) => Schedule::parse_string(&s),
            ScheduleSpec::Fields(f) => {
                let field =
                    |o: Option<ScalarString>| o.map(|s| s.0).unwrap_or_else(|| "*".to_string());
                Schedule::from_fields(
                    &field(f.minute),
                    &field(f.hour),
                    &field(f.day_of_month),
                    &field(f.month),
                    &field(f.day_of_week),
                    &field(f.year),
                )
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvVarSpec {
    key: String,
    value: ScalarString,
}

/// The fully-merged job, before schedule/timezone/user/env post-processing.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJob {
    name: String,
    command: Command,
    schedule: ScheduleSpec,
    shell: String,
    #[serde(rename = "concurrencyPolicy")]
    concurrency_policy: ConcurrencyPolicy,
    #[serde(rename = "captureStderr")]
    capture_stderr: bool,
    #[serde(rename = "captureStdout")]
    capture_stdout: bool,
    #[serde(rename = "saveLimit")]
    save_limit: i64,
    #[serde(rename = "maxLineLength")]
    max_line_length: i64,
    utc: bool,
    #[serde(default)]
    timezone: Option<String>,
    enabled: bool,
    #[serde(rename = "failsWhen")]
    fails_when: FailsWhen,
    #[serde(rename = "onFailure")]
    on_failure: OnFailureRaw,
    #[serde(rename = "onPermanentFailure")]
    on_permanent_failure: OnReportRaw,
    #[serde(rename = "onSuccess")]
    on_success: OnReportRaw,
    environment: Vec<EnvVarSpec>,
    #[serde(default)]
    env_file: Option<String>,
    #[serde(default, rename = "executionTimeout")]
    execution_timeout: Option<f64>,
    #[serde(rename = "killTimeout")]
    kill_timeout: f64,
    #[serde(default)]
    statsd: Option<Statsd>,
    #[serde(default)]
    user: Option<IdSpec>,
    #[serde(default)]
    group: Option<IdSpec>,
    #[serde(rename = "streamPrefix")]
    stream_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnFailureRaw {
    retry: Retry,
    report: Report,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnReportRaw {
    report: Report,
}

// ---------------------------------------------------------------------------
// Final resolved job
// ---------------------------------------------------------------------------

/// A fully-resolved, validated job ready for scheduling and execution.
#[derive(Debug)]
pub struct Job {
    pub name: String,
    pub command: Command,
    pub schedule: Schedule,
    pub shell: String,
    pub concurrency_policy: ConcurrencyPolicy,
    pub capture_stderr: bool,
    pub capture_stdout: bool,
    pub save_limit: i64,
    pub max_line_length: i64,
    /// Retained for inspection; the resolved `timezone` already encodes UTC.
    #[allow(dead_code)]
    pub utc: bool,
    pub timezone: Timezone,
    pub enabled: bool,
    pub fails_when: FailsWhen,
    pub retry: Retry,
    pub on_failure_report: Report,
    pub on_permanent_failure_report: Report,
    pub on_success_report: Report,
    pub environment: Vec<EnvVar>,
    pub execution_timeout: Option<f64>,
    pub kill_timeout: f64,
    pub statsd: Option<Statsd>,
    pub stream_prefix: String,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    /// Resolved login name, for `initgroups` when dropping privileges.
    pub username: Option<String>,
}

impl Job {
    fn from_raw(raw: RawJob) -> ConfigResult<Job> {
        let name = raw.name;
        let schedule = raw.schedule.resolve()?;
        let timezone = resolve_timezone(raw.timezone.as_deref(), raw.utc)?;

        // environment: start from the merged config vars, then fold in env_file
        // (config wins, file-only vars appended preserving file order).
        let mut environment: Vec<EnvVar> = raw
            .environment
            .into_iter()
            .map(|e| EnvVar {
                key: e.key,
                value: e.value.0,
            })
            .collect();
        if let Some(path) = &raw.env_file {
            environment = merge_env_file(path, environment)?;
        }

        let (uid, gid, username) = resolve_user_group(&name, raw.user, raw.group)?;

        let job = Job {
            name,
            command: raw.command,
            schedule,
            shell: raw.shell,
            concurrency_policy: raw.concurrency_policy,
            capture_stderr: raw.capture_stderr,
            capture_stdout: raw.capture_stdout,
            save_limit: raw.save_limit,
            max_line_length: raw.max_line_length,
            utc: raw.utc,
            timezone,
            enabled: raw.enabled,
            fails_when: raw.fails_when,
            retry: raw.on_failure.retry,
            on_failure_report: raw.on_failure.report,
            on_permanent_failure_report: raw.on_permanent_failure.report,
            on_success_report: raw.on_success.report,
            environment,
            execution_timeout: raw.execution_timeout,
            kill_timeout: raw.kill_timeout,
            statsd: raw.statsd,
            stream_prefix: raw.stream_prefix,
            uid,
            gid,
            username,
        };
        job.validate()?;
        Ok(job)
    }

    /// Fail fast on out-of-range numeric settings (strictyaml only checked the
    /// type, not the value).
    fn validate(&self) -> ConfigResult<()> {
        let bad = |msg: &str| Err(cfg_err!("Job {}: {}", self.name, msg));
        if self.save_limit < 0 {
            return bad("saveLimit must be >= 0");
        }
        if self.max_line_length <= 0 {
            return bad("maxLineLength must be > 0");
        }
        if self.kill_timeout < 0.0 {
            return bad("killTimeout must be >= 0");
        }
        if let Some(t) = self.execution_timeout {
            if t <= 0.0 {
                return bad("executionTimeout must be > 0 when set");
            }
        }
        let r = &self.retry;
        if r.maximum_retries < -1 {
            return bad("onFailure.retry.maximumRetries must be >= -1");
        }
        if r.initial_delay < 0.0 {
            return bad("onFailure.retry.initialDelay must be >= 0");
        }
        if r.maximum_delay <= 0.0 {
            return bad("onFailure.retry.maximumDelay must be > 0");
        }
        if r.backoff_multiplier <= 0.0 {
            return bad("onFailure.retry.backoffMultiplier must be > 0");
        }
        Ok(())
    }
}

fn resolve_timezone(timezone: Option<&str>, utc: bool) -> ConfigResult<Timezone> {
    if let Some(name) = timezone {
        let tz: chrono_tz::Tz = name
            .parse()
            .map_err(|_| cfg_err!("unknown timezone: {name}"))?;
        Ok(Timezone::Named(tz))
    } else if utc {
        Ok(Timezone::Utc)
    } else {
        Ok(Timezone::Local)
    }
}

fn merge_env_file(path: &str, config_env: Vec<EnvVar>) -> ConfigResult<Vec<EnvVar>> {
    let file_env = envfile::parse(path)?;
    let mut merged: Vec<EnvVar> = file_env
        .into_iter()
        .map(|(key, value)| EnvVar { key, value })
        .collect();
    for var in config_env {
        if let Some(existing) = merged.iter_mut().find(|e| e.key == var.key) {
            existing.value = var.value;
        } else {
            merged.push(var);
        }
    }
    Ok(merged)
}

#[cfg(unix)]
fn resolve_user_group(
    job_name: &str,
    user: Option<IdSpec>,
    group: Option<IdSpec>,
) -> ConfigResult<(Option<u32>, Option<u32>, Option<String>)> {
    use nix::unistd::{Gid, Group, Uid, User};

    let mut uid = None;
    let mut gid = None;
    let mut username = None;

    if let Some(user) = user {
        match user {
            IdSpec::Id(id) => {
                uid = Some(id);
                // Derive the primary gid and login name from passwd so a
                // numeric user without an explicit group does not silently
                // keep yacron2's gid.
                if let Ok(Some(pw)) = User::from_uid(Uid::from_raw(id)) {
                    username = Some(pw.name);
                    if gid.is_none() {
                        gid = Some(pw.gid.as_raw());
                    }
                }
            }
            IdSpec::Name(name) => match User::from_name(&name) {
                Ok(Some(pw)) => {
                    uid = Some(pw.uid.as_raw());
                    gid = Some(pw.gid.as_raw());
                    username = Some(pw.name);
                }
                _ => return Err(cfg_err!("User not found: {name:?}")),
            },
        }
    }

    if let Some(group) = group {
        match group {
            IdSpec::Id(id) => gid = Some(id),
            IdSpec::Name(name) => match Group::from_name(&name) {
                Ok(Some(gr)) => gid = Some(gr.gid.as_raw()),
                _ => return Err(cfg_err!("Group not found: {name:?}")),
            },
        }
        let _ = Gid::from_raw(0); // keep the import meaningful on all paths
    }

    if (uid.is_some() || gid.is_some()) && !Uid::effective().is_root() {
        return Err(cfg_err!(
            "Job {job_name} wants to change user or group, but yacron2 is \
             not running as superuser"
        ));
    }

    Ok((uid, gid, username))
}

#[cfg(not(unix))]
fn resolve_user_group(
    job_name: &str,
    user: Option<IdSpec>,
    group: Option<IdSpec>,
) -> ConfigResult<(Option<u32>, Option<u32>, Option<String>)> {
    if user.is_some() || group.is_some() {
        return Err(cfg_err!(
            "Job {job_name}: changing user/group is not supported on this \
             platform"
        ));
    }
    Ok((None, None, None))
}

// ---------------------------------------------------------------------------
// Web / logging / top-level document
// ---------------------------------------------------------------------------

/// Web REST API configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebConfig {
    pub listen: Vec<String>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default, rename = "authToken")]
    pub auth_token: Option<SecretRef>,
    #[serde(default, rename = "socketMode")]
    pub socket_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DocSpec {
    #[serde(default)]
    defaults: Option<Value>,
    #[serde(default)]
    jobs: Option<Vec<Value>>,
    #[serde(default)]
    web: Option<WebConfig>,
    #[serde(default)]
    include: Option<Vec<String>>,
    #[serde(default)]
    logging: Option<Value>,
}

/// A loaded, fully-resolved configuration.
#[derive(Debug)]
pub struct Config {
    pub jobs: Vec<Job>,
    pub web: Option<WebConfig>,
    /// Retained for compatibility/validation; not applied (see README).
    pub logging: Option<Value>,
}

/// The result of parsing a single file (internal — `defaults_value` is the
/// file's own merged defaults, needed when a parent file includes it).
struct ParsedFile {
    jobs: Vec<Job>,
    web: Option<WebConfig>,
    logging: Option<Value>,
    defaults_value: Value,
}

// ---------------------------------------------------------------------------
// Built-in defaults
// ---------------------------------------------------------------------------

const DEFAULT_BODY_TEMPLATE: &str = "
{% if fail_reason -%}
(job failed because {{fail_reason}})
{% endif %}
{% if stdout and stderr -%}
STDOUT:
---
{{stdout}}
---
STDERR:
{{stderr}}
{% elif stdout -%}
{{stdout}}
{% elif stderr -%}
{{stderr}}
{% else -%}
(no output was captured)
{% endif %}
";

const DEFAULT_SUBJECT_TEMPLATE: &str =
    "Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}";

/// Build the built-in default configuration as a YAML value, mirroring the
/// original `DEFAULT_CONFIG` / `_REPORT_DEFAULTS`.
fn builtin_defaults() -> Value {
    let sentry_body = format!("{DEFAULT_SUBJECT_TEMPLATE}\n{DEFAULT_BODY_TEMPLATE}");

    // Constructed from a YAML literal for readability; the template strings are
    // injected so they stay in one place.
    let yaml = format!(
        r#"
shell: /bin/sh
concurrencyPolicy: Allow
captureStderr: true
captureStdout: false
saveLimit: 4096
maxLineLength: 16777216
utc: true
failsWhen:
  producesStdout: false
  producesStderr: true
  nonzeroReturn: true
  always: false
onFailure:
  retry:
    maximumRetries: 0
    initialDelay: 1.0
    maximumDelay: 300.0
    backoffMultiplier: 2.0
  report: &report
    sentry:
      dsn:
        value: null
        fromFile: null
        fromEnvVar: null
      fingerprint:
        - yacron2
        - "{{{{ environment.HOSTNAME }}}}"
        - "{{{{ name }}}}"
      environment: null
      maxStringLength: 8192
      body: {sentry_body}
    mail:
      from: null
      to: null
      smtpHost: null
      smtpPort: 25
      tls: false
      starttls: false
      validate_certs: true
      html: false
      subject: {subject}
      body: {body}
      username: null
      password:
        value: null
        fromFile: null
        fromEnvVar: null
    shell:
      shell: /bin/sh
      command: null
onPermanentFailure:
  report: *report
onSuccess:
  report: *report
environment: []
killTimeout: 30.0
streamPrefix: "[{{job_name}} {{stream_name}}] "
enabled: true
"#,
        sentry_body = yaml_quote(&sentry_body),
        subject = yaml_quote(DEFAULT_SUBJECT_TEMPLATE),
        body = yaml_quote(DEFAULT_BODY_TEMPLATE),
    );

    serde_yaml::from_str(&yaml).expect("built-in defaults are valid YAML")
}

/// Quote an arbitrary string as a single-line YAML double-quoted scalar.
fn yaml_quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

// ---------------------------------------------------------------------------
// YAML value merge (faithful to the original `mergedicts`)
// ---------------------------------------------------------------------------

/// Merge `over` onto `base` under the given field name. Mirrors `mergedicts`:
/// recursive for maps; a map overridden by null keeps the map; sequences are
/// concatenated except `environment` (merged by key) and `fingerprint`
/// (replaced); everything else takes the override.
fn merge_field(key: &str, base: Value, over: Value) -> Value {
    match (base, over) {
        (Value::Mapping(b), Value::Mapping(o)) => merge_maps(b, o),
        (Value::Mapping(b), Value::Null) => Value::Mapping(b),
        (Value::Sequence(b), Value::Sequence(o)) => match key {
            "environment" => merge_env(b, o),
            "fingerprint" => Value::Sequence(o),
            _ => {
                let mut v = b;
                v.extend(o);
                Value::Sequence(v)
            }
        },
        (_, over) => over,
    }
}

fn merge_maps(mut base: Mapping, over: Mapping) -> Value {
    for (k, ov) in over {
        let key = k.as_str().unwrap_or("").to_string();
        let merged = match base.remove(&k) {
            Some(bv) => merge_field(&key, bv, ov),
            None => ov,
        };
        base.insert(k, merged);
    }
    Value::Mapping(base)
}

/// Merge two `environment` sequences by key: entries from `over` override those
/// in `base` with the same key; new keys are appended (base order preserved).
fn merge_env(base: Vec<Value>, over: Vec<Value>) -> Value {
    let key_of = |v: &Value| v.get("key").and_then(|k| k.as_str()).map(|s| s.to_string());
    let mut result = base;
    for entry in over {
        match key_of(&entry) {
            Some(k) => {
                if let Some(slot) = result.iter_mut().find(|e| key_of(e).as_deref() == Some(&k)) {
                    *slot = entry;
                } else {
                    result.push(entry);
                }
            }
            None => result.push(entry),
        }
    }
    Value::Sequence(result)
}

// ---------------------------------------------------------------------------
// Parsing entry points
// ---------------------------------------------------------------------------

/// Parse configuration from a file or directory path.
pub fn parse_config(path: &str) -> ConfigResult<Config> {
    let parsed = if Path::new(path).is_dir() {
        parse_config_dir(path)?
    } else {
        let mut seen = Vec::new();
        parse_config_file(path, &mut seen).map_err(|e| ConfigError::new(e.to_string()))?
    };
    Ok(Config {
        jobs: parsed.jobs,
        web: parsed.web,
        logging: parsed.logging,
    })
}

/// Parse a configuration string (used for tests and `Cron::from_yaml`).
#[allow(dead_code)]
pub fn parse_config_string(data: &str, path: &str) -> ConfigResult<Config> {
    let mut seen = Vec::new();
    let parsed = parse_config_string_inner(data, path, &mut seen)?;
    Ok(Config {
        jobs: parsed.jobs,
        web: parsed.web,
        logging: parsed.logging,
    })
}

fn parse_config_file(path: &str, seen: &mut Vec<PathBuf>) -> ConfigResult<ParsedFile> {
    let abspath = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
    if seen.contains(&abspath) {
        return Err(cfg_err!("include cycle detected at {path}"));
    }
    seen.push(abspath);
    let data = std::fs::read_to_string(path).map_err(|e| cfg_err!("{e}"))?;
    parse_config_string_inner(&data, path, seen)
}

fn parse_config_string_inner(
    data: &str,
    path: &str,
    seen: &mut Vec<PathBuf>,
) -> ConfigResult<ParsedFile> {
    let root: Value = serde_yaml::from_str(data).map_err(|e| cfg_err!("{e}"))?;
    let doc: DocSpec = if root.is_null() {
        DocSpec {
            defaults: None,
            jobs: None,
            web: None,
            include: None,
            logging: None,
        }
    } else {
        serde_yaml::from_value(root).map_err(|e| cfg_err!("{e}"))?
    };

    let mut web = doc.web;
    let mut logging = doc.logging;
    let mut jobs: Vec<Job> = Vec::new();

    // Process includes first (their jobs come before this file's jobs), while
    // accumulating their merged defaults to apply to this file's inline jobs.
    let mut inc_defaults = Value::Mapping(Mapping::new());
    if let Some(includes) = &doc.include {
        let base_dir = Path::new(path).parent().unwrap_or(Path::new(""));
        for include in includes {
            let inc_path = base_dir.join(include);
            let inc_path_str = inc_path.to_string_lossy().to_string();
            let inc = parse_config_file(&inc_path_str, seen)?;
            inc_defaults = merge_field("", inc_defaults, inc.defaults_value);
            jobs.extend(inc.jobs);
            if let Some(inc_web) = inc.web {
                if web.is_some() {
                    return Err(cfg_err!("multiple web configs"));
                }
                web = Some(inc_web);
            }
            if let Some(inc_logging) = inc.logging {
                if logging.is_some() {
                    return Err(cfg_err!("multiple logging configs"));
                }
                logging = Some(inc_logging);
            }
        }
    }

    let doc_defaults = doc.defaults.unwrap_or(Value::Null);
    // This file's effective defaults (no built-in baked in — the parent adds
    // it), to hand back to an including parent.
    let file_defaults = merge_field("", inc_defaults, doc_defaults);
    // The base every inline job inherits: built-in ⊕ this file's defaults.
    let job_base = merge_field("", builtin_defaults(), file_defaults.clone());

    if let Some(job_values) = doc.jobs {
        for job_value in job_values {
            let merged = merge_field("", job_base.clone(), job_value);
            let raw: RawJob = serde_yaml::from_value(merged).map_err(|e| cfg_err!("{e}"))?;
            jobs.push(Job::from_raw(raw)?);
        }
    }

    Ok(ParsedFile {
        jobs,
        web,
        logging,
        defaults_value: file_defaults,
    })
}

fn parse_config_dir(dir: &str) -> ConfigResult<ParsedFile> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| cfg_err!("{e}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    // Sort by file name for deterministic job order and error messages.
    entries.sort_by_key(|p| p.file_name().map(|n| n.to_os_string()));

    let mut jobs: Vec<Job> = Vec::new();
    let mut web: Option<WebConfig> = None;
    let mut web_source: Option<String> = None;
    let mut logging: Option<Value> = None;
    let mut logging_source: Option<String> = None;
    let mut errors: Vec<String> = Vec::new();

    for path in entries {
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        let stem = name.split('.').next().unwrap_or("");
        if stem
            .chars()
            .next()
            .map(|c| c == '_' || c == '.')
            .unwrap_or(true)
        {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "yml" && ext != "yaml" {
            continue;
        }

        let path_str = path.to_string_lossy().to_string();
        let mut seen = Vec::new();
        let parsed = match parse_config_file(&path_str, &mut seen) {
            Ok(p) => p,
            Err(err) => {
                errors.push(err.to_string());
                continue;
            }
        };
        jobs.extend(parsed.jobs);
        if let Some(w) = parsed.web {
            if web.is_some() {
                return Err(cfg_err!(
                    "Multiple 'web' configurations found: first in {}, now \
                     in {path_str}",
                    web_source.as_deref().unwrap_or("?")
                ));
            }
            web = Some(w);
            web_source = Some(path_str.clone());
        }
        if let Some(l) = parsed.logging {
            if logging.is_some() {
                return Err(cfg_err!(
                    "Multiple 'logging' configurations found: first in {}, \
                     now in {path_str}",
                    logging_source.as_deref().unwrap_or("?")
                ));
            }
            logging = Some(l);
            logging_source = Some(path_str.clone());
        }
    }

    if !errors.is_empty() {
        return Err(cfg_err!("{}", errors.join("\n---")));
    }

    Ok(ParsedFile {
        jobs,
        web,
        logging,
        defaults_value: Value::Mapping(Mapping::new()),
    })
}

#[cfg(test)]
mod tests;
