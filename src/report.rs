//! Outcome reporters: e-mail (SMTP via `lettre`), shell command, and a Sentry
//! stub. All three run concurrently; an error in one is logged and does not
//! affect the others.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use lettre::message::{Mailbox, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use tracing::{debug, error, warn};

use crate::config::{Job, MailReport, Report, SentryReport, ShellReport};
use crate::job::JobOutcome;
use crate::template;

type ReportResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Run all reporters for a finished job concurrently, logging any failures.
pub async fn run_reports(report: &Report, success: bool, cfg: &Job, outcome: &JobOutcome) {
    let (mail, shell, sentry) = tokio::join!(
        report_mail(&report.mail, success, cfg, outcome),
        report_shell(&report.shell, cfg, outcome),
        report_sentry(&report.sentry, cfg, outcome),
    );
    for (name, result) in [("mail", mail), ("shell", shell), ("sentry", sentry)] {
        if let Err(err) = result {
            error!("Problem reporting job {} via {name}: {err}", cfg.name);
        }
    }
}

/// Build the Jinja2 rendering context for the report templates.
fn template_context(cfg: &Job, outcome: &JobOutcome, success: bool) -> minijinja::Value {
    // The `environment` variable exposes the child's environment (yacron2's own
    // environment plus the job overrides) when overrides were configured.
    let environment: Option<BTreeMap<String, String>> = outcome.env.as_ref().map(|overrides| {
        let mut map: BTreeMap<String, String> = std::env::vars().collect();
        for var in overrides {
            map.insert(var.key.clone(), var.value.clone());
        }
        map
    });

    minijinja::context! {
        name => cfg.name,
        success => success,
        fail_reason => outcome.fail_reason(cfg),
        stdout => outcome.stdout,
        stderr => outcome.stderr,
        exit_code => outcome.retcode,
        command => cfg.command.display(),
        shell => cfg.shell,
        environment => environment,
    }
}

// ---------------------------------------------------------------------------
// E-mail
// ---------------------------------------------------------------------------

async fn report_mail(
    mail: &MailReport,
    success: bool,
    cfg: &Job,
    outcome: &JobOutcome,
) -> ReportResult {
    let (from, to) = match (&mail.from, &mail.to) {
        (Some(f), Some(t)) if !f.is_empty() && !t.is_empty() => (f, t),
        _ => return Ok(()), // e-mail reporting disabled
    };

    let password = match resolve_mail_password(mail)? {
        PasswordOutcome::Use(p) => Some(p),
        PasswordOutcome::None => None,
        PasswordOutcome::SkipSend => return Ok(()),
    };

    let ctx = template_context(cfg, outcome, success);
    let body = template::render(&mail.body, ctx.clone())?;
    if success && body.trim().is_empty() {
        debug!("body is empty, not sending email");
        return Ok(());
    }
    let subject = template::render(&mail.subject, ctx)?;

    let mut builder = Message::builder()
        .from(from.trim().parse::<Mailbox>()?)
        .subject(subject.trim())
        .date_now();
    for addr in to.split(',') {
        let addr = addr.trim();
        if !addr.is_empty() {
            builder = builder.to(addr.parse::<Mailbox>()?);
        }
    }
    let part = if mail.html {
        SinglePart::html(body)
    } else {
        SinglePart::plain(body)
    };
    let email = builder.singlepart(part)?;

    let host = mail.smtp_host.as_deref().unwrap_or("localhost");
    let tls = if mail.tls {
        Tls::Wrapper(tls_parameters(host, mail.validate_certs)?)
    } else if mail.starttls {
        Tls::Required(tls_parameters(host, mail.validate_certs)?)
    } else {
        Tls::None
    };

    let mut transport_builder = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host)
        .port(mail.smtp_port)
        .tls(tls);
    if let (Some(username), Some(password)) = (&mail.username, &password) {
        transport_builder =
            transport_builder.credentials(Credentials::new(username.clone(), password.clone()));
    }
    let transport = transport_builder.build();
    transport.send(email).await?;
    Ok(())
}

fn tls_parameters(
    host: &str,
    validate_certs: bool,
) -> Result<TlsParameters, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = TlsParameters::builder(host.to_string());
    if !validate_certs {
        builder = builder.dangerous_accept_invalid_certs(true);
    }
    Ok(builder.build()?)
}

enum PasswordOutcome {
    Use(String),
    None,
    SkipSend,
}

fn resolve_mail_password(
    mail: &MailReport,
) -> Result<PasswordOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let pw = &mail.password;
    if let Some(value) = pw.value.as_ref().filter(|v| !v.is_empty()) {
        return Ok(PasswordOutcome::Use(value.clone()));
    }
    if let Some(file) = &pw.from_file {
        let contents = std::fs::read_to_string(file)?;
        return Ok(PasswordOutcome::Use(contents.trim().to_string()));
    }
    if let Some(var) = &pw.from_env_var {
        return Ok(match std::env::var(var) {
            Ok(v) if !v.is_empty() => PasswordOutcome::Use(v),
            _ => {
                // The env var name is config-derived and tied to a secret, so
                // we don't echo it to the logs.
                error!("mail: password env var is not set; not sending email");
                PasswordOutcome::SkipSend
            }
        });
    }
    Ok(PasswordOutcome::None)
}

// ---------------------------------------------------------------------------
// Shell command
// ---------------------------------------------------------------------------

async fn report_shell(shell: &ShellReport, cfg: &Job, outcome: &JobOutcome) -> ReportResult {
    let Some(command) = &shell.command else {
        return Ok(());
    };
    let (program, args) = crate::job::resolve_argv(command, &shell.shell);

    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args);
    for (key, value) in shell_env(cfg, outcome) {
        cmd.env(key, value);
    }

    debug!("Executing shell report cmd for job {}", cfg.name);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            error!("Error executing shell reporter of job {}: {err}", cfg.name);
            return Ok(());
        }
    };
    let status = child.wait().await?;
    if !status.success() {
        error!(
            "Error executing shell reporter of job {} with return code {:?}",
            cfg.name,
            status.code()
        );
    }
    Ok(())
}

/// Build the `YACRON2_*` environment exposed to the shell reporter.
fn shell_env(cfg: &Job, outcome: &JobOutcome) -> Vec<(String, String)> {
    const MAX_ARG: usize = 16 * 1024;

    let fail_reason = outcome.fail_reason(cfg);
    let failed = fail_reason.is_some();
    let stderr = outcome.stderr.clone().unwrap_or_default();
    let stdout = outcome.stdout.clone().unwrap_or_default();

    let too_long = stderr.chars().count() > MAX_ARG
        || stdout.chars().count() > MAX_ARG
        || stderr.chars().count() + stdout.chars().count() > MAX_ARG;
    let truncate = |s: &str| -> String {
        if too_long {
            s.chars().take(MAX_ARG).collect()
        } else {
            s.to_string()
        }
    };
    let stderr_safe = truncate(&stderr);
    let stdout_safe = truncate(&stdout);
    let stderr_truncated = stderr_safe.chars().count() != stderr.chars().count();
    let stdout_truncated = stdout_safe.chars().count() != stdout.chars().count();

    let retcode = match outcome.retcode {
        Some(c) => c.to_string(),
        None => "None".to_string(),
    };

    vec![
        (
            "YACRON2_FAIL_REASON".into(),
            fail_reason.unwrap_or_default(),
        ),
        ("YACRON2_JOB_NAME".into(), cfg.name.clone()),
        ("YACRON2_JOB_COMMAND".into(), cfg.command.display()),
        (
            "YACRON2_JOB_SCHEDULE".into(),
            cfg.schedule.display().to_string(),
        ),
        (
            "YACRON2_FAILED".into(),
            if failed { "1" } else { "0" }.into(),
        ),
        ("YACRON2_RETCODE".into(), retcode),
        ("YACRON2_STDERR".into(), stderr_safe),
        ("YACRON2_STDOUT".into(), stdout_safe),
        (
            "YACRON2_STDERR_TRUNCATED".into(),
            if stderr_truncated { "1" } else { "0" }.into(),
        ),
        (
            "YACRON2_STDOUT_TRUNCATED".into(),
            if stdout_truncated { "1" } else { "0" }.into(),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Sentry (stub)
// ---------------------------------------------------------------------------

static SENTRY_WARNED: AtomicBool = AtomicBool::new(false);

async fn report_sentry(sentry: &SentryReport, _cfg: &Job, _outcome: &JobOutcome) -> ReportResult {
    // A DSN configured means the user expects Sentry reporting. This build does
    // not implement it (parity-minus-Sentry); warn once so the gap is visible.
    let dsn_configured = sentry.dsn.resolve().ok().flatten().is_some();
    if dsn_configured && !SENTRY_WARNED.swap(true, Ordering::Relaxed) {
        warn!(
            "Sentry reporting is configured but not implemented in this build; \
             Sentry reports will be skipped"
        );
    }
    Ok(())
}
