//! yacron2 — a modern, rootless-container-friendly cron replacement.
//!
//! Entry point: parse the CLI, initialise logging, load and validate the
//! configuration, install signal handlers, and run the scheduler.

mod config;
mod cron;
mod error;
mod job;
mod report;
mod schedule;
mod statsd;
mod template;
mod version;
mod web;

use std::path::Path;

use clap::Parser;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::cron::Cron;
use crate::version::VERSION;

const CONFIG_DEFAULT: &str = "/etc/yacron2.d";

#[derive(Parser)]
#[command(name = "yacron2", disable_version_flag = true)]
struct Args {
    /// Configuration file, or directory containing configuration files.
    #[arg(
        short = 'c',
        long = "config",
        default_value = CONFIG_DEFAULT,
        value_name = "FILE-OR-DIR"
    )]
    config: String,

    /// Log level: DEBUG, INFO, WARNING, ERROR (overridable via RUST_LOG).
    #[arg(short = 'l', long = "log-level", default_value = "INFO")]
    log_level: String,

    /// Validate the configuration and exit.
    #[arg(short = 'v', long = "validate-config")]
    validate_config: bool,

    /// Print the version and exit.
    #[arg(long = "version")]
    version: bool,
}

fn main() {
    let args = Args::parse();

    if args.version {
        println!("{VERSION}");
        std::process::exit(0);
    }

    init_logging(&args.log_level);
    ensure_hostname();

    if args.config == CONFIG_DEFAULT && !Path::new(&args.config).exists() {
        eprintln!(
            "yacron2 error: configuration file not found, please provide one \
             with the --config option"
        );
        std::process::exit(1);
    }

    let cron = match Cron::new(args.config) {
        Ok(cron) => cron,
        Err(err) => {
            error!("Configuration error: {err}");
            std::process::exit(1);
        }
    };

    if args.validate_config {
        info!("Configuration is valid.");
        std::process::exit(0);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async move {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            wait_for_shutdown().await;
            let _ = shutdown_tx.send(true);
        });
        cron.run(shutdown_rx).await;
    });
}

/// Configure logging. `RUST_LOG` takes precedence; otherwise the `--log-level`
/// argument selects the level (Python level names are accepted).
fn init_logging(log_level: &str) {
    let directive = match log_level.to_uppercase().as_str() {
        "CRITICAL" | "ERROR" => "error",
        "WARNING" | "WARN" => "warn",
        "DEBUG" => "debug",
        "TRACE" | "NOTSET" => "trace",
        _ => "info",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(directive));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .init();
}

/// Mirror the original behaviour of ensuring `$HOSTNAME` is set, so jobs and
/// report templates can rely on it.
fn ensure_hostname() {
    if std::env::var_os("HOSTNAME").is_none() {
        if let Ok(name) = gethostname::gethostname().into_string() {
            std::env::set_var("HOSTNAME", name);
        }
    }
}

/// Resolve when a shutdown signal (SIGINT/SIGTERM, or Ctrl-C on Windows) is
/// received.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    info!("Signalling shutdown");
}
