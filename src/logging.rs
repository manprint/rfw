use std::fs;
use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialize tracing: console output, plus a rolling JSON file log when
/// `log_dir` is non-empty.
///
/// Both layers honour `RUST_LOG`. The file layer used to be pinned at `info`,
/// which meant `RUST_LOG=warn` still wrote every per-connection record to disk —
/// the one knob an operator reaches for could not actually turn the volume down.
///
/// Returns a guard that must be kept alive for the app lifetime (it flushes the
/// non-blocking writer on drop); `None` when file logging is disabled.
pub fn setup(log_dir: &str) -> Result<Option<WorkerGuard>, anyhow::Error> {
    // Console subscriber (human-friendly, color)
    let console_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(false)
        .with_ansi(true)
        .with_span_events(FmtSpan::NONE)
        .pretty()
        .with_filter(env_filter());

    if log_dir.is_empty() {
        tracing_subscriber::registry().with(console_layer).init();
        return Ok(None);
    }

    let log_dir = Path::new(log_dir);
    fs::create_dir_all(log_dir).map_err(|e| {
        anyhow::anyhow!(
            "cannot create log directory {}: {e} (use --log-dir \"\" to log to the console only)",
            log_dir.display()
        )
    })?;

    // File appender (rolling daily).
    //
    // NOTE: rotated files are never deleted. On a long-lived staging/production
    // host, pair this with logrotate or an ephemeral volume, or run with
    // `--log-dir ""` and let the container runtime collect stdout.
    let file_appender = tracing_appender::rolling::daily(log_dir, "rfw.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    // File subscriber (JSON, no color)
    let file_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_ansi(false)
        .json()
        .with_writer(non_blocking_file)
        .with_filter(env_filter());

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok(Some(guard))
}
