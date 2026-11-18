use std::fs;
use std::io::IsTerminal;
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
pub fn setup(log_dir: &str, max_files: usize) -> Result<Option<WorkerGuard>, anyhow::Error> {
    // Colour only for a terminal: escape codes in `docker logs`, journald or a
    // redirected file are noise. `NO_COLOR` (no-color.org) turns it off too.
    let ansi = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();

    // Console subscriber (human-friendly)
    let console_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(false)
        .with_ansi(ansi)
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

    // File appender (rolling daily). Only the newest `max_files` are kept, so
    // a long-lived host does not fill its disk; 0 keeps everything.
    let mut builder = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("rfw.log");
    if max_files > 0 {
        builder = builder.max_log_files(max_files);
    }
    let file_appender = builder.build(log_dir).map_err(|e| {
        anyhow::anyhow!(
            "cannot open a log file in {}: {e} (use --log-dir \"\" to log to the console only)",
            log_dir.display()
        )
    })?;
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
