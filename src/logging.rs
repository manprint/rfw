use std::fs;
use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

/// Initialize tracing with console output + rolling file logger.
/// Returns a _guard that must be kept alive for the app lifetime.
pub fn setup() -> Result<WorkerGuard, anyhow::Error> {
    // Ensure log directory exists
    let log_dir = Path::new("logs");
    fs::create_dir_all(log_dir)?;

    // File appender (rolling daily, keeps 7 days)
    let file_appender = tracing_appender::rolling::daily(log_dir, "rfw.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    // Console subscriber (human-friendly, color)
    let console_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(false)
        .with_ansi(true)
        .with_span_events(FmtSpan::NONE)
        .pretty()
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")));

    // File subscriber (JSON, no color, all levels)
    let file_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_ansi(false)
        .json()
        .with_writer(non_blocking_file)
        .with_filter(EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}
