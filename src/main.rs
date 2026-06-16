mod config;
mod forwarder;
mod logging;
mod manager;
mod metrics;
mod stats;

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::config::CliArgs;
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup logging (keep guard alive for app lifetime)
    let _guard = logging::setup()?;

    // Parse CLI
    let cli = CliArgs::parse();

    // Load forwarder configurations (YAML + CLI + env, merged with precedence)
    let forwarders = config::load_forwarders(&cli)?;

    // Load runtime settings (YAML settings: block + CLI overrides)
    let settings = config::load_settings(&cli)?;

    if forwarders.is_empty() {
        anyhow::bail!(
            "No forwarders configured.\n\
             Usage: rfw [OPTIONS] [FORWARDERS...]\n\
             Examples:\n\
               rfw localhost:8080:172.16.0.5:80\n\
               rfw -f forwarders.yml\n\
               rfw localhost:8080:example.com:80 localhost:8081:other.com:443\n\n\
             Environment: RFW_FORWARDER_1, RFW_FORWARDER_2, ... override everything.\n\n\
             Forwarder format: local_host:local_port:remote_host:remote_port"
        );
    }

    info!("Starting rfw with {} forwarder(s):", forwarders.len());
    for fwd in &forwarders {
        info!(
            "  {} -> {}:{}",
            fwd.local_addr(),
            fwd.remote_host,
            fwd.remote_port
        );
    }

    // Create manager and start all forwarders
    let manager = Arc::new(manager::ForwarderManager::new(settings.buffer_bytes));
    manager.start_all(&forwarders).await;

    // Start config file watcher (if config file provided)
    let config_cancel = CancellationToken::new();
    if let Some(config_path) = &cli.config_file {
        let path = config_path.clone();
        let mgr = manager.clone();
        let cancel = config_cancel.clone();
        tokio::spawn(async move {
            manager::watch_config_file(path, mgr, cancel).await;
        });
    }

    // Start stats reporter + throughput-rate sampler
    let stats_cancel = CancellationToken::new();
    let _stats_handle = manager::start_stats_reporter(
        manager.stats_registry(),
        stats_cancel.clone(),
        Duration::from_secs(settings.report_interval_secs),
    );
    let _sampler_handle = manager::start_rate_sampler(
        manager.stats_registry(),
        stats_cancel.clone(),
        Duration::from_secs(settings.sample_interval_secs),
    );

    // Start the HTTP stats endpoint if configured (opt-in)
    if let Some(metrics_addr) = settings.metrics_addr.clone() {
        let registry = manager.stats_registry();
        let cancel = stats_cancel.clone();
        tokio::spawn(async move {
            metrics::serve(metrics_addr, registry, cancel).await;
        });
    }

    // Wait for shutdown signal
    info!("rfw is running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received. Stopping all forwarders...");

    // Graceful shutdown
    stats_cancel.cancel();
    config_cancel.cancel();
    manager.shutdown_all().await;

    info!("rfw stopped.");
    Ok(())
}
