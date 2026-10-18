mod config;
mod forwarder;
mod logging;
mod manager;
mod metrics;
mod monitor;
mod notify;
mod stats;
mod testutil;
mod udp;

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::config::{CliArgs, Protocol};
use crate::notify::{Event, InstanceInfo, Notifier, WebhookConfig};
use clap::Parser;
use tokio_util::sync::CancellationToken;

/// How long shutdown waits for queued alerts (typically just "stopping") to
/// reach the webhook before giving up on them.
const WEBHOOK_FLUSH_GRACE: Duration = Duration::from_secs(3);

/// Wait for the first shutdown signal.
///
/// SIGTERM matters as much as Ctrl+C here: it is what Docker, Kubernetes, and
/// systemd send first. Listening only for SIGINT meant every container stop
/// killed in-flight transfers outright, and the documented graceful drain never
/// ran outside an interactive terminal.
async fn shutdown_signal() -> anyhow::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        tokio::select! {
            _ = sigterm.recv() => Ok("SIGTERM"),
            _ = sigint.recv() => Ok("SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("Ctrl+C")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI first: logging setup depends on --log-dir.
    let cli = CliArgs::parse();

    // Healthcheck mode: probe a running instance and exit. Runs before logging
    // setup so a probe never touches the log directory or competes for the log
    // file with the instance it is checking.
    if let Some(addr) = &cli.health_check {
        return match metrics::health_check(addr).await {
            Ok(()) => {
                println!("ok");
                Ok(())
            }
            Err(e) => Err(e),
        };
    }

    // Setup logging (keep guard alive for app lifetime)
    let _guard = logging::setup(&cli.log_dir)?;

    // Load forwarder configurations (YAML + CLI + env, merged with precedence)
    let (forwarders, source) = config::load_forwarders_with_source(&cli)?;

    // Load runtime settings (YAML settings: block + CLI overrides).
    // Invalid values are rejected here rather than panicking a background task
    // or wedging the copy loop later.
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
             Forwarder format: local_host:local_port:remote_host:remote_port[/tcp|/udp]"
        );
    }

    info!("Starting rfw with {} forwarder(s):", forwarders.len());
    for fwd in &forwarders {
        info!(
            "  {} -> {}:{}{}",
            fwd.local_addr(),
            fwd.remote_host,
            fwd.remote_port,
            if fwd.protocol == Protocol::Udp {
                " (udp)"
            } else {
                ""
            }
        );
    }
    if let Some(path) = &cli.config_file {
        if source != config::ForwarderSource::File {
            info!(
                "Forwarders come from {source}; the forwarders list in {} is ignored (its settings block still applies and hot-reloads)",
                path.display()
            );
        }
    }

    // splice is Linux-only; warn once at startup if requested elsewhere
    #[cfg(not(target_os = "linux"))]
    if settings.use_splice {
        tracing::warn!("--splice / use_splice is Linux-only; using the portable copy path");
    }

    // Webhook alerting (opt-in). Built before the manager so every forwarder,
    // monitor and the reload path share one delivery queue.
    let (notifier, notifier_handle) = match &settings.webhook_url {
        Some(url) => {
            // Validated already by `Settings::validate`; re-parse for the typed URL.
            let url = notify::validate_webhook_url(url)
                .map_err(|e| anyhow::anyhow!("webhook_url is invalid: {e}"))?;
            let (n, h) = notify::start(WebhookConfig {
                url,
                instance: InstanceInfo::new(settings.instance_name.clone()),
            })?;
            (n, Some(h))
        }
        None => (Notifier::disabled(), None),
    };

    // Every UDP session holds one upstream socket: make room for them.
    if forwarders.iter().any(|f| f.protocol == Protocol::Udp) {
        udp::ensure_fd_budget(settings.udp_max_sessions);
    }

    // Create manager and start all forwarders
    let manager = Arc::new(manager::ForwarderManager::new(&settings, notifier.clone())?);
    manager.start_all(&forwarders).await;

    // Start config file watcher (if config file provided)
    let config_cancel = CancellationToken::new();
    if let Some(config_path) = &cli.config_file {
        let path = config_path.clone();
        let mgr = manager.clone();
        let cancel = config_cancel.clone();
        let policy = manager::ReloadPolicy {
            cli: cli.clone(),
            source,
        };
        tokio::spawn(async move {
            manager::watch_config_file(path, mgr, cancel, policy).await;
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
        let n = notifier.clone();
        tokio::spawn(async move {
            metrics::serve(metrics_addr, registry, cancel, n).await;
        });
    }

    // Startup report: probe every target once and tell the operator how the
    // process came up. Runs in the background so a slow or dead target never
    // delays serving traffic on the ports that are fine.
    {
        let mgr = manager.clone();
        let n = notifier.clone();
        let configs = forwarders.clone();
        tokio::spawn(async move {
            let report = mgr.startup_report(&configs).await;
            for r in &report {
                match &r.outcome {
                    Ok((addr, latency)) if r.protocol == Protocol::Udp => info!(
                        forwarder = %r.label,
                        target = %r.target,
                        via = %addr,
                        latency_ms = latency.as_millis(),
                        "Startup probe: target resolved (udp, passive monitoring)"
                    ),
                    Ok((addr, latency)) => info!(
                        forwarder = %r.label,
                        target = %r.target,
                        via = %addr,
                        latency_ms = latency.as_millis(),
                        "Startup probe: target reachable"
                    ),
                    Err(e) => warn!(
                        forwarder = %r.label,
                        target = %r.target,
                        error = %e,
                        "Startup probe: target unreachable"
                    ),
                }
            }
            n.notify(Event::Started { forwarders: report });
        });
    }

    // Wait for shutdown signal
    info!("rfw is running. Press Ctrl+C (or send SIGTERM) to stop.");
    let signal = shutdown_signal().await?;
    info!("{signal} received. Stopping all forwarders...");

    // Tell the operator first: the message goes out while connections drain.
    let active: u64 = manager
        .stats_registry()
        .collect()
        .await
        .iter()
        // only TCP connections are drained; UDP sessions end at once
        .filter(|s| s.protocol == Protocol::Tcp)
        .map(|s| s.active_connections)
        .sum();
    notifier.notify(Event::Stopping {
        signal: signal.to_string(),
        active_connections: active,
    });

    // Graceful shutdown
    stats_cancel.cancel();
    config_cancel.cancel();
    manager.shutdown_all().await;

    if let Some(handle) = notifier_handle {
        handle.shutdown(WEBHOOK_FLUSH_GRACE).await;
    }

    info!("rfw stopped.");
    Ok(())
}
