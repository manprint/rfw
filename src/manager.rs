use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{load_yaml_forwarders, ForwarderConfig};
use crate::forwarder::{run_forwarder, stats_reporter};
use crate::stats::StatsRegistry;

/// Manages lifecycle of all forwarders: start, stop, hot-reload.
pub struct ForwarderManager {
    /// Map of label -> CancellationToken for running forwarders
    forwarders: RwLock<HashMap<String, ForwarderHandle>>,
    stats_registry: Arc<StatsRegistry>,
}

struct ForwarderHandle {
    token: CancellationToken,
}

impl ForwarderManager {
    pub fn new() -> Self {
        Self {
            forwarders: RwLock::new(HashMap::new()),
            stats_registry: Arc::new(StatsRegistry::new()),
        }
    }

    pub fn stats_registry(&self) -> Arc<StatsRegistry> {
        self.stats_registry.clone()
    }

    /// Start all forwarders from the given configs.
    pub async fn start_all(&self, configs: &[ForwarderConfig]) {
        for cfg in configs {
            self.start_one(cfg).await;
        }
    }

    /// Start a single forwarder.
    async fn start_one(&self, cfg: &ForwarderConfig) {
        let label = cfg.label();
        let mut fwds = self.forwarders.write().await;

        if fwds.contains_key(&label) {
            warn!(forwarder = %label, "Already running, skipping");
            return;
        }

        let token = CancellationToken::new();
        let stats = self.stats_registry.register(&label).await;
        let cfg_clone = cfg.clone();
        let token_clone = token.clone();

        tokio::spawn(async move {
            run_forwarder(cfg_clone, token_clone, stats).await;
        });

        fwds.insert(label.clone(), ForwarderHandle { token });
        info!(forwarder = %label, "Started");
    }

    /// Stop a single forwarder by label.
    async fn stop_one(&self, label: &str) {
        let mut fwds = self.forwarders.write().await;
        if let Some(handle) = fwds.remove(label) {
            handle.token.cancel();
            self.stats_registry.remove(label).await;
            info!(forwarder = %label, "Stopped");
        }
    }

    /// Sync running forwarders to match a new config list.
    /// Starts new ones, stops removed ones.
    pub async fn sync(&self, configs: &[ForwarderConfig]) {
        let fwds = self.forwarders.read().await;
        let current_labels: std::collections::HashSet<_> = fwds.keys().cloned().collect();
        let desired_labels: std::collections::HashSet<_> =
            configs.iter().map(|c| c.label()).collect();
        drop(fwds);

        // Stop removed
        for label in current_labels.difference(&desired_labels) {
            self.stop_one(label).await;
        }

        // Start added
        for cfg in configs {
            if !current_labels.contains(&cfg.label()) {
                self.start_one(cfg).await;
            }
        }
    }

    /// Shut down all forwarders.
    pub async fn shutdown_all(&self) {
        let labels: Vec<String> = {
            let fwds = self.forwarders.read().await;
            fwds.keys().cloned().collect()
        };

        for label in &labels {
            self.stop_one(label).await;
        }
    }

    /// Get current labels of running forwarders.
    #[allow(dead_code)]
    pub async fn running_labels(&self) -> Vec<String> {
        let fwds = self.forwarders.read().await;
        fwds.keys().cloned().collect()
    }
}

impl Default for ForwarderManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Watch a YAML config file for changes and hot-reload forwarders.
pub async fn watch_config_file(
    config_path: PathBuf,
    manager: Arc<ForwarderManager>,
    cancel: CancellationToken,
) {
    let path = match config_path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot resolve config path: {e}");
            return;
        }
    };

    // Use notify to watch the file
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);

    let mut watcher = match RecommendedWatcher::new(
        move |event: Result<Event, notify::Error>| {
            let tx = tx.clone();
            if let Ok(event) = event {
                let _ = tx.try_send(event);
            }
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Cannot create file watcher: {e}");
            return;
        }
    };

    // Watch the parent directory (notify watches files by their inode;
    // editors like vim create a new file on save, so we watch the dir)
    let watch_dir = path
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf();

    if let Err(e) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        error!("Cannot watch directory {}: {e}", watch_dir.display());
        return;
    }

    info!(
        "Watching config file {} for changes (hot-reload enabled)",
        path.display()
    );

    // Keep watcher alive by not dropping it
    let _watcher = watcher;

    let mut last_reload = std::time::Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Config watcher shutting down");
                return;
            }
            Some(event) = rx.recv() => {
                // Debounce: ignore events within 1 second of last reload
                if last_reload.elapsed() < Duration::from_secs(1) {
                    continue;
                }

                // Check if event is relevant to our file
                let is_relevant = match &event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_) => true,
                    _ => false,
                };

                if !is_relevant {
                    continue;
                }

                // Check if path matches
                let path_matches = event.paths.iter().any(|p| {
                    p.canonicalize().map(|c| c == path).unwrap_or(false)
                });

                if !path_matches {
                    continue;
                }

                last_reload = std::time::Instant::now();

                // Short delay to let the file finish writing
                tokio::time::sleep(Duration::from_millis(200)).await;

                info!("Config file changed, reloading...");
                match load_yaml_forwarders(&config_path) {
                    Ok(configs) => {
                        manager.sync(&configs).await;
                        info!("Config reloaded: {} forwarders active", configs.len());
                    }
                    Err(e) => {
                        error!("Failed to reload config: {e}");
                    }
                }
            }
        }
    }
}

/// Start the periodic stats reporter task.
pub fn start_stats_reporter(
    stats_registry: Arc<StatsRegistry>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        stats_reporter(stats_registry, cancel).await;
    })
}
