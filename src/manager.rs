use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::config::{LookupIpStrategy, ResolverOpts};
use hickory_resolver::TokioResolver;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::config::{
    apply_cli_overrides, load_yaml_config, CliArgs, ForwarderConfig, ForwarderSource, Protocol,
    RuntimeKnobs, Settings, MAX_UDP_SESSIONS,
};
use crate::forwarder::{rate_sampler, run_forwarder, stats_reporter};
use crate::monitor::{probe_all, run_monitor};
use crate::notify::{Event as Alert, Notifier, ProbeReport};
use crate::stats::StatsRegistry;
use crate::udp::run_udp_forwarder;

/// How long to wait for in-flight connections to finish on shutdown before
/// abandoning them (the runtime drops the rest at process exit).
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(10);

/// How long `stop_one` waits for a forwarder task to actually release its
/// listening socket. Without this wait, a reload that re-targets a forwarder on
/// the same local port races the old listener and hits `EADDRINUSE`, costing a
/// multi-second outage on a port that never needed to go down.
const STOP_LISTENER_GRACE: Duration = Duration::from_secs(5);

/// Apply rfw's resolver policy on top of whatever the system config provides.
///
/// Shared with the DNS tests so they exercise the production strategy rather
/// than hickory's default.
///
/// hickory's default `ip_strategy` is `Ipv6AndIpv4`: it issues the A and AAAA
/// queries together and waits for *both* to finish. A resolver that drops or
/// delays AAAA responses (some captive/legacy resolvers do) then stalls every
/// lookup to `dns_timeout` and fails the connection even though the A answer
/// arrived immediately. `Ipv4thenIpv6` returns as soon as A yields addresses
/// and only asks for AAAA when it does not, which also halves query volume on
/// the IPv4-only targets this forwarder is typically pointed at.
pub(crate) fn apply_resolver_policy(opts: &mut ResolverOpts) {
    opts.ip_strategy = LookupIpStrategy::Ipv4thenIpv6;
}

/// Manages lifecycle of all forwarders: start, stop, hot-reload.
pub struct ForwarderManager {
    /// Map of label -> handle for running forwarders
    forwarders: RwLock<HashMap<String, ForwarderHandle>>,
    stats_registry: Arc<StatsRegistry>,
    /// One DNS resolver shared by every forwarder (shared cache, no per-forwarder build).
    resolver: Arc<TokioResolver>,
    /// Live data-plane knobs (buffer size, socket buffers, splice), updatable on reload.
    knobs: Arc<RuntimeKnobs>,
    /// Tracks all in-flight connection tasks so shutdown can drain them.
    conn_tracker: TaskTracker,
    /// Global cap on concurrently proxied connections. `None` = unbounded.
    conn_limit: Option<Arc<Semaphore>>,
    /// Global cap on concurrent UDP sessions (`udp_max_sessions`, startup-only).
    udp_limit: Arc<Semaphore>,
    /// Alert sink shared by every forwarder, monitor and the reload path.
    notifier: Notifier,
}

struct ForwarderHandle {
    token: CancellationToken,
    /// Join handle of the accept-loop supervisor. Awaited by `stop_one` so
    /// the listening socket is closed before anything tries to rebind the port.
    task: tokio::task::JoinHandle<()>,
    /// The target health monitor; shares `token`, so it exits on its own.
    monitor: tokio::task::JoinHandle<()>,
}

impl ForwarderManager {
    /// Build the manager. Fails if the DNS resolver cannot be constructed
    /// (surfaced once at startup instead of panicking per connection).
    pub fn new(settings: &Settings, notifier: Notifier) -> anyhow::Result<Self> {
        let mut builder = TokioResolver::builder_tokio()
            .map_err(|e| anyhow::anyhow!("failed to init DNS resolver builder: {e}"))?;
        apply_resolver_policy(builder.options_mut());
        let resolver = builder
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build DNS resolver: {e}"))?;
        Ok(Self {
            forwarders: RwLock::new(HashMap::new()),
            stats_registry: Arc::new(StatsRegistry::new()),
            resolver: Arc::new(resolver),
            knobs: Arc::new(RuntimeKnobs::from_settings(settings)),
            conn_tracker: TaskTracker::new(),
            conn_limit: settings
                .max_connections
                .filter(|n| *n > 0)
                .map(|n| Arc::new(Semaphore::new(n))),
            udp_limit: Arc::new(Semaphore::new(
                settings.udp_max_sessions.clamp(1, MAX_UDP_SESSIONS),
            )),
            notifier,
        })
    }

    pub fn stats_registry(&self) -> Arc<StatsRegistry> {
        self.stats_registry.clone()
    }

    pub fn notifier(&self) -> Notifier {
        self.notifier.clone()
    }

    /// Probe every configured target once, for the startup alert and log.
    pub async fn startup_report(&self, configs: &[ForwarderConfig]) -> Vec<ProbeReport> {
        probe_all(configs, self.resolver.clone(), &self.knobs).await
    }

    /// Apply reloaded data-plane knobs (buffer/socket-buffer/splice) live.
    /// New connections pick them up; interval/metrics settings still need a restart.
    pub fn update_knobs(&self, settings: &Settings) {
        self.knobs.update(settings);
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
        let stats = self
            .stats_registry
            .register_with_protocol(&label, cfg.protocol)
            .await;
        let cfg_clone = cfg.clone();
        let token_clone = token.clone();
        let resolver = self.resolver.clone();
        let knobs = self.knobs.clone();
        let conn_tracker = self.conn_tracker.clone();
        let conn_limit = self.conn_limit.clone();
        let udp_limit = self.udp_limit.clone();
        let notifier = self.notifier.clone();

        // Supervisor: the accept loop runs in its own task so a panic inside
        // it is observed here (and alerted) the moment it happens, instead of
        // surfacing only when the forwarder is eventually stopped.
        let task = {
            let label = label.clone();
            let notifier = notifier.clone();
            tokio::spawn(async move {
                let inner = match cfg_clone.protocol {
                    Protocol::Tcp => tokio::spawn(run_forwarder(
                        cfg_clone,
                        token_clone,
                        stats,
                        resolver,
                        knobs,
                        conn_tracker,
                        conn_limit,
                        notifier.clone(),
                    )),
                    Protocol::Udp => tokio::spawn(run_udp_forwarder(
                        cfg_clone,
                        token_clone,
                        stats,
                        resolver,
                        knobs,
                        udp_limit,
                        notifier.clone(),
                    )),
                };
                if let Err(e) = inner.await {
                    if e.is_panic() {
                        error!(forwarder = %label, error = %e, "Forwarder task panicked; its port is no longer served");
                        notifier.notify(Alert::Internal {
                            component: "forwarder",
                            message: format!(
                                "accept loop of {label} panicked: {e}. The port is no longer served until the process restarts"
                            ),
                        });
                    }
                }
            })
        };

        let monitor = tokio::spawn(run_monitor(
            Arc::new(cfg.clone()),
            token.clone(),
            self.stats_registry
                .register_with_protocol(&label, cfg.protocol)
                .await,
            self.resolver.clone(),
            self.knobs.clone(),
            notifier,
        ));

        fwds.insert(
            label.clone(),
            ForwarderHandle {
                token,
                task,
                monitor,
            },
        );
        info!(forwarder = %label, "Started");
    }

    /// Stop a single forwarder by label.
    ///
    /// Waits (bounded) for the accept loop to exit so the listening socket is
    /// released before the caller tries to bind the same local address again.
    async fn stop_one(&self, label: &str) {
        // Take the handle out under the lock, then release it: awaiting the
        // task while holding the write lock would stall every other manager
        // operation for up to the grace window.
        let handle = {
            let mut fwds = self.forwarders.write().await;
            fwds.remove(label)
        };

        let Some(handle) = handle else { return };
        handle.token.cancel();
        match tokio::time::timeout(STOP_LISTENER_GRACE, handle.task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(forwarder = %label, error = %e, "Forwarder task ended abnormally"),
            Err(_) => warn!(
                forwarder = %label,
                "Forwarder did not stop within {}s; its port may briefly stay bound",
                STOP_LISTENER_GRACE.as_secs()
            ),
        }
        // The monitor exits on the shared token; abort covers a probe still
        // in flight so no stale verdict lands on a removed forwarder.
        handle.monitor.abort();
        self.stats_registry.remove(label).await;
        info!(forwarder = %label, "Stopped");
    }

    /// Sync running forwarders to match a new config list.
    /// Starts new ones, stops removed ones.
    ///
    /// Stops run to completion before starts, so a forwarder that is re-targeted
    /// (same local address, new remote) rebinds its port without racing the
    /// listener it is replacing.
    pub async fn sync(&self, configs: &[ForwarderConfig]) {
        let current_labels: std::collections::HashSet<_> = {
            let fwds = self.forwarders.read().await;
            fwds.keys().cloned().collect()
        };
        let desired_labels: std::collections::HashSet<_> =
            configs.iter().map(|c| c.label()).collect();

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
        // Cancel every accept loop first so they wind down in parallel, then
        // reap them; stopping them one at a time would serialize the grace
        // window across forwarders.
        let handles: Vec<(String, ForwarderHandle)> = {
            let mut fwds = self.forwarders.write().await;
            fwds.drain().collect()
        };
        for (_, handle) in &handles {
            handle.token.cancel();
        }
        for (label, handle) in handles {
            if tokio::time::timeout(STOP_LISTENER_GRACE, handle.task)
                .await
                .is_err()
            {
                warn!(forwarder = %label, "Forwarder did not stop within the grace window");
            }
            handle.monitor.abort();
            self.stats_registry.remove(&label).await;
            info!(forwarder = %label, "Stopped");
        }

        // Drain in-flight connection tasks so short transfers can finish instead
        // of being truncated at process exit. Bounded by a grace window.
        self.conn_tracker.close();
        if !self.conn_tracker.is_empty() {
            info!(
                "Draining {} in-flight connection(s) (up to {}s)...",
                self.conn_tracker.len(),
                SHUTDOWN_DRAIN_GRACE.as_secs()
            );
            if tokio::time::timeout(SHUTDOWN_DRAIN_GRACE, self.conn_tracker.wait())
                .await
                .is_err()
            {
                warn!(
                    "Shutdown grace elapsed; {} connection(s) still active, abandoning them",
                    self.conn_tracker.len()
                );
            }
        }
    }

    /// Get current labels of running forwarders.
    pub async fn running_labels(&self) -> Vec<String> {
        let fwds = self.forwarders.read().await;
        fwds.keys().cloned().collect()
    }
}

impl Default for ForwarderManager {
    fn default() -> Self {
        Self::new(&Settings::default(), Notifier::disabled())
            .expect("failed to build default ForwarderManager")
    }
}

fn is_relevant_config_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
    )
}

fn watched_path_matches_event_path(watched_path: &Path, event_path: &Path) -> bool {
    event_path == watched_path
        || event_path
            .canonicalize()
            .map(|canonical_path| canonical_path == watched_path)
            .unwrap_or(false)
}

fn should_debounce_reload(
    last_reload: Option<Instant>,
    now: Instant,
    debounce_window: Duration,
) -> bool {
    last_reload.is_some_and(|previous| now.duration_since(previous) < debounce_window)
}

/// Sleep for `dur` unless cancelled. Returns `true` if cancelled.
async fn cancelled_during(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

/// What a hot-reload may change, fixed at startup.
#[derive(Debug, Clone)]
pub struct ReloadPolicy {
    /// Re-applied over the file's `settings:` on every reload, so CLI flags keep winning.
    pub cli: CliArgs,
    /// Where the running forwarders came from. Only `File` lets a reload sync the list.
    pub source: ForwarderSource,
}

/// Watch a YAML config file for changes and hot-reload forwarders.
pub async fn watch_config_file(
    config_path: PathBuf,
    manager: Arc<ForwarderManager>,
    cancel: CancellationToken,
    policy: ReloadPolicy,
) {
    let notifier = manager.notifier();
    let path = match config_path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot resolve config path: {e}");
            notifier.notify(Alert::Internal {
                component: "config-watcher",
                message: format!(
                    "cannot resolve {}: {e}. Hot-reload is disabled",
                    config_path.display()
                ),
            });
            return;
        }
    };

    // Use notify to watch the file.
    //
    // Relevance filtering happens inside the watcher callback, so the channel
    // only ever carries "the config is dirty" signals. That lets a depth-1
    // channel coalesce a burst of writes without the risk of a full channel
    // dropping the one event that mattered.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

    let watched = path.clone();
    let mut watcher = match RecommendedWatcher::new(
        move |event: Result<Event, notify::Error>| {
            let Ok(event) = event else { return };
            if !is_relevant_config_event(&event.kind) {
                return;
            }
            let matches = event
                .paths
                .iter()
                .any(|event_path| watched_path_matches_event_path(&watched, event_path));
            if matches {
                // Full channel means a reload is already queued: nothing to do.
                let _ = tx.try_send(());
            }
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            error!("Cannot create file watcher: {e}");
            notifier.notify(Alert::Internal {
                component: "config-watcher",
                message: format!("cannot create the file watcher: {e}. Hot-reload is disabled"),
            });
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
        notifier.notify(Alert::Internal {
            component: "config-watcher",
            message: format!(
                "cannot watch {}: {e}. Hot-reload is disabled",
                watch_dir.display()
            ),
        });
        return;
    }

    info!(
        "Watching config file {} for changes (hot-reload enabled)",
        path.display()
    );

    // Keep watcher alive by not dropping it
    let _watcher = watcher;

    const RELOAD_DEBOUNCE: Duration = Duration::from_secs(1);
    const RELOAD_SETTLE_DELAY: Duration = Duration::from_millis(200);

    let mut last_reload = None;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Config watcher shutting down");
                return;
            }
            signal = rx.recv() => {
                // `None` means every sender is gone: the watcher died and no
                // further reloads can arrive. Keep serving traffic, but say so
                // rather than spinning on a closed channel.
                if signal.is_none() {
                    error!("Config watcher channel closed; hot-reload is no longer active");
                    notifier.notify(Alert::Internal {
                        component: "config-watcher",
                        message: "the file watcher stopped delivering events; hot-reload is no longer active (restart to re-arm)".to_string(),
                    });
                    return;
                }

                // Debounce by *delaying*, never by discarding.
                //
                // Dropping an event here loses an edit outright: a save that
                // lands inside the window would be swallowed, and with the
                // watcher channel already holding a coalesced token, nothing
                // would ever come along to trigger the reload it needed.
                // Waiting out the remainder of the window rate-limits reload
                // storms while still guaranteeing every save is applied.
                if should_debounce_reload(last_reload, Instant::now(), RELOAD_DEBOUNCE) {
                    let since = Instant::now().duration_since(
                        last_reload.expect("should_debounce_reload is false for None"),
                    );
                    if cancelled_during(&cancel, RELOAD_DEBOUNCE - since).await {
                        info!("Config watcher shutting down");
                        return;
                    }
                }

                // Short delay to let the file finish writing.
                if cancelled_during(&cancel, RELOAD_SETTLE_DELAY).await {
                    info!("Config watcher shutting down");
                    return;
                }

                // Collapse the rest of this save's event burst into this one
                // reload. Draining *after* the settle delay is what makes a
                // multi-write save (truncate, then write) a single reload.
                while rx.try_recv().is_ok() {}

                last_reload = Some(Instant::now());

                info!("Config file changed, reloading...");
                // Parse the whole file once and apply both halves together. A
                // file that fails to parse (a mid-save truncation, a typo)
                // leaves the running config *and* the data-plane knobs exactly
                // as they were, instead of silently resetting the knobs to
                // their defaults while the forwarders keep the old list.
                match load_yaml_config(&config_path) {
                    Ok(loaded) => {
                        let before: std::collections::HashSet<String> =
                            manager.running_labels().await.into_iter().collect();
                        let mut settings = loaded.settings;
                        apply_cli_overrides(&mut settings, &policy.cli);
                        manager.update_knobs(&settings);
                        if policy.source == ForwarderSource::File {
                            manager.sync(&loaded.forwarders).await;
                        } else {
                            info!(
                                source = %policy.source,
                                "Forwarders are pinned by {}: the file's forwarders list is ignored on reload (settings applied)",
                                policy.source
                            );
                        }
                        let after: std::collections::HashSet<String> =
                            manager.running_labels().await.into_iter().collect();
                        let mut added: Vec<String> = after.difference(&before).cloned().collect();
                        let mut removed: Vec<String> = before.difference(&after).cloned().collect();
                        added.sort();
                        removed.sort();
                        info!(
                            added = ?added,
                            removed = ?removed,
                            "Config reloaded: {} forwarders active",
                            after.len()
                        );
                        notifier.notify(Alert::ConfigReloaded {
                            active: after.len(),
                            added,
                            removed,
                        });
                    }
                    Err(e) => {
                        error!("Failed to reload config, keeping the running configuration: {e}");
                        notifier.notify(Alert::ConfigReloadFailed {
                            path: config_path.display().to_string(),
                            error: e.to_string(),
                        });
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
    report_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        stats_reporter(stats_registry, cancel, report_interval).await;
    })
}

/// Start the periodic throughput-rate sampler task.
pub fn start_rate_sampler(
    stats_registry: Arc<StatsRegistry>,
    cancel: CancellationToken,
    sample_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        rate_sampler(stats_registry, cancel, sample_interval).await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_debounce_reload_allows_first_event() {
        let now = Instant::now();
        assert!(!should_debounce_reload(None, now, Duration::from_secs(1)));
    }

    #[test]
    fn test_should_debounce_reload_blocks_quick_repeat_events() {
        let now = Instant::now();
        assert!(should_debounce_reload(
            Some(now - Duration::from_millis(500)),
            now,
            Duration::from_secs(1),
        ));
        assert!(!should_debounce_reload(
            Some(now - Duration::from_secs(2)),
            now,
            Duration::from_secs(1),
        ));
    }

    #[test]
    fn test_watched_path_matches_deleted_target_path() {
        let path = std::env::temp_dir().join("rfw-watch-config-does-not-exist.yml");
        let _ = std::fs::remove_file(&path);

        assert!(watched_path_matches_event_path(&path, &path));
    }

    use crate::config::{Protocol, Settings};
    use tokio::net::TcpListener;

    fn cfg(local_port: u16, remote_port: u16) -> ForwarderConfig {
        ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port,
            remote_host: "127.0.0.1".into(),
            remote_port,
            protocol: Protocol::Tcp,
        }
    }

    /// Reserve a free localhost port, never returning the same one twice.
    ///
    /// Unit tests run in parallel, and a naive "bind 0, read the port, drop"
    /// hands the same port to two tests before either rebinds it — which shows
    /// up as a spurious `EADDRINUSE` in whichever test binds second.
    async fn free_port() -> u16 {
        use std::collections::HashSet;
        use std::sync::{Mutex, OnceLock};
        static CLAIMED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
        let claimed = CLAIMED.get_or_init(|| Mutex::new(HashSet::new()));

        for _ in 0..1000 {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            if claimed.lock().unwrap().insert(p) {
                return p;
            }
        }
        panic!("could not reserve an unused ephemeral port");
    }

    /// Poll until `port` accepts, or fail after ~3s.
    async fn wait_listening(port: u16) {
        for _ in 0..150 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("port {port} never started listening");
    }

    fn manager() -> Arc<ForwarderManager> {
        Arc::new(
            ForwarderManager::new(&Settings::default(), Notifier::disabled()).expect("manager"),
        )
    }

    #[tokio::test]
    async fn startup_report_probes_every_configured_target_in_order() {
        let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_port = live.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((s, _)) = live.accept().await {
                drop(s);
            }
        });
        let dead_port = free_port().await;

        let m = manager();
        let configs = [
            cfg(free_port().await, dead_port),
            cfg(free_port().await, live_port),
        ];
        let report = m.startup_report(&configs).await;
        assert_eq!(report.len(), 2);
        assert_eq!(report[0].label, configs[0].label());
        assert!(report[0].outcome.is_err(), "{:?}", report[0]);
        assert_eq!(report[1].label, configs[1].label());
        assert!(report[1].outcome.is_ok(), "{:?}", report[1]);
    }

    #[tokio::test]
    async fn every_started_forwarder_gets_a_monitor_that_reports_reachability() {
        let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_port = live.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((s, _)) = live.accept().await {
                drop(s);
            }
        });
        let m = manager();
        m.start_all(&[cfg(free_port().await, live_port)]).await;
        for _ in 0..200 {
            let snaps = m.stats_registry().collect().await;
            if snaps.iter().all(|s| s.target_reachable == Some(true)) {
                m.shutdown_all().await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("monitor never reported the target reachable");
    }

    #[tokio::test]
    async fn start_all_registers_every_forwarder() {
        let m = manager();
        let (a, b) = (free_port().await, free_port().await);
        m.start_all(&[cfg(a, 1), cfg(b, 2)]).await;

        let mut labels = m.running_labels().await;
        labels.sort();
        assert_eq!(labels.len(), 2);
        assert_eq!(m.stats_registry().collect().await.len(), 2);

        m.shutdown_all().await;
        assert!(m.running_labels().await.is_empty());
        assert!(m.stats_registry().collect().await.is_empty());
    }

    #[tokio::test]
    async fn starting_the_same_label_twice_is_a_no_op() {
        let m = manager();
        let p = free_port().await;
        m.start_all(&[cfg(p, 1), cfg(p, 1)]).await;
        assert_eq!(m.running_labels().await.len(), 1);
        assert_eq!(
            m.stats_registry().collect().await.len(),
            1,
            "a duplicate label must not create a second metrics series"
        );
        m.shutdown_all().await;
    }

    #[tokio::test]
    async fn stop_one_releases_the_listening_port_before_returning() {
        // This is the hot-reload rebind race. `stop_one` used to cancel the
        // token and return immediately, so re-targeting a forwarder on the same
        // local port hit EADDRINUSE and left the port dark for the 5s bind
        // backoff. Awaiting the accept task is what makes the port reusable.
        let m = manager();
        let port = free_port().await;
        m.start_all(&[cfg(port, 1)]).await;
        wait_listening(port).await;

        let label = cfg(port, 1).label();
        m.stop_one(&label).await;

        // The port must be immediately bindable, with no retry loop.
        TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("port must be free the moment stop_one returns");
    }

    #[tokio::test]
    async fn stop_one_releases_the_udp_port() {
        let m = manager();
        let port = free_port().await;
        let udp = ForwarderConfig {
            protocol: Protocol::Udp,
            ..cfg(port, 9)
        };
        m.start_all(&[udp]).await;
        let mut listening = false;
        for _ in 0..150 {
            if m.stats_registry()
                .collect()
                .await
                .iter()
                .any(|s| s.listening)
            {
                listening = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(listening, "the UDP forwarder must start listening");

        m.sync(&[]).await;

        std::net::UdpSocket::bind(("127.0.0.1", port))
            .expect("UDP port must be free the moment sync returns");
    }

    #[tokio::test]
    async fn sync_retargets_a_forwarder_on_the_same_local_port() {
        // End-to-end shape of the reload race: same local port, new remote.
        let m = manager();
        let port = free_port().await;
        m.start_all(&[cfg(port, 1)]).await;
        wait_listening(port).await;

        m.sync(&[cfg(port, 2)]).await;

        let labels = m.running_labels().await;
        assert_eq!(labels.len(), 1);
        assert!(labels[0].ends_with(":2"), "must be re-targeted: {labels:?}");
        wait_listening(port).await; // rebound without a bind-failure backoff

        m.shutdown_all().await;
    }

    #[tokio::test]
    async fn sync_adds_and_removes_forwarders() {
        let m = manager();
        let (a, b, c) = (free_port().await, free_port().await, free_port().await);

        m.start_all(&[cfg(a, 1), cfg(b, 1)]).await;
        assert_eq!(m.running_labels().await.len(), 2);

        // Drop `a`, keep `b`, add `c`.
        m.sync(&[cfg(b, 1), cfg(c, 1)]).await;
        let labels = m.running_labels().await;
        assert_eq!(labels.len(), 2);
        assert!(!labels.contains(&cfg(a, 1).label()), "removed: {labels:?}");
        assert!(labels.contains(&cfg(b, 1).label()), "kept: {labels:?}");
        assert!(labels.contains(&cfg(c, 1).label()), "added: {labels:?}");

        // Stats follow the same lifecycle.
        let series: Vec<_> = m
            .stats_registry()
            .collect()
            .await
            .into_iter()
            .map(|s| s.label)
            .collect();
        assert_eq!(series.len(), 2);
        assert!(!series.contains(&cfg(a, 1).label()));

        m.shutdown_all().await;
    }

    #[tokio::test]
    async fn sync_to_an_empty_list_stops_everything() {
        let m = manager();
        let p = free_port().await;
        m.start_all(&[cfg(p, 1)]).await;
        wait_listening(p).await;

        m.sync(&[]).await;
        assert!(m.running_labels().await.is_empty());
        TcpListener::bind(("127.0.0.1", p))
            .await
            .expect("port released");
    }

    #[tokio::test]
    async fn sync_is_idempotent() {
        let m = manager();
        let p = free_port().await;
        let configs = [cfg(p, 1)];
        m.start_all(&configs).await;
        wait_listening(p).await;

        // Re-applying an unchanged config must not restart anything (a restart
        // would briefly drop the listener and reset the counters).
        m.sync(&configs).await;
        m.sync(&configs).await;

        assert_eq!(m.running_labels().await.len(), 1);
        assert_eq!(m.stats_registry().collect().await.len(), 1);
        m.shutdown_all().await;
    }

    #[tokio::test]
    async fn shutdown_all_is_safe_to_call_twice() {
        let m = manager();
        m.start_all(&[cfg(free_port().await, 1)]).await;
        m.shutdown_all().await;
        m.shutdown_all().await; // must not panic or hang
        assert!(m.running_labels().await.is_empty());
    }

    #[tokio::test]
    async fn update_knobs_applies_reloaded_values() {
        let m = manager();
        m.update_knobs(&Settings {
            buffer_bytes: 4096,
            use_splice: true,
            ..Settings::default()
        });
        let snap = m.knobs.snapshot();
        assert_eq!(snap.buffer_bytes, 4096);
        assert!(snap.use_splice);
    }

    #[tokio::test]
    async fn update_knobs_clamps_a_dangerous_reload() {
        // A reloaded file is not validated the way startup args are, so the
        // knobs themselves must refuse to install a spinning buffer size.
        let m = manager();
        m.update_knobs(&Settings {
            buffer_bytes: 0,
            ..Settings::default()
        });
        assert!(m.knobs.snapshot().buffer_bytes >= crate::config::MIN_BUFFER_BYTES);
    }

    #[test]
    fn resolver_policy_prefers_ipv4_then_ipv6() {
        // The default joins A and AAAA and waits for both; a dropped AAAA
        // would stall every connection to the DNS timeout. See
        // `apply_resolver_policy`.
        let mut opts = ResolverOpts::default();
        apply_resolver_policy(&mut opts);
        assert_eq!(opts.ip_strategy, LookupIpStrategy::Ipv4thenIpv6);
    }

    #[test]
    fn relevant_config_events_cover_editor_save_patterns() {
        use notify::event::{CreateKind, ModifyKind, RemoveKind};
        // Editors save by rename/replace, so create and remove matter as much
        // as modify.
        assert!(is_relevant_config_event(&EventKind::Modify(
            ModifyKind::Any
        )));
        assert!(is_relevant_config_event(&EventKind::Create(
            CreateKind::Any
        )));
        assert!(is_relevant_config_event(&EventKind::Remove(
            RemoveKind::Any
        )));
        assert!(!is_relevant_config_event(&EventKind::Access(
            notify::event::AccessKind::Read
        )));
        assert!(!is_relevant_config_event(&EventKind::Other));
    }

    #[test]
    fn watched_path_ignores_unrelated_files_in_the_same_directory() {
        let dir = std::env::temp_dir();
        let watched = dir.join("rfw-watched.yml");
        let other = dir.join("rfw-other.yml");
        assert!(!watched_path_matches_event_path(&watched, &other));
        assert!(watched_path_matches_event_path(&watched, &watched));
    }

    #[test]
    fn debounce_boundary_is_exclusive() {
        let now = Instant::now();
        let window = Duration::from_secs(1);
        // Exactly at the window edge the event is allowed through.
        assert!(!should_debounce_reload(Some(now - window), now, window));
        assert!(should_debounce_reload(
            Some(now - window + Duration::from_millis(1)),
            now,
            window
        ));
    }

    // ---- reload policy (B1) -------------------------------------------------

    fn temp_yaml(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rfw-mgr-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("forwarders.yml");
        std::fs::write(&path, body).expect("write yaml");
        path
    }

    fn yaml(fwd: &ForwarderConfig, settings: &str) -> String {
        format!(
            "forwarders:\n  - {{local_host: \"{}\", local_port: {}, remote_host: \"{}\", remote_port: {}}}\n{settings}",
            fwd.local_host, fwd.local_port, fwd.remote_host, fwd.remote_port
        )
    }

    #[tokio::test]
    async fn reload_reapplies_cli_overrides_over_file_settings() {
        use crate::config::{load_settings, CliArgs, ForwarderSource};
        let (p, r) = (free_port().await, free_port().await);
        let path = temp_yaml(
            "cli-over-file",
            &yaml(
                &cfg(p, r),
                "settings:\n  buffer_bytes: 4096\n  probe_interval_secs: 30\n",
            ),
        );
        let cli = CliArgs {
            config_file: Some(path.clone()),
            buffer_bytes: Some(2048),
            ..Default::default()
        };
        let settings = load_settings(&cli).unwrap();
        let mgr = Arc::new(ForwarderManager::new(&settings, Notifier::disabled()).unwrap());
        mgr.start_all(&[cfg(p, r)]).await;

        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch_config_file(
            path.clone(),
            mgr.clone(),
            cancel.clone(),
            ReloadPolicy {
                cli: cli.clone(),
                source: ForwarderSource::File,
            },
        ));

        let reloaded = yaml(
            &cfg(p, r),
            "settings:\n  buffer_bytes: 8192\n  probe_interval_secs: 7\n",
        );
        let mut applied = false;
        for _ in 0..30 {
            std::fs::write(&path, &reloaded).unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            if mgr.knobs.probe_interval() == Duration::from_secs(7) {
                applied = true;
                break;
            }
        }
        assert!(applied, "reload never applied");
        assert_eq!(mgr.knobs.buffer_bytes(), 2048);

        cancel.cancel();
        let _ = watcher.await;
        mgr.shutdown_all().await;
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn reload_does_not_sync_pinned_forwarders() {
        use crate::config::{CliArgs, ForwarderSource};
        let (pa, pb, r) = (free_port().await, free_port().await, free_port().await);
        let path = temp_yaml("pinned", &yaml(&cfg(pb, r), ""));
        let mgr = manager();
        mgr.start_all(&[cfg(pa, r)]).await;
        wait_listening(pa).await;

        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch_config_file(
            path.clone(),
            mgr.clone(),
            cancel.clone(),
            ReloadPolicy {
                cli: CliArgs::default(),
                source: ForwarderSource::Cli,
            },
        ));

        let reloaded = yaml(&cfg(pb, r), "settings:\n  probe_interval_secs: 7\n");
        let mut applied = false;
        for _ in 0..30 {
            std::fs::write(&path, &reloaded).unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            if mgr.knobs.probe_interval() == Duration::from_secs(7) {
                applied = true;
                break;
            }
        }
        assert!(applied, "reload never applied");
        assert_eq!(mgr.running_labels().await, vec![cfg(pa, r).label()]);
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", pb))
            .await
            .is_err());

        cancel.cancel();
        let _ = watcher.await;
        mgr.shutdown_all().await;
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn reload_syncs_file_forwarders() {
        use crate::config::{CliArgs, ForwarderSource};
        let (pa, pb, r) = (free_port().await, free_port().await, free_port().await);
        let path = temp_yaml("file-sync", &yaml(&cfg(pa, r), ""));
        let mgr = manager();
        mgr.start_all(&[cfg(pa, r)]).await;
        wait_listening(pa).await;

        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch_config_file(
            path.clone(),
            mgr.clone(),
            cancel.clone(),
            ReloadPolicy {
                cli: CliArgs::default(),
                source: ForwarderSource::File,
            },
        ));

        let reloaded = yaml(&cfg(pb, r), "settings:\n  probe_interval_secs: 7\n");
        let want = cfg(pb, r).label();
        let mut applied = false;
        for _ in 0..30 {
            std::fs::write(&path, &reloaded).unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            if mgr.running_labels().await.contains(&want) {
                applied = true;
                break;
            }
        }
        assert!(applied, "reload never applied");
        assert!(!mgr.running_labels().await.contains(&cfg(pa, r).label()));
        wait_listening(pb).await;

        cancel.cancel();
        let _ = watcher.await;
        mgr.shutdown_all().await;
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
