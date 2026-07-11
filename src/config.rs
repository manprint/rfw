use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Forwarder definition: local_host:local_port:remote_host:remote_port
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwarderConfig {
    pub local_host: String,
    pub local_port: u16,
    pub remote_host: String,
    pub remote_port: u16,
}

impl ForwarderConfig {
    pub fn label(&self) -> String {
        format!(
            "{}:{}->{}:{}",
            self.local_host, self.local_port, self.remote_host, self.remote_port
        )
    }

    pub fn local_addr(&self) -> String {
        format!("{}:{}", self.local_host, self.local_port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigFile {
    forwarders: Vec<ForwarderConfig>,
    #[serde(default)]
    settings: Settings,
}

fn default_report_interval() -> u64 {
    60
}
fn default_sample_interval() -> u64 {
    1
}
fn default_buffer_bytes() -> usize {
    65536
}
fn default_use_splice() -> bool {
    false
}

/// Process-global runtime settings (one HTTP endpoint, one sampler, one reporter).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// `host:port` for the HTTP stats endpoint. `None` disables it (default).
    #[serde(default)]
    pub metrics_addr: Option<String>,
    /// Periodic log report cadence, seconds.
    #[serde(default = "default_report_interval")]
    pub report_interval_secs: u64,
    /// Throughput-rate sampling cadence, seconds.
    #[serde(default = "default_sample_interval")]
    pub sample_interval_secs: u64,
    /// Per-direction copy buffer size, bytes.
    #[serde(default = "default_buffer_bytes")]
    pub buffer_bytes: usize,
    /// Kernel socket buffer size (`SO_SNDBUF`/`SO_RCVBUF`) per socket, bytes.
    /// `None` leaves the OS default. Raising it helps on high bandwidth-delay
    /// (WAN / high-latency) links.
    #[serde(default)]
    pub socket_buffer_bytes: Option<usize>,
    /// Use the Linux `splice(2)` zero-copy data path (opt-in, Linux only).
    /// Ignored with a warning on non-Linux targets.
    #[serde(default = "default_use_splice")]
    pub use_splice: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            metrics_addr: None,
            report_interval_secs: default_report_interval(),
            sample_interval_secs: default_sample_interval(),
            buffer_bytes: default_buffer_bytes(),
            socket_buffer_bytes: None,
            use_splice: default_use_splice(),
        }
    }
}

/// Live, lock-free copy of the data-plane knobs shared by every connection task.
///
/// Held behind an `Arc`; each new connection reads the current values with a
/// `Relaxed` load. Hot-reload updates them via [`RuntimeKnobs::update`], so new
/// connections pick up changes to `buffer_bytes` / `socket_buffer_bytes` /
/// `use_splice` without a restart (in-flight connections keep their values).
#[derive(Debug)]
pub struct RuntimeKnobs {
    buffer_bytes: AtomicUsize,
    /// `0` means "leave the OS default".
    socket_buffer_bytes: AtomicUsize,
    use_splice: AtomicBool,
}

impl RuntimeKnobs {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            buffer_bytes: AtomicUsize::new(s.buffer_bytes),
            socket_buffer_bytes: AtomicUsize::new(s.socket_buffer_bytes.unwrap_or(0)),
            use_splice: AtomicBool::new(s.use_splice),
        }
    }

    /// Apply reloaded settings to the shared knobs (called on hot-reload).
    pub fn update(&self, s: &Settings) {
        self.buffer_bytes.store(s.buffer_bytes, Ordering::Relaxed);
        self.socket_buffer_bytes
            .store(s.socket_buffer_bytes.unwrap_or(0), Ordering::Relaxed);
        self.use_splice.store(s.use_splice, Ordering::Relaxed);
    }

    pub fn buffer_bytes(&self) -> usize {
        self.buffer_bytes.load(Ordering::Relaxed)
    }

    /// Returns `None` when the OS default should be left in place.
    pub fn socket_buffer_bytes(&self) -> Option<usize> {
        match self.socket_buffer_bytes.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    pub fn use_splice(&self) -> bool {
        self.use_splice.load(Ordering::Relaxed)
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "rfw",
    version,
    about = "Rust Forwarder — TCP port forwarder with hot-reload, auto-reconnect, and dynamic DNS"
)]
pub struct CliArgs {
    /// Path to YAML configuration file
    #[arg(short = 'f', long = "file")]
    pub config_file: Option<PathBuf>,

    /// Bind address (host:port) for the HTTP stats endpoint (/metrics, /stats)
    #[arg(long = "metrics-addr")]
    pub metrics_addr: Option<String>,

    /// Periodic traffic-report log cadence, seconds (default 60)
    #[arg(long = "report-interval")]
    pub report_interval_secs: Option<u64>,

    /// Throughput-rate sampling cadence, seconds (default 1)
    #[arg(long = "sample-interval")]
    pub sample_interval_secs: Option<u64>,

    /// Per-direction copy buffer size, bytes (default 65536)
    #[arg(long = "buffer-bytes")]
    pub buffer_bytes: Option<usize>,

    /// Kernel socket buffer size (SO_SNDBUF/SO_RCVBUF) per socket, bytes
    /// (default: OS default)
    #[arg(long = "socket-buffer-bytes")]
    pub socket_buffer_bytes: Option<usize>,

    /// Use the Linux splice(2) zero-copy data path (Linux only)
    #[arg(long = "splice")]
    pub use_splice: bool,

    /// Forwarders in format local_host:local_port:remote_host:remote_port
    #[arg(trailing_var_arg = true)]
    pub forwarders: Vec<String>,
}

/// Parse a forwarder string "host:port:remote_host:remote_port"
pub fn parse_forwarder(s: &str) -> Result<ForwarderConfig, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 4 {
        return Err(format!(
            "Invalid forwarder format '{s}': expected local_host:local_port:remote_host:remote_port"
        ));
    }
    let local_port: u16 = parts[1]
        .parse()
        .map_err(|e| format!("Invalid local port '{}': {}", parts[1], e))?;
    let remote_port: u16 = parts[3]
        .parse()
        .map_err(|e| format!("Invalid remote port '{}': {}", parts[3], e))?;
    Ok(ForwarderConfig {
        local_host: parts[0].to_string(),
        local_port,
        remote_host: parts[2].to_string(),
        remote_port,
    })
}

/// Load forwarders from RFW_FORWARDER_N env vars
fn load_env_forwarders() -> Vec<ForwarderConfig> {
    let mut entries: Vec<_> = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("RFW_FORWARDER_")
                .and_then(|suffix| suffix.parse::<usize>().ok())
                .map(|index| (index, key, value))
        })
        .collect();

    entries.sort_by_key(|(index, _, _)| *index);

    let mut forwarders = Vec::with_capacity(entries.len());
    for (_, key, value) in entries {
        match parse_forwarder(&value) {
            Ok(forwarder) => forwarders.push(forwarder),
            Err(error) => {
                warn!(env_var = %key, error = %error, "Ignoring invalid env forwarder");
            }
        }
    }

    forwarders
}

/// Load forwarders from a YAML config file
pub(crate) fn load_yaml_forwarders(
    path: &std::path::Path,
) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    let content = std::fs::read_to_string(path)?;
    let cf: ConfigFile = serde_yaml::from_str(&content)?;
    Ok(cf.forwarders)
}

/// Load forwarders merging YAML file, CLI args, and env vars.
///
/// Precedence: env vars > CLI args > YAML file.
/// - If env vars are set, they completely replace all other sources.
/// - If CLI args are given, they replace the YAML config.
/// - Otherwise the YAML file is used.
pub fn load_forwarders(cli: &CliArgs) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    let configured_forwarders = if !cli.forwarders.is_empty() {
        // CLI args: parse and use instead of YAML
        cli.forwarders
            .iter()
            .map(|s| parse_forwarder(s).map_err(|e| anyhow::anyhow!("{}", e)))
            .collect::<Result<Vec<_>, _>>()?
    } else if let Some(path) = &cli.config_file {
        // YAML file
        load_yaml_forwarders(path)?
    } else {
        Vec::new()
    };

    // Env vars override everything
    let env_forwarders = load_env_forwarders();
    let forwarders = if env_forwarders.is_empty() {
        configured_forwarders
    } else {
        env_forwarders
    };

    validate_no_duplicates(&forwarders)?;
    Ok(forwarders)
}

/// Load runtime settings: YAML `settings:` block (if a config file is given),
/// with CLI flags overriding individual fields. Missing/unreadable file falls
/// back to defaults (the forwarder load path already surfaces file errors).
pub fn load_settings(cli: &CliArgs) -> Result<Settings, anyhow::Error> {
    let mut settings = match &cli.config_file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(content) => serde_yaml::from_str::<ConfigFile>(&content)?.settings,
            Err(_) => Settings::default(),
        },
        None => Settings::default(),
    };

    if let Some(addr) = &cli.metrics_addr {
        settings.metrics_addr = Some(addr.clone());
    }
    if let Some(v) = cli.report_interval_secs {
        settings.report_interval_secs = v;
    }
    if let Some(v) = cli.sample_interval_secs {
        settings.sample_interval_secs = v;
    }
    if let Some(v) = cli.buffer_bytes {
        settings.buffer_bytes = v;
    }
    if let Some(v) = cli.socket_buffer_bytes {
        settings.socket_buffer_bytes = Some(v);
    }
    if cli.use_splice {
        settings.use_splice = true;
    }

    Ok(settings)
}

/// Read only the `settings:` block from a YAML config file, returning defaults
/// if it is unreadable or unparseable. Used by hot-reload (CLI overrides are not
/// re-applied here — reloaded values come straight from the file).
pub fn load_settings_from_file(path: &std::path::Path) -> Settings {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|content| serde_yaml::from_str::<ConfigFile>(&content).ok())
        .map(|cf| cf.settings)
        .unwrap_or_default()
}

/// Check for duplicate local addresses among forwarders
fn validate_no_duplicates(fwd: &[ForwarderConfig]) -> Result<(), anyhow::Error> {
    let mut seen = std::collections::HashSet::new();
    for f in fwd {
        let addr = f.local_addr();
        if !seen.insert(addr.clone()) {
            anyhow::bail!(
                "Duplicate local address '{}'. Each forwarder needs a unique local endpoint.",
                addr
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct ForwarderEnvGuard {
        saved: Vec<(String, String)>,
    }

    impl ForwarderEnvGuard {
        fn set(vars: &[(&str, &str)]) -> Self {
            let saved = std::env::vars()
                .filter(|(key, _)| key.starts_with("RFW_FORWARDER_"))
                .collect::<Vec<_>>();

            clear_forwarder_env();
            for (key, value) in vars {
                unsafe {
                    std::env::set_var(key, value);
                }
            }

            Self { saved }
        }
    }

    impl Drop for ForwarderEnvGuard {
        fn drop(&mut self) {
            clear_forwarder_env();
            for (key, value) in self.saved.drain(..) {
                unsafe {
                    std::env::set_var(key, value);
                }
            }
        }
    }

    fn clear_forwarder_env() {
        let keys = std::env::vars()
            .filter(|(key, _)| key.starts_with("RFW_FORWARDER_"))
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        for key in keys {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn test_settings_defaults() {
        let yaml =
            "forwarders:\n  - {local_host: a, local_port: 1, remote_host: b, remote_port: 2}\n";
        let cf: ConfigFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cf.settings.report_interval_secs, 60);
        assert_eq!(cf.settings.sample_interval_secs, 1);
        assert_eq!(cf.settings.buffer_bytes, 65536);
        assert!(cf.settings.metrics_addr.is_none());
    }

    #[test]
    fn test_cli_overrides_settings() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::parse_from([
            "rfw",
            "--report-interval",
            "5",
            "--buffer-bytes",
            "1234",
            "127.0.0.1:1:b:2",
        ]);
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.report_interval_secs, 5);
        assert_eq!(s.buffer_bytes, 1234);
        assert_eq!(s.sample_interval_secs, 1); // untouched -> default
        assert!(s.metrics_addr.is_none());
    }

    #[test]
    fn test_parse_forwarder_ok() {
        let f = parse_forwarder("localhost:8080:172.16.0.5:80").unwrap();
        assert_eq!(f.local_host, "localhost");
        assert_eq!(f.local_port, 8080);
        assert_eq!(f.remote_host, "172.16.0.5");
        assert_eq!(f.remote_port, 80);
    }

    #[test]
    fn test_parse_forwarder_ipv6() {
        // IPv6 addresses contain colons - this won't parse correctly with simple split
        // For now this is expected to fail; IPv6 support requires smarter parsing
        let result = parse_forwarder(":::8080:::80");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_forwarder_invalid_parts() {
        assert!(parse_forwarder("a:b:c").is_err());
        assert!(parse_forwarder("a:b:c:d:e").is_err());
    }

    #[test]
    fn test_parse_forwarder_invalid_port() {
        assert!(parse_forwarder("localhost:bad:remote:80").is_err());
    }

    #[test]
    fn test_validate_duplicates() {
        let f = vec![
            ForwarderConfig {
                local_host: "0.0.0.0".into(),
                local_port: 8080,
                remote_host: "a.com".into(),
                remote_port: 80,
            },
            ForwarderConfig {
                local_host: "0.0.0.0".into(),
                local_port: 8080,
                remote_host: "b.com".into(),
                remote_port: 80,
            },
        ];
        assert!(validate_no_duplicates(&f).is_err());
    }

    #[test]
    fn test_label() {
        let f = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: 9090,
            remote_host: "example.com".into(),
            remote_port: 443,
        };
        assert_eq!(f.label(), "127.0.0.1:9090->example.com:443");
    }

    #[test]
    fn test_load_forwarders_env_overrides_cli_with_sparse_indices() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_3", "127.0.0.1:9003:three.example:80"),
            ("RFW_FORWARDER_1", "127.0.0.1:9001:one.example:80"),
        ]);

        let cli = CliArgs {
            config_file: None,
            metrics_addr: None,
            report_interval_secs: None,
            sample_interval_secs: None,
            buffer_bytes: None,
            socket_buffer_bytes: None,
            use_splice: false,
            forwarders: vec!["127.0.0.1:8000:cli.example:80".into()],
        };

        let forwarders = load_forwarders(&cli).expect("env forwarders should load");

        assert_eq!(forwarders.len(), 2);
        assert_eq!(forwarders[0].local_port, 9001);
        assert_eq!(forwarders[1].local_port, 9003);
    }

    #[test]
    fn test_load_forwarders_validates_duplicates_from_env() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_1", "0.0.0.0:8080:first.example:80"),
            ("RFW_FORWARDER_2", "0.0.0.0:8080:second.example:443"),
        ]);

        let cli = CliArgs {
            config_file: None,
            metrics_addr: None,
            report_interval_secs: None,
            sample_interval_secs: None,
            buffer_bytes: None,
            socket_buffer_bytes: None,
            use_splice: false,
            forwarders: Vec::new(),
        };

        let error = load_forwarders(&cli).expect_err("duplicate env forwarders must fail");
        assert!(error
            .to_string()
            .contains("Duplicate local address '0.0.0.0:8080'"));
    }
}
