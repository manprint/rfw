use std::path::PathBuf;

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
pub(crate) fn load_yaml_forwarders(path: &std::path::Path) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
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
            .map(|s| {
                parse_forwarder(s)
                    .map_err(|e| anyhow::anyhow!("{}", e))
            })
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
            forwarders: Vec::new(),
        };

        let error = load_forwarders(&cli).expect_err("duplicate env forwarders must fail");
        assert!(error
            .to_string()
            .contains("Duplicate local address '0.0.0.0:8080'"));
    }
}
