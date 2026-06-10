use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};

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
    let mut res = Vec::new();
    for i in 1.. {
        let key = format!("RFW_FORWARDER_{}", i);
        match std::env::var(&key) {
            Ok(val) => match parse_forwarder(&val) {
                Ok(f) => res.push(f),
                Err(e) => eprintln!("Warning: env {}: {}", key, e),
            },
            Err(_) => break,
        }
    }
    res
}

/// Load forwarders from a YAML config file
pub(crate) fn load_yaml_forwarders(path: &PathBuf) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
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
    let forwarders = if !cli.forwarders.is_empty() {
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
    if !env_forwarders.is_empty() {
        return Ok(env_forwarders);
    }

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
}
