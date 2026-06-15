use anyhow::{Context, Result};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single access rule: glob patterns on tool names → required Rancher role.
#[derive(Debug, Clone, Deserialize)]
pub struct RoleRule {
    /// Tool name glob patterns.  Supports `*` (any sequence) and `?` (one char).
    pub tools: Vec<String>,
    /// Rancher GlobalRole the caller must hold to match this rule.
    pub role: String,
}

/// Configuration for one upstream MCP server.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Used as the URL path segment: `/<name>/mcp`.
    /// Use URL-safe characters (letters, digits, `-`, `_`).
    pub name: String,
    /// Full URL of the upstream MCP endpoint.
    pub url: String,
    /// Optional system prompt / personality shown to the AI agent in
    /// `ServerInfo.instructions`.  Different agents can be pointed at
    /// different `/<name>/mcp` endpoints to get different behaviours.
    pub instructions: Option<String>,
    /// Access rules evaluated in order; first match wins.
    /// If no rule matches a tool call, the call is denied.
    #[serde(default)]
    pub rules: Vec<RoleRule>,
}

impl ServerConfig {
    /// Return the required Rancher GlobalRole for `tool_name`, or `None` if
    /// no rule matches (call should be denied).
    pub fn required_role_for(&self, tool_name: &str) -> Option<&str> {
        self.rules
            .iter()
            .find(|rule| rule.tools.iter().any(|pat| glob_match(pat, tool_name)))
            .map(|rule| rule.role.as_str())
    }
}

/// Top-level gateway configuration.
///
/// Example `config.yaml`:
/// ```yaml
/// servers:
///   - name: opencost
///     url: http://opencost.opencost.svc:9003/mcp
///     rules:
///       - tools: ["get_*", "list_*"]
///         role: cost-viewer
///       - tools: ["*"]
///         role: cost-admin
///   - name: another-mcp
///     url: http://another.svc:8080/mcp
///     rules:
///       - tools: ["*"]
///         role: mcp-user
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    pub servers: Vec<ServerConfig>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl GatewayConfig {
    /// Load from a TOML file at `path`.
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read gateway config from {path}"))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse gateway config YAML from {path}"))
    }

    /// Single-server catch-all — used when no config file is present and
    /// `UPSTREAM_MCP_URL` / `REQUIRED_ROLE` env vars are set.
    pub fn single_server(url: String, role: String) -> Self {
        Self {
            servers: vec![ServerConfig {
                name: "upstream".to_string(),
                url,
                instructions: None,
                rules: vec![RoleRule {
                    tools: vec!["*".to_string()],
                    role,
                }],
            }],
        }
    }
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

/// Match `name` against `pattern` using `*` (any sequence) and `?` (one char).
pub fn glob_match(pattern: &str, name: &str) -> bool {
    match_bytes(pattern.as_bytes(), name.as_bytes())
}

fn match_bytes(p: &[u8], n: &[u8]) -> bool {
    match (p, n) {
        ([], []) => true,
        ([b'*', rp @ ..], _) => match_bytes(rp, n) || (!n.is_empty() && match_bytes(p, &n[1..])),
        ([b'?', rp @ ..], [_, rn @ ..]) => match_bytes(rp, rn),
        ([pc, rp @ ..], [nc, rn @ ..]) if pc == nc => match_bytes(rp, rn),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(glob_match("get_allocation", "get_allocation"));
        assert!(!glob_match("get_allocation", "get_assets"));
    }

    #[test]
    fn star_prefix() {
        assert!(glob_match("get_*", "get_allocation"));
        assert!(glob_match("get_*", "get_"));
        assert!(!glob_match("get_*", "set_budget"));
    }

    #[test]
    fn star_suffix() {
        assert!(glob_match("*_allocation", "get_allocation"));
        assert!(glob_match("*_allocation", "set_allocation"));
        assert!(!glob_match("*_allocation", "get_assets"));
    }

    #[test]
    fn star_only() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn question_mark() {
        assert!(glob_match("get_?", "get_a"));
        assert!(!glob_match("get_?", "get_al"));
        assert!(!glob_match("get_?", "get_"));
    }

    #[test]
    fn rule_first_match_wins() {
        let server = ServerConfig {
            name: "test".into(),
            url: "http://x".into(),
            instructions: None,
            rules: vec![
                RoleRule { tools: vec!["get_*".into(), "list_*".into()], role: "viewer".into() },
                RoleRule { tools: vec!["*".into()], role: "admin".into() },
            ],
        };
        assert_eq!(server.required_role_for("get_allocation"), Some("viewer"));
        assert_eq!(server.required_role_for("list_assets"), Some("viewer"));
        assert_eq!(server.required_role_for("delete_budget"), Some("admin"));
    }

    #[test]
    fn no_matching_rule_returns_none() {
        let server = ServerConfig {
            name: "test".into(),
            url: "http://x".into(),
            instructions: None,
            rules: vec![RoleRule { tools: vec!["get_*".into()], role: "viewer".into() }],
        };
        assert_eq!(server.required_role_for("set_budget"), None);
    }

    #[test]
    fn yaml_round_trip() {
        let yaml = r#"
servers:
  - name: opencost
    url: http://opencost:9003/mcp
    rules:
      - tools: ["get_*"]
        role: viewer
      - tools: ["*"]
        role: admin
"#;
        let cfg: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].rules.len(), 2);
        assert_eq!(cfg.servers[0].required_role_for("get_foo"), Some("viewer"));
        assert_eq!(cfg.servers[0].required_role_for("delete_foo"), Some("admin"));
    }

    #[test]
    fn single_server_helper() {
        let cfg = GatewayConfig::single_server("http://x".into(), "mcp-user".into());
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].required_role_for("any_tool"), Some("mcp-user"));
    }
}
