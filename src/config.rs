use anyhow::{Context, Result};
use serde::Deserialize;

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

impl GatewayConfig {
    /// Load from a YAML file at `path`.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read gateway config from {}", path.display()))?;
        let config: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse gateway config YAML from {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for server in &self.servers {
            if server.name.is_empty()
                || !server.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                anyhow::bail!(
                    "server name {:?} is invalid; use only ASCII letters, digits, `-`, and `_`",
                    server.name
                );
            }
            if !seen.insert(&server.name) {
                anyhow::bail!("duplicate server name {:?}", server.name);
            }
        }
        Ok(())
    }

    /// Single-server catch-all — used when no config file is present and
    /// `UPSTREAM_MCP_URL` / `REQUIRED_ROLE` env vars are set.
    pub fn single_server(url: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            servers: vec![ServerConfig {
                name: "upstream".to_string(),
                url: url.into(),
                instructions: None,
                rules: vec![RoleRule {
                    tools: vec!["*".to_string()],
                    role: role.into(),
                }],
            }],
        }
    }
}

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
        let cfg = GatewayConfig::single_server("http://x", "mcp-user");
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].required_role_for("any_tool"), Some("mcp-user"));
    }

    // -----------------------------------------------------------------------
    // Glob edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn glob_empty_pattern_matches_empty_name() {
        assert!(glob_match("", ""));
    }

    #[test]
    fn glob_empty_pattern_rejects_nonempty_name() {
        assert!(!glob_match("", "tool"));
    }

    #[test]
    fn glob_star_matches_empty_string() {
        assert!(glob_match("*", ""));
        assert!(glob_match("prefix_*", "prefix_"));
    }

    #[test]
    fn glob_consecutive_stars_act_like_one_star() {
        assert!(glob_match("**", "anything"));
        assert!(glob_match("**", ""));
        assert!(glob_match("get_**", "get_allocation"));
    }

    #[test]
    fn glob_middle_wildcard() {
        assert!(glob_match("get_*_cost", "get_namespace_cost"));
        assert!(glob_match("get_*_cost", "get_a_cost"));
        // The literal "_" between "*" and "cost" must appear in the name.
        assert!(!glob_match("get_*_cost", "get_cost"));
        assert!(!glob_match("get_*_cost", "set_namespace_cost"));
    }

    #[test]
    fn glob_multiple_question_marks() {
        assert!(glob_match("get_??", "get_ab"));
        assert!(!glob_match("get_??", "get_a"));
        assert!(!glob_match("get_??", "get_abc"));
    }

    #[test]
    fn glob_question_mark_at_start() {
        assert!(glob_match("?et_tool", "get_tool"));
        assert!(glob_match("?et_tool", "set_tool"));
        assert!(!glob_match("?et_tool", "tool"));
    }

    #[test]
    fn glob_is_case_sensitive() {
        assert!(!glob_match("Get_*", "get_tool"));
        assert!(glob_match("Get_*", "Get_tool"));
    }

    // -----------------------------------------------------------------------
    // Config YAML parsing
    // -----------------------------------------------------------------------

    #[test]
    fn yaml_multiple_servers_parsed_in_order() {
        let yaml = r#"
servers:
  - name: alpha
    url: http://alpha/mcp
    rules:
      - tools: ["*"]
        role: alpha-user
  - name: beta
    url: http://beta/mcp
    rules:
      - tools: ["*"]
        role: beta-user
"#;
        let cfg: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.servers.len(), 2);
        assert_eq!(cfg.servers[0].name, "alpha");
        assert_eq!(cfg.servers[1].name, "beta");
        assert_eq!(cfg.servers[0].required_role_for("any"), Some("alpha-user"));
        assert_eq!(cfg.servers[1].required_role_for("any"), Some("beta-user"));
    }

    #[test]
    fn yaml_instructions_field_parsed() {
        let yaml = r#"
servers:
  - name: cost
    url: http://cost/mcp
    instructions: "You are a cost assistant."
    rules:
      - tools: ["*"]
        role: viewer
"#;
        let cfg: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg.servers[0].instructions.as_deref(),
            Some("You are a cost assistant.")
        );
    }

    #[test]
    fn yaml_absent_rules_defaults_to_empty() {
        let yaml = r#"
servers:
  - name: noauth
    url: http://noauth/mcp
"#;
        let cfg: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.servers[0].rules.len(), 0);
        assert_eq!(cfg.servers[0].required_role_for("any_tool"), None);
    }

    #[test]
    fn yaml_absent_instructions_defaults_to_none() {
        let yaml = "servers:\n  - name: x\n    url: http://x\n";
        let cfg: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.servers[0].instructions.is_none());
    }

    // -----------------------------------------------------------------------
    // from_file
    // -----------------------------------------------------------------------

    #[test]
    fn validate_rejects_name_with_slash() {
        let cfg = GatewayConfig {
            servers: vec![ServerConfig {
                name: "bad/name".into(),
                url: "http://x".into(),
                instructions: None,
                rules: vec![],
            }],
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("invalid"), "got: {err}");
    }

    #[test]
    fn validate_rejects_name_with_space() {
        let cfg = GatewayConfig {
            servers: vec![ServerConfig {
                name: "bad name".into(),
                url: "http://x".into(),
                instructions: None,
                rules: vec![],
            }],
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_server_names() {
        let cfg = GatewayConfig {
            servers: vec![
                ServerConfig { name: "alpha".into(), url: "http://a".into(), instructions: None, rules: vec![] },
                ServerConfig { name: "alpha".into(), url: "http://b".into(), instructions: None, rules: vec![] },
            ],
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate"), "got: {err}");
    }

    #[test]
    fn validate_accepts_valid_names() {
        let cfg = GatewayConfig {
            servers: vec![
                ServerConfig { name: "opencost".into(), url: "http://a".into(), instructions: None, rules: vec![] },
                ServerConfig { name: "platform-ops".into(), url: "http://b".into(), instructions: None, rules: vec![] },
                ServerConfig { name: "server_2".into(), url: "http://c".into(), instructions: None, rules: vec![] },
            ],
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn from_file_not_found_returns_error() {
        let result = GatewayConfig::from_file("/nonexistent/path/config.yaml");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("failed to read"));
    }

    #[test]
    fn from_file_invalid_yaml_returns_error() {
        let mut file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        std::io::Write::write_all(&mut file, b"this: [is: invalid yaml{{{").unwrap();
        let result = GatewayConfig::from_file(file.path());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("failed to parse"));
    }

    #[test]
    fn from_file_valid_config_round_trips() {
        let mut file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        std::io::Write::write_all(
            &mut file,
            b"servers:\n  - name: test\n    url: http://test\n    rules:\n      - tools: [\"*\"]\n        role: tester\n",
        ).unwrap();
        let cfg = GatewayConfig::from_file(file.path()).unwrap();
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].name, "test");
        assert_eq!(cfg.servers[0].required_role_for("anything"), Some("tester"));
    }
}
