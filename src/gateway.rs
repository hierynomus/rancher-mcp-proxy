use std::sync::Arc;

use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, RequestParamsMeta, ServerCapabilities,
        ServerInfo, Tool,
    },
    service::{RequestContext, RoleServer},
};
use tracing::{info, warn};

use crate::{
    config::ServerConfig,
    rancher_auth::AuthContext,
    upstream::{NotificationRelay, UpstreamMcpClient},
};

/// MCP server proxy for a single upstream.
///
/// Mounted at `/<name>/mcp` by the axum router; the HTTP layer handles
/// routing between servers so there are no tool name collisions.
///
/// Each instance carries its own `ServerConfig` (rules + instructions),
/// so different AI agents pointed at different endpoints can get different
/// behaviours via `ServerInfo.instructions`.
#[derive(Clone)]
pub struct ServerProxy {
    config: ServerConfig,
    cached_tools: Arc<[Tool]>,
    upstream: UpstreamMcpClient,
}

impl ServerProxy {
    pub fn new(config: ServerConfig, upstream: UpstreamMcpClient, tools: Vec<Tool>) -> Self {
        Self {
            config,
            cached_tools: tools.into(),
            upstream,
        }
    }

    fn authorize(&self, tool_name: &str, parts: &http::request::Parts) -> Result<(), McpError> {
        let required_role =
            self.config.required_role_for(tool_name).ok_or_else(|| {
                warn!(tool = %tool_name, server = %self.config.name, "No matching role rule");
                McpError::invalid_request(
                    format!("Tool \"{tool_name}\" is not accessible: no role rule matches it"),
                    None,
                )
            })?;

        match parts.extensions.get::<AuthContext>() {
            None => {
                warn!(tool = %tool_name, server = %self.config.name, "Rejected: no auth context");
                Err(McpError::invalid_request(
                    "Authentication required. Please provide R_token and R_url headers.",
                    None,
                ))
            }
            Some(ctx) if !ctx.roles.iter().any(|r| r == required_role) => {
                warn!(
                    user = %ctx.display_name,
                    tool = %tool_name,
                    server = %self.config.name,
                    %required_role,
                    actual = ?ctx.roles,
                    "Rejected: missing required role"
                );
                Err(McpError::invalid_request(
                    format!(
                        "Forbidden: user \"{}\" needs role \"{}\" to call \"{}\"",
                        ctx.display_name, required_role, tool_name,
                    ),
                    None,
                ))
            }
            Some(ctx) => {
                info!(
                    user = %ctx.display_name,
                    tool = %tool_name,
                    server = %self.config.name,
                    %required_role,
                    "Authorized"
                );
                Ok(())
            }
        }
    }
}

impl ServerHandler for ServerProxy {
    fn get_info(&self) -> ServerInfo {
        let fallback;
        let instructions = match self.config.instructions.as_deref() {
            Some(s) => s,
            None => {
                fallback = format!(
                    "MCP gateway endpoint for \"{}\". \
                     Provide R_token and R_url headers to authenticate.",
                    self.config.name,
                );
                &fallback
            }
        };
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(instructions)
    }

    /// Returns this server's tool list — no auth required.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _cx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(self.cached_tools.to_vec()))
    }

    /// Enforces per-tool Rancher RBAC, then proxies the call to the upstream.
    async fn call_tool(
        &self,
        mut request: CallToolRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let parts = cx.extensions.get::<http::request::Parts>().ok_or_else(|| {
            McpError::invalid_request(
                "Authentication required. Please provide R_token and R_url headers.",
                None,
            )
        })?;

        self.authorize(request.name.as_ref(), parts)?;
        let tool_name = request.name.clone();

        // rmcp strips `_meta` out of `params` while decoding the JSON-RPC
        // envelope (it lives on `RequestContext::meta`, not on `request`
        // itself), so the caller's progress token has to be copied back onto
        // `request` before we forward it upstream.
        let relay: Option<&dyn NotificationRelay> = match cx.meta.get_progress_token() {
            Some(token) => {
                request.set_progress_token(token);
                Some(&cx.peer as &dyn NotificationRelay)
            }
            None => None,
        };

        // `cx.ct` is cancelled when the client sends a `notifications/cancelled`
        // for this request. Racing it against the upstream call drops the
        // in-flight HTTP request on cancellation instead of running it to
        // completion against the upstream for a caller that already gave up.
        tokio::select! {
            result = self.upstream.proxy_call(request, relay) => result,
            () = cx.ct.cancelled() => {
                warn!(tool = %tool_name, server = %self.config.name, "Call cancelled by client");
                Err(McpError::internal_error("Call cancelled by client", None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RoleRule;
    use crate::rancher_auth::AuthContext;
    use crate::upstream::UpstreamMcpClient;

    fn make_proxy(rules: Vec<RoleRule>, instructions: Option<&str>) -> ServerProxy {
        let config = ServerConfig {
            name: "test-server".into(),
            url: "http://unused".into(),
            instructions: instructions.map(String::from),
            rules,
            upstream_timeout_secs: None,
        };
        ServerProxy::new(config, UpstreamMcpClient::new("http://unused", false, 30), vec![])
    }

    fn parts_with_roles(roles: &[&str]) -> http::request::Parts {
        let (mut parts, _) = http::Request::new(()).into_parts();
        parts.extensions.insert(AuthContext {
            display_name: "Alice".into(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
        });
        parts
    }

    fn parts_no_auth() -> http::request::Parts {
        let (parts, _) = http::Request::new(()).into_parts();
        parts
    }

    #[test]
    fn authorize_no_auth_context_requires_authentication() {
        let proxy = make_proxy(
            vec![RoleRule { tools: vec!["*".into()], role: "admin".into() }],
            None,
        );
        let err = proxy.authorize("any_tool", &parts_no_auth()).unwrap_err();
        assert!(err.message.contains("Authentication required"));
    }

    #[test]
    fn authorize_no_matching_rule_denies_call() {
        let proxy = make_proxy(
            vec![RoleRule { tools: vec!["get_*".into()], role: "viewer".into() }],
            None,
        );
        // "delete_budget" does not match "get_*"
        let err = proxy.authorize("delete_budget", &parts_with_roles(&["viewer", "admin"])).unwrap_err();
        assert!(err.message.contains("no role rule matches"));
    }

    #[test]
    fn authorize_empty_rules_denies_everything() {
        let proxy = make_proxy(vec![], None);
        let err = proxy.authorize("any_tool", &parts_with_roles(&["super-admin"])).unwrap_err();
        assert!(err.message.contains("no role rule matches"));
    }

    #[test]
    fn authorize_wrong_role_returns_forbidden() {
        let proxy = make_proxy(
            vec![RoleRule { tools: vec!["*".into()], role: "admin".into() }],
            None,
        );
        let err = proxy.authorize("delete_budget", &parts_with_roles(&["viewer"])).unwrap_err();
        assert!(err.message.contains("Forbidden"));
        assert!(err.message.contains("admin"));
        assert!(err.message.contains("Alice"));
        assert!(err.message.contains("delete_budget"));
    }

    #[test]
    fn authorize_correct_role_allows_call() {
        let proxy = make_proxy(
            vec![RoleRule { tools: vec!["*".into()], role: "admin".into() }],
            None,
        );
        assert!(proxy.authorize("delete_budget", &parts_with_roles(&["admin"])).is_ok());
    }

    #[test]
    fn authorize_one_of_many_roles_matches() {
        let proxy = make_proxy(
            vec![RoleRule { tools: vec!["*".into()], role: "cost-admin".into() }],
            None,
        );
        assert!(proxy
            .authorize("any_tool", &parts_with_roles(&["viewer", "cost-viewer", "cost-admin"]))
            .is_ok());
    }

    #[test]
    fn authorize_uses_first_matching_rule() {
        // "get_*" maps to viewer; catch-all maps to admin.
        // A user with only "viewer" should be allowed for get_ tools.
        let proxy = make_proxy(
            vec![
                RoleRule { tools: vec!["get_*".into()], role: "viewer".into() },
                RoleRule { tools: vec!["*".into()], role: "admin".into() },
            ],
            None,
        );
        assert!(proxy.authorize("get_allocation", &parts_with_roles(&["viewer"])).is_ok());
        assert!(proxy.authorize("delete_budget", &parts_with_roles(&["viewer"])).is_err());
    }

    #[test]
    fn get_info_uses_custom_instructions() {
        let proxy = make_proxy(vec![], Some("You are a cost analysis assistant."));
        let info = proxy.get_info();
        assert_eq!(
            info.instructions.as_deref(),
            Some("You are a cost analysis assistant.")
        );
    }

    #[test]
    fn get_info_default_instructions_mention_server_name_and_headers() {
        let proxy = make_proxy(vec![], None);
        let info = proxy.get_info();
        let instructions = info.instructions.as_deref().unwrap_or("");
        assert!(instructions.contains("test-server"), "should contain server name");
        assert!(instructions.contains("R_token"), "should mention auth header");
    }
}
