use std::sync::Arc;

use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    model::*,
    service::{RequestContext, RoleServer},
};
use tracing::{info, warn};

use crate::{
    config::ServerConfig,
    rancher_auth::AuthContext,
    upstream::UpstreamMcpClient,
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
    cached_tools: Arc<Vec<Tool>>,
    upstream: UpstreamMcpClient,
}

impl ServerProxy {
    pub fn new(config: ServerConfig, upstream: UpstreamMcpClient, tools: Vec<Tool>) -> Self {
        Self {
            config,
            cached_tools: Arc::new(tools),
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
        let default_instructions = format!(
            "MCP gateway endpoint for \"{}\". \
             Provide R_token and R_url headers to authenticate.",
            self.config.name,
        );
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(
                self.config.instructions.as_deref().unwrap_or(&default_instructions),
            )
    }

    /// Returns this server's tool list — no auth required.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _cx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items((*self.cached_tools).clone()))
    }

    /// Enforces per-tool Rancher RBAC, then proxies the call to the upstream.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let parts = cx.extensions.get::<http::request::Parts>().ok_or_else(|| {
            McpError::invalid_request(
                "Authentication required. Please provide R_token and R_url headers.",
                None,
            )
        })?;

        self.authorize(request.name.as_ref(), parts)?;

        self.upstream.proxy_call(request).await
    }
}
