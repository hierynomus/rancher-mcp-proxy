use anyhow::{Context, Result};
use axum::{Router, middleware, routing::get};
use tracing_subscriber::prelude::*;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod config;
mod gateway;
mod rancher_auth;
mod upstream;

use config::GatewayConfig;
use gateway::ServerProxy;
use rancher_auth::{RancherAuthState, rancher_auth_middleware};
use upstream::UpstreamMcpClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);
    let tls_verify = std::env::var("RANCHER_TLS_VERIFY")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    info!(
        "Rancher TLS verification: {}",
        if tls_verify { "enabled" } else { "DISABLED" }
    );

    // Config file takes precedence; env vars are the single-server fallback.
    let config_file = std::env::var("ROLE_CONFIG_FILE")
        .unwrap_or_else(|_| "/etc/rancher-mcp-proxy/config.yaml".to_string());

    let gateway_config = if std::path::Path::new(&config_file).exists() {
        info!(%config_file, "Loading gateway config from file");
        GatewayConfig::from_file(&config_file)?
    } else {
        let url = std::env::var("UPSTREAM_MCP_URL")
            .context("UPSTREAM_MCP_URL is required when no config file is present")?;
        let role = std::env::var("REQUIRED_ROLE").unwrap_or_else(|_| "mcp-user".into());
        info!(%url, %role, "No config file found; using single-server mode from env vars");
        GatewayConfig::single_server(url, role)
    };

    info!(server_count = gateway_config.servers.len(), "Discovering tools from upstream servers...");

    // Discover tools from every configured server in parallel.
    let mut handles = Vec::new();
    for server in gateway_config.servers {
        let client = UpstreamMcpClient::new(server.url.clone(), tls_verify);
        handles.push(tokio::spawn(async move {
            let tools = client
                .discover_tools()
                .await
                .with_context(|| {
                    format!("failed to discover tools from \"{}\" at {}", server.name, server.url)
                })?;
            info!(server = %server.name, tool_count = tools.len(), "Tools discovered");
            Ok::<_, anyhow::Error>((server, client, tools))
        }));
    }

    let mut server_tools = Vec::new();
    for handle in handles {
        server_tools.push(handle.await??);
    }

    // Build the axum app: one StreamableHttpService per server, each mounted
    // at /<name>/mcp.  The HTTP layer provides isolation — no tool name
    // collisions are possible, and each endpoint carries its own personality
    // via ServerInfo.instructions.
    let ct = CancellationToken::new();
    let auth_state = RancherAuthState::new(tls_verify);

    let mut app = Router::new().route("/health", get(health));

    let bind_addr = format!("0.0.0.0:{port}");
    info!("Mounting MCP endpoints:");

    for (server, client, tools) in server_tools {
        let mount_path = format!("/{}/mcp", server.name);
        let proxy = Arc::new(ServerProxy::new(server, client, tools));

        let svc = StreamableHttpService::new(
            move || Ok((*proxy).clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(ct.child_token())
                .disable_allowed_hosts(),
        );

        let mcp_router = Router::new()
            .fallback_service(svc)
            .layer(middleware::from_fn_with_state(
                auth_state.clone(),
                rancher_auth_middleware,
            ));

        info!("  {bind_addr}{mount_path}");
        app = app.nest(&mount_path, mcp_router);
    }

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("rancher-mcp-gateway listening on {bind_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
            info!("Shutting down gracefully...");
            ct.cancel();
        })
        .await?;

    Ok(())
}

async fn health() -> &'static str {
    "OK"
}
