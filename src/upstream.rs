use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rmcp::{ErrorData as McpError, model::{CallToolRequestParams, CallToolResult, ListToolsResult, Tool}};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Minimal JSON-RPC envelope types — only what we need to forward MCP calls.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RpcRequest<P: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: P,
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct UpstreamMcpClient {
    http: reqwest::Client,
    url: String,
    counter: Arc<AtomicU64>,
}

impl UpstreamMcpClient {
    pub fn new(url: String, tls_verify: bool) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(!tls_verify)
            .build()
            .expect("failed to build upstream http client");
        Self { http, url, counter: Arc::new(AtomicU64::new(1)) }
    }

    fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn rpc<P, R>(&self, method: &'static str, params: P) -> Result<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let body = RpcRequest { jsonrpc: "2.0", id: self.next_id(), method, params };

        let resp = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("upstream MCP unreachable at {}", self.url))?;

        // Streaming (SSE) responses from upstream are not yet supported.
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if content_type.starts_with("text/event-stream") {
            bail!("upstream returned an SSE stream; streaming tool results are not yet supported by this proxy");
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream returned {status}: {text}");
        }

        let rpc_resp: RpcResponse<R> =
            resp.json().await.context("failed to parse upstream MCP response")?;

        match (rpc_resp.result, rpc_resp.error) {
            (Some(r), _) => Ok(r),
            (_, Some(e)) => bail!("upstream MCP error {}: {}", e.code, e.message),
            _ => bail!("upstream MCP returned neither result nor error"),
        }
    }

    /// Fetch the upstream tool list. Called once at startup; result is cached.
    pub async fn discover_tools(&self) -> Result<Vec<Tool>> {
        let result: ListToolsResult =
            self.rpc("tools/list", serde_json::json!({})).await?;
        Ok(result.tools)
    }

    /// Forward a call_tool request to the upstream MCP verbatim.
    pub async fn proxy_call(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, McpError> {
        self.rpc("tools/call", request)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))
    }
}
