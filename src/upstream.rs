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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, body::Body, http::{header, StatusCode}, response::Response, routing::post};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// Spin up an in-process axum server on a random port and return its base URL.
    async fn start_mock(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Bind then immediately drop a listener so the port is closed; any
    /// connection attempt will get an immediate "connection refused".
    async fn closed_port_url() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    fn client(url: &str) -> UpstreamMcpClient {
        UpstreamMcpClient::new(url.to_string(), false)
    }

    // -----------------------------------------------------------------------
    // discover_tools
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn discover_tools_empty_list() {
        let base = start_mock(Router::new().route("/", post(|| async {
            Json(json!({"jsonrpc":"2.0","id":1,"result":{"tools":[]}}))
        }))).await;
        assert!(client(&base).discover_tools().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn discover_tools_returns_named_tools() {
        let base = start_mock(Router::new().route("/", post(|| async {
            Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {
                    "tools": [
                        {"name":"get_cost","inputSchema":{"type":"object","properties":{}}},
                        {"name":"list_namespaces","inputSchema":{"type":"object","properties":{}}}
                    ]
                }
            }))
        }))).await;
        let tools = client(&base).discover_tools().await.unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name.as_ref(), "get_cost");
        assert_eq!(tools[1].name.as_ref(), "list_namespaces");
    }

    // -----------------------------------------------------------------------
    // proxy_call
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn proxy_call_returns_text_content() {
        let base = start_mock(Router::new().route("/", post(|| async {
            Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {"content": [{"type": "text", "text": "$42.00"}]}
            }))
        }))).await;
        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let result = client(&base).proxy_call(req).await.unwrap();
        assert_eq!(result.content.len(), 1);
    }

    // -----------------------------------------------------------------------
    // rpc error paths
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sse_content_type_returns_error() {
        let base = start_mock(Router::new().route("/", post(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from("data: {}\n\n"))
                .unwrap()
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("SSE"), "got: {err}");
    }

    #[tokio::test]
    async fn http_error_status_returns_error() {
        let base = start_mock(Router::new().route("/", post(|| async {
            (StatusCode::INTERNAL_SERVER_ERROR, "server exploded")
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500") || msg.contains("upstream returned"), "got: {msg}");
    }

    #[tokio::test]
    async fn jsonrpc_error_propagated() {
        let base = start_mock(Router::new().route("/", post(|| async {
            Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "error": {"code": -32600, "message": "Invalid Request"}
            }))
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("Invalid Request"), "got: {err}");
    }

    #[tokio::test]
    async fn response_with_neither_result_nor_error_fails() {
        let base = start_mock(Router::new().route("/", post(|| async {
            // Valid JSON-RPC envelope but neither "result" nor "error" field.
            Json(json!({"jsonrpc": "2.0", "id": 1}))
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("neither result nor error"), "got: {err}");
    }

    #[tokio::test]
    async fn unreachable_server_returns_error() {
        let url = closed_port_url().await;
        let err = client(&url).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("unreachable"), "got: {err}");
    }

    // -----------------------------------------------------------------------
    // Request ID counter
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn request_ids_are_unique_and_strictly_increasing() {
        let captured: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));
        let captured_clone = captured.clone();

        let base = start_mock(Router::new().route("/", post(
            move |Json(body): Json<serde_json::Value>| {
                let captured = captured_clone.clone();
                async move {
                    if let Some(id) = body["id"].as_u64() {
                        captured.lock().unwrap().push(id);
                    }
                    Json(json!({"jsonrpc":"2.0","id":body["id"],"result":{"tools":[]}}))
                }
            }
        ))).await;

        let c = client(&base);
        c.discover_tools().await.unwrap();
        c.discover_tools().await.unwrap();
        c.discover_tools().await.unwrap();

        let ids = captured.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids[0] < ids[1], "ids not increasing: {ids:?}");
        assert!(ids[1] < ids[2], "ids not increasing: {ids:?}");
    }
}
