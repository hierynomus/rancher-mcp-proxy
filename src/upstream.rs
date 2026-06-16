use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use rmcp::{
    ErrorData as McpError, Peer, RoleServer,
    model::{
        CallToolRequestParams, CallToolResult, ListToolsResult, Notification,
        ProgressNotificationParam, ServerNotification, Tool,
    },
};
use serde::{Deserialize, Serialize};


#[derive(Serialize)]
struct RpcRequest<P: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: P,
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    #[serde(default)]
    id: Option<serde_json::Value>,
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

/// A JSON-RPC notification, as distinguished from a response by the presence
/// of a `method` field (responses never have one).
#[derive(Deserialize)]
struct RpcNotification {
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

type RelayFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Receives progress notifications relayed from an upstream SSE stream while
/// a `tools/call` is in flight. A trait (rather than a direct `Peer<RoleServer>`
/// parameter) so tests can substitute a recording fake — `Peer::new` is
/// private to rmcp and can't be constructed outside a live session.
pub trait ProgressRelay: Send + Sync {
    fn relay(&self, progress: ProgressNotificationParam) -> RelayFuture<'_>;
}

impl ProgressRelay for Peer<RoleServer> {
    fn relay(&self, progress: ProgressNotificationParam) -> RelayFuture<'_> {
        Box::pin(async move {
            let notification = ServerNotification::ProgressNotification(Notification::new(progress));
            // Best-effort: if the downstream client disconnected mid-call, the
            // final result still gets returned; don't fail the call over it.
            let _ = self.send_notification(notification).await;
        })
    }
}


#[derive(Clone)]
pub struct UpstreamMcpClient {
    http: reqwest::Client,
    url: String,
    counter: Arc<AtomicU64>,
}

impl UpstreamMcpClient {
    pub fn new(url: impl Into<String>, tls_verify: bool, timeout_secs: u64) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .danger_accept_invalid_certs(!tls_verify)
            .build()
            .expect("failed to build upstream http client");
        Self { http, url: url.into(), counter: Arc::new(AtomicU64::new(1)) }
    }

    fn next_id(&self) -> u64 {
        // Relaxed is correct: we only need uniqueness, not synchronisation with
        // any other shared state.
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn rpc<P, R>(
        &self,
        method: &'static str,
        params: P,
        relay: Option<&dyn ProgressRelay>,
    ) -> Result<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let id = self.next_id();
        let body = RpcRequest { jsonrpc: "2.0", id, method, params };

        let resp = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("upstream MCP unreachable at {}", self.url))?;

        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("text/event-stream"));

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream returned {status}: {text}");
        }

        if is_event_stream {
            return read_sse_response(resp, id, relay).await;
        }

        let body_text = resp.text().await.context("failed to read upstream MCP response")?;
        let rpc_resp: RpcResponse<R> = serde_json::from_str(&body_text)
            .context("failed to parse upstream MCP response")?;

        match (rpc_resp.result, rpc_resp.error) {
            (Some(r), _) => Ok(r),
            (_, Some(e)) => bail!("upstream MCP error {}: {}", e.code, e.message),
            _ => bail!("upstream MCP returned neither result nor error"),
        }
    }

    /// Fetch the upstream tool list. Called once at startup; result is cached.
    pub async fn discover_tools(&self) -> Result<Vec<Tool>> {
        let result: ListToolsResult =
            self.rpc("tools/list", serde_json::json!({}), None).await?;
        Ok(result.tools)
    }

    /// Forward a call_tool request to the upstream MCP verbatim. If `relay`
    /// is given, any `notifications/progress` events the upstream sends
    /// before its final response are relayed to it as they arrive.
    pub async fn proxy_call(
        &self,
        request: CallToolRequestParams,
        relay: Option<&dyn ProgressRelay>,
    ) -> Result<CallToolResult, McpError> {
        self.rpc("tools/call", request, relay)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))
    }
}

/// Read an SSE response body incrementally, relaying any `notifications/progress`
/// events as they arrive and returning as soon as the JSON-RPC response matching
/// `expected_id` is seen — without waiting for the upstream to close the connection.
async fn read_sse_response<R: for<'de> Deserialize<'de>>(
    resp: reqwest::Response,
    expected_id: u64,
    relay: Option<&dyn ProgressRelay>,
) -> Result<R> {
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut current_event: Vec<String> = Vec::new();

    loop {
        while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = buf.drain(..=newline_pos).collect();
            let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]);
            let line = line.trim_end_matches('\r');

            if line.is_empty() {
                if !current_event.is_empty() {
                    let data = current_event.join("\n");
                    current_event.clear();
                    if let Some(outcome) = handle_sse_event(&data, expected_id, relay).await {
                        return outcome;
                    }
                }
                continue;
            }
            if let Some(data) = line.strip_prefix("data:") {
                current_event.push(data.strip_prefix(' ').unwrap_or(data).to_string());
            }
        }

        match stream.next().await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            Some(Err(e)) => return Err(e).context("failed to read upstream SSE stream"),
            None => break,
        }
    }

    if !current_event.is_empty() {
        let data = current_event.join("\n");
        if let Some(outcome) = handle_sse_event(&data, expected_id, relay).await {
            return outcome;
        }
    }

    bail!("SSE stream ended without a JSON-RPC response for request id {expected_id}")
}

/// Inspect one SSE `data:` payload. Relays it if it's a matching progress
/// notification and keeps reading (`None`); returns `Some` with the final
/// result/error once the JSON-RPC response matching `expected_id` is found.
async fn handle_sse_event<R: for<'de> Deserialize<'de>>(
    data: &str,
    expected_id: u64,
    relay: Option<&dyn ProgressRelay>,
) -> Option<Result<R>> {
    if let Ok(notification) = serde_json::from_str::<RpcNotification>(data) {
        if notification.method == "notifications/progress" {
            if let (Some(relay), Ok(params)) = (
                relay,
                serde_json::from_value::<ProgressNotificationParam>(notification.params),
            ) {
                relay.relay(params).await;
            }
        }
        return None;
    }

    let resp = serde_json::from_str::<RpcResponse<R>>(data).ok()?;
    let is_match = resp.id.as_ref().is_some_and(|id| id.as_u64() == Some(expected_id));
    if !is_match {
        return None;
    }
    Some(match (resp.result, resp.error) {
        (Some(r), _) => Ok(r),
        (_, Some(e)) => Err(anyhow!("upstream MCP error {}: {}", e.code, e.message)),
        _ => Err(anyhow!("upstream MCP returned neither result nor error")),
    })
}


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
        UpstreamMcpClient::new(url, false, 30)
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
        let result = client(&base).proxy_call(req, None).await.unwrap();
        assert_eq!(result.content.len(), 1);
    }

    // -----------------------------------------------------------------------
    // rpc error paths
    // -----------------------------------------------------------------------

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
    // SSE responses
    // -----------------------------------------------------------------------

    fn sse_body(text: &str) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(text.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn sse_response_is_parsed() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n")
        }))).await;
        assert!(client(&base).discover_tools().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sse_skips_notifications_before_matching_response() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n",
            )
        }))).await;
        assert!(client(&base).discover_tools().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sse_jsonrpc_error_is_propagated() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32600,\"message\":\"Invalid Request\"}}\n\n")
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("Invalid Request"), "got: {err}");
    }

    #[tokio::test]
    async fn sse_stream_without_matching_response_errors() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body("data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\n")
        }))).await;
        let err = client(&base).discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("without a JSON-RPC response"), "got: {err}");
    }

    // -----------------------------------------------------------------------
    // Progress notification relay
    // -----------------------------------------------------------------------

    /// Test double for `ProgressRelay`: `Peer<RoleServer>` can't be
    /// constructed outside a live rmcp session, so unit tests record
    /// relayed progress here instead.
    #[derive(Default)]
    struct RecordingRelay {
        received: Mutex<Vec<ProgressNotificationParam>>,
    }

    impl RecordingRelay {
        fn snapshot(&self) -> Vec<ProgressNotificationParam> {
            self.received.lock().unwrap().clone()
        }
    }

    impl ProgressRelay for RecordingRelay {
        fn relay(&self, progress: ProgressNotificationParam) -> RelayFuture<'_> {
            Box::pin(async move {
                self.received.lock().unwrap().push(progress);
            })
        }
    }

    /// Stream `chunks` as separate SSE body writes (5ms apart), so the
    /// client genuinely receives them as separate `bytes_stream()` items
    /// rather than one buffered read.
    fn chunked_sse_body(chunks: Vec<String>) -> Response {
        let body_stream = futures_util::stream::iter(chunks).then(|chunk| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok::<_, std::io::Error>(chunk)
        });
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(body_stream))
            .unwrap()
    }

    #[tokio::test]
    async fn sse_progress_events_are_relayed_in_order() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.3,\"message\":\"starting\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.8,\"message\":\"almost done\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n\n",
            )
        }))).await;

        let relay = RecordingRelay::default();
        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let result = client(&base).proxy_call(req, Some(&relay)).await.unwrap();
        assert_eq!(result.content.len(), 1);

        let events = relay.snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].progress, 0.3);
        assert_eq!(events[0].message.as_deref(), Some("starting"));
        assert_eq!(events[1].progress, 0.8);
        assert_eq!(events[1].message.as_deref(), Some("almost done"));
    }

    #[tokio::test]
    async fn sse_relay_not_invoked_for_non_progress_notification() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n",
            )
        }))).await;

        let relay = RecordingRelay::default();
        let result: Result<ListToolsResult> = client(&base).rpc("tools/list", json!({}), Some(&relay)).await;
        assert!(result.unwrap().tools.is_empty());
        assert!(relay.snapshot().is_empty());
    }

    #[tokio::test]
    async fn sse_streamed_across_multiple_chunks_relays_progress_and_returns_result() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body(vec![
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.5}}\n\n".to_string(),
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n".to_string(),
            ])
        }))).await;

        let relay = RecordingRelay::default();
        let result: Result<ListToolsResult> = client(&base).rpc("tools/list", json!({}), Some(&relay)).await;
        assert!(result.unwrap().tools.is_empty());
        assert_eq!(relay.snapshot().len(), 1);
        assert_eq!(relay.snapshot()[0].progress, 0.5);
    }

    #[tokio::test]
    async fn sse_line_split_across_chunk_boundary_still_parses() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body(vec![
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"resul".to_string(),
                "t\":{\"tools\":[]}}\n\n".to_string(),
            ])
        }))).await;
        assert!(client(&base).discover_tools().await.unwrap().is_empty());
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

    // -----------------------------------------------------------------------
    // End-to-end: progress relayed to a real connected MCP client
    // -----------------------------------------------------------------------

    /// Mirrors the relay wiring planned for `ServerProxy::call_tool`
    /// (gateway.rs), minus the HTTP-only auth step — that step needs
    /// `cx.extensions` populated by the axum transport layer, which a raw
    /// `tokio::io::duplex` transport has no way to supply. Auth is already
    /// covered separately by gateway.rs's own tests; what this test proves
    /// is that `cx.peer.send_notification(...)` actually reaches a real,
    /// connected MCP client through rmcp's own notification dispatch.
    struct RelayingToolServer {
        upstream: UpstreamMcpClient,
    }

    impl rmcp::ServerHandler for RelayingToolServer {
        async fn call_tool(
            &self,
            mut request: CallToolRequestParams,
            cx: rmcp::service::RequestContext<RoleServer>,
        ) -> Result<CallToolResult, McpError> {
            match cx.meta.get_progress_token() {
                Some(token) => {
                    rmcp::model::RequestParamsMeta::set_progress_token(&mut request, token);
                    self.upstream.proxy_call(request, Some(&cx.peer)).await
                }
                None => self.upstream.proxy_call(request, None).await,
            }
        }
    }

    #[derive(Clone)]
    struct RecordingClientHandler {
        // rmcp dispatches each incoming notification on its own spawned task
        // rather than awaiting it inline, so the `call_tool` response can
        // resolve before `on_progress` has actually run. An mpsc channel lets
        // the test await delivery deterministically instead of racing a
        // shared `Vec`.
        sender: tokio::sync::mpsc::UnboundedSender<ProgressNotificationParam>,
    }

    impl rmcp::ClientHandler for RecordingClientHandler {
        async fn on_progress(
            &self,
            params: ProgressNotificationParam,
            _context: rmcp::service::NotificationContext<rmcp::RoleClient>,
        ) {
            let _ = self.sender.send(params);
        }
    }

    #[tokio::test]
    async fn progress_is_relayed_to_a_real_connected_mcp_client() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body(vec![
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"progress-1\",\"progress\":0.5,\"message\":\"halfway\"}}\n\n".to_string(),
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n\n".to_string(),
            ])
        }))).await;

        let server = RelayingToolServer { upstream: client(&base) };
        let (client_io, server_io) = tokio::io::duplex(8192);

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let client_handler = RecordingClientHandler { sender };

        let (server_result, client_result) = tokio::join!(
            rmcp::serve_server(server, server_io),
            rmcp::serve_client(client_handler, client_io),
        );
        let _server_running = server_result.unwrap();
        let client_running = client_result.unwrap();

        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let result = client_running.call_tool(req).await.unwrap();
        assert_eq!(result.content.len(), 1);

        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out waiting for relayed progress notification")
            .expect("notification channel closed unexpectedly");
        assert_eq!(event.progress, 0.5);
        assert_eq!(event.message.as_deref(), Some("halfway"));
    }
}
