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
        CallToolRequestParams, CallToolResult, ListToolsResult, LoggingMessageNotificationParam,
        Notification, ProgressNotificationParam, RequestParamsMeta, ServerNotification, Tool,
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

/// Receives notifications relayed from an upstream SSE stream while a
/// `tools/call` is in flight: progress updates and log messages. A trait
/// (rather than a direct `Peer<RoleServer>` parameter) so tests can
/// substitute a recording fake — `Peer::new` is private to rmcp and can't be
/// constructed outside a live session.
pub trait NotificationRelay: Send + Sync {
    fn relay_progress(&self, progress: ProgressNotificationParam) -> RelayFuture<'_>;
    fn relay_log(&self, log: LoggingMessageNotificationParam) -> RelayFuture<'_>;
}

impl NotificationRelay for Peer<RoleServer> {
    fn relay_progress(&self, progress: ProgressNotificationParam) -> RelayFuture<'_> {
        Box::pin(async move {
            let notification = ServerNotification::ProgressNotification(Notification::new(progress));
            // Best-effort: if the downstream client disconnected mid-call, the
            // final result still gets returned; don't fail the call over it.
            let _ = self.send_notification(notification).await;
        })
    }

    fn relay_log(&self, log: LoggingMessageNotificationParam) -> RelayFuture<'_> {
        Box::pin(async move {
            let notification = ServerNotification::LoggingMessageNotification(Notification::new(log));
            let _ = self.send_notification(notification).await;
        })
    }
}


#[derive(Clone)]
pub struct UpstreamMcpClient {
    http: reqwest::Client,
    url: String,
    counter: Arc<AtomicU64>,
    /// Longest gap allowed between two received events (connect, response
    /// headers, one SSE chunk) before a call is aborted. This is an *idle*
    /// timeout rather than a flat total-duration timeout, so a long-running
    /// tool that keeps emitting `notifications/progress` stays alive
    /// indefinitely; only a genuinely stalled upstream gets killed.
    idle_timeout: Duration,
}

impl UpstreamMcpClient {
    pub fn new(url: impl Into<String>, tls_verify: bool, idle_timeout_secs: u64) -> Self {
        let idle_timeout = Duration::from_secs(idle_timeout_secs);
        let http = reqwest::Client::builder()
            .connect_timeout(idle_timeout)
            .danger_accept_invalid_certs(!tls_verify)
            .build()
            .expect("failed to build upstream http client");
        Self { http, url: url.into(), counter: Arc::new(AtomicU64::new(1)), idle_timeout }
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
        relay: Option<&dyn NotificationRelay>,
    ) -> Result<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let id = self.next_id();
        let body = RpcRequest { jsonrpc: "2.0", id, method, params };

        let resp = tokio::time::timeout(self.idle_timeout, self.http.post(&self.url)
            .header(reqwest::header::ACCEPT, "application/json, text/event-stream")
            .json(&body)
            .send())
            .await
            .map_err(|_| {
                anyhow!("upstream MCP at {} did not respond within {:?}", self.url, self.idle_timeout)
            })?
            .with_context(|| format!("upstream MCP unreachable at {}", self.url))?;

        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("text/event-stream"));

        if !resp.status().is_success() {
            let status = resp.status();
            let text = tokio::time::timeout(self.idle_timeout, resp.text())
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_default();
            bail!("upstream returned {status}: {text}");
        }

        if is_event_stream {
            return read_sse_response(resp, id, relay, self.idle_timeout).await;
        }

        let body_text = tokio::time::timeout(self.idle_timeout, resp.text())
            .await
            .map_err(|_| {
                anyhow!("upstream MCP response body from {} timed out after {:?}", self.url, self.idle_timeout)
            })?
            .context("failed to read upstream MCP response")?;
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
    /// is given, any `notifications/progress` or `notifications/message`
    /// events the upstream sends before its final response are relayed live.
    pub async fn proxy_call(
        &self,
        request: CallToolRequestParams,
        relay: Option<&dyn NotificationRelay>,
    ) -> Result<CallToolResult, McpError> {
        self.rpc("tools/call", request, relay)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))
    }
}

/// Read an SSE response body incrementally, relaying any `notifications/progress`
/// or `notifications/message` events as they arrive, and returning as soon as
/// the JSON-RPC response matching `expected_id` is seen — without waiting for
/// the upstream to close the connection.
///
/// `idle_timeout` bounds the gap between consecutive chunks, not the total
/// stream duration: every received chunk resets it, so a long-running tool
/// that keeps emitting notifications stays alive indefinitely.
async fn read_sse_response<R: for<'de> Deserialize<'de>>(
    resp: reqwest::Response,
    expected_id: u64,
    relay: Option<&dyn NotificationRelay>,
    idle_timeout: Duration,
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

        match tokio::time::timeout(idle_timeout, stream.next()).await {
            Ok(Some(Ok(chunk))) => buf.extend_from_slice(&chunk),
            Ok(Some(Err(e))) => return Err(e).context("failed to read upstream SSE stream"),
            Ok(None) => break,
            Err(_) => bail!(
                "upstream SSE stream for request id {expected_id} idle for longer than {idle_timeout:?}"
            ),
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

/// Inspect one SSE `data:` payload. Relays it if it's a known notification type
/// and keeps reading (`None`); returns `Some` with the final result/error once
/// the JSON-RPC response matching `expected_id` is found.
async fn handle_sse_event<R: for<'de> Deserialize<'de>>(
    data: &str,
    expected_id: u64,
    relay: Option<&dyn NotificationRelay>,
) -> Option<Result<R>> {
    if let Ok(notification) = serde_json::from_str::<RpcNotification>(data) {
        match notification.method.as_str() {
            "notifications/progress" => {
                if let (Some(relay), Ok(params)) = (relay, serde_json::from_value::<ProgressNotificationParam>(notification.params)) {
                    relay.relay_progress(params).await;
                }
            }
            "notifications/message" => {
                if let Ok(params) = serde_json::from_value::<LoggingMessageNotificationParam>(notification.params) {
                    tracing::debug!(level = ?params.level, logger = params.logger.as_deref(), data = %params.data, "upstream log");
                    if let Some(relay) = relay {
                        relay.relay_log(params).await;
                    }
                }
            }
            _ => {}
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

    /// Like `client`, but with millisecond-level control over the idle
    /// timeout so timeout tests don't need multi-second sleeps.
    fn client_with_idle_timeout(url: &str, idle_timeout: Duration) -> UpstreamMcpClient {
        UpstreamMcpClient {
            http: reqwest::Client::builder()
                .connect_timeout(idle_timeout)
                .build()
                .unwrap(),
            url: url.to_string(),
            counter: Arc::new(AtomicU64::new(1)),
            idle_timeout,
        }
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
    async fn request_includes_accept_json_and_event_stream() {
        use axum::http::HeaderMap;
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();
        let base = start_mock(Router::new().route("/", post(
            move |headers: HeaderMap, _body: axum::body::Bytes| {
                let captured = captured_clone.clone();
                async move {
                    let accept = headers
                        .get(header::ACCEPT)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    *captured.lock().unwrap() = Some(accept);
                    Json(json!({"jsonrpc":"2.0","id":1,"result":{"tools":[]}}))
                }
            }
        ))).await;
        client(&base).discover_tools().await.unwrap();
        let accept = captured.lock().unwrap().clone().unwrap_or_default();
        assert!(accept.contains("application/json"), "Accept was: {accept}");
        assert!(accept.contains("text/event-stream"), "Accept was: {accept}");
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

    /// Test double for `NotificationRelay`: `Peer<RoleServer>` can't be
    /// constructed outside a live rmcp session, so unit tests record
    /// relayed notifications here instead.
    #[derive(Default)]
    struct RecordingRelay {
        progress: Mutex<Vec<ProgressNotificationParam>>,
        logs: Mutex<Vec<LoggingMessageNotificationParam>>,
    }

    impl RecordingRelay {
        fn progress_snapshot(&self) -> Vec<ProgressNotificationParam> {
            self.progress.lock().unwrap().clone()
        }
        fn log_snapshot(&self) -> Vec<LoggingMessageNotificationParam> {
            self.logs.lock().unwrap().clone()
        }
    }

    impl NotificationRelay for RecordingRelay {
        fn relay_progress(&self, progress: ProgressNotificationParam) -> RelayFuture<'_> {
            Box::pin(async move {
                self.progress.lock().unwrap().push(progress);
            })
        }
        fn relay_log(&self, log: LoggingMessageNotificationParam) -> RelayFuture<'_> {
            Box::pin(async move {
                self.logs.lock().unwrap().push(log);
            })
        }
    }

    /// Stream `chunks` as separate SSE body writes, `delay` apart, so the
    /// client genuinely receives them as separate `bytes_stream()` items
    /// rather than one buffered read.
    fn chunked_sse_body_with_delay(chunks: Vec<String>, delay: Duration) -> Response {
        let body_stream = futures_util::stream::iter(chunks).then(move |chunk| async move {
            tokio::time::sleep(delay).await;
            Ok::<_, std::io::Error>(chunk)
        });
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(body_stream))
            .unwrap()
    }

    fn chunked_sse_body(chunks: Vec<String>) -> Response {
        chunked_sse_body_with_delay(chunks, Duration::from_millis(5))
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

        let events = relay.progress_snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].progress, 0.3);
        assert_eq!(events[0].message.as_deref(), Some("starting"));
        assert_eq!(events[1].progress, 0.8);
        assert_eq!(events[1].message.as_deref(), Some("almost done"));
    }

    #[tokio::test]
    async fn sse_log_messages_are_relayed_in_order() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\",\"data\":\"fetching data\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"warning\",\"data\":\"retrying\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n\n",
            )
        }))).await;

        let relay = RecordingRelay::default();
        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        client(&base).proxy_call(req, Some(&relay)).await.unwrap();

        let logs = relay.log_snapshot();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].data, serde_json::Value::String("fetching data".into()));
        assert_eq!(logs[1].data, serde_json::Value::String("retrying".into()));
        assert!(relay.progress_snapshot().is_empty());
    }

    #[tokio::test]
    async fn sse_progress_and_log_events_relayed_independently() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok\",\"progress\":0.3}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\",\"data\":\"working\"}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok\",\"progress\":0.9}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n\n",
            )
        }))).await;

        let relay = RecordingRelay::default();
        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        client(&base).proxy_call(req, Some(&relay)).await.unwrap();

        let progress = relay.progress_snapshot();
        assert_eq!(progress.len(), 2);
        assert_eq!(progress[0].progress, 0.3);
        assert_eq!(progress[1].progress, 0.9);

        let logs = relay.log_snapshot();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].data, serde_json::Value::String("working".into()));
    }

    #[tokio::test]
    async fn sse_relay_not_invoked_for_unknown_notification() {
        let base = start_mock(Router::new().route("/", post(|| async {
            sse_body(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/something_unknown\",\"params\":{}}\n\n\
                 data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n",
            )
        }))).await;

        let relay = RecordingRelay::default();
        let result: Result<ListToolsResult> = client(&base).rpc("tools/list", json!({}), Some(&relay)).await;
        assert!(result.unwrap().tools.is_empty());
        assert!(relay.progress_snapshot().is_empty());
        assert!(relay.log_snapshot().is_empty());
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
        assert_eq!(relay.progress_snapshot().len(), 1);
        assert_eq!(relay.progress_snapshot()[0].progress, 0.5);
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
    // Idle timeout
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn idle_timeout_during_initial_request_times_out() {
        let base = start_mock(Router::new().route("/", post(|| async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Json(json!({"jsonrpc":"2.0","id":1,"result":{"tools":[]}}))
        }))).await;

        let c = client_with_idle_timeout(&base, Duration::from_millis(30));
        let err = c.discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("did not respond within"), "got: {err}");
    }

    #[tokio::test]
    async fn idle_timeout_kills_stalled_sse_stream() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body_with_delay(
                vec!["data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n".to_string()],
                Duration::from_millis(200),
            )
        }))).await;

        let c = client_with_idle_timeout(&base, Duration::from_millis(30));
        let err = c.discover_tools().await.unwrap_err();
        assert!(err.to_string().contains("idle for longer than"), "got: {err}");
    }

    #[tokio::test]
    async fn idle_timeout_survives_total_duration_exceeding_timeout_via_steady_progress() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body_with_delay(
                vec![
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.25}}\n\n".to_string(),
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.5}}\n\n".to_string(),
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.75}}\n\n".to_string(),
                    "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n\n".to_string(),
                ],
                Duration::from_millis(30),
            )
        }))).await;

        // 4 chunks * 30ms ~= 120ms total, well past a 60ms idle timeout — but
        // no single gap between chunks exceeds it, so the call must still
        // succeed. A flat total-duration timeout would have killed this.
        let c = client_with_idle_timeout(&base, Duration::from_millis(60));
        let result: ListToolsResult = c.rpc("tools/list", json!({}), None).await.unwrap();
        assert!(result.tools.is_empty());
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
            let relay: Option<&dyn NotificationRelay> = match cx.meta.get_progress_token() {
                Some(token) => {
                    request.set_progress_token(token);
                    Some(&cx.peer as &dyn NotificationRelay)
                }
                None => None,
            };

            // Mirrors gateway.rs's `ServerProxy::call_tool`: racing the upstream
            // call against `cx.ct` drops the in-flight HTTP request when the
            // client cancels, instead of running it to completion.
            tokio::select! {
                result = self.upstream.proxy_call(request, relay) => result,
                () = cx.ct.cancelled() => {
                    Err(McpError::internal_error("Call cancelled by client", None))
                }
            }
        }
    }

    #[derive(Clone)]
    struct RecordingClientHandler {
        // rmcp dispatches each incoming notification on its own spawned task
        // rather than awaiting it inline, so the `call_tool` response can
        // resolve before `on_progress` has actually run. mpsc channels let
        // the test await delivery deterministically instead of racing a
        // shared `Vec`.
        progress_tx: tokio::sync::mpsc::UnboundedSender<ProgressNotificationParam>,
        log_tx: tokio::sync::mpsc::UnboundedSender<LoggingMessageNotificationParam>,
    }

    impl RecordingClientHandler {
        fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<ProgressNotificationParam>, tokio::sync::mpsc::UnboundedReceiver<LoggingMessageNotificationParam>) {
            let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();
            let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
            (Self { progress_tx, log_tx }, progress_rx, log_rx)
        }
    }

    impl rmcp::ClientHandler for RecordingClientHandler {
        async fn on_progress(
            &self,
            params: ProgressNotificationParam,
            _context: rmcp::service::NotificationContext<rmcp::RoleClient>,
        ) {
            let _ = self.progress_tx.send(params);
        }

        async fn on_logging_message(
            &self,
            params: LoggingMessageNotificationParam,
            _context: rmcp::service::NotificationContext<rmcp::RoleClient>,
        ) {
            let _ = self.log_tx.send(params);
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

        let (client_handler, mut progress_rx, _log_rx) = RecordingClientHandler::new();

        let (server_result, client_result) = tokio::join!(
            rmcp::serve_server(server, server_io),
            rmcp::serve_client(client_handler, client_io),
        );
        let _server_running = server_result.unwrap();
        let client_running = client_result.unwrap();

        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let result = client_running.call_tool(req).await.unwrap();
        assert_eq!(result.content.len(), 1);

        let event = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
            .await
            .expect("timed out waiting for relayed progress notification")
            .expect("notification channel closed unexpectedly");
        assert_eq!(event.progress, 0.5);
        assert_eq!(event.message.as_deref(), Some("halfway"));
    }

    #[tokio::test]
    async fn log_message_is_relayed_to_a_real_connected_mcp_client() {
        let base = start_mock(Router::new().route("/", post(|| async {
            chunked_sse_body(vec![
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\",\"data\":\"fetching prices\"}}\n\n".to_string(),
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n\n".to_string(),
            ])
        }))).await;

        let server = RelayingToolServer { upstream: client(&base) };
        let (client_io, server_io) = tokio::io::duplex(8192);
        let (client_handler, _progress_rx, mut log_rx) = RecordingClientHandler::new();

        let (server_result, client_result) = tokio::join!(
            rmcp::serve_server(server, server_io),
            rmcp::serve_client(client_handler, client_io),
        );
        let _server_running = server_result.unwrap();
        let client_running = client_result.unwrap();

        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let result = client_running.call_tool(req).await.unwrap();
        assert_eq!(result.content.len(), 1);

        let log_event = tokio::time::timeout(Duration::from_secs(2), log_rx.recv())
            .await
            .expect("timed out waiting for relayed log notification")
            .expect("log notification channel closed unexpectedly");
        assert_eq!(log_event.data, serde_json::Value::String("fetching prices".into()));
    }

    // -----------------------------------------------------------------------
    // End-to-end: cancellation aborts the in-flight upstream request
    // -----------------------------------------------------------------------

    /// Fires `tx` when dropped, so a test can observe that some other value
    /// holding this guard (here, an SSE body stream) was actually torn down.
    struct SignalOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    /// An SSE body that emits a progress event every 10ms forever. `guard`
    /// is threaded through the stream's state so it's dropped at the exact
    /// moment axum gives up on (i.e. stops polling) this body — which is
    /// what happens once the downstream client's connection is gone.
    fn never_ending_progress_sse_body(guard: SignalOnDrop) -> Response {
        let stream = futures_util::stream::unfold(guard, |guard| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let chunk = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"tok-1\",\"progress\":0.1}}\n\n".to_string();
            Some((Ok::<_, std::io::Error>(chunk), guard))
        });
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .unwrap()
    }

    #[tokio::test]
    async fn cancelling_a_call_aborts_the_in_flight_upstream_request() {
        let (drop_tx, drop_rx) = tokio::sync::oneshot::channel();
        let drop_tx = Arc::new(Mutex::new(Some(drop_tx)));
        let base = start_mock(Router::new().route("/", post(move || {
            let drop_tx = drop_tx.clone();
            async move {
                let guard = SignalOnDrop(drop_tx.lock().unwrap().take());
                never_ending_progress_sse_body(guard)
            }
        }))).await;

        let server = RelayingToolServer { upstream: client(&base) };
        let (client_io, server_io) = tokio::io::duplex(8192);
        let (client_handler, mut progress_rx, _log_rx) = RecordingClientHandler::new();

        let (server_result, client_result) = tokio::join!(
            rmcp::serve_server(server, server_io),
            rmcp::serve_client(client_handler, client_io),
        );
        let _server_running = server_result.unwrap();
        let client_running = client_result.unwrap();

        let req: CallToolRequestParams = serde_json::from_value(json!({"name": "get_cost"})).unwrap();
        let handle = client_running
            .send_cancellable_request(
                rmcp::model::ClientRequest::CallToolRequest(rmcp::model::CallToolRequest::new(req)),
                rmcp::service::PeerRequestOptions::no_options(),
            )
            .await
            .unwrap();

        // Wait for at least one relayed progress event so the call has
        // actually reached the (never-ending) upstream before cancelling it.
        tokio::time::timeout(Duration::from_secs(1), progress_rx.recv())
            .await
            .expect("timed out waiting for the call to actually start")
            .expect("notification channel closed unexpectedly");

        handle.cancel(Some("client gave up".into())).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), drop_rx)
            .await
            .expect("upstream request was not aborted after cancellation")
            .expect("drop signal sender was dropped without firing");
    }

    // -----------------------------------------------------------------------
    // End-to-end: real HTTP/SSE transport, concurrent calls in one session
    // -----------------------------------------------------------------------

    /// Echoes back whatever `progressToken` the caller sent, tagging the
    /// progress message with the tool name — lets the test attribute a
    /// received progress event to the call that produced it.
    async fn echo_progress_upstream(Json(body): Json<serde_json::Value>) -> Response {
        let id = body["id"].clone();
        let token = body["params"]["_meta"]["progressToken"].clone();
        let tool = body["params"]["name"].as_str().unwrap_or("unknown").to_string();
        let progress_chunk = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{\"progressToken\":{token},\"progress\":0.5,\"message\":\"progress-for-{tool}\"}}}}\n\n"
        );
        let result_chunk = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{tool}-done\"}}]}}}}\n\n"
        );
        chunked_sse_body_with_delay(vec![progress_chunk, result_chunk], Duration::from_millis(20))
    }

    /// Stands in for `rancher_auth_middleware`: skips the real Rancher calls
    /// and inserts a fixed `AuthContext` directly, so this test can exercise
    /// the real HTTP transport without a live Rancher server.
    async fn inject_test_auth_context(
        mut req: axum::http::Request<Body>,
        next: axum::middleware::Next,
    ) -> Response {
        req.extensions_mut().insert(crate::rancher_auth::AuthContext {
            display_name: "tester".into(),
            roles: vec!["tester".into()],
        });
        next.run(req).await
    }

    /// Sibling of `progress_is_relayed_to_a_real_connected_mcp_client` that
    /// exercises the production `StreamableHttpService` + `LocalSessionManager`
    /// stack over real HTTP/SSE instead of a raw `tokio::io::duplex`
    /// transport. A `duplex` transport has only one logical channel for
    /// everything the server sends, so it can't catch a bug in how rmcp's
    /// session manager demultiplexes `notifications/progress` by
    /// `progressToken` across the *separate* SSE response streams that two
    /// concurrent `tools/call`s get over real HTTP. This makes two
    /// concurrent calls over one MCP session and asserts each one's
    /// progress notification is routed back to the right caller.
    #[tokio::test]
    async fn concurrent_calls_route_progress_to_the_correct_http_response() {
        use rmcp::model::{CallToolRequest, ClientRequest};
        use rmcp::service::PeerRequestOptions;
        use rmcp::transport::streamable_http_client::{
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
        };
        use rmcp::transport::streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        };

        use crate::config::{RoleRule, ServerConfig};
        use crate::gateway::ServerProxy;

        let upstream_base = start_mock(Router::new().route("/", post(echo_progress_upstream))).await;

        let config = ServerConfig {
            name: "test-server".into(),
            url: upstream_base.clone(),
            rules: vec![RoleRule { tools: vec!["*".into()], role: "tester".into() }],
            ..Default::default()
        };
        let proxy = Arc::new(ServerProxy::new(config, client(&upstream_base), vec![]));
        let svc = StreamableHttpService::new(
            move || Ok((*proxy).clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default().disable_allowed_hosts(),
        );
        let app = Router::new()
            .fallback_service(svc)
            .layer(axum::middleware::from_fn(inject_test_auth_context));
        let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway_addr = gateway_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(gateway_listener, app).await.unwrap();
        });

        let transport = StreamableHttpClientTransport::with_client(
            reqwest::Client::default(),
            StreamableHttpClientTransportConfig::with_uri(format!("http://{gateway_addr}/")),
        );
        let (client_handler, mut progress_rx, _log_rx) = RecordingClientHandler::new();
        let client_running = rmcp::serve_client(client_handler, transport)
            .await
            .unwrap();

        let req_a: CallToolRequestParams = serde_json::from_value(json!({"name": "tool_a"})).unwrap();
        let req_b: CallToolRequestParams = serde_json::from_value(json!({"name": "tool_b"})).unwrap();

        // Issue both calls before awaiting either, so they're genuinely
        // concurrent within the same MCP session.
        let handle_a = client_running
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(req_a)),
                PeerRequestOptions::no_options(),
            )
            .await
            .unwrap();
        let handle_b = client_running
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(req_b)),
                PeerRequestOptions::no_options(),
            )
            .await
            .unwrap();
        let token_a = handle_a.progress_token.clone();
        let token_b = handle_b.progress_token.clone();
        assert_ne!(token_a, token_b, "concurrent calls must get distinct progress tokens");

        let (result_a, result_b) =
            tokio::join!(handle_a.await_response(), handle_b.await_response());
        result_a.unwrap();
        result_b.unwrap();

        let event1 = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
            .await
            .expect("timed out waiting for first progress notification")
            .expect("notification channel closed unexpectedly");
        let event2 = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
            .await
            .expect("timed out waiting for second progress notification")
            .expect("notification channel closed unexpectedly");
        let events = [event1, event2];

        let event_a = events
            .iter()
            .find(|e| e.progress_token == token_a)
            .expect("no progress event routed back for tool_a's call");
        let event_b = events
            .iter()
            .find(|e| e.progress_token == token_b)
            .expect("no progress event routed back for tool_b's call");
        assert_eq!(event_a.message.as_deref(), Some("progress-for-tool_a"));
        assert_eq!(event_b.message.as_deref(), Some("progress-for-tool_b"));
    }
}
