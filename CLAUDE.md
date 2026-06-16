# CLAUDE.md

Guidance for Claude Code (or other AI agents) working in this repository.
See [README.md](README.md) for what this project does and how to run it.

## rmcp gotcha: `_meta` / progress tokens

If you're reading or writing a request's `_meta` field (progress tokens, task
metadata, etc.), don't trust the request-params struct's own `.meta` field —
it is **always `None` on inbound requests**, even when the wire JSON has a
real `_meta` object.

Why: rmcp's `Request<M, P>` envelope (vendored crate, `model/serde_impl.rs`)
intercepts the literal `_meta` JSON key into its own `extensions` field
*before* `P` (e.g. `CallToolRequestParams`) is deserialized via
`serde(flatten)`. The dispatch loop then moves that `Meta` onto
`RequestContext.meta` — not onto `request.meta`.

- **Reading** an inbound `_meta` (e.g. a caller's progress token): use
  `cx.meta.get_progress_token()` on the `RequestContext`, not
  `request.meta`.
- **Forwarding** params to another JSON-RPC peer by re-serializing them
  directly (as [src/upstream.rs](src/upstream.rs) does for the upstream MCP
  call, bypassing rmcp's own `Request<M,P>` wrapper): explicitly copy the
  token back with `RequestParamsMeta::set_progress_token(&mut request,
  token)` first, or the outbound `_meta` will be empty too.

See [src/gateway.rs](src/gateway.rs)'s `call_tool` for the working example.

## Investigating rmcp internals

rmcp's public docs lag its actual behavior in places. When in doubt, read the
vendored source directly rather than guessing:

```sh
find ~/.cargo/registry/src -maxdepth 1 -iname "rmcp-*"
```

A real end-to-end test (e.g. `tokio::io::duplex()` plus `rmcp::serve_server`
/ `rmcp::serve_client`, see `progress_is_relayed_to_a_real_connected_mcp_client`
in [src/upstream.rs](src/upstream.rs)) is more trustworthy than a unit test
that fakes rmcp's context structs — it's what caught the `_meta` gotcha
above; a hand-built `RequestContext` in a unit test would not have.
