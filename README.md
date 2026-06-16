# Rancher MCP Proxy

A lightweight **MCP gateway** that enforces **Rancher RBAC** in front of one
or more [Model Context Protocol (MCP)](https://modelcontextprotocol.io) servers.

Each upstream MCP server gets its own namespaced endpoint (`/<name>/mcp`),
allowing multiple servers to be fronted by a single gateway without tool name
collisions. Per-endpoint `instructions` let you give each AI agent a distinct
persona. Tool *discovery* (`tools/list`) works without credentials so MCP
clients can enumerate available tools before presenting a token.

## How it works

```
Claude Desktop (or any MCP client)
        │
        │  POST /<server-name>/mcp  +  R_token: <rancher-token>
        │                              R_url:   https://rancher.example.com
        ▼
┌──────────────────────────────────────┐
│   Rancher MCP Gateway                │
│                                      │
│  /opencost/mcp ──────────────────┐   │
│  /platform-ops/mcp ─────────┐   │   │
│                              │   │   │
│  Per endpoint:               │   │   │
│  1. Validate token against   │   │   │
│     Rancher /v3/principals   │   │   │
│  2. Fetch GlobalRoleBindings │   │   │
│     for the user/groups      │   │   │
│  3. Check per-tool role rule │   │   │
│  4. Forward call upstream    │   │   │
└──────────────────────────────┼───┼───┘
                               ▼   ▼
                   platform-mcp  opencost
                   :8080/mcp     :9003/mcp
```

### Per-request auth

Credentials are passed as HTTP headers on every MCP request (not just once at
connection time). This means:

- Different users can share the same proxy endpoint — each call is
  independently authorised.
- The proxy never stores or caches tokens.
- Rotating a user's Rancher token takes effect immediately on the next call.

### What is checked

The proxy calls two Rancher v3 API endpoints on every tool call:

| Endpoint | Purpose |
|---|---|
| `GET /v3/principals` | Resolves the token to a user identity + group memberships |
| `GET /v3/globalRoleBindings` | Lists the global roles bound to that user or their groups |

If the resolved user holds a `GlobalRoleBinding` whose `globalRoleId` matches
the role required by the matching rule for the called tool, the call is
forwarded. Otherwise a `403 Forbidden` MCP error is returned.

---

## Configuration

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `UPSTREAM_MCP_URL` | see note | — | URL of a single upstream MCP server. Required when no config file is present. Ignored when a config file is loaded. |
| `ROLE_CONFIG_FILE` | no | `/etc/rancher-mcp-proxy/config.yaml` | Path to the gateway config file. When the file exists it takes full precedence over `UPSTREAM_MCP_URL` / `REQUIRED_ROLE`. |
| `REQUIRED_ROLE` | no | `mcp-user` | Catch-all Rancher GlobalRole — used only when no config file is present. |
| `RANCHER_TLS_VERIFY` | no | `true` | Set to `false` to skip TLS cert verification when calling the Rancher API (useful for self-signed certs in lab environments) |
| `PORT` | no | `3000` | Port the proxy listens on |
| `RUST_LOG` | no | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

### Gateway config file (`config.yaml`)

Each server gets its own namespaced endpoint: `/<name>/mcp`.  Because
routing is done at the HTTP level there are **no tool name collisions** — two
servers can expose tools with identical names without any conflict.

At startup the gateway discovers tools from every server in parallel and logs
the full list of mounted endpoints.

```yaml
# config.yaml

servers:
  - name: opencost
    url: http://opencost.opencost.svc:9003/mcp
    # instructions is returned as ServerInfo.instructions — use it to give the
    # AI agent a specific persona or restrict what it talks about.
    instructions: |
      You are a Kubernetes cost analysis assistant. Use the available tools to
      answer questions about cluster spend, namespace costs, and resource usage.
      Do not speculate about costs — always fetch real data first.
    rules:
      - tools: ["get_*", "list_*"]
        role: cost-viewer
      - tools: ["*"]
        role: cost-admin

  - name: platform-ops
    url: http://platform-mcp.svc:8080/mcp
    instructions: "You are a platform operations assistant. Help engineers diagnose and resolve cluster issues."
    rules:
      - tools: ["*"]
        role: platform-engineer
```

This produces two independent endpoints:

```
https://gateway.example.com/opencost/mcp
https://gateway.example.com/platform-ops/mcp
```

Point different AI agent configurations at different endpoints to get
different tool sets and personas from the same gateway deployment.

**Pattern syntax** — each entry in `tools` is a glob:

| Pattern | Matches |
|---|---|
| `*` | any sequence of characters (including empty) |
| `?` | exactly one character |
| `get_allocation` | literal exact match |
| `get_*` | any tool whose name starts with `get_` |
| `*_budget` | any tool whose name ends with `_budget` |

Rules within each server are evaluated in order; the **first match wins**.
If no rule matches a tool, the call is **denied** — omitting a catch-all
`["*"]` rule is a valid way to explicitly allowlist specific tools.

**Server name** must be URL-safe (letters, digits, `-`, `_`).
Tool names come from each upstream's `tools/list` response; use
`RUST_LOG=debug` to see them at startup.

---

## Rancher permissions

The proxy uses Rancher's **GlobalRole** system as the authorisation source.
You need to:

1. Create a `GlobalRole` whose name matches `REQUIRED_ROLE`.
2. Bind users or groups to that role with `GlobalRoleBinding` resources.

GlobalRoles are managed by the Rancher management server; apply the YAML below
with `kubectl` against the cluster running Rancher (not a downstream cluster).

### 1 — Create the GlobalRole

A GlobalRole used purely as an access marker needs no Kubernetes RBAC rules.

```yaml
apiVersion: management.cattle.io/v3
kind: GlobalRole
metadata:
  name: mcp-user          # must match REQUIRED_ROLE
displayName: "MCP User"
description: >
  Grants access to call tools on MCP servers protected by the
  Rancher MCP Proxy. Assign this role to any user or group that
  should be allowed to use the AI tooling.
rules: []                 # no extra Kubernetes permissions needed
```

If you want separate roles for different MCP servers (e.g. one for OpenCost,
one for another tool), create one `GlobalRole` per access tier and deploy a
proxy instance per server with the matching `REQUIRED_ROLE`.

### 2 — Bind individual users

Find the Rancher user ID from the UI (*Users & Authentication → Users*) or:

```sh
kubectl get users.management.cattle.io -o custom-columns='NAME:.metadata.name,LOGIN:.spec.username'
```

```yaml
apiVersion: management.cattle.io/v3
kind: GlobalRoleBinding
metadata:
  name: alice-mcp-user          # any unique name
globalRoleId: mcp-user          # must match the GlobalRole above
userId: u-abc123                # Rancher internal user ID
```

### 3 — Bind a group (LDAP / Active Directory / OIDC)

When Rancher is backed by an external identity provider the `groupPrincipalId`
is the provider-specific group identifier. You can look it up by searching for
the group in the Rancher UI under *Users & Authentication → Groups*, or by
inspecting the `id` field returned by `GET /v3/principals?search=<group-name>`.

```yaml
# LDAP / Active Directory group
apiVersion: management.cattle.io/v3
kind: GlobalRoleBinding
metadata:
  name: devteam-mcp-user
globalRoleId: mcp-user
groupPrincipalId: "ldap_group://CN=Developers,OU=Groups,DC=example,DC=com"
```

```yaml
# OIDC / Keycloak group
apiVersion: management.cattle.io/v3
kind: GlobalRoleBinding
metadata:
  name: platform-team-mcp-user
globalRoleId: mcp-user
groupPrincipalId: "oidc_group://platform-team"
```

> **Tip:** The exact `groupPrincipalId` format depends on your auth provider.
> Run `kubectl get globalrolebindings.management.cattle.io -o yaml` on an
> existing binding to see the format used in your environment.

---

## Client configuration

MCP clients must supply two headers with every request:

| Header | Value |
|---|---|
| `R_token` | A Rancher API token (`token-xxxxx:yyyyyyyyyy`) |
| `R_url` | Base URL of the Rancher management server (`https://rancher.example.com`) |

`tools/list` calls work without these headers — clients can always discover
what tools are available before authenticating.

### Claude Desktop (`claude_desktop_config.json`)

Each server gets its own named endpoint.  Point different `mcpServers` entries
at different `/<name>/mcp` paths to give each AI agent the right tool set and
persona.

```json
{
  "mcpServers": {
    "opencost": {
      "type": "http",
      "url": "https://rancher-mcp-proxy.example.com/opencost/mcp",
      "headers": {
        "R_token": "token-xxxxx:yyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        "R_url": "https://rancher.example.com"
      }
    },
    "platform-ops": {
      "type": "http",
      "url": "https://rancher-mcp-proxy.example.com/platform-ops/mcp",
      "headers": {
        "R_token": "token-xxxxx:yyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        "R_url": "https://rancher.example.com"
      }
    }
  }
}
```

When only a single server is configured via `UPSTREAM_MCP_URL` (no config
file), it is mounted at `/upstream/mcp`.

### Generating a Rancher API token

In the Rancher UI: *top-right avatar → Account & API Keys → Create API Key*.
Choose **no expiry** only for service accounts; use a short TTL for personal
tokens used in development.

---

## Deployment

### Helm

**Single-server mode** (simple, no config file):

```sh
helm upgrade --install rancher-mcp-proxy ./charts/rancher-mcp-proxy \
  --namespace mcp-system --create-namespace \
  --set upstreamMcpUrl=http://opencost.opencost.svc:9003/mcp \
  --set rancherAuth.requiredRole=mcp-user \
  --set ingress.host=rancher-mcp-proxy.example.com
```

This mounts the upstream at `/upstream/mcp`.

**Multi-server gateway mode** (`gatewayConfig` takes precedence):

```yaml
# values.yaml

rancherAuth:
  tlsVerify: "true"          # set "false" for self-signed Rancher certs

# Inline YAML config — each servers entry becomes its own /<name>/mcp endpoint.
gatewayConfig: |
  servers:
    - name: opencost
      url: http://opencost.opencost.svc:9003/mcp
      instructions: "You are a Kubernetes cost analysis assistant."
      rules:
        - tools: ["get_*", "list_*"]
          role: cost-viewer
        - tools: ["*"]
          role: cost-admin
    - name: platform-ops
      url: http://platform-mcp.svc:8080/mcp
      instructions: "You are a platform operations assistant."
      rules:
        - tools: ["*"]
          role: platform-engineer

ingress:
  enabled: true
  className: traefik
  host: rancher-mcp-proxy.example.com
  tls:
    enabled: true
    certManager:
      enabled: true
      clusterIssuer: letsencrypt-prod
```

When `gatewayConfig` is set, Helm creates a ConfigMap from the YAML content
and mounts it at `/etc/rancher-mcp-proxy/config.yaml`.  The resulting
endpoints are:

```
https://rancher-mcp-proxy.example.com/opencost/mcp
https://rancher-mcp-proxy.example.com/platform-ops/mcp
```

### Docker / plain Kubernetes

```sh
docker run --rm \
  -e UPSTREAM_MCP_URL=http://opencost.opencost.svc:9003/mcp \
  -e REQUIRED_ROLE=mcp-user \
  -p 3000:3000 \
  ghcr.io/hierynomus/rancher-mcp-proxy:main
```

---

## Limitations

| Limitation | Notes |
|---|---|
| **Per-role, not per-user** | All users holding the required role for a tool can call it; there is no per-user or per-tenant scoping within a role. |
| **Only progress notifications are relayed** | If an upstream responds with `text/event-stream`, `notifications/progress` events are forwarded to the MCP client live, as they arrive — but only when the client's call included a progress token (`_meta.progressToken`), which the upstream's notifications must echo back. Other notification types sent before the final response are still not forwarded. |
| **Tools cached at startup** | The upstream tool list is fetched once when the proxy starts. If the upstream adds or removes tools, restart the proxy to pick up the change. |
| **Rancher global roles only** | Project-scoped or cluster-scoped Rancher roles are not checked — only `GlobalRoleBindings`. |

---

## Development

```sh
# Run locally (requires a reachable Rancher and upstream MCP)
UPSTREAM_MCP_URL=http://localhost:9003/mcp \
REQUIRED_ROLE=mcp-user \
RANCHER_TLS_VERIFY=false \
cargo run

# Tests
cargo test
```
