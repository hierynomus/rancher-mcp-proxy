use std::time::Duration;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Auth context — inserted into request extensions by middleware,
// extracted by tool handlers to enforce role checks and query Rancher.
// ---------------------------------------------------------------------------

/// Holds the authenticated user's identity and their Rancher global roles.
/// Inserted into request extensions by the auth middleware; read by tool
/// handlers to enforce RBAC before proxying calls upstream.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub display_name: String,
    pub roles: Vec<String>,
}

// ---------------------------------------------------------------------------
// Auth error
// ---------------------------------------------------------------------------

pub(crate) enum AuthError {
    RancherUnreachable(String),
    InvalidToken(String),
    BadGateway(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        match self {
            Self::RancherUnreachable(detail) => {
                error!(detail, "Auth failed: could not reach Rancher");
                (StatusCode::BAD_GATEWAY, "Authentication failed: unable to reach Rancher server")
                    .into_response()
            }
            Self::InvalidToken(detail) => {
                warn!(detail, "Auth failed: invalid or expired token");
                (StatusCode::UNAUTHORIZED, "Unauthorized: invalid or expired Rancher token")
                    .into_response()
            }
            Self::BadGateway(detail) => {
                error!(detail, "Auth failed: unexpected Rancher response");
                (StatusCode::BAD_GATEWAY, "Authentication failed: unexpected response from Rancher")
                    .into_response()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rancher API types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RancherPrincipal {
    id: String,
    login_name: Option<String>,
    display_name: Option<String>,
    principal_type: Option<String>,
    me: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RancherCollection<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlobalRoleBinding {
    global_role_id: String,
    user_id: Option<String>,
    group_principal_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Auth state + Rancher client logic
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RancherAuthState {
    http_client: reqwest::Client,
}

impl RancherAuthState {
    pub fn new(tls_verify: bool) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(!tls_verify)
            .build()
            .expect("failed to build reqwest client");

        Self { http_client }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        token: &str,
    ) -> Result<T, AuthError> {
        let resp = self
            .http_client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| AuthError::RancherUnreachable(format!("{url}: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::InvalidToken(format!(
                "Rancher returned {status} for {url}. Body: {body}"
            )));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| AuthError::BadGateway(format!("failed to read body from {url}: {e}")))?;

        tracing::debug!(url, body, "Rancher API response");

        serde_json::from_str(&body)
            .map_err(|e| AuthError::BadGateway(format!("failed to parse {url}: {e}. Body: {body}")))
    }

    async fn identify(&self, token: &str, rancher_url: &str) -> Result<UserIdentity, AuthError> {
        let principals_url = format!("{rancher_url}/v3/principals");
        let principals: RancherCollection<RancherPrincipal> =
            self.get_json(&principals_url, token).await?;

        let me = principals
            .data
            .iter()
            .find(|p| p.me == Some(true))
            .or_else(|| principals.data.first())
            .ok_or_else(|| AuthError::InvalidToken("no principals returned".into()))?;

        let display_name = resolve_display_name(&principals.data);
        let principal_ids: Vec<String> = principals.data.iter().map(|p| p.id.clone()).collect();

        info!(
            %display_name,
            principal_id = %me.id,
            principal_type = me.principal_type.as_deref().unwrap_or("unknown"),
            "Authenticated Rancher principal"
        );

        Ok(UserIdentity { display_name, principal_ids })
    }

    async fn fetch_roles(
        &self,
        token: &str,
        rancher_url: &str,
        principal_ids: &[String],
    ) -> Result<Vec<String>, AuthError> {
        let grb_url = format!("{rancher_url}/v3/globalRoleBindings");
        let bindings: RancherCollection<GlobalRoleBinding> =
            self.get_json(&grb_url, token).await?;

        let roles = match_roles(&bindings.data, principal_ids);
        info!(?roles, "User's matching global roles");
        Ok(roles)
    }
}

struct UserIdentity {
    display_name: String,
    principal_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Pure helpers — extracted for testability
// ---------------------------------------------------------------------------

fn resolve_display_name(principals: &[RancherPrincipal]) -> String {
    principals
        .iter()
        .find(|p| p.me == Some(true))
        .or_else(|| principals.first())
        .and_then(|p| p.display_name.as_deref().or(p.login_name.as_deref()))
        .unwrap_or("unknown")
        .to_string()
}

fn match_roles(bindings: &[GlobalRoleBinding], principal_ids: &[String]) -> Vec<String> {
    bindings
        .iter()
        .filter(|b| {
            let user_match = b
                .user_id
                .as_deref()
                .map_or(false, |uid| principal_ids.iter().any(|pid| pid.ends_with(uid)));
            let group_match = b
                .group_principal_id
                .as_deref()
                .map_or(false, |gid| principal_ids.iter().any(|pid| pid == gid));
            user_match || group_match
        })
        .map(|b| b.global_role_id.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

pub async fn rancher_auth_middleware(
    State(state): State<RancherAuthState>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, AuthError> {
    let headers = req.headers();
    let r_token = headers
        .get("R_token")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());
    let r_url = headers
        .get("R_url")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());

    // No auth headers → allow through (MCP discovery / initialization)
    let (token, url) = match r_token.zip(r_url) {
        Some((t, u)) => (t.to_string(), u.trim_end_matches('/').to_string()),
        None => {
            info!("Auth middleware: no auth headers, allowing through (discovery)");
            return Ok(next.run(req).await);
        }
    };

    let identity = state.identify(&token, &url).await?;
    let roles = state
        .fetch_roles(&token, &url, &identity.principal_ids)
        .await?;

    info!(
        display_name = %identity.display_name,
        ?roles,
        "Auth context attached to request"
    );

    req.extensions_mut().insert(AuthContext {
        display_name: identity.display_name,
        roles,
    });

    Ok(next.run(req).await)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    // -----------------------------------------------------------------------
    // resolve_display_name
    // -----------------------------------------------------------------------

    fn principal(id: &str, display: Option<&str>, login: Option<&str>, me: Option<bool>) -> RancherPrincipal {
        RancherPrincipal {
            id: id.into(),
            display_name: display.map(String::from),
            login_name: login.map(String::from),
            principal_type: None,
            me,
        }
    }

    #[test]
    fn display_name_prefers_me_principal() {
        let principals = vec![
            principal("local://other", Some("Other User"), None, None),
            principal("local://alice", Some("Alice"), None, Some(true)),
        ];
        assert_eq!(resolve_display_name(&principals), "Alice");
    }

    #[test]
    fn display_name_falls_back_to_first_when_no_me() {
        let principals = vec![
            principal("local://first", Some("First"), None, None),
            principal("local://second", Some("Second"), None, None),
        ];
        assert_eq!(resolve_display_name(&principals), "First");
    }

    #[test]
    fn display_name_uses_login_when_no_display_name() {
        let principals = vec![principal("local://alice", None, Some("alice@example.com"), Some(true))];
        assert_eq!(resolve_display_name(&principals), "alice@example.com");
    }

    #[test]
    fn display_name_returns_unknown_when_no_names() {
        let principals = vec![principal("local://alice", None, None, Some(true))];
        assert_eq!(resolve_display_name(&principals), "unknown");
    }

    #[test]
    fn display_name_returns_unknown_for_empty_list() {
        assert_eq!(resolve_display_name(&[]), "unknown");
    }

    // -----------------------------------------------------------------------
    // match_roles
    // -----------------------------------------------------------------------

    fn binding(role: &str, user_id: Option<&str>, group_id: Option<&str>) -> GlobalRoleBinding {
        GlobalRoleBinding {
            global_role_id: role.into(),
            user_id: user_id.map(String::from),
            group_principal_id: group_id.map(String::from),
        }
    }

    #[test]
    fn match_roles_user_id_suffix_match() {
        // Rancher stores principals as "local://u-abc123"; userId is just "u-abc123".
        let bindings = vec![binding("mcp-user", Some("u-abc123"), None)];
        let principal_ids = vec!["local://u-abc123".to_string()];
        assert_eq!(match_roles(&bindings, &principal_ids), vec!["mcp-user"]);
    }

    #[test]
    fn match_roles_group_principal_exact_match() {
        let bindings = vec![binding("cost-viewer", None, Some("ldap_group://devs"))];
        let principal_ids = vec!["ldap_group://devs".to_string()];
        assert_eq!(match_roles(&bindings, &principal_ids), vec!["cost-viewer"]);
    }

    #[test]
    fn match_roles_no_match_returns_empty() {
        let bindings = vec![binding("admin", Some("u-other"), None)];
        let principal_ids = vec!["local://u-alice".to_string()];
        assert!(match_roles(&bindings, &principal_ids).is_empty());
    }

    #[test]
    fn match_roles_multiple_matching_bindings() {
        let bindings = vec![
            binding("cost-viewer", Some("u-alice"), None),
            binding("platform-user", None, Some("ldap_group://platform")),
            binding("unrelated", Some("u-bob"), None),
        ];
        let principal_ids = vec![
            "local://u-alice".to_string(),
            "ldap_group://platform".to_string(),
        ];
        let mut roles = match_roles(&bindings, &principal_ids);
        roles.sort();
        assert_eq!(roles, vec!["cost-viewer", "platform-user"]);
    }

    #[test]
    fn match_roles_group_requires_exact_match_not_suffix() {
        // Group principal IDs must match exactly — no suffix matching.
        let bindings = vec![binding("admin", None, Some("ldap_group://admins"))];
        let principal_ids = vec!["prefix_ldap_group://admins".to_string()];
        assert!(match_roles(&bindings, &principal_ids).is_empty());
    }

    #[test]
    fn match_roles_empty_bindings() {
        let principal_ids = vec!["local://u-alice".to_string()];
        assert!(match_roles(&[], &principal_ids).is_empty());
    }

    // -----------------------------------------------------------------------
    // AuthError HTTP status codes
    // -----------------------------------------------------------------------

    #[test]
    fn auth_error_unreachable_returns_502() {
        let resp = AuthError::RancherUnreachable("timeout".into()).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn auth_error_invalid_token_returns_401() {
        let resp = AuthError::InvalidToken("expired".into()).into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn auth_error_bad_gateway_returns_502() {
        let resp = AuthError::BadGateway("parse error".into()).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }
}
