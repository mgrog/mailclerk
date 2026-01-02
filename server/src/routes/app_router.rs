use std::{sync::Arc, time::Duration};

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use http::HeaderValue;
use tower_cookies::CookieManagerLayer;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::cors::CorsLayer;

use crate::{request_tracing, ServerState};

use super::{account_connection, auth, custom_email_rule, gmail_labels};

pub struct AppRouter;

impl AppRouter {
    pub fn create(state: ServerState) -> Router {
        let origins = [
            "https://mailclerk.io",
            "https://localhost:3000",
            "http://localhost:3000",
        ]
        .into_iter()
        .map(|origin| origin.parse::<HeaderValue>().unwrap())
        .collect::<Vec<_>>();

        let cors_layer = CorsLayer::new()
            .allow_origin(origins)
            .allow_credentials(true);

        let ip_limiter_conf = Arc::new(GovernorConfigBuilder::default().finish().unwrap());

        let strict_ip_limiter = Arc::new(
            GovernorConfigBuilder::default()
                .per_second(1)
                .burst_size(1)
                .finish()
                .unwrap(),
        );

        let ip_limiter = ip_limiter_conf.limiter().clone();
        let strict_ip_limiter = strict_ip_limiter.limiter().clone();
        let interval = Duration::from_secs(60);
        // a separate background task to clean up
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                ip_limiter.retain_recent();
                strict_ip_limiter.retain_recent();
            }
        });

        Router::new()
            .route("/", get(|| async { "Mailclerk server" }))
            .route("/auth/gmail", get(auth::handler_auth_gmail))
            .route("/auth/callback", get(auth::handler_auth_gmail_callback))
            .route("/auth/login", post(auth::handler_login))
            .route(
                "/check_account_connection",
                get(account_connection::check_account_connection),
            )
            .route(
                "/refresh_user_token/:user_email",
                get(auth::handler_refresh_user_token),
            )
            .route("/custom_email_rule/test", post(custom_email_rule::test))
            .route("/gmail/labels", get(gmail_labels::get_user_gmail_labels))
            .layer(CookieManagerLayer::new())
            .layer(GovernorLayer {
                config: ip_limiter_conf,
            })
            .layer(request_tracing::trace_with_request_id_layer())
            .layer(cors_layer)
            .with_state(state.clone())
            .fallback(handler_404)
    }
}

pub async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Route does not exist")
}
