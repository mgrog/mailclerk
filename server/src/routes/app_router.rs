use std::{sync::Arc, time::Duration};

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Router,
};
use http::HeaderValue;
use tower_cookies::CookieManagerLayer;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::cors::CorsLayer;

use crate::{request_tracing, ServerState};

#[cfg(debug_assertions)]
use super::handlers::dev_only;
use super::handlers::{account_connection, auth, email, gmail_labels, user, user_email_rule};

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

        let router = Router::new()
            .route("/", get(|| async { "Mailclerk server" }))
            .route("/auth/gmail", get(auth::handler_auth_gmail))
            .route("/auth/callback", get(auth::handler_auth_gmail_callback))
            .route("/auth/login", post(auth::handler_login))
            .route("/auth/me", get(auth::handler_me))
            .route(
                "/check-account-connection",
                get(account_connection::check_account_connection),
            )
            .route("/user-email-rule/test", post(user_email_rule::test))
            .route("/gmail/labels", get(gmail_labels::get_user_gmail_labels))
            .nest(
                "/user",
                Router::new()
                    .route(
                        "/unlock-daily-limit",
                        post(user::handler_unlock_daily_limit),
                    )
                    .with_state(state.clone()),
            )
            .nest(
                "/email",
                Router::new()
                    .route("/", get(email::get_all))
                    .route("/:id", get(email::get_sanitized_message))
                    .route("/feed", get(email::get_feed))
                    .route("/search", get(email::search))
                    .route(
                        "/send",
                        post(email::send).layer(DefaultBodyLimit::max(25 * 1024 * 1024)), // 25MB limit for attachments
                    )
                    .route("/mark-as-read", put(email::mark_as_read))
                    .route("/mark-as-unread", put(email::mark_as_unread))
                    .with_state(state.clone()),
            )
            .layer(CookieManagerLayer::new())
            .layer(GovernorLayer {
                config: ip_limiter_conf,
            })
            .layer(request_tracing::trace_with_request_id_layer())
            .layer(cors_layer)
            .with_state(state.clone())
            .fallback(handler_404);

        #[cfg(debug_assertions)]
        let router = router
            .route(
                "/dev/refresh_user_token/:user_email",
                get(dev_only::refresh_user_token).with_state(state.clone()),
            )
            .route(
                "/dev/token",
                get(dev_only::dev_token).with_state(state.clone()),
            )
            .route(
                "/dev/messages",
                get(email::get_messages_by_ids).with_state(state.clone()),
            )
            .route(
                "/dev/message/:id",
                get(email::get_message_by_id).with_state(state.clone()),
            );

        router
    }
}

pub async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Route does not exist")
}
