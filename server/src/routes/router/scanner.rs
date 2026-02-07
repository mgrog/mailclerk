use std::{sync::Arc, time::Duration};

use axum::{routing::get, Router};
use http::HeaderValue;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::cors::CorsLayer;

use crate::{request_tracing, ServerState};
use crate::routes::handlers::scan;
#[cfg(debug_assertions)]
use crate::routes::handlers::dev_only;

use super::handler_404;

pub struct ScannerRouter;

impl ScannerRouter {
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
            .route("/", get(|| async { "OK" }))
            .route("/scan/initial", get(scan::start_initial_scan_ws));

        #[cfg(debug_assertions)]
        let router =
            router.route("/dev/scan/mock-initial", get(dev_only::mock_initial_scan_ws));

        let router = router
            .layer(GovernorLayer {
                config: ip_limiter_conf,
            })
            .layer(request_tracing::trace_with_request_id_layer())
            .layer(cors_layer)
            .with_state(state)
            .fallback(handler_404);

        router
    }
}
