//! Development-only handlers. These are only compiled in debug builds.

use std::time::Instant;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use futures::{SinkExt, StreamExt};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{sleep, Duration};

use crate::{
    auth::jwt::{generate_dev_token, Claims},
    error::AppJsonResult,
    model::user::{UserAccessCtrl, UserCtrl},
    routes::handlers::scan::{DetailedAnalysis, ScanStatusUpdate},
    HttpClient, ServerState,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevTokenParams {
    pub user_id: Option<i32>,
    pub email: Option<String>,
}

#[derive(Serialize)]
struct DevTokenResponse {
    token: String,
}

pub async fn dev_token(
    State(state): State<ServerState>,
    Query(params): Query<DevTokenParams>,
) -> impl IntoResponse {
    // Look up user by user_id or email
    let user = match (params.user_id, params.email) {
        (Some(user_id), _) => UserCtrl::get_by_id(&state.conn, user_id).await,
        (None, Some(email)) => UserCtrl::get_by_email(&state.conn, &email).await,
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                "Must provide either user_id or email",
            )
                .into_response()
        }
    };

    match user {
        Ok(user) => match generate_dev_token(user.id, &user.email) {
            Ok(token) => (StatusCode::OK, Json(DevTokenResponse { token })).into_response(),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create token").into_response(),
        },
        Err(_) => (StatusCode::NOT_FOUND, "User not found").into_response(),
    }
}

pub async fn refresh_user_token(
    user_email: Path<String>,
    State(http_client): State<HttpClient>,
    State(conn): State<DatabaseConnection>,
) -> AppJsonResult<serde_json::Value> {
    let mut user = UserCtrl::get_with_account_access_by_email(&conn, user_email.as_str()).await?;
    UserAccessCtrl::get_refreshed_token(&http_client, &conn, &mut user).await?;

    Ok(Json(json!({
        "message": "Token refreshed"
    })))
}

const TOTAL_EMAILS: usize = 2000;

#[derive(Deserialize)]
pub struct MockScanParams {
    pub repeat: Option<bool>,
}

/// Mock WebSocket handler that simulates an initial scan through all stages.
pub async fn mock_initial_scan_ws(
    ws: WebSocketUpgrade,
    claims: Claims,
    Query(params): Query<MockScanParams>,
) -> impl IntoResponse {
    let repeat = params.repeat.unwrap_or(true);
    ws.on_upgrade(move |socket| handle_mock_scan_socket(socket, claims.sub, claims.email, repeat))
}

async fn handle_mock_scan_socket(
    socket: WebSocket,
    user_id: i32,
    user_email: String,
    repeat: bool,
) {
    let (mut sender, mut receiver) = socket.split();
    let total_emails = TOTAL_EMAILS;

    // Handle incoming messages (for client-side close)
    let close_token = tokio_util::sync::CancellationToken::new();
    let close_token_clone = close_token.clone();
    tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Close(_)) => {
                    close_token_clone.cancel();
                    break;
                }
                Err(_) => {
                    close_token_clone.cancel();
                    break;
                }
                _ => {}
            }
        }
    });

    let total_to_fetch = total_emails + total_emails / 5; // simulate pruning ~20%
    let total_to_scan = total_emails;

    // Helper to send a JSON message, returns false if the connection is closed
    macro_rules! send_msg {
        ($msg:expr) => {
            if close_token.is_cancelled() {
                let _ = sender.close().await;
                return;
            }
            if sender
                .send(Message::Text(serde_json::to_string(&$msg).unwrap()))
                .await
                .is_err()
            {
                return;
            }
        };
    }

    loop {
        let started_at = Instant::now();

        // --- Started ---
        send_msg!(ScanStatusUpdate::Started {
            user_id,
            user_email: user_email.clone(),
        });
        sleep(Duration::from_millis(300)).await;

        // --- Fetching ---
        let fetch_batch_size = (total_to_fetch / 20).max(1);
        let mut fetched = 0;
        while fetched < total_to_fetch {
            fetched = (fetched + fetch_batch_size).min(total_to_fetch);
            let pct = (fetched as f32 / total_to_fetch as f32) * 10.0;
            send_msg!(ScanStatusUpdate::Progress {
                phase: "Fetching".to_string(),
                fetched_emails: fetched,
                total_to_fetch,
                total_to_scan,
                processed_emails: 0,
                percentage: pct,
                elapsed_secs: started_at.elapsed().as_secs(),
            });
            sleep(Duration::from_millis(100)).await;
        }

        // After fetching, total_emails is known (simulates pruning)
        send_msg!(ScanStatusUpdate::Progress {
            phase: "Fetching".to_string(),
            fetched_emails: total_to_fetch,
            total_to_fetch,
            total_to_scan,
            processed_emails: 0,
            percentage: 10.0,
            elapsed_secs: started_at.elapsed().as_secs(),
        });
        sleep(Duration::from_millis(300)).await;

        // --- Categorizing ---
        let categorize_steps = 20;
        let batch = (total_emails / categorize_steps).max(1);
        let mut processed = 0;
        for _ in 0..categorize_steps {
            processed = (processed + batch).min(total_emails);
            let pct = 10.0 + (processed as f32 / total_emails as f32) * 70.0;
            send_msg!(ScanStatusUpdate::Progress {
                phase: "Categorizing".to_string(),
                fetched_emails: total_to_fetch,
                total_to_fetch,
                total_to_scan,
                processed_emails: processed,
                percentage: pct,
                elapsed_secs: started_at.elapsed().as_secs(),
            });
            sleep(Duration::from_millis(100)).await;
        }

        // --- Extracting ---
        let extract_steps = 3;
        let extract_batch = (total_emails / extract_steps / 5).max(1);
        for i in 1..=extract_steps {
            let extra = extract_batch * i;
            let pct = 80.0 + (extra as f32 / (extract_batch * extract_steps) as f32) * 15.0;
            send_msg!(ScanStatusUpdate::Progress {
                phase: "Extracting".to_string(),
                fetched_emails: total_to_fetch,
                total_to_fetch,
                total_to_scan,
                processed_emails: processed,
                percentage: pct,
                elapsed_secs: started_at.elapsed().as_secs(),
            });
            sleep(Duration::from_millis(100)).await;
        }

        // --- Inserting ---
        send_msg!(ScanStatusUpdate::Progress {
            phase: "Inserting".to_string(),
            fetched_emails: total_to_fetch,
            total_to_fetch,
            total_to_scan,
            processed_emails: total_to_scan,
            percentage: 100.0,
            elapsed_secs: started_at.elapsed().as_secs(),
        });
        sleep(Duration::from_millis(100)).await;

        // --- Complete ---
        use crate::state::email_scanner::initial_scan::CategorySummary;
        let category_analysis = std::collections::HashMap::from([
            (
                "newsletters".to_string(),
                CategorySummary {
                    count: 42,
                    priority: 1,
                },
            ),
            (
                "ads/promotions".to_string(),
                CategorySummary {
                    count: 28,
                    priority: 0,
                },
            ),
            (
                "receipts/updates".to_string(),
                CategorySummary {
                    count: 15,
                    priority: 2,
                },
            ),
            (
                "travel".to_string(),
                CategorySummary {
                    count: 8,
                    priority: 3,
                },
            ),
            (
                "alerts".to_string(),
                CategorySummary {
                    count: 5,
                    priority: 3,
                },
            ),
            (
                "user_engagement".to_string(),
                CategorySummary {
                    count: 12,
                    priority: 1,
                },
            ),
            (
                "uncategorized".to_string(),
                CategorySummary {
                    count: 3,
                    priority: 2,
                },
            ),
        ]);
        send_msg!(ScanStatusUpdate::Complete {
            processed_emails: processed,
            elapsed_secs: started_at.elapsed().as_secs(),
            detailed_analysis: Some(DetailedAnalysis {
                category_analysis,
                extracted_tasks_count: 23,
                emails_with_tasks_count: 14,
            }),
        });

        if !repeat {
            let _ = sender.close().await;
            return;
        }

        // Wait 10 seconds before looping back to Started
        sleep(Duration::from_secs(10)).await;
    }
}
