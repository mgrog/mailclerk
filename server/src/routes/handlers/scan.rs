use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde::Serialize;

use crate::{
    auth::jwt::Claims, error::AppError, model::user::UserCtrl, observability::ScanPhase,
    state::email_scanner::initial_scan::run_initial_scan_for_user, ServerState,
};

/// Status update sent over WebSocket
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ScanStatusUpdate {
    Started {
        user_id: i32,
        user_email: String,
    },
    Progress {
        phase: String,
        total_emails: usize,
        processed_emails: usize,
        percentage: f32,
        elapsed_secs: u64,
    },
    Complete {
        total_emails: usize,
        elapsed_secs: u64,
    },
    Failed {
        error: String,
    },
    Error {
        message: String,
    },
}

/// WebSocket handler for starting an initial scan with real-time status updates.
pub async fn start_initial_scan_ws(
    ws: WebSocketUpgrade,
    claims: Claims,
    State(state): State<ServerState>,
) -> Result<impl IntoResponse, AppError> {
    let user = UserCtrl::get_with_account_access_and_usage_by_email(&state.conn, &claims.email)
        .await
        .map_err(|_| AppError::NotFound("User not found".to_string()))?;

    Ok(ws.on_upgrade(move |socket| handle_scan_socket(socket, state, user)))
}

async fn handle_scan_socket(
    socket: WebSocket,
    state: ServerState,
    user: crate::model::user::UserWithAccountAccessAndUsage,
) {
    let (mut sender, mut receiver) = socket.split();

    let user_id = user.id;
    let user_email = user.email.clone();
    let scan_tracker = state.scan_tracker.clone();

    // Send initial started message
    let started_msg = ScanStatusUpdate::Started {
        user_id,
        user_email: user_email.clone(),
    };
    if let Err(e) = sender
        .send(Message::Text(serde_json::to_string(&started_msg).unwrap()))
        .await
    {
        tracing::error!("Failed to send started message: {:?}", e);
        return;
    }

    // Spawn the scan task
    let scan_state = state.clone();
    let scan_user = user.clone();
    let scan_tracker_clone = scan_tracker.clone();
    tokio::spawn(async move {
        if let Err(e) = run_initial_scan_for_user(scan_state, scan_user, scan_tracker_clone).await {
            tracing::error!("Initial scan failed for user {}: {:?}", user_id, e);
        }
    });

    // Poll for status updates
    let poll_tracker = scan_tracker.clone();
    let status_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let mut last_phase: Option<String> = None;
        let mut last_processed: usize = 0;

        loop {
            interval.tick().await;

            if let Some(entry) = poll_tracker.get_scan(user_id) {
                let current_phase = format!("{}", entry.phase);
                let current_processed = entry.progress.processed_emails;

                // Only send update if something changed
                if last_phase.as_ref() != Some(&current_phase)
                    || last_processed != current_processed
                {
                    last_phase = Some(current_phase.clone());
                    last_processed = current_processed;

                    let update = ScanStatusUpdate::Progress {
                        phase: entry.phase.short_name().to_string(),
                        total_emails: entry.progress.total_emails,
                        processed_emails: entry.progress.processed_emails,
                        percentage: entry.progress.percentage(),
                        elapsed_secs: entry.elapsed_secs(),
                    };

                    if let Err(e) = sender
                        .send(Message::Text(serde_json::to_string(&update).unwrap()))
                        .await
                    {
                        tracing::error!("Failed to send progress update: {:?}", e);
                        break;
                    }
                }
            } else {
                // Scan is no longer being tracked - it's complete or failed
                // Check if there was a phase set before removal
                let final_msg = if last_phase.as_deref() == Some("Failed") {
                    ScanStatusUpdate::Failed {
                        error: "Scan failed".to_string(),
                    }
                } else {
                    ScanStatusUpdate::Complete {
                        total_emails: last_processed,
                        elapsed_secs: 0, // We don't have this info after removal
                    }
                };

                let _ = sender
                    .send(Message::Text(serde_json::to_string(&final_msg).unwrap()))
                    .await;
                let _ = sender.close().await;
                break;
            }
        }
    });

    // Handle incoming messages (for client-side close)
    tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Close(_)) => {
                    tracing::info!("Client closed WebSocket connection");
                    break;
                }
                Err(e) => {
                    tracing::error!("WebSocket error: {:?}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for the status task to complete
    let _ = status_task.await;
}

// Helper trait to expose short_name publicly
trait ScanPhaseExt {
    fn short_name(&self) -> &str;
}

impl ScanPhaseExt for ScanPhase {
    fn short_name(&self) -> &str {
        match self {
            ScanPhase::Fetching => "Fetching",
            ScanPhase::Categorizing { .. } => "Categorizing",
            ScanPhase::ExtractingTasks { .. } => "Extracting",
            ScanPhase::Inserting => "Inserting",
            ScanPhase::Complete => "Complete",
            ScanPhase::Failed { .. } => "Failed",
        }
    }
}
