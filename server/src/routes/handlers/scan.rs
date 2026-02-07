use std::{collections::HashMap, time::Duration};

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
    auth::jwt::Claims,
    error::AppError,
    model::user::UserCtrl,
    observability::ScanPhase,
    state::email_scanner::initial_scan::{run_initial_scan_for_user, CategorySummary, ScanResult},
    ServerState,
};

/// Detailed analysis results from the scan.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailedAnalysis {
    pub category_analysis: HashMap<String, CategorySummary>,
    pub extracted_tasks_count: usize,
    pub emails_with_tasks_count: usize,
}

/// Status update sent over WebSocket
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ScanStatusUpdate {
    #[serde(rename_all = "camelCase")]
    Started {
        user_id: i32,
        user_email: String,
    },
    #[serde(rename_all = "camelCase")]
    Progress {
        phase: String,
        fetched_emails: usize,
        total_to_fetch: usize,
        total_to_scan: usize,
        processed_emails: usize,
        percentage: f32,
        elapsed_secs: u64,
    },
    #[serde(rename_all = "camelCase")]
    Complete {
        processed_emails: usize,
        elapsed_secs: u64,
        detailed_analysis: Option<DetailedAnalysis>,
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

    // Spawn the scan task with a channel to communicate the result
    let (result_tx, mut result_rx) =
        tokio::sync::oneshot::channel::<Result<Option<ScanResult>, String>>();
    let scan_state = state.clone();
    let scan_user = user.clone();
    let scan_tracker_clone = scan_tracker.clone();
    tokio::spawn(async move {
        let result = run_initial_scan_for_user(scan_state, scan_user, scan_tracker_clone).await;
        let send_result = match result {
            Ok(scan_result) => Ok(scan_result),
            Err(e) => {
                tracing::error!("Initial scan failed for user {}: {:?}", user_id, e);
                Err(e.to_string())
            }
        };
        let _ = result_tx.send(send_result);
    });

    // Poll for status updates
    let poll_tracker = scan_tracker.clone();
    let status_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let mut last_phase: Option<String> = None;
        let mut last_processed: usize = 0;
        let mut last_elapsed_secs: u64 = 0;

        loop {
            tokio::select! {
                // Check if the scan task completed (success or failure)
                result = &mut result_rx => {
                    let final_msg = match result {
                        Ok(Ok(scan_result)) => ScanStatusUpdate::Complete {
                            processed_emails: last_processed,
                            elapsed_secs: last_elapsed_secs,
                            detailed_analysis: scan_result.map(|r| DetailedAnalysis {
                                category_analysis: r.category_summary,
                                extracted_tasks_count: r.extracted_tasks_count,
                                emails_with_tasks_count: r.emails_with_tasks_count,
                            }),
                        },
                        Ok(Err(error)) => ScanStatusUpdate::Failed { error },
                        Err(_) => ScanStatusUpdate::Failed {
                            error: "Scan task was cancelled".to_string(),
                        },
                    };

                    let _ = sender
                        .send(Message::Text(serde_json::to_string(&final_msg).unwrap()))
                        .await;
                    let _ = sender.close().await;
                    break;
                }

                // Poll for progress updates
                _ = interval.tick() => {
                    if let Some(entry) = poll_tracker.get_scan(user_id) {
                        let current_phase = format!("{}", entry.phase);
                        let current_processed = entry.scan_progress.current;

                        // Only send update if something changed
                        if last_phase.as_ref() != Some(&current_phase)
                            || last_processed != current_processed
                        {
                            last_phase = Some(current_phase.clone());
                            last_processed = current_processed;
                            last_elapsed_secs = entry.elapsed_secs();

                            let update = ScanStatusUpdate::Progress {
                                phase: entry.phase.short_name().to_string(),
                                fetched_emails: entry.fetch_progress.current,
                                total_to_fetch: entry.fetch_progress.total,
                                total_to_scan: entry.scan_progress.total,
                                processed_emails: entry.scan_progress.current,
                                percentage: entry.scan_progress.percentage(),
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
                    }
                }
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
