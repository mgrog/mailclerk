use axum::{
    extract::State,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    error::AppJsonResult,
    model::processed_email::ProcessedEmailCtrl,
    ServerState,
};

use super::super::common::fetch_email_client;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkAsReadBody {
    pub message_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkAsReadResponse {
    pub id: String,
    pub is_read: bool,
}

/// # PUT /email/mark-as-read
///
/// Marks an email as read in Gmail and updates the processed_email record.
///
/// Request body:
/// - `messageId`: The Gmail message ID to mark as read
pub async fn mark_as_read(
    claims: Claims,
    State(state): State<ServerState>,
    Json(body): Json<MarkAsReadBody>,
) -> AppJsonResult<MarkAsReadResponse> {
    let user_id = claims.sub;
    let user_email = claims.email;

    // Mark as read in Gmail
    let email_client = fetch_email_client(state.clone(), user_email).await?;
    email_client.mark_email_as_read(&body.message_id).await?;

    // Update processed_email record in database
    ProcessedEmailCtrl::mark_as_read(&state.conn, &body.message_id, user_id).await?;

    Ok(Json(MarkAsReadResponse {
        id: body.message_id,
        is_read: true,
    }))
}

/// # PUT /email/mark-as-unread
///
/// Marks an email as unread in Gmail and updates the processed_email record.
///
/// Request body:
/// - `messageId`: The Gmail message ID to mark as unread
pub async fn mark_as_unread(
    claims: Claims,
    State(state): State<ServerState>,
    Json(body): Json<MarkAsReadBody>,
) -> AppJsonResult<MarkAsReadResponse> {
    let user_id = claims.sub;
    let user_email = claims.email;

    // Mark as unread in Gmail
    let email_client = fetch_email_client(state.clone(), user_email).await?;
    email_client.mark_email_as_unread(&body.message_id).await?;

    // Update processed_email record in database
    ProcessedEmailCtrl::mark_as_unread(&state.conn, &body.message_id, user_id).await?;

    Ok(Json(MarkAsReadResponse {
        id: body.message_id,
        is_read: false,
    }))
}
