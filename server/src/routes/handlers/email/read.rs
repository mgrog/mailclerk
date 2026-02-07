use axum::{
    extract::{Path, Query, State},
    response::Html,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    email::{
        client::{MessageFormat, ThreadListOptions},
        sanitized_message::{parsed_and_sanitized_gmail_thread, SanitizedMessage, SanitizedThread},
    },
    error::{AppError, AppJsonResult},
    ServerState,
};

use super::super::common::fetch_email_client;

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMessagesByIdsParams {
    /// User email to fetch messages for
    pub email: String,
    /// Comma-separated list of message IDs
    pub message_ids: String,
}

pub async fn get_messages_by_ids(
    State(state): State<ServerState>,
    Query(params): Query<GetMessagesByIdsParams>,
) -> AppJsonResult<Vec<google_gmail1::api::Message>> {
    let message_ids: Vec<String> = params
        .message_ids
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if message_ids.is_empty() {
        return Err(AppError::BadRequest("No message IDs provided".into()));
    }

    let email_client = fetch_email_client(state, params.email).await?;
    let messages = email_client
        .get_messages_by_ids(&message_ids, MessageFormat::Raw, None)
        .await?;

    Ok(Json(messages))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetMessageByIdParams {
    pub email: String,
}

pub async fn get_message_by_id(
    State(state): State<ServerState>,
    id: Path<String>,
    params: Query<GetMessageByIdParams>,
) -> Result<Html<String>, AppError> {
    let user_email = &params.email;
    let email_client = fetch_email_client(state, user_email.clone()).await?;
    let messages = email_client
        .get_messages_by_ids(&[id.as_str()], MessageFormat::Raw, None)
        .await?;

    let message = messages
        .into_iter()
        .filter_map(|m| SanitizedMessage::from_gmail_message(m).ok())
        .next()
        .ok_or_else(|| AppError::NotFound("Message not found".into()))?;

    Ok(Html(message.webview.unwrap()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAllEmailsQuery {
    /// Cursor for pagination (opaque string from previous response)
    pub cursor: Option<String>,
    /// Number of items to return (defaults to 20, max 100)
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAllEmailsResponse {
    pub threads: Vec<SanitizedThread>,
    /// Cursor for the next page, None if no more results
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// # GET /email/:id
///
/// Returns a single email message by ID as a SanitizedMessage
pub async fn get_sanitized_message(
    claims: Claims,
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> AppJsonResult<SanitizedMessage> {
    let email_client = fetch_email_client(state, claims.email).await?;
    let messages = email_client
        .get_messages_by_ids(&[id.as_str()], MessageFormat::Raw, None)
        .await?;

    let message = messages
        .into_iter()
        .filter_map(|m| SanitizedMessage::from_gmail_message(m).ok())
        .next()
        .ok_or_else(|| AppError::NotFound("Message not found".into()))?;

    Ok(Json(message))
}

/// # GET /email
///
/// Query parameters:
/// - `cursor`: Optional cursor for pagination (from previous response)
/// - `limit`: Optional number of items to return (default: 20, max: 100)
pub async fn get_all(
    claims: Claims,
    State(state): State<ServerState>,
    Query(query): Query<GetAllEmailsQuery>,
) -> AppJsonResult<GetAllEmailsResponse> {
    let user_email = &claims.email;
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let cursor = query.cursor;

    // Get the user's email client from the cache
    let email_client = fetch_email_client(state.clone(), user_email.clone()).await?;

    let options = ThreadListOptions {
        page_token: cursor,
        max_results: Some(limit + 1),
        ..Default::default()
    };
    let response = email_client.get_threads(options).await?;
    let list = response.threads.unwrap_or_default();

    // Get thread IDs for batching
    let ids: Vec<String> = list.iter().filter_map(|t| t.id.clone()).collect();

    // Batch fetch full threads with messages
    let threads = email_client.get_threads_by_ids(&ids).await?;

    let has_more = threads.len() > limit as usize;
    let threads_to_return: Vec<google_gmail1::api::Thread> = if has_more {
        threads[..limit as usize].to_vec()
    } else {
        threads
    };
    let next_cursor = if has_more {
        threads_to_return.last().and_then(|m| m.id.clone())
    } else {
        None
    };

    let threads: Vec<_> = threads_to_return
        .into_iter()
        .filter_map(|t| parsed_and_sanitized_gmail_thread(t).ok())
        .collect();

    Ok(Json(GetAllEmailsResponse {
        threads,
        next_cursor,
        has_more,
    }))
}
