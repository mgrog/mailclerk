use axum::{
    extract::{Query, State},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    email::{
        client::MessageFormat,
        sanitized_message::SanitizedMessage,
    },
    error::{AppError, AppJsonResult},
    model::processed_email::{FeedCursor, FeedEmail, ProcessedEmailCtrl},
    ServerState,
};

use super::shared::fetch_email_client;

const DEFAULT_FEED_PAGE_SIZE: u64 = 20;
const MAX_FEED_PAGE_SIZE: u64 = 100;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFeedQuery {
    /// Number of items to return (defaults to 50, max 100)
    pub limit: Option<u64>,
    /// Base64-encoded cursor for pagination
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedResponseItem {
    #[serde(flatten)]
    pub feed_email: FeedEmail,
    pub message: Option<SanitizedMessage>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFeedResponse {
    pub emails: Vec<FeedResponseItem>,
    /// Base64-encoded cursor for the next page, None if no more results
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// # GET /email/feed
///
/// Returns processed emails sorted by weighted priority:
/// 1. Due date urgency (unless tasks_done or 7+ days overdue)
/// 2. New replies on unread emails
/// 3. Unread emails by category priority
/// 4. Everything else
///
/// Query parameters:
/// - `limit`: Optional number of items to return (default: 20, max: 100)
/// - `cursor`: Optional base64-encoded cursor for pagination
pub async fn get_feed(
    claims: Claims,
    State(state): State<ServerState>,
    Query(query): Query<GetFeedQuery>,
) -> AppJsonResult<GetFeedResponse> {
    let user_id = claims.sub;
    let user_email = claims.email;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_FEED_PAGE_SIZE)
        .min(MAX_FEED_PAGE_SIZE);

    // Decode cursor if provided
    let cursor = query
        .cursor
        .map(|c| {
            let decoded = URL_SAFE_NO_PAD
                .decode(&c)
                .map_err(|_| AppError::BadRequest("Invalid cursor".into()))?;
            serde_json::from_slice::<FeedCursor>(&decoded)
                .map_err(|_| AppError::BadRequest("Invalid cursor format".into()))
        })
        .transpose()?;

    // Fetch one extra to determine if there are more results
    let emails =
        ProcessedEmailCtrl::get_feed_by_user(&state.conn, user_id, limit + 1, cursor).await?;

    let has_more = emails.len() > limit as usize;
    let emails: Vec<FeedEmail> = if has_more {
        emails.into_iter().take(limit as usize).collect()
    } else {
        emails
    };

    // Generate next cursor from the last item
    let next_cursor = if has_more {
        emails.last().map(|email| {
            let cursor = FeedCursor {
                score: email.priority_score,
                history_id: email.history_id,
            };
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap())
        })
    } else {
        None
    };

    let email_ids: Vec<&str> = emails.iter().map(|e| e.id.as_str()).collect();
    let email_client = fetch_email_client(state, user_email).await?;
    let messages = email_client
        .get_messages_by_ids(&email_ids, MessageFormat::Raw)
        .await?;
    let emails = emails
        .into_iter()
        .zip(messages)
        .map(|(e, m)| FeedResponseItem {
            feed_email: e,
            message: SanitizedMessage::from_gmail_message(m).ok(),
        })
        .collect();

    Ok(Json(GetFeedResponse {
        emails,
        next_cursor,
        has_more,
    }))
}
