//! Email semantic search endpoint

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::{
    auth::jwt::Claims,
    embed::embed_text,
    error::{AppError, AppJsonResult},
    model::email_embedding::{EmailEmbeddingCtrl, EmailSearchResult},
    ServerState,
};

const DEFAULT_SEARCH_LIMIT: u32 = 10;
const MAX_SEARCH_LIMIT: u32 = 50;
const DEFAULT_MIN_SIMILARITY: f64 = 0.3;
const MIN_QUERY_LENGTH: usize = 3;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchQuery {
    /// The search query string
    pub q: String,
    /// Maximum number of results (default: 10, max: 50)
    pub limit: Option<u32>,
    /// Minimum similarity threshold (default: 0.3)
    pub min_similarity: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponse {
    pub results: Vec<EmailSearchResult>,
    pub query: String,
    pub total: usize,
}

/// # GET /email/search
///
/// Search emails using semantic similarity.
///
/// Query parameters:
/// - `q`: Search query string (required, min 3 characters)
/// - `limit`: Maximum results to return (default: 10, max: 50)
/// - `min_similarity`: Minimum similarity threshold 0-1 (default: 0.3)
///
/// Returns emails matching the query, sorted by similarity score.
/// Only searches within the authenticated user's emails.
pub async fn search(
    claims: Claims,
    State(state): State<ServerState>,
    axum::extract::Query(query): axum::extract::Query<SearchQuery>,
) -> AppJsonResult<SearchResponse> {
    let user_id = claims.sub;

    // Validate query
    if query.q.trim().len() < MIN_QUERY_LENGTH {
        return Err(AppError::BadRequest(format!(
            "Query must be at least {} characters",
            MIN_QUERY_LENGTH
        )));
    }

    let limit = query
        .limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .min(MAX_SEARCH_LIMIT);
    let min_similarity = query
        .min_similarity
        .unwrap_or(DEFAULT_MIN_SIMILARITY)
        .clamp(0.0, 1.0);

    // Embed the search query
    let query_embedding = embed_text(&state.http_client, query.q.trim())
        .await
        .map_err(|e| AppError::Internal(e.context("Failed to embed search query")))?;

    // Search for similar emails
    let results = EmailEmbeddingCtrl::search_by_similarity(
        &state.conn,
        user_id,
        &query_embedding,
        limit,
        min_similarity,
    )
    .await
    .map_err(|e| AppError::Internal(e.context("Search failed")))?;

    let total = results.len();

    Ok(Json(SearchResponse {
        results,
        query: query.q,
        total,
    }))
}
