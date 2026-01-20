//! Email embedding model and search functionality

use anyhow::Result;
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::Serialize;

use crate::util::format_vector;

pub struct EmailEmbeddingCtrl;

/// Search result containing email ID and similarity score
#[derive(Debug, Clone, FromQueryResult, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub email_id: String,
    pub chunk_text: String,
    pub similarity: f64,
}

/// Aggregated search result grouped by email
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailSearchResult {
    pub email_id: String,
    pub best_similarity: f64,
    pub matching_chunks: Vec<ChunkMatch>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkMatch {
    pub text: String,
    pub similarity: f64,
}

impl EmailEmbeddingCtrl {
    /// Search for similar emails using vector cosine similarity.
    ///
    /// This performs a per-user scoped search using pgvector's cosine distance operator.
    /// Results are grouped by email_id and sorted by best match.
    pub async fn search_by_similarity(
        conn: &DatabaseConnection,
        user_id: i32,
        query_embedding: &[f32],
        limit: u32,
        min_similarity: f64,
    ) -> Result<Vec<EmailSearchResult>> {
        let vector = format_vector(query_embedding);

        // Use raw SQL for pgvector cosine similarity search
        // 1 - (embedding <=> query) converts distance to similarity
        let sql = format!(
            r#"
            SELECT
                email_id,
                chunk_text,
                1 - (embedding <=> '{vector}'::halfvec) as similarity
            FROM email_embedding
            WHERE user_id = {user_id}
                AND 1 - (embedding <=> '{vector}'::halfvec) >= {min_similarity}
            ORDER BY similarity DESC
            LIMIT {limit_expanded}
            "#,
            vector = vector,
            user_id = user_id,
            min_similarity = min_similarity,
            limit_expanded = limit * 3, // Get extra for grouping
        );

        let results: Vec<SearchResult> = conn
            .query_all(Statement::from_string(
                sea_orm::DatabaseBackend::Postgres,
                sql,
            ))
            .await?
            .into_iter()
            .filter_map(|row| SearchResult::from_query_result(&row, "").ok())
            .collect();

        // Group results by email_id
        let mut email_map: std::collections::HashMap<String, EmailSearchResult> =
            std::collections::HashMap::new();

        for result in results {
            email_map
                .entry(result.email_id.clone())
                .and_modify(|e| {
                    if result.similarity > e.best_similarity {
                        e.best_similarity = result.similarity;
                    }
                    e.matching_chunks.push(ChunkMatch {
                        text: result.chunk_text.clone(),
                        similarity: result.similarity,
                    });
                })
                .or_insert(EmailSearchResult {
                    email_id: result.email_id,
                    best_similarity: result.similarity,
                    matching_chunks: vec![ChunkMatch {
                        text: result.chunk_text,
                        similarity: result.similarity,
                    }],
                });
        }

        // Sort by best similarity and take limit
        let mut results: Vec<EmailSearchResult> = email_map.into_values().collect();
        results.sort_by(|a, b| b.best_similarity.partial_cmp(&a.best_similarity).unwrap());
        results.truncate(limit as usize);

        Ok(results)
    }
}
