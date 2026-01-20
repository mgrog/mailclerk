//! Email embedding service for generating and storing embeddings

use anyhow::{Context, Result};
use entity::email_embedding;
use sea_orm::sea_query::{Expr, Query};
use sea_orm::{ConnectionTrait, DatabaseConnection};

use crate::{
    email::simplified_message::SimplifiedMessage, embed::chunker::chunk_email_text,
    rate_limiters::RateLimiters, server_config::cfg, util::format_vector, HttpClient,
};

use super::embed_text;

/// Result of embedding an email
#[derive(Debug)]
pub struct EmbeddingResult {
    pub chunks_created: usize,
    pub email_id: String,
}

/// Check if an email should be embedded based on rule priority
pub fn should_embed_email(rule_priority: i32) -> bool {
    rule_priority >= 2
}

/// Generate embeddings for a single email and store them in the database.
///
/// This function:
/// 1. Combines subject and body into embeddable text
/// 2. Splits into sentence chunks
/// 3. Generates embeddings for each chunk (with rate limiting)
/// 4. Stores all chunks in the database
pub async fn embed_and_store_email(
    http_client: &HttpClient,
    conn: &DatabaseConnection,
    rate_limiters: &RateLimiters,
    email: &SimplifiedMessage,
    user_id: i32,
) -> Result<EmbeddingResult> {
    // Combine subject and body for embedding
    let email_text = format!(
        "{} {}",
        email.subject.as_deref().unwrap_or(""),
        email.body.as_deref().unwrap_or("")
    );

    let chunks = chunk_email_text(&email_text);

    if chunks.is_empty() {
        return Ok(EmbeddingResult {
            chunks_created: 0,
            email_id: email.id.clone(),
        });
    }

    // Build custom insert query with proper halfvec casting
    let mut insert_stmt = Query::insert()
        .into_table(email_embedding::Entity)
        .columns([
            email_embedding::Column::EmailId,
            email_embedding::Column::UserId,
            email_embedding::Column::ChunkIndex,
            email_embedding::Column::Embedding,
            email_embedding::Column::ChunkText,
        ])
        .to_owned();

    let mut chunks_count = 0;

    for chunk in &chunks {
        // Rate limit embedding API calls
        rate_limiters.acquire_one().await;

        let embedding = embed_text(http_client, &chunk.text).await.context(format!(
            "Failed to embed chunk {} for email {}",
            chunk.index, email.id
        ))?;

        let embedding_str = format_vector(&embedding);
        let chunk_text: Option<String> = if cfg.settings.training_mode {
            Some(chunk.text.clone())
        } else {
            None
        };

        insert_stmt.values_panic([
            email.id.clone().into(),
            user_id.into(),
            (chunk.index as i32).into(),
            Expr::cust(format!("'{}'::halfvec", embedding_str)).into(),
            chunk_text.into(),
        ]);

        chunks_count += 1;
    }

    // Execute the batch insert
    let builder = conn.get_database_backend();
    conn.execute(builder.build(&insert_stmt))
        .await
        .context("Failed to insert email embeddings")?;

    Ok(EmbeddingResult {
        chunks_created: chunks_count,
        email_id: email.id.clone(),
    })
}

/// Delete all embeddings for an email (used when re-processing)
pub async fn delete_email_embeddings(conn: &DatabaseConnection, email_id: &str) -> Result<()> {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

    email_embedding::Entity::delete_many()
        .filter(email_embedding::Column::EmailId.eq(email_id))
        .exec(conn)
        .await
        .context("Failed to delete email embeddings")?;

    Ok(())
}
