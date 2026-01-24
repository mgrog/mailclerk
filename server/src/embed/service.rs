//! Email embedding service for generating and storing embeddings

use anyhow::{Context, Result};

use crate::{server_config::cfg, HttpClient};

use serde_json::json;

/// Embed multiple texts in a single batch API call to Mistral
pub async fn embed_batch(http_client: &HttpClient, texts: &[String]) -> Result<Vec<Vec<f32>>> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    let resp = http_client
        .post("https://api.mistral.ai/v1/embeddings")
        .bearer_auth(&cfg.api.key)
        .json(&json!({
            "model": "mistral-embed",
            "input": texts,
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let data = resp["data"]
        .as_array()
        .context("No data array in embedding response")?;

    let embeddings: Vec<Vec<f32>> = data
        .iter()
        .map(|item| {
            serde_json::from_value(item["embedding"].clone()).context("Failed to parse embedding")
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(embeddings)
}

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
