pub mod chunker;
pub mod service;


use crate::server_config::cfg;
use anyhow::Context;
use serde_json::json;

use crate::HttpClient;

async fn get_category_embeds(
    http_client: &HttpClient,
) -> std::collections::HashMap<&'static str, Vec<f32>> {
    let mut m = std::collections::HashMap::new();
    // Add known model -> embedding dimension pairs here
    m.insert(
        "billing",
        embed_text(
            http_client,
            "Invoices, subscription renewals, payment issues",
        )
        .await
        .unwrap(),
    );
    m.insert(
        "meetings",
        embed_text(
            http_client,
            "Scheduling meetings, calendar invites, availability",
        )
        .await
        .unwrap(),
    );

    m.insert(
        "work",
        embed_text(http_client, "Project updates, tasks, reports")
            .await
            .unwrap(),
    );

    m.insert(
        "marketing",
        embed_text(
            http_client,
            "Product announcements, promotions, newsletters",
        )
        .await
        .unwrap(),
    );

    m
}

pub async fn embed_text(http_client: &HttpClient, s: &str) -> anyhow::Result<Vec<f32>> {
    let resp = http_client
        .post("https://api.mistral.ai/v1/embeddings")
        .bearer_auth(&cfg.api.key)
        .json(&json!(
          {
            "model": "mistral-embed",
            "input": s,
          }
        ))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let data = resp["data"].as_array().context("No data array")?;
    let first = data.first().context("No first element")?;
    let embedding_value = &first["embedding"];
    let embedding: Vec<f32> = serde_json::from_value(embedding_value.clone())
        .context("Failed to parse embedding as Vec<f32>")?;
    Ok(embedding)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (norm_a * norm_b)
}

const MIN_SIMILARITY: f32 = 0.30; // Reject weak matches
const MIN_MARGIN: f32 = 0.05; // Reject ambiguous matches

#[derive(Debug)]
pub struct ClassificationResult {
    label: String,
    score: f32,
    confident: bool,
}

pub async fn categorize_email_with_confidence(
    http_client: &HttpClient,
    email_text: &str,
) -> anyhow::Result<ClassificationResult> {
    let categories = get_category_embeds(http_client).await;
    let email_vec = embed_text(http_client, email_text)
        .await
        .context("Embedding failed!")?;

    let mut scores: Vec<(&str, f32)> = categories
        .iter()
        .map(|(label, vec)| {
            let score = cosine_similarity(&email_vec, vec);
            (*label, score)
        })
        .collect();

    // Sort by descending similarity
    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let (best_label, best_score) = scores[0];
    let second_score = scores.get(1).map(|s| s.1).unwrap_or(0.0);

    let confident = best_score >= MIN_SIMILARITY && (best_score - second_score) >= MIN_MARGIN;

    Ok(ClassificationResult {
        label: if confident {
            best_label.to_string()
        } else {
            "uncategorized".to_string()
        },
        score: best_score,
        confident,
    })
}
