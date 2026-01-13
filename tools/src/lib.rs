pub async fn embed(http_client: &reqwest::Client, s: &str) -> anyhow::Result<Vec<f32>> {
    let api_key = std::env::var("MISTRAL_API_KEY").expect("MISTRAL_API_KEY not set");
    let resp = http_client
        .post("https://api.mistral.ai/v1/embeddings")
        .bearer_auth(&api_key)
        .json(&serde_json::json!({
            "model": "mistral-embed",
            "input": s,
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let data = resp["data"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("No data array"))?;
    let first = data
        .first()
        .ok_or_else(|| anyhow::anyhow!("No first element"))?;
    let embedding_value = &first["embedding"];
    let embedding: Vec<f32> = serde_json::from_value(embedding_value.clone())?;
    Ok(embedding)
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (norm_a * norm_b)
}
