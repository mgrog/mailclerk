use itertools::Itertools;
use std::env;
use tools::{cosine_similarity, embed};

fn similarity_label(sim: f32) -> &'static str {
    match sim {
        s if s >= 0.7 => "HIGH",
        s if s >= 0.4 => "AVERAGE",
        s if s >= 0.2 => "LOW",
        _ => "DISSIMILAR",
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().skip(1).collect();

    if args.len() < 2 {
        eprintln!("Usage: cosine-similarity <word1> <word2> [word3] ...");
        std::process::exit(1);
    }

    let http_client = reqwest::Client::new();

    let mut embeddings = Vec::with_capacity(args.len());
    for word in &args {
        match embed(&http_client, word).await {
            Ok(emb) => embeddings.push(emb),
            Err(e) => {
                eprintln!("Error embedding '{}': {:?}", word, e);
                std::process::exit(1);
            }
        }
    }

    println!("Pairwise cosine similarities:\n");

    let pairs: Vec<_> = (0..args.len()).combinations(2).collect();
    let similarities: Vec<f32> = pairs
        .iter()
        .map(|pair| cosine_similarity(&embeddings[pair[0]], &embeddings[pair[1]]))
        .collect();

    for (pair, sim) in pairs.iter().zip(&similarities) {
        println!(
            "  {} <-> {}: {:.4} [{}]",
            args[pair[0]],
            args[pair[1]],
            sim,
            similarity_label(*sim)
        );
    }

    let avg = similarities.iter().sum::<f32>() / similarities.len() as f32;
    println!("\nAverage similarity: {:.4} [{}]", avg, similarity_label(avg));
}
