use std::sync::OnceLock;

use kitoken::Kitoken;

static TOKENIZER: OnceLock<Kitoken> = OnceLock::new();

fn get_tokenizer() -> &'static Kitoken {
    TOKENIZER.get_or_init(|| {
        let tokenizer_path = std::env::var("TOKENIZER_PATH")
            .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/tekken.json").to_string());
        Kitoken::from_file(&tokenizer_path).expect("Failed to load tokenizer")
    })
}

pub fn token_count(text: &str) -> Result<usize, kitoken::EncodeError> {
    let tokenizer = get_tokenizer();
    let tokens = tokenizer.encode(text, true)?;
    Ok(tokens.len())
}
