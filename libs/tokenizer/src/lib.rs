use std::sync::OnceLock;

pub use kitoken::{Kitoken, TokenId};

static TOKENIZER: OnceLock<Kitoken> = OnceLock::new();

pub fn get_tokenizer() -> &'static Kitoken {
    TOKENIZER.get_or_init(|| {
        let tokenizer_path = std::env::var("TOKENIZER_PATH")
            .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/tekken.json").to_string());
        Kitoken::from_file(&tokenizer_path).expect("Failed to load tokenizer")
    })
}

pub fn encode(text: &str) -> Result<Vec<TokenId>, kitoken::EncodeError> {
    let tokenizer = get_tokenizer();
    tokenizer.encode(text, true)
}

pub fn token_count(text: &str) -> Result<usize, kitoken::EncodeError> {
    let tokenizer = get_tokenizer();
    let tokens = tokenizer.encode(text, true)?;
    Ok(tokens.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_count() {
        let count = token_count("Hello, world!").unwrap();
        assert_eq!(count, 4);
    }
}
