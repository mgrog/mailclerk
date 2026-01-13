use std::env;
use std::io::{self, BufRead, Write};

use tokenizer::Kitoken;

fn main() {
    let tokenizer = tokenizer::get_tokenizer();

    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        // Process command line arguments
        let text = args[1..].join(" ");
        test_text(tokenizer, &text);
    } else {
        // Interactive mode - read from stdin
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        for line in stdin.lock().lines() {
            match line {
                Ok(text) if !text.is_empty() => {
                    test_text(tokenizer, &text);
                    stdout.flush().unwrap();
                }
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Error reading input: {}", e);
                    break;
                }
            }
        }
    }
}

fn test_text(tokenizer: &Kitoken, text: &str) {
    match tokenizer.encode(text, true) {
        Ok(tokens) => {
            println!("Text: {}", text);
            println!("Token count: {}", tokens.len());
            println!("Tokens: {:?}", tokens);
        }
        Err(e) => {
            eprintln!("Error encoding text: {:?}", e);
        }
    }
}
