use std::env;

use dotenvy::dotenv;
use lib_utils::crypt;

pub type Result<T> = core::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>; // Ok for tools.

fn main() {
    dotenv().ok();
    let args: Vec<String> = env::args().collect();
    let content = args.get(1).expect("No content to decrypt provided!");
    let decrypted = crypt::decrypt(content).unwrap();
    println!("\nDecrypted:\n\n{}\n\n", decrypted);
}
