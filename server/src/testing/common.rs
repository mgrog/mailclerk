use chrono::Utc;
use reqwest::Certificate;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::env;

use crate::{
    auth::jwt::Claims, email::client::EmailClient, model::user::UserCtrl, server_config::get_cert,
    HttpClient,
};

pub async fn setup() -> (DatabaseConnection, HttpClient) {
    dotenvy::dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let mut db_options = ConnectOptions::new(db_url);
    db_options.sqlx_logging(false);

    let conn = Database::connect(db_options)
        .await
        .expect("Database connection failed");

    let cert = get_cert();
    let http_client = reqwest::ClientBuilder::new()
        .use_rustls_tls()
        .add_root_certificate(Certificate::from_pem(&cert).unwrap())
        .build()
        .unwrap();
    (conn, http_client)
}

pub async fn setup_email_client(user_email: &str) -> EmailClient {
    let (conn, http_client) = setup().await;
    let user = UserCtrl::get_with_account_access_by_email(&conn, user_email)
        .await
        .unwrap();
    EmailClient::new(http_client, conn, user).await.unwrap()
}

pub fn get_test_user_claims(email: &str) -> Claims {
    Claims {
        sub: 1,
        email: email.to_string(),
        company: "mailclerk.io".to_string(),
        exp: Utc::now().timestamp() as usize + (5 * 60),
    }
}

pub fn dump_to_file(filename: &str, content: &str) {
    let debug_dir = format!("{}/debug", env!("CARGO_MANIFEST_DIR"));
    let _ = std::fs::create_dir_all(&debug_dir);
    let path = format!("{}/{}_{}.txt", debug_dir, filename, Utc::now().timestamp_millis());
    let _ = std::fs::write(&path, content);
}
