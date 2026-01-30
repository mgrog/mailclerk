pub mod scanner;
pub mod server;

pub use scanner::ScannerRouter;
pub use server::ServerRouter;

use axum::{http::StatusCode, response::IntoResponse};

pub async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Route does not exist")
}
