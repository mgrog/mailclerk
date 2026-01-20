use chrono::{Duration, Utc};
use sea_orm::prelude::DateTimeWithTimeZone;

const EXPIRY_MARGIN_SECS: i64 = 30;

/// Simple HTML escaping for metadata strings
pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn check_expired(expires_at: DateTimeWithTimeZone) -> bool {
    let now_with_margin = Utc::now().fixed_offset() + Duration::seconds(EXPIRY_MARGIN_SECS);
    now_with_margin > expires_at.fixed_offset()
}

/// Format a vector as a pgvector-compatible string: [0.1,0.2,...]
pub fn format_vector<T: std::fmt::Display>(v: &[T]) -> String {
    let inner = v.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",");
    format!("[{inner}]")
}
