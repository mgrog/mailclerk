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
    let inner = v
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
}

/// Format a message as a fixed-width banner: `--------MESSAGE--------`
/// Total width is always 70 characters. Messages longer than 64 chars are truncated.
pub fn banner(msg: &str) -> String {
    const TOTAL_WIDTH: usize = 70;
    const MIN_DASHES: usize = 2;
    const PADDING: usize = 1;
    let max_msg_len = TOTAL_WIDTH - (MIN_DASHES * 2) - (PADDING * 2);

    let msg = if msg.len() > max_msg_len {
        &msg[..max_msg_len]
    } else {
        msg
    };

    let remaining = TOTAL_WIDTH - msg.len();
    let left = remaining / 2;
    let right = remaining - left;

    format!(
        "{}- {} -{}",
        "-".repeat(left - 1 - PADDING),
        msg,
        "-".repeat(right - 1 - PADDING)
    )
}
