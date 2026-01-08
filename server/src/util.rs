use chrono::{Duration, Utc};
use sea_orm::prelude::DateTimeWithTimeZone;

const EXPIRY_MARGIN_SECS: i64 = 30;

pub fn check_expired(expires_at: DateTimeWithTimeZone) -> bool {
    let now_with_margin = Utc::now().fixed_offset() + Duration::seconds(EXPIRY_MARGIN_SECS);
    now_with_margin > expires_at.fixed_offset()
}
