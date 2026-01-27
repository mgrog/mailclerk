use crate::ServerState;
use anyhow::Context;
use chrono::Duration as ChronoDuration;
use tokio::time::{timeout, Duration};

/// Cache idle timeout - remove clients not used within this duration
const CACHE_IDLE_TIMEOUT_HOURS: i64 = 8;

pub async fn email_client_cleanup(state: ServerState) -> anyhow::Result<()> {
    let mut cache = timeout(Duration::from_secs(5), state.email_client_cache.write())
        .await
        .context("Timed out acquiring email_client_cache lock")?;

    let now = chrono::Utc::now();
    let idle_threshold = now - ChronoDuration::hours(CACHE_IDLE_TIMEOUT_HOURS);

    // Keep client if: token is not expired AND client was used recently
    cache.retain(|_email, client| {
        let token_valid = client.expires_at > now;
        let recently_used = client.last_used() > idle_threshold;
        token_valid && recently_used
    });

    Ok(())
}
