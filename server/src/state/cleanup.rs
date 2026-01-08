use crate::ServerState;
use anyhow::Context;
use tokio::time::{timeout, Duration};

pub async fn email_client_cleanup(state: ServerState) -> anyhow::Result<()> {
    let mut cache = timeout(Duration::from_secs(5), state.email_client_cache.write())
        .await
        .context("Timed out acquiring email_client_cache lock")?;

    cache.retain(|_email, client| client.expires_at > chrono::Utc::now());

    Ok(())
}
