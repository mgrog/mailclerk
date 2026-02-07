use crate::{email::client::EmailClient, model::user::UserCtrl, util::check_expired, ServerState};

pub(super) async fn fetch_email_client(
    ServerState {
        email_client_cache,
        http_client,
        conn,
        ..
    }: ServerState,
    user_email: String,
) -> anyhow::Result<EmailClient> {
    // Try to get from cache with read lock - touch() is atomic so no write lock needed
    {
        let cache = email_client_cache.read().await;
        if let Some(client) = cache.get(&user_email) {
            if !check_expired(client.expires_at) {
                client.touch();
                return Ok(client.clone());
            }
        }
    }

    // Client missing or expired - create new one and insert with write lock
    let user = UserCtrl::get_with_account_access_by_email(&conn, &user_email).await?;
    let client = EmailClient::new(http_client, conn, user).await?;

    let mut cache = email_client_cache.write().await;
    cache.insert(user_email, client.clone());

    Ok(client)
}
