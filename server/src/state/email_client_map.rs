use std::collections::HashMap;

use futures::future::join_all;
use sea_orm::DatabaseConnection;

use crate::email::client::EmailClient;
use crate::model::user::UserWithAccountAccessAndUsage;
use crate::HttpClient;

pub struct EmailClientMap {
    client_map: HashMap<String, EmailClient>,
}

impl EmailClientMap {
    pub fn builder() -> EmailClientMapBuilder {
        EmailClientMapBuilder::new()
    }

    pub fn get(&self, email: &str) -> Option<&EmailClient> {
        self.client_map.get(email)
    }

    pub fn get_mut(&mut self, email: &str) -> Option<&mut EmailClient> {
        self.client_map.get_mut(email)
    }

    pub fn insert(&mut self, email: String, client: EmailClient) {
        self.client_map.insert(email, client);
    }

    pub fn remove(&mut self, email: &str) -> Option<EmailClient> {
        self.client_map.remove(email)
    }

    pub fn contains(&self, email: &str) -> bool {
        self.client_map.contains_key(email)
    }

    pub fn len(&self) -> usize {
        self.client_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.client_map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &EmailClient)> {
        self.client_map.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut EmailClient)> {
        self.client_map.iter_mut()
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&String, &mut EmailClient) -> bool,
    {
        self.client_map.retain(f);
    }
}

pub struct EmailClientMapBuilder {
    http_client: Option<HttpClient>,
    conn: Option<DatabaseConnection>,
    users: Vec<UserWithAccountAccessAndUsage>,
}

impl EmailClientMapBuilder {
    pub fn new() -> Self {
        Self {
            http_client: None,
            conn: None,
            users: Vec::new(),
        }
    }

    pub fn with_http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }

    pub fn with_connection(mut self, conn: DatabaseConnection) -> Self {
        self.conn = Some(conn);
        self
    }

    pub fn with_users(mut self, users: Vec<UserWithAccountAccessAndUsage>) -> Self {
        self.users = users;
        self
    }

    pub async fn build(self) -> anyhow::Result<EmailClientMap> {
        let http_client = self
            .http_client
            .ok_or_else(|| anyhow::anyhow!("http_client is required"))?;
        let conn = self
            .conn
            .ok_or_else(|| anyhow::anyhow!("connection is required"))?;

        let client_futures = self.users.into_iter().map(|user| {
            let http_client = http_client.clone();
            let conn = conn.clone();
            async move {
                let email = user.email.clone();
                match EmailClient::new(http_client, conn, user).await {
                    Ok(client) => Some((email, client)),
                    Err(e) => {
                        tracing::error!("Failed to create email client for {}: {:?}", email, e);
                        None
                    }
                }
            }
        });

        let results = join_all(client_futures).await;
        let client_map: HashMap<String, EmailClient> = results.into_iter().flatten().collect();

        Ok(EmailClientMap { client_map })
    }
}

impl Default for EmailClientMapBuilder {
    fn default() -> Self {
        Self::new()
    }
}
