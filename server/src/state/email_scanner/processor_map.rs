use std::collections::HashSet;
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;

use super::processor::EmailProcessor;
use crate::model::user::UserWithAccountAccessAndUsage;
use crate::ServerState;

use super::processor::ProcessorStatus;

type EmailProcessorMap = HashMap<String, Arc<EmailProcessor>>;

#[derive(Clone)]
pub struct ActiveEmailProcessorMap {
    server_state: ServerState,
    active_processors: Arc<RwLock<EmailProcessorMap>>,
}

impl ActiveEmailProcessorMap {
    pub fn new(server_state: ServerState) -> Self {
        Self {
            server_state,
            active_processors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert_processor(
        &self,
        user: UserWithAccountAccessAndUsage,
    ) -> anyhow::Result<Arc<EmailProcessor>> {
        let user_email = user.email.clone();
        let last_updated_email_rules = user.last_updated_email_rules;

        if let Some(processor) = self.active_processors.read().unwrap().get(&user_email) {
            match processor.status() {
                ProcessorStatus::Cancelled
                | ProcessorStatus::Failed
                | ProcessorStatus::QuotaExceeded => {
                    tracing::info!("Recreating processor for {}", user_email);
                }
                _ if processor.current_token_usage() != user.tokens_consumed => {
                    tracing::info!(
                        "Token usage has changed, recreating processor for {}",
                        user_email
                    );
                    processor.cancel();
                }
                _ if processor.created_at < last_updated_email_rules => {
                    tracing::info!(
                        "Rules have changed, recreating processor for {}",
                        user_email
                    );
                    processor.cancel();
                }
                _ => {
                    tracing::info!("Processor for {} already exists", user_email);
                    return Ok(processor.clone());
                }
            };
        }

        let proc = Arc::new(
            EmailProcessor::new(self.server_state.clone(), user)
                .await
                .map_err(|e| anyhow!("Could not create email processor {:?}", e))?,
        );

        self.active_processors
            .write()
            .unwrap()
            .insert(user_email, proc.clone());

        self.get_current_state();

        Ok(proc)
    }

    pub fn get_current_state(&self) -> Option<String> {
        let active_processors = self.active_processors.read().unwrap();
        if active_processors.is_empty() {
            return None;
        }

        let mut display_str = format!("Active Processors:{}\n", active_processors.len());

        for (email, proc) in active_processors.iter() {
            let status = proc.get_current_state();
            display_str.push_str(&format!("\t{} -> {:?}\n", email, status));
        }

        Some(display_str)
    }

    pub fn entries(&self) -> Vec<(String, Arc<EmailProcessor>)> {
        self.active_processors
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn cleanup_processors(&self, set: HashSet<String>) {
        self.active_processors
            .write()
            .unwrap()
            .retain(|_, proc| !set.contains(&proc.email_address));

        self.get_current_state();
    }

    pub fn cancel_processor(&self, email_address: &str) {
        if let Some(processor) = self.active_processors.read().unwrap().get(email_address) {
            tracing::info!("Cancelling processor for {}", email_address);
            processor.cancel();
        } else {
            tracing::info!("No active processor found for {}", email_address);
        }
    }

    pub fn get(&self, email_address: &str) -> Option<Arc<EmailProcessor>> {
        self.active_processors
            .read()
            .unwrap()
            .get(email_address)
            .cloned()
    }

    pub fn total_emails_processed(&self) -> i64 {
        self.active_processors
            .read()
            .unwrap()
            .values()
            .map(|p| p.total_emails_processed())
            .sum()
    }

    pub fn len(&self) -> usize {
        self.active_processors.read().unwrap().len()
    }

    pub fn retain<F>(&self, f: F)
    where
        F: FnMut(&String, &mut Arc<EmailProcessor>) -> bool,
    {
        self.active_processors.write().unwrap().retain(f);
    }
}
