use std::sync::atomic::Ordering::Relaxed;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::time::Duration;

use leaky_bucket::RateLimiter;

use crate::server_config::cfg;

#[derive(Clone)]
pub struct RateLimiters {
    prompt: Arc<RateLimiter>,
    tokens: Arc<RateLimiter>,
    backoff: Arc<AtomicBool>,
    backoff_duration: Duration,
}

impl RateLimiters {
    pub fn new(
        prompt_limit_per_sec: usize,
        prompt_interval_ms: usize,
        prompt_refill: usize,
        token_limit_per_min: usize,
        token_interval_ms: usize,
        token_refill: usize,
    ) -> Self {
        let prompt = RateLimiter::builder()
            .initial(1)
            .interval(Duration::from_millis(prompt_interval_ms as u64))
            .max(prompt_limit_per_sec)
            .refill(prompt_refill)
            .build();

        let tokens = RateLimiter::builder()
            .initial(token_limit_per_min / 2) // Start with half capacity to avoid burst
            .interval(Duration::from_millis(token_interval_ms as u64))
            .max(token_limit_per_min)
            .refill(token_refill)
            .build();

        Self {
            prompt: Arc::new(prompt),
            tokens: Arc::new(tokens),
            backoff: Arc::new(AtomicBool::new(false)),
            backoff_duration: Duration::from_secs(60),
        }
    }

    pub fn from_env() -> Self {
        let prompt_limit_per_sec = cfg.api.prompt_limits.rate_limit_per_sec;
        let prompt_interval_ms = cfg.api.prompt_limits.refill_interval_ms;
        let prompt_refill = cfg.api.prompt_limits.refill_amount;
        let token_limit_per_min = cfg.api.token_limits.rate_limit_per_min;
        let token_interval_ms = cfg.api.token_limits.refill_interval_ms;
        let token_refill = cfg.api.token_limits.refill_amount;
        Self::new(
            prompt_limit_per_sec,
            prompt_interval_ms,
            prompt_refill,
            token_limit_per_min,
            token_interval_ms,
            token_refill,
        )
    }

    pub async fn acquire_one(&self) {
        if self.backoff.load(Relaxed) {
            tokio::time::sleep(self.backoff_duration).await;
        }
        self.prompt.acquire_one().await;
    }

    /// Acquire tokens for estimated token usage before making an API call
    pub async fn acquire_tokens(&self, estimated_tokens: usize) {
        if self.backoff.load(Relaxed) {
            tokio::time::sleep(self.backoff_duration).await;
        }
        self.tokens.acquire(estimated_tokens).await;
    }

    pub fn trigger_backoff(&self) {
        tracing::info!("Triggering backoff...");
        self.backoff
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let self_ = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            tracing::info!("Backoff expired");
            self_
                .backoff
                .store(false, std::sync::atomic::Ordering::Relaxed);
        });
    }

    pub fn get_status(&self) -> String {
        let prompt_bucket = format!("{}/{}", self.prompt.balance(), self.prompt.max());
        let token_bucket = format!("{}/{}", self.tokens.balance(), self.tokens.max());
        if self.backoff.load(Relaxed) {
            format!("prompts: {} tokens: {} (BACKOFF)", prompt_bucket, token_bucket)
        } else {
            format!("prompts: {} tokens: {}", prompt_bucket, token_bucket)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bucket_fills() {
        let limiter = RateLimiter::builder()
            .initial(1)
            .interval(Duration::from_millis(100))
            .max(10)
            .refill(1)
            .build();

        println!("Initial balance: {}", limiter.balance());
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("After 1s: {}", limiter.balance());

        // Try acquiring to "wake up" the internal state
        limiter.acquire_one().await;
        println!("After acquire: {}", limiter.balance());
    }
}
