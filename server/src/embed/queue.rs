//! Deferred embedding queue for low-priority processing
//!
//! Embeddings are computationally expensive and should not block
//! the main email processing pipeline. This queue allows embedding
//! tasks to be processed at low priority.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

/// Data needed to embed an email
#[derive(Debug, Clone)]
pub struct EmbeddingTask {
    pub email_id: String,
    pub user_id: i32,
    pub user_email: String,
    pub subject: Option<String>,
    pub body: Option<String>,
}

/// Guard that removes an email from the in_processing set when dropped.
pub struct EmbeddingProcessingGuard {
    email_id: String,
    in_processing_set: Arc<RwLock<HashSet<String>>>,
}

impl Drop for EmbeddingProcessingGuard {
    fn drop(&mut self) {
        let mut set = self.in_processing_set.write().unwrap();
        set.remove(&self.email_id);
    }
}

#[derive(Debug, Default)]
struct EmbeddingQueueInner {
    // Per-user queues for fair round-robin processing
    queues: std::collections::HashMap<String, VecDeque<EmbeddingTask>>,
    // Round-robin user ordering
    user_order: VecDeque<String>,
}

impl EmbeddingQueueInner {
    fn push(&mut self, task: EmbeddingTask) {
        let user_email = task.user_email.clone();
        let is_new_user = !self.queues.contains_key(&user_email);

        self.queues
            .entry(user_email.clone())
            .or_default()
            .push_back(task);

        if is_new_user {
            self.user_order.push_back(user_email);
        }
    }

    fn pop(&mut self) -> Option<EmbeddingTask> {
        let mut attempts = self.user_order.len();
        while attempts > 0 {
            if let Some(user) = self.user_order.pop_front() {
                if let Some(queue) = self.queues.get_mut(&user) {
                    if let Some(task) = queue.pop_front() {
                        // If user still has items, put them back for round-robin
                        if !queue.is_empty() {
                            self.user_order.push_back(user);
                        } else {
                            self.queues.remove(&user);
                        }
                        return Some(task);
                    }
                }
                self.queues.remove(&user);
            }
            attempts -= 1;
        }
        None
    }

    fn len(&self) -> usize {
        self.queues.values().map(|q| q.len()).sum()
    }

    fn user_count(&self, user_email: &str) -> usize {
        self.queues.get(user_email).map(|q| q.len()).unwrap_or(0)
    }
}

/// Queue for deferred embedding tasks
#[derive(Debug, Clone)]
pub struct EmbeddingQueue {
    inner: Arc<Mutex<EmbeddingQueueInner>>,
    in_processing_set: Arc<RwLock<HashSet<String>>>,
}

impl EmbeddingQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EmbeddingQueueInner::default())),
            in_processing_set: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Push an embedding task to the queue.
    /// Returns false if the email is already queued or being processed.
    pub fn push(&self, task: EmbeddingTask) -> bool {
        {
            let mut in_processing = self.in_processing_set.write().unwrap();
            if !in_processing.insert(task.email_id.clone()) {
                return false;
            }
        }

        let mut inner = self.inner.lock().unwrap();
        inner.push(task);
        true
    }

    /// Pop the next embedding task from the queue (round-robin across users)
    pub fn pop(&self) -> Option<EmbeddingTask> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop()
    }

    /// Create a guard that removes the email from in_processing when dropped
    pub fn create_processing_guard(&self, email_id: String) -> EmbeddingProcessingGuard {
        EmbeddingProcessingGuard {
            email_id,
            in_processing_set: self.in_processing_set.clone(),
        }
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.len()
    }

    pub fn num_in_processing(&self) -> usize {
        self.in_processing_set.read().unwrap().len()
    }

    pub fn user_count(&self, user_email: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.user_count(user_email)
    }
}

impl Default for EmbeddingQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_distribution() {
        let queue = EmbeddingQueue::new();

        // User A adds 3 tasks
        for i in 0..3 {
            queue.push(EmbeddingTask {
                email_id: format!("a{}", i),
                user_id: 1,
                user_email: "user_a@test.com".to_string(),
                subject: None,
                body: None,
            });
        }

        // User B adds 2 tasks
        for i in 0..2 {
            queue.push(EmbeddingTask {
                email_id: format!("b{}", i),
                user_id: 2,
                user_email: "user_b@test.com".to_string(),
                subject: None,
                body: None,
            });
        }

        // Should round-robin: A, B, A, B, A
        let results: Vec<_> = std::iter::from_fn(|| queue.pop())
            .map(|t| t.user_email)
            .collect();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0], "user_a@test.com");
        assert_eq!(results[1], "user_b@test.com");
        assert_eq!(results[2], "user_a@test.com");
        assert_eq!(results[3], "user_b@test.com");
        assert_eq!(results[4], "user_a@test.com");
    }

    #[test]
    fn test_prevents_duplicate() {
        let queue = EmbeddingQueue::new();

        let task = EmbeddingTask {
            email_id: "email1".to_string(),
            user_id: 1,
            user_email: "user@test.com".to_string(),
            subject: None,
            body: None,
        };

        assert!(queue.push(task.clone()));
        assert!(!queue.push(task)); // Same email_id should be rejected
    }

    #[test]
    fn test_processing_guard() {
        let queue = EmbeddingQueue::new();

        queue.push(EmbeddingTask {
            email_id: "email1".to_string(),
            user_id: 1,
            user_email: "user@test.com".to_string(),
            subject: None,
            body: None,
        });

        assert_eq!(queue.num_in_processing(), 1);

        let task = queue.pop().unwrap();
        {
            let _guard = queue.create_processing_guard(task.email_id.clone());
            assert_eq!(queue.num_in_processing(), 1);
        }
        // Guard dropped
        assert_eq!(queue.num_in_processing(), 0);
    }
}
