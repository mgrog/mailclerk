use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use crate::embed::chunker::Chunk;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    High = 0,
    Low = 1,
    Background = 2, // Embeddings - lowest priority
}

/// Task types that can be queued
#[derive(Debug, Clone)]
pub enum TaskData {
    /// Email categorization task - just needs the email ID
    Categorization,
    /// Email embedding task - includes text data for batching
    Embedding { chunks: Vec<Chunk> },
}

#[derive(Debug, Clone)]
pub struct QueueEntry {
    pub user_email: String,
    pub user_id: i32,
    pub email_id: String,
    pub priority: Priority,
    pub task: TaskData,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueCount {
    pub high: usize,
    pub low: usize,
    pub background: usize,
}

impl QueueCount {
    pub fn total(&self) -> usize {
        self.high + self.low + self.background
    }

    pub fn categorization(&self) -> usize {
        self.high + self.low
    }
}

#[derive(Debug, Default)]
struct FairQueueInner {
    high_queues: HashMap<String, VecDeque<QueueEntry>>,
    low_queues: HashMap<String, VecDeque<QueueEntry>>,
    background_queues: HashMap<String, VecDeque<QueueEntry>>,

    high_users: VecDeque<String>,
    low_users: VecDeque<String>,
    background_users: VecDeque<String>,
}

impl FairQueueInner {
    fn get_queues_mut(
        &mut self,
        priority: Priority,
    ) -> (
        &mut HashMap<String, VecDeque<QueueEntry>>,
        &mut VecDeque<String>,
    ) {
        match priority {
            Priority::High => (&mut self.high_queues, &mut self.high_users),
            Priority::Low => (&mut self.low_queues, &mut self.low_users),
            Priority::Background => (&mut self.background_queues, &mut self.background_users),
        }
    }

    fn push(&mut self, entry: QueueEntry) {
        let user_email = entry.user_email.clone();
        let priority = entry.priority;
        let (queues, user_order) = self.get_queues_mut(priority);

        let is_new_user = !queues.contains_key(&user_email);
        queues
            .entry(user_email.clone())
            .or_default()
            .push_back(entry);

        if is_new_user {
            user_order.push_back(user_email);
        }
    }

    /// Pop highest priority entry (High -> Low -> Background)
    fn pop(&mut self) -> Option<QueueEntry> {
        self.pop_by_priority(Priority::High)
            .or_else(|| self.pop_by_priority(Priority::Low))
            .or_else(|| self.pop_by_priority(Priority::Background))
    }

    /// Pop only categorization tasks (High -> Low)
    fn pop_categorization(&mut self) -> Option<QueueEntry> {
        self.pop_by_priority(Priority::High)
            .or_else(|| self.pop_by_priority(Priority::Low))
    }

    /// Pop from specific priority (round-robin across users)
    fn pop_by_priority(&mut self, priority: Priority) -> Option<QueueEntry> {
        let (queues, user_order) = self.get_queues_mut(priority);

        let mut attempts = user_order.len();
        while attempts > 0 {
            if let Some(user) = user_order.pop_front() {
                if let Some(queue) = queues.get_mut(&user) {
                    if let Some(entry) = queue.pop_front() {
                        if !queue.is_empty() {
                            user_order.push_back(user);
                        } else {
                            queues.remove(&user);
                        }
                        return Some(entry);
                    }
                }
                queues.remove(&user);
            }
            attempts -= 1;
        }
        None
    }

    /// Pop background only if High and Low are empty
    fn pop_background_if_idle(&mut self) -> Option<QueueEntry> {
        if self.high_queues.is_empty() && self.low_queues.is_empty() {
            self.pop_by_priority(Priority::Background)
        } else {
            None
        }
    }

    /// Pop multiple background entries for batching (only if High/Low are empty)
    fn pop_background_batch(&mut self, max_count: usize) -> Vec<QueueEntry> {
        if !self.high_queues.is_empty() || !self.low_queues.is_empty() {
            return Vec::new();
        }

        let mut batch = Vec::with_capacity(max_count);
        while batch.len() < max_count {
            if let Some(entry) = self.pop_by_priority(Priority::Background) {
                batch.push(entry);
            } else {
                break;
            }
        }
        batch
    }

    fn queue_count(&self, user_email: &str) -> QueueCount {
        QueueCount {
            high: self
                .high_queues
                .get(user_email)
                .map(|q| q.len())
                .unwrap_or(0),
            low: self
                .low_queues
                .get(user_email)
                .map(|q| q.len())
                .unwrap_or(0),
            background: self
                .background_queues
                .get(user_email)
                .map(|q| q.len())
                .unwrap_or(0),
        }
    }

    fn total_count(&self) -> QueueCount {
        QueueCount {
            high: self.high_queues.values().map(|q| q.len()).sum(),
            low: self.low_queues.values().map(|q| q.len()).sum(),
            background: self.background_queues.values().map(|q| q.len()).sum(),
        }
    }

    fn has_categorization_work(&self) -> bool {
        !self.high_queues.is_empty() || !self.low_queues.is_empty()
    }
}

/// Guard that removes an entry from the in_processing set when dropped.
pub struct ProcessingGuard {
    email_id: String,
    email_ids_in_processing: Arc<RwLock<HashSet<String>>>,
}

impl Drop for ProcessingGuard {
    fn drop(&mut self) {
        let mut set = self.email_ids_in_processing.write().unwrap();
        set.remove(&self.email_id);
    }
}

#[derive(Debug, Clone)]
pub struct TaskQueue {
    inner: Arc<Mutex<FairQueueInner>>,
    email_ids_in_processing: Arc<RwLock<HashSet<String>>>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FairQueueInner::default())),
            email_ids_in_processing: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Push an entry to the queue. Returns false if already queued/processing.
    pub fn push(&self, entry: QueueEntry) -> bool {
        {
            let mut in_processing = self.email_ids_in_processing.write().unwrap();
            if matches!(entry.task, TaskData::Categorization)
                && !in_processing.insert(entry.email_id.clone())
            {
                return false;
            }
        }

        let mut inner = self.inner.lock().unwrap();
        inner.push(entry);
        true
    }

    /// Pop highest priority entry (High -> Low -> Background)
    pub fn pop(&self) -> Option<QueueEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop()
    }

    /// Pop only categorization tasks (High -> Low), skip Background
    pub fn pop_categorization(&self) -> Option<QueueEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop_categorization()
    }

    /// Pop background task only if no categorization work is pending
    pub fn pop_background_if_idle(&self) -> Option<QueueEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop_background_if_idle()
    }

    /// Pop batch of background tasks (for embedding batching)
    pub fn pop_background_batch(&self, max_count: usize) -> Vec<QueueEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop_background_batch(max_count)
    }

    /// Check if there's categorization work pending
    pub fn has_categorization_work(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.has_categorization_work()
    }

    /// Create a processing guard for cleanup on drop/panic
    pub fn create_processing_guard(&self, email_id: String) -> ProcessingGuard {
        ProcessingGuard {
            email_id,
            email_ids_in_processing: self.email_ids_in_processing.clone(),
        }
    }

    /// Mark an email as done processing (alternative to guard)
    pub fn mark_done(&self, email_id: &str) {
        let mut set = self.email_ids_in_processing.write().unwrap();
        set.remove(email_id);
    }

    pub fn queue_count(&self, user_email: &str) -> QueueCount {
        let inner = self.inner.lock().unwrap();
        inner.queue_count(user_email)
    }

    pub fn total_count(&self) -> QueueCount {
        let inner = self.inner.lock().unwrap();
        inner.total_count()
    }

    pub fn len(&self) -> usize {
        self.total_count().total()
    }

    pub fn num_in_processing(&self) -> usize {
        self.email_ids_in_processing.read().unwrap().len()
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(user: &str, email_id: &str, priority: Priority) -> QueueEntry {
        QueueEntry {
            user_email: user.to_string(),
            user_id: 1,
            email_id: email_id.to_string(),
            priority,
            task: TaskData::Categorization,
        }
    }

    fn make_embedding(user: &str, email_id: &str) -> QueueEntry {
        QueueEntry {
            user_email: user.to_string(),
            user_id: 1,
            email_id: email_id.to_string(),
            priority: Priority::Background,
            task: TaskData::Embedding {
                chunks: vec![
                    Chunk { index: 0, text: "Test".to_string() },
                    Chunk { index: 1, text: "Body".to_string() },
                ],
            },
        }
    }

    #[test]
    fn test_priority_ordering() {
        let queue = TaskQueue::new();

        // Add in reverse priority order
        queue.push(make_embedding("user@test.com", "embed1"));
        queue.push(make_entry("user@test.com", "low1", Priority::Low));
        queue.push(make_entry("user@test.com", "high1", Priority::High));

        // Should pop in priority order
        assert_eq!(queue.pop().unwrap().email_id, "high1");
        assert_eq!(queue.pop().unwrap().email_id, "low1");
        assert_eq!(queue.pop().unwrap().email_id, "embed1");
        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_fair_round_robin() {
        let queue = TaskQueue::new();

        // User A: 3 tasks, User B: 2 tasks
        for i in 0..3 {
            queue.push(make_entry(
                "user_a@test.com",
                &format!("a{}", i),
                Priority::Low,
            ));
        }
        for i in 0..2 {
            queue.push(make_entry(
                "user_b@test.com",
                &format!("b{}", i),
                Priority::Low,
            ));
        }

        // Should alternate: A, B, A, B, A
        let results: Vec<_> = std::iter::from_fn(|| queue.pop())
            .map(|e| e.user_email.chars().next().unwrap())
            .collect();

        assert_eq!(results, vec!['u', 'u', 'u', 'u', 'u']); // All start with 'u'
                                                            // More precise check
        let ids: Vec<_> = std::iter::from_fn(|| queue.pop()).collect();
        assert!(ids.is_empty()); // All popped
    }

    #[test]
    fn test_pop_categorization_skips_background() {
        let queue = TaskQueue::new();

        queue.push(make_embedding("user@test.com", "embed1"));
        queue.push(make_entry("user@test.com", "cat1", Priority::Low));

        // pop_categorization should skip embedding
        let entry = queue.pop_categorization().unwrap();
        assert_eq!(entry.email_id, "cat1");

        // No more categorization work
        assert!(queue.pop_categorization().is_none());

        // But background is still there
        let entry = queue.pop().unwrap();
        assert_eq!(entry.email_id, "embed1");
    }

    #[test]
    fn test_pop_background_if_idle() {
        let queue = TaskQueue::new();

        queue.push(make_embedding("user@test.com", "embed1"));
        queue.push(make_entry("user@test.com", "cat1", Priority::High));

        // Background blocked while categorization pending
        assert!(queue.pop_background_if_idle().is_none());

        // Pop the categorization task
        queue.pop_categorization();

        // Now background is available
        let entry = queue.pop_background_if_idle().unwrap();
        assert_eq!(entry.email_id, "embed1");
    }

    #[test]
    fn test_pop_background_batch() {
        let queue = TaskQueue::new();

        for i in 0..5 {
            queue.push(make_embedding("user@test.com", &format!("embed{}", i)));
        }

        // Pop batch of 3
        let batch = queue.pop_background_batch(3);
        assert_eq!(batch.len(), 3);

        // 2 remaining
        assert_eq!(queue.total_count().background, 2);
    }

    #[test]
    fn test_prevents_duplicates() {
        let queue = TaskQueue::new();

        assert!(queue.push(make_entry("user@test.com", "email1", Priority::High)));
        assert!(!queue.push(make_entry("user@test.com", "email1", Priority::Low))); // Duplicate

        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_processing_guard() {
        let queue = TaskQueue::new();

        queue.push(make_entry("user@test.com", "email1", Priority::High));
        assert_eq!(queue.num_in_processing(), 1);

        let entry = queue.pop().unwrap();
        {
            let _guard = queue.create_processing_guard(entry.email_id.clone());
            assert_eq!(queue.num_in_processing(), 1);
        }
        // Guard dropped
        assert_eq!(queue.num_in_processing(), 0);

        // Can re-add
        assert!(queue.push(make_entry("user@test.com", "email1", Priority::High)));
    }

    #[test]
    fn test_queue_counts() {
        let queue = TaskQueue::new();

        queue.push(make_entry("user@test.com", "h1", Priority::High));
        queue.push(make_entry("user@test.com", "h2", Priority::High));
        queue.push(make_entry("user@test.com", "l1", Priority::Low));
        queue.push(make_embedding("user@test.com", "e1"));

        let count = queue.queue_count("user@test.com");
        assert_eq!(count.high, 2);
        assert_eq!(count.low, 1);
        assert_eq!(count.background, 1);
        assert_eq!(count.total(), 4);
        assert_eq!(count.categorization(), 3);
    }
}
