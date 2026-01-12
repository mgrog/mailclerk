use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Low,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PromptQueueEmailEntry {
    pub user_email: String,
    pub email_id: u128,
    pub priority: Priority,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueCount {
    pub high_priority: usize,
    pub low_priority: usize,
}

#[derive(Debug, Default)]
struct FairQueueInner {
    // Per-user queues, split by priority
    high_priority_queues: HashMap<String, VecDeque<u128>>,
    low_priority_queues: HashMap<String, VecDeque<u128>>,

    // Round-robin user ordering
    high_priority_users: VecDeque<String>,
    low_priority_users: VecDeque<String>,
}

impl FairQueueInner {
    fn push(&mut self, user_email: String, email_id: u128, priority: Priority) {
        let (queues, user_order) = match priority {
            Priority::High => (&mut self.high_priority_queues, &mut self.high_priority_users),
            Priority::Low => (&mut self.low_priority_queues, &mut self.low_priority_users),
        };

        let is_new_user = !queues.contains_key(&user_email);
        queues
            .entry(user_email.clone())
            .or_default()
            .push_back(email_id);

        // Add user to round-robin rotation if this is their first item at this priority
        if is_new_user {
            user_order.push_back(user_email);
        }
    }

    fn pop(&mut self) -> Option<PromptQueueEmailEntry> {
        // Try high priority first (round-robin across users)
        if let Some(entry) = self.pop_round_robin(Priority::High) {
            return Some(entry);
        }

        // Then low priority (round-robin across users)
        self.pop_round_robin(Priority::Low)
    }

    fn pop_round_robin(&mut self, priority: Priority) -> Option<PromptQueueEmailEntry> {
        let (queues, user_order) = match priority {
            Priority::High => (&mut self.high_priority_queues, &mut self.high_priority_users),
            Priority::Low => (&mut self.low_priority_queues, &mut self.low_priority_users),
        };

        // Try each user in round-robin order
        let mut attempts = user_order.len();
        while attempts > 0 {
            if let Some(user) = user_order.pop_front() {
                if let Some(queue) = queues.get_mut(&user) {
                    if let Some(email_id) = queue.pop_front() {
                        // If user still has items, put them back at the end for round-robin
                        if !queue.is_empty() {
                            user_order.push_back(user.clone());
                        } else {
                            // User's queue is empty, remove from map
                            queues.remove(&user);
                        }

                        return Some(PromptQueueEmailEntry {
                            user_email: user,
                            email_id,
                            priority,
                        });
                    }
                }
                // User's queue is empty or doesn't exist, remove from map if present
                queues.remove(&user);
            }
            attempts -= 1;
        }
        None
    }

    fn queue_count(&self, email_address: &str) -> QueueCount {
        let high_priority = self
            .high_priority_queues
            .get(email_address)
            .map(|q| q.len())
            .unwrap_or(0);
        let low_priority = self
            .low_priority_queues
            .get(email_address)
            .map(|q| q.len())
            .unwrap_or(0);

        QueueCount {
            high_priority,
            low_priority,
        }
    }

    fn total_high_priority(&self) -> usize {
        self.high_priority_queues.values().map(|q| q.len()).sum()
    }

    fn total_len(&self) -> usize {
        self.high_priority_queues.values().map(|q| q.len()).sum::<usize>()
            + self.low_priority_queues.values().map(|q| q.len()).sum::<usize>()
    }
}

/// Guard that removes an email from the in_processing set when dropped.
/// This ensures cleanup happens even if processing panics or returns early.
pub struct ProcessingGuard {
    email_id: u128,
    in_processing_set: Arc<RwLock<HashSet<u128>>>,
}

impl Drop for ProcessingGuard {
    fn drop(&mut self) {
        let mut set = self.in_processing_set.write().unwrap();
        set.remove(&self.email_id);
    }
}

#[derive(Debug, Clone)]
pub struct PromptPriorityQueue {
    inner: Arc<Mutex<FairQueueInner>>,
    in_processing_set: Arc<RwLock<HashSet<u128>>>,
}

impl PromptPriorityQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FairQueueInner::default())),
            in_processing_set: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn push(&self, user_email: String, email_id: u128, priority: Priority) -> bool {
        // Check if the email is already in processing
        // so emails are not processed multiple times
        {
            let mut in_processing = self.in_processing_set.write().unwrap();
            if !in_processing.insert(email_id) {
                return false;
            }
        }

        let mut inner = self.inner.lock().unwrap();
        inner.push(user_email, email_id, priority);
        true
    }

    pub fn pop(&self) -> Option<PromptQueueEmailEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop()
    }

    /// Creates a guard that will remove the email from the in_processing set when dropped.
    /// Use this to ensure cleanup happens even if processing panics or returns early.
    pub fn create_processing_guard(&self, email_id: u128) -> ProcessingGuard {
        ProcessingGuard {
            email_id,
            in_processing_set: self.in_processing_set.clone(),
        }
    }

    pub fn num_in_queue(&self, email_address: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        let count = inner.queue_count(email_address);
        count.high_priority + count.low_priority
    }

    pub fn num_high_priority_in_queue(&self, email_address: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.queue_count(email_address).high_priority
    }

    pub fn num_low_priority_in_queue(&self, email_address: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.queue_count(email_address).low_priority
    }

    pub fn all_high_priority(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.total_high_priority()
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.total_len()
    }

    pub fn num_in_processing(&self) -> usize {
        self.in_processing_set.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_round_robin_distribution() {
        let queue = PromptPriorityQueue::new();

        // User A adds 5 emails
        for i in 0..5 {
            queue.push("user_a@test.com".to_string(), i, Priority::Low);
        }

        // User B adds 2 emails
        for i in 10..12 {
            queue.push("user_b@test.com".to_string(), i, Priority::Low);
        }

        // User C adds 3 emails
        for i in 20..23 {
            queue.push("user_c@test.com".to_string(), i, Priority::Low);
        }

        // Pop should round-robin: A, B, C, A, B, C, A, C, A, A
        let mut results = Vec::new();
        while let Some(entry) = queue.pop() {
            results.push(entry.user_email.clone());
        }

        // First 3 should be one from each user (order depends on insertion)
        assert_eq!(results.len(), 10);

        // Count distribution in first 6 pops (2 rounds)
        let first_six: Vec<_> = results.iter().take(6).collect();
        let a_count = first_six.iter().filter(|u| **u == "user_a@test.com").count();
        let b_count = first_six.iter().filter(|u| **u == "user_b@test.com").count();
        let c_count = first_six.iter().filter(|u| **u == "user_c@test.com").count();

        // Each user should get exactly 2 in the first 6 (fair distribution)
        assert_eq!(a_count, 2);
        assert_eq!(b_count, 2);
        assert_eq!(c_count, 2);
    }

    #[test]
    fn test_high_priority_before_low() {
        let queue = PromptPriorityQueue::new();

        // Add low priority first
        queue.push("user_a@test.com".to_string(), 1, Priority::Low);
        queue.push("user_b@test.com".to_string(), 2, Priority::Low);

        // Add high priority
        queue.push("user_c@test.com".to_string(), 3, Priority::High);
        queue.push("user_d@test.com".to_string(), 4, Priority::High);

        // High priority should come first
        let first = queue.pop().unwrap();
        assert_eq!(first.priority, Priority::High);

        let second = queue.pop().unwrap();
        assert_eq!(second.priority, Priority::High);

        // Then low priority
        let third = queue.pop().unwrap();
        assert_eq!(third.priority, Priority::Low);

        let fourth = queue.pop().unwrap();
        assert_eq!(fourth.priority, Priority::Low);
    }

    #[test]
    fn test_prevents_duplicate_processing() {
        let queue = PromptPriorityQueue::new();

        // First push should succeed
        assert!(queue.push("user@test.com".to_string(), 1, Priority::Low));

        // Same email_id should be rejected
        assert!(!queue.push("user@test.com".to_string(), 1, Priority::Low));
        assert!(!queue.push("other@test.com".to_string(), 1, Priority::High));

        // Different email_id should succeed
        assert!(queue.push("user@test.com".to_string(), 2, Priority::Low));
    }

    #[test]
    fn test_processing_guard() {
        let queue = PromptPriorityQueue::new();

        queue.push("user@test.com".to_string(), 1, Priority::Low);
        assert_eq!(queue.num_in_processing(), 1);

        // Pop and process
        let _ = queue.pop();
        assert_eq!(queue.num_in_processing(), 1); // Still in processing

        // Create guard and drop it to mark as done
        {
            let _guard = queue.create_processing_guard(1);
            assert_eq!(queue.num_in_processing(), 1); // Still in processing while guard exists
        }
        // Guard dropped, email removed from processing
        assert_eq!(queue.num_in_processing(), 0);

        // Can now re-add the same email_id
        assert!(queue.push("user@test.com".to_string(), 1, Priority::Low));
    }

    #[test]
    fn test_queue_counts() {
        let queue = PromptPriorityQueue::new();

        queue.push("user@test.com".to_string(), 1, Priority::High);
        queue.push("user@test.com".to_string(), 2, Priority::High);
        queue.push("user@test.com".to_string(), 3, Priority::Low);

        assert_eq!(queue.num_high_priority_in_queue("user@test.com"), 2);
        assert_eq!(queue.num_low_priority_in_queue("user@test.com"), 1);
        assert_eq!(queue.num_in_queue("user@test.com"), 3);
        assert_eq!(queue.all_high_priority(), 2);
        assert_eq!(queue.len(), 3);

        // Pop one high priority
        let _ = queue.pop();
        assert_eq!(queue.num_high_priority_in_queue("user@test.com"), 1);
        assert_eq!(queue.len(), 2);
    }
}
