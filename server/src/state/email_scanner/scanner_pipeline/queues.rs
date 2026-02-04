//! Scanner Pipeline Queues
//!
//! Thread-safe queue management for the scanner pipeline.

use std::{
    collections::{HashMap, HashSet},
    sync::RwLock,
    time::{Duration, Instant},
};

use super::types::{FailedItem, PipelineItem};

/// A set with TTL-based expiration for entries.
/// Used to track recently processed email IDs to prevent re-processing.
pub struct TtlSet {
    entries: RwLock<HashMap<String, Instant>>,
    ttl: Duration,
}

impl TtlSet {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Insert an entry with the current timestamp
    pub fn insert(&self, key: String) {
        let mut entries = self.entries.write().unwrap();
        entries.insert(key, Instant::now());
    }

    /// Check if an entry exists and is not expired
    pub fn contains(&self, key: &str) -> bool {
        let entries = self.entries.read().unwrap();
        if let Some(inserted_at) = entries.get(key) {
            inserted_at.elapsed() < self.ttl
        } else {
            false
        }
    }

    /// Remove expired entries and return the count of remaining entries
    pub fn prune_expired(&self) -> usize {
        let mut entries = self.entries.write().unwrap();
        let before = entries.len();
        entries.retain(|_, inserted_at| inserted_at.elapsed() < self.ttl);
        let after = entries.len();
        tracing::debug!(
            "TtlSet pruned {} expired entries, {} remaining",
            before - after,
            after
        );
        after
    }

    /// Get the count of entries (including potentially expired ones)
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }
}

/// All queues used by the batch pipeline
pub struct PipelineQueues {
    /// user_id -> Set<email_id> awaiting fetch
    pending_email_ids: RwLock<HashMap<i32, HashSet<String>>>,

    /// Items ready for main categorization (system rules)
    main_categorization_queue: RwLock<Vec<PipelineItem>>,

    /// Items needing user-defined categorization (user rules)
    user_defined_categorization_queue: RwLock<Vec<PipelineItem>>,

    /// Items needing task extraction
    task_extraction_queue: RwLock<Vec<PipelineItem>>,

    /// Completed items ready for DB insert
    done_queue: RwLock<Vec<PipelineItem>>,

    /// Failed items to retry next interval
    failed_queue: RwLock<Vec<FailedItem>>,

    /// All email_ids currently in pipeline (dedup)
    in_pipeline: RwLock<HashSet<String>>,

    /// Recently processed email_ids with TTL eviction
    recently_processed: TtlSet,
}

impl PipelineQueues {
    pub fn new(recently_processed_ttl_secs: u64) -> Self {
        Self {
            pending_email_ids: RwLock::new(HashMap::new()),
            main_categorization_queue: RwLock::new(Vec::new()),
            user_defined_categorization_queue: RwLock::new(Vec::new()),
            task_extraction_queue: RwLock::new(Vec::new()),
            done_queue: RwLock::new(Vec::new()),
            failed_queue: RwLock::new(Vec::new()),
            in_pipeline: RwLock::new(HashSet::new()),
            recently_processed: TtlSet::new(recently_processed_ttl_secs),
        }
    }

    // ========================================================================
    // Pending Email IDs (from poller)
    // ========================================================================

    /// Add a pending email ID for a user.
    /// Returns true if the email was added (not already in pipeline or recently processed).
    pub fn add_pending_email_id(&self, user_id: i32, email_id: String) -> bool {
        // Check if already in pipeline or recently processed
        if self.is_in_pipeline(&email_id) || self.is_recently_processed(&email_id) {
            return false;
        }

        // Add to in_pipeline set first
        {
            let mut in_pipeline = self.in_pipeline.write().unwrap();
            if !in_pipeline.insert(email_id.clone()) {
                return false; // Already being processed
            }
        }

        // Add to pending map
        {
            let mut pending = self.pending_email_ids.write().unwrap();
            pending.entry(user_id).or_default().insert(email_id);
        }

        true
    }

    /// Get all user IDs that have pending emails
    pub fn pending_user_ids(&self) -> Vec<i32> {
        let pending = self.pending_email_ids.read().unwrap();
        pending
            .iter()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(user_id, _)| *user_id)
            .collect()
    }

    /// Take all pending email IDs for a specific user
    pub fn take_pending_for_user(&self, user_id: i32) -> Vec<String> {
        let mut pending = self.pending_email_ids.write().unwrap();
        pending
            .remove(&user_id)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default()
    }

    /// Get count of pending emails for a user
    pub fn pending_count_for_user(&self, user_id: i32) -> usize {
        let pending = self.pending_email_ids.read().unwrap();
        pending.get(&user_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Get total count of pending emails across all users
    pub fn total_pending_emails(&self) -> usize {
        let pending = self.pending_email_ids.read().unwrap();
        pending.values().map(|s| s.len()).sum()
    }

    /// Get count of users with pending emails
    pub fn pending_users_count(&self) -> usize {
        let pending = self.pending_email_ids.read().unwrap();
        pending.iter().filter(|(_, ids)| !ids.is_empty()).count()
    }

    // ========================================================================
    // Main Categorization Queue (system rules - first pass)
    // ========================================================================

    /// Push an item to the main categorization queue
    pub fn push_to_main_categorization_queue(&self, item: PipelineItem) {
        let mut queue = self.main_categorization_queue.write().unwrap();
        queue.push(item);
    }

    /// Drain all items from the main categorization queue
    pub fn drain_main_categorization_queue(&self) -> Vec<PipelineItem> {
        let mut queue = self.main_categorization_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Get count of items in main categorization queue
    pub fn main_categorization_queue_len(&self) -> usize {
        self.main_categorization_queue.read().unwrap().len()
    }

    // ========================================================================
    // User-Defined Categorization Queue (user rules - second pass)
    // ========================================================================

    /// Push an item to the user-defined categorization queue
    pub fn push_to_user_defined_categorization_queue(&self, item: PipelineItem) {
        let mut queue = self.user_defined_categorization_queue.write().unwrap();
        queue.push(item);
    }

    /// Drain all items from the user-defined categorization queue
    pub fn drain_user_defined_categorization_queue(&self) -> Vec<PipelineItem> {
        let mut queue = self.user_defined_categorization_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Get count of items in user-defined categorization queue
    pub fn user_defined_categorization_queue_len(&self) -> usize {
        self.user_defined_categorization_queue.read().unwrap().len()
    }

    // ========================================================================
    // Task Extraction Queue
    // ========================================================================

    /// Push an item to the task extraction queue
    pub fn push_to_task_extraction_queue(&self, item: PipelineItem) {
        let mut queue = self.task_extraction_queue.write().unwrap();
        queue.push(item);
    }

    /// Drain all items from the task extraction queue
    pub fn drain_task_extraction_queue(&self) -> Vec<PipelineItem> {
        let mut queue = self.task_extraction_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Get count of items in task extraction queue
    pub fn task_extraction_queue_len(&self) -> usize {
        self.task_extraction_queue.read().unwrap().len()
    }

    // ========================================================================
    // Done Queue
    // ========================================================================

    /// Push an item to the done queue
    pub fn push_to_done_queue(&self, item: PipelineItem) {
        let mut queue = self.done_queue.write().unwrap();
        queue.push(item);
    }

    /// Drain all items from the done queue
    pub fn drain_done_queue(&self) -> Vec<PipelineItem> {
        let mut queue = self.done_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Get count of items in done queue
    pub fn done_queue_len(&self) -> usize {
        self.done_queue.read().unwrap().len()
    }

    // ========================================================================
    // Failed Queue
    // ========================================================================

    /// Push a failed item to the failed queue
    pub fn push_to_failed_queue(&self, item: FailedItem) {
        let mut queue = self.failed_queue.write().unwrap();
        queue.push(item);
    }

    /// Drain all items from the failed queue
    pub fn drain_failed_queue(&self) -> Vec<FailedItem> {
        let mut queue = self.failed_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Get count of items in failed queue
    pub fn failed_queue_len(&self) -> usize {
        self.failed_queue.read().unwrap().len()
    }

    // ========================================================================
    // Pipeline Tracking
    // ========================================================================

    /// Check if an email is currently in the pipeline
    pub fn is_in_pipeline(&self, email_id: &str) -> bool {
        self.in_pipeline.read().unwrap().contains(email_id)
    }

    /// Check if an email was recently processed
    pub fn is_recently_processed(&self, email_id: &str) -> bool {
        self.recently_processed.contains(email_id)
    }

    /// Mark an email as complete: remove from in_pipeline, add to recently_processed
    pub fn mark_complete(&self, email_id: &str) {
        {
            let mut in_pipeline = self.in_pipeline.write().unwrap();
            in_pipeline.remove(email_id);
        }
        self.recently_processed.insert(email_id.to_string());
    }

    /// Remove an email from the pipeline (e.g., on permanent failure)
    pub fn remove_from_pipeline(&self, email_id: &str) {
        let mut in_pipeline = self.in_pipeline.write().unwrap();
        in_pipeline.remove(email_id);
    }

    /// Get total count of items in pipeline
    pub fn total_in_pipeline(&self) -> usize {
        self.in_pipeline.read().unwrap().len()
    }

    /// Prune expired entries from recently_processed
    pub fn prune_recently_processed(&self) -> usize {
        self.recently_processed.prune_expired()
    }

    /// Get count of recently processed items
    pub fn recently_processed_count(&self) -> usize {
        self.recently_processed.len()
    }

    // ========================================================================
    // User Estimated Tokens Tracking
    // ========================================================================

    /// Get estimated tokens for a user currently in the pipeline.
    /// Computed on-demand by summing estimated_content_tokens from all items in all queues.
    pub fn get_user_estimated_tokens(&self, user_id: i32) -> i64 {
        let sum_tokens = |items: &[PipelineItem]| -> i64 {
            items
                .iter()
                .filter(|item| item.user_id == user_id)
                .map(|item| item.estimated_content_tokens)
                .sum()
        };

        let main_queue = self.main_categorization_queue.read().unwrap();
        let user_defined_queue = self.user_defined_categorization_queue.read().unwrap();
        let task_queue = self.task_extraction_queue.read().unwrap();
        let done_queue = self.done_queue.read().unwrap();

        sum_tokens(&main_queue)
            + sum_tokens(&user_defined_queue)
            + sum_tokens(&task_queue)
            + sum_tokens(&done_queue)
    }

    /// Get estimated tokens bucketed by user_id for all items in the pipeline.
    /// Returns a map of user_id -> estimated_tokens.
    pub fn get_all_user_estimated_tokens(&self) -> HashMap<i32, i64> {
        let mut user_tokens: HashMap<i32, i64> = HashMap::new();

        let accumulate = |items: &[PipelineItem], map: &mut HashMap<i32, i64>| {
            for item in items {
                *map.entry(item.user_id).or_default() += item.estimated_content_tokens;
            }
        };

        let main_queue = self.main_categorization_queue.read().unwrap();
        let user_defined_queue = self.user_defined_categorization_queue.read().unwrap();
        let task_queue = self.task_extraction_queue.read().unwrap();
        let done_queue = self.done_queue.read().unwrap();

        accumulate(&main_queue, &mut user_tokens);
        accumulate(&user_defined_queue, &mut user_tokens);
        accumulate(&task_queue, &mut user_tokens);
        accumulate(&done_queue, &mut user_tokens);

        user_tokens
    }

    /// Get total estimated tokens across all users in the pipeline
    pub fn total_estimated_tokens(&self) -> i64 {
        self.get_all_user_estimated_tokens().values().sum()
    }

    // ========================================================================
    // Stats
    // ========================================================================

    /// Get a summary of all queue counts
    pub fn get_queue_counts(&self) -> QueueCounts {
        QueueCounts {
            pending_fetch_users: self.pending_users_count(),
            pending_fetch_emails: self.total_pending_emails(),
            main_categorization_queue: self.main_categorization_queue_len(),
            user_defined_categorization_queue: self.user_defined_categorization_queue_len(),
            task_extraction_queue: self.task_extraction_queue_len(),
            done_queue: self.done_queue_len(),
            failed_queue: self.failed_queue_len(),
            total_in_pipeline: self.total_in_pipeline(),
            recently_processed: self.recently_processed_count(),
            total_estimated_tokens: self.total_estimated_tokens(),
        }
    }
}

/// Summary of queue counts
#[derive(Debug, Clone)]
pub struct QueueCounts {
    pub pending_fetch_users: usize,
    pub pending_fetch_emails: usize,
    pub main_categorization_queue: usize,
    pub user_defined_categorization_queue: usize,
    pub task_extraction_queue: usize,
    pub done_queue: usize,
    pub failed_queue: usize,
    pub total_in_pipeline: usize,
    pub recently_processed: usize,
    pub total_estimated_tokens: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_set() {
        let set = TtlSet::new(1); // 1 second TTL
        set.insert("test".to_string());
        assert!(set.contains("test"));
        assert!(!set.contains("other"));

        // Wait for expiration
        std::thread::sleep(Duration::from_secs(2));
        assert!(!set.contains("test"));
    }

    #[test]
    fn test_add_pending_dedup() {
        let queues = PipelineQueues::new(3600);

        // First add should succeed
        assert!(queues.add_pending_email_id(1, "email1".to_string()));

        // Duplicate should fail
        assert!(!queues.add_pending_email_id(1, "email1".to_string()));
        assert!(!queues.add_pending_email_id(2, "email1".to_string())); // Different user, same email

        // Different email should succeed
        assert!(queues.add_pending_email_id(1, "email2".to_string()));
    }

    #[test]
    fn test_mark_complete() {
        let queues = PipelineQueues::new(3600);

        queues.add_pending_email_id(1, "email1".to_string());
        assert!(queues.is_in_pipeline("email1"));
        assert!(!queues.is_recently_processed("email1"));

        queues.mark_complete("email1");
        assert!(!queues.is_in_pipeline("email1"));
        assert!(queues.is_recently_processed("email1"));

        // Can't add again while in recently_processed
        assert!(!queues.add_pending_email_id(1, "email1".to_string()));
    }

    fn make_test_item(user_id: i32, email_id: &str, estimated_tokens: i64) -> PipelineItem {
        use crate::email::simplified_message::SimplifiedMessage;

        PipelineItem {
            user_id,
            user_email: format!("user{}@test.com", user_id),
            email_id: email_id.to_string(),
            thread_id: "thread1".to_string(),
            is_read: false,
            has_new_reply: false,
            label_ids: vec![],
            history_id: 0,
            internal_date: 0,
            simplified_message: SimplifiedMessage {
                id: email_id.to_string(),
                label_ids: vec![],
                thread_id: "thread1".to_string(),
                history_id: 0,
                internal_date: 0,
                from: Some("test@test.com".to_string()),
                subject: Some("Test".to_string()),
                snippet: Some("Test snippet".to_string()),
                body: Some("Test body".to_string()),
            },
            first_pass_result: None,
            second_pass_result: None,
            extracted_tasks: vec![],
            estimated_content_tokens: estimated_tokens,
            actual_tokens: 0,
        }
    }

    #[test]
    fn test_user_estimated_tokens() {
        let queues = PipelineQueues::new(3600);

        // Initially no tokens
        assert_eq!(queues.get_user_estimated_tokens(1), 0);
        assert_eq!(queues.get_user_estimated_tokens(2), 0);
        assert_eq!(queues.total_estimated_tokens(), 0);

        // Add items for user 1 to main categorization queue
        queues.push_to_main_categorization_queue(make_test_item(1, "email1", 100));
        queues.push_to_main_categorization_queue(make_test_item(1, "email2", 150));

        // Add items for user 2
        queues.push_to_user_defined_categorization_queue(make_test_item(2, "email3", 200));

        // Check per-user tokens
        assert_eq!(queues.get_user_estimated_tokens(1), 250); // 100 + 150
        assert_eq!(queues.get_user_estimated_tokens(2), 200);
        assert_eq!(queues.total_estimated_tokens(), 450);

        // Add more items in different queues for user 1
        queues.push_to_task_extraction_queue(make_test_item(1, "email4", 75));
        queues.push_to_done_queue(make_test_item(1, "email5", 25));

        assert_eq!(queues.get_user_estimated_tokens(1), 350); // 100 + 150 + 75 + 25
        assert_eq!(queues.total_estimated_tokens(), 550);

        // Check bucketed tokens
        let bucketed = queues.get_all_user_estimated_tokens();
        assert_eq!(bucketed.get(&1), Some(&350));
        assert_eq!(bucketed.get(&2), Some(&200));
    }
}
