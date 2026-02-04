//! Scan Tracker Module
//!
//! Provides observability for batch scans (initial scan, re-categorization, etc.)

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tracing::info;

use super::common::{format_elapsed_secs, format_table, Progress};

// ============================================================================
// Batch Scan Tracking
// ============================================================================

/// Type of scan being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanType {
    InitialScan,
    Recategorization,
}

impl fmt::Display for ScanType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanType::InitialScan => write!(f, "Initial"),
            ScanType::Recategorization => write!(f, "Recat"),
        }
    }
}

/// Current phase of the scan
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPhase {
    Fetching,
    Categorizing { job_id: Option<String> },
    ExtractingTasks { job_id: Option<String> },
    Inserting,
    Complete,
    Failed { error: String },
}

impl ScanPhase {
    /// Get short display name for phase
    fn short_name(&self) -> &str {
        match self {
            ScanPhase::Fetching => "Fetching",
            ScanPhase::Categorizing { .. } => "Categorizing",
            ScanPhase::ExtractingTasks { .. } => "Extracting",
            ScanPhase::Inserting => "Inserting",
            ScanPhase::Complete => "Complete",
            ScanPhase::Failed { .. } => "Failed",
        }
    }

    /// Get job ID if present
    fn job_id(&self) -> Option<&str> {
        match self {
            ScanPhase::Categorizing { job_id } => job_id.as_deref(),
            ScanPhase::ExtractingTasks { job_id } => job_id.as_deref(),
            _ => None,
        }
    }

    /// Check if this is a terminal phase (Complete or Failed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, ScanPhase::Complete | ScanPhase::Failed { .. })
    }

    /// Check if this is a success terminal state
    pub fn is_success(&self) -> bool {
        matches!(self, ScanPhase::Complete)
    }
}

impl fmt::Display for ScanPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanPhase::Fetching => write!(f, "Fetching"),
            ScanPhase::Categorizing { job_id } => {
                if let Some(id) = job_id {
                    write!(f, "Categorizing (job: {})", id)
                } else {
                    write!(f, "Categorizing")
                }
            }
            ScanPhase::ExtractingTasks { job_id } => {
                if let Some(id) = job_id {
                    write!(f, "ExtractingTasks (job: {})", id)
                } else {
                    write!(f, "ExtractingTasks")
                }
            }
            ScanPhase::Inserting => write!(f, "Inserting"),
            ScanPhase::Complete => write!(f, "Complete"),
            ScanPhase::Failed { error } => write!(f, "Failed: {}", error),
        }
    }
}

/// An individual scan entry being tracked
#[derive(Debug, Clone)]
pub struct ScanEntry {
    pub scan_type: ScanType,
    pub user_email: String,
    pub user_id: i32,
    pub phase: ScanPhase,
    pub fetch_progress: Progress,
    pub scan_progress: Progress,
    pub started_at: Instant,
}

impl ScanEntry {
    pub fn new(scan_type: ScanType, user_email: String, user_id: i32) -> Self {
        Self {
            scan_type,
            user_email,
            user_id,
            phase: ScanPhase::Fetching,
            fetch_progress: Progress::new(0),
            scan_progress: Progress::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn elapsed_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Format elapsed time as "Xm Ys" or "Ys"
    fn format_elapsed(&self) -> String {
        format_elapsed_secs(self.elapsed_secs())
    }
}

/// Thread-safe tracker for active scans
#[derive(Clone)]
pub struct ScanTracker {
    /// Map of user_id -> ScanEntry
    active_scans: Arc<RwLock<HashMap<i32, ScanEntry>>>,
}

impl ScanTracker {
    pub fn new() -> Self {
        Self {
            active_scans: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ========================================================================
    // Scan Tracking Methods
    // ========================================================================

    /// Register a new scan for a user and log the status table
    pub fn register_scan(&self, scan_type: ScanType, user_email: String, user_id: i32) {
        let entry = ScanEntry::new(scan_type, user_email, user_id);
        self.active_scans.write().unwrap().insert(user_id, entry);
        // Log the status table
        if let Some(table) = self.get_scans_table() {
            info!("Scan Status Update:\n{}", table);
        }
    }

    /// Update the phase of an active scan and log the status table
    pub fn set_phase(&self, user_id: i32, phase: ScanPhase) {
        {
            let mut scans = self.active_scans.write().unwrap();
            if let Some(entry) = scans.get_mut(&user_id) {
                entry.phase = phase;
            }
        }
        // Log the status table after releasing the write lock
        if let Some(table) = self.get_scans_table() {
            info!("Scan Status Update:\n{}", table);
        }
    }

    /// Set the total number of emails to fetch by ID
    pub fn set_fetch_total(&self, user_id: i32, total: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.fetch_progress.total = total;
        }
    }

    /// Set the number of emails fetched by ID
    pub fn set_fetched_count(&self, user_id: i32, fetched: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.fetch_progress.current = fetched;
        }
    }

    /// Increment the fetched email count
    pub fn increment_fetched(&self, user_id: i32, count: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.fetch_progress.current += count;
        }
    }

    /// Update the total email count for a scan
    pub fn set_total_emails(&self, user_id: i32, total: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.scan_progress.total = total;
        }
    }

    /// Update the processed email count for a scan
    pub fn set_processed_emails(&self, user_id: i32, processed: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.scan_progress.current = processed;
        }
    }

    /// Increment the processed email count
    pub fn increment_processed(&self, user_id: i32, count: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.scan_progress.current += count;
        }
    }

    /// Mark a scan as complete, log status table, then remove
    pub fn complete_scan(&self, user_id: i32) {
        {
            let mut scans = self.active_scans.write().unwrap();
            if let Some(entry) = scans.get_mut(&user_id) {
                entry.phase = ScanPhase::Complete;
            }
        }
        // Log the status table after releasing the write lock
        if let Some(table) = self.get_scans_table() {
            info!("Scan Status Update:\n{}", table);
        }
        // Remove the completed scan
        self.active_scans.write().unwrap().remove(&user_id);
    }

    /// Mark a scan as failed, log status table, then remove
    pub fn fail_scan(&self, user_id: i32, error: String) {
        {
            let mut scans = self.active_scans.write().unwrap();
            if let Some(entry) = scans.get_mut(&user_id) {
                entry.phase = ScanPhase::Failed { error };
            }
        }
        // Log the status table after releasing the write lock
        if let Some(table) = self.get_scans_table() {
            info!("Scan Status Update:\n{}", table);
        }
        // Remove the failed scan
        self.active_scans.write().unwrap().remove(&user_id);
    }

    /// Remove a scan by user_id
    pub fn remove_scan(&self, user_id: i32) {
        self.active_scans.write().unwrap().remove(&user_id);
    }

    /// Get the number of active scans
    pub fn scan_count(&self) -> usize {
        self.active_scans.read().unwrap().len()
    }

    /// Check if a user has an active scan
    pub fn has_active_scan(&self, user_id: i32) -> bool {
        self.active_scans.read().unwrap().contains_key(&user_id)
    }

    /// Get a clone of the scan entry for a user
    pub fn get_scan(&self, user_id: i32) -> Option<ScanEntry> {
        self.active_scans.read().unwrap().get(&user_id).cloned()
    }

    // ========================================================================
    // Display Methods
    // ========================================================================

    /// Get formatted table of batch scans
    pub fn get_scans_table(&self) -> Option<String> {
        let scans = self.active_scans.read().unwrap();
        if scans.is_empty() {
            return None;
        }

        let headers = [
            "User",
            "Type",
            "Phase",
            "Job ID",
            "Emails Fetched",
            "Emails Scanned",
            "Elapsed",
        ];
        let mut rows: Vec<Vec<String>> = scans
            .values()
            .map(|s| {
                vec![
                    s.user_email.clone(),
                    s.scan_type.to_string(),
                    s.phase.short_name().to_string(),
                    s.phase.job_id().unwrap_or("-").to_string(),
                    s.fetch_progress.format_progress(),
                    s.scan_progress.format_progress(),
                    s.format_elapsed(),
                ]
            })
            .collect();

        // Sort by user email
        rows.sort_by(|a, b| a[0].cmp(&b[0]));

        Some(format!(
            "Batch Scans ({}):\n{}",
            scans.len(),
            format_table(&headers, &rows)
        ))
    }
}

impl Default for ScanTracker {
    fn default() -> Self {
        Self::new()
    }
}
