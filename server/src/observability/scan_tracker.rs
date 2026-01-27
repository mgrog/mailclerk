//! Scan Tracker Module
//!
//! Provides observability for batch scans (initial scan, re-categorization, etc.)
//! and queue processor tracking with rolling window metrics.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tracing::info;

use crate::state::email_scanner::queue_processor::ProcessorStatus;

// ============================================================================
// Table Formatting Utilities
// ============================================================================

/// Format a table with headers and rows
fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    // Calculate column widths (max of header and all row values)
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut output = String::new();

    // Header row
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
        .collect();
    output.push_str(&format!("| {} |\n", header_line.join(" | ")));

    // Separator
    let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    output.push_str(&format!("|-{}-|\n", separator.join("-|-")));

    // Data rows
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let width = widths.get(i).copied().unwrap_or(cell.len());
                format!("{:width$}", cell, width = width)
            })
            .collect();
        output.push_str(&format!("| {} |\n", cells.join(" | ")));
    }

    output
}

// ============================================================================
// Processor Tracking (Rolling Window Rate)
// ============================================================================

/// Rolling window size for emails/sec calculation (in seconds)
const ROLLING_WINDOW_SECS: u64 = 10;

/// A timestamped count for rolling window calculations
#[derive(Debug, Clone)]
struct TimestampedCount {
    timestamp: Instant,
    count: u64,
}

/// Per-user processor stats
#[derive(Debug, Clone)]
struct ProcessorStats {
    user_email: String,
    /// Current status of the processor
    status: ProcessorStatus,
    /// Rolling window of (timestamp, cumulative_count) for rate calculation
    count_history: VecDeque<TimestampedCount>,
    /// Current queue count for this user
    queue_count: usize,
}

impl ProcessorStats {
    fn new(user_email: String) -> Self {
        Self {
            user_email,
            status: ProcessorStatus::Queueing,
            count_history: VecDeque::new(),
            queue_count: 0,
        }
    }

    /// Record a new cumulative count, pruning old entries
    fn record_count(&mut self, count: u64) {
        let now = Instant::now();

        // Add new entry
        self.count_history.push_back(TimestampedCount {
            timestamp: now,
            count,
        });

        // Prune entries older than the window
        let cutoff = now - std::time::Duration::from_secs(ROLLING_WINDOW_SECS);
        while let Some(front) = self.count_history.front() {
            if front.timestamp < cutoff {
                self.count_history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Calculate emails per second based on rolling window
    fn emails_per_sec(&self) -> f64 {
        if self.count_history.len() < 2 {
            return 0.0;
        }

        let oldest = self.count_history.front().unwrap();
        let newest = self.count_history.back().unwrap();

        let count_diff = newest.count.saturating_sub(oldest.count);
        let time_diff = newest.timestamp.duration_since(oldest.timestamp);

        if time_diff.as_secs_f64() < 0.1 {
            return 0.0;
        }

        count_diff as f64 / time_diff.as_secs_f64()
    }
}

/// Tracker for queue processor stats
#[derive(Clone)]
pub struct ProcessorTracker {
    /// Map of user_email -> ProcessorStats
    stats: Arc<RwLock<HashMap<String, ProcessorStats>>>,
}

impl ProcessorTracker {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update stats for a user (call periodically with cumulative processed count)
    pub fn update_user(
        &self,
        user_email: &str,
        status: ProcessorStatus,
        processed_count: u64,
        queue_count: usize,
    ) {
        let mut stats = self.stats.write().unwrap();
        let entry = stats
            .entry(user_email.to_string())
            .or_insert_with(|| ProcessorStats::new(user_email.to_string()));
        entry.status = status;
        entry.record_count(processed_count);
        entry.queue_count = queue_count;
    }

    /// Remove a user from tracking
    pub fn remove_user(&self, user_email: &str) {
        self.stats.write().unwrap().remove(user_email);
    }

    /// Get formatted table of processor stats
    pub fn get_table(&self) -> Option<String> {
        let stats = self.stats.read().unwrap();
        if stats.is_empty() {
            return None;
        }

        // Calculate totals
        let total_emails_per_sec: f64 = stats.values().map(|s| s.emails_per_sec()).sum();
        let total_in_queue: usize = stats.values().map(|s| s.queue_count).sum();

        let headers = [
            "User",
            "Status",
            &format!("Emails/sec ({:.1})", total_emails_per_sec),
            &format!("In Queue ({})", total_in_queue),
        ];

        let mut rows: Vec<Vec<String>> = stats
            .values()
            .map(|s| {
                vec![
                    s.user_email.clone(),
                    s.status.to_string(),
                    format!("{:.1}", s.emails_per_sec()),
                    s.queue_count.to_string(),
                ]
            })
            .collect();

        // Sort by user email for consistent ordering
        rows.sort_by(|a, b| a[0].cmp(&b[0]));

        Some(format!(
            "Queue Processors ({}):\n{}",
            stats.len(),
            format_table(&headers, &rows)
        ))
    }

    /// Check if there are any active processors
    pub fn is_empty(&self) -> bool {
        self.stats.read().unwrap().is_empty()
    }
}

impl Default for ProcessorTracker {
    fn default() -> Self {
        Self::new()
    }
}

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

/// Progress information for a scan
#[derive(Debug, Clone)]
pub struct ScanProgress {
    pub total_emails: usize,
    pub processed_emails: usize,
}

impl ScanProgress {
    pub fn new(total: usize) -> Self {
        Self {
            total_emails: total,
            processed_emails: 0,
        }
    }

    pub fn percentage(&self) -> f32 {
        if self.total_emails == 0 {
            0.0
        } else {
            (self.processed_emails as f32 / self.total_emails as f32) * 100.0
        }
    }

    /// Format as "processed/total (pct%)"
    fn format_progress(&self) -> String {
        format!(
            "{}/{} ({:.0}%)",
            self.processed_emails,
            self.total_emails,
            self.percentage()
        )
    }
}

/// An individual scan entry being tracked
#[derive(Debug, Clone)]
pub struct ScanEntry {
    pub scan_type: ScanType,
    pub user_email: String,
    pub user_id: i32,
    pub phase: ScanPhase,
    pub progress: ScanProgress,
    pub started_at: Instant,
}

impl ScanEntry {
    pub fn new(scan_type: ScanType, user_email: String, user_id: i32) -> Self {
        Self {
            scan_type,
            user_email,
            user_id,
            phase: ScanPhase::Fetching,
            progress: ScanProgress::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn elapsed_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Format elapsed time as "Xm Ys" or "Ys"
    fn format_elapsed(&self) -> String {
        let secs = self.elapsed_secs();
        if secs >= 60 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        }
    }
}

/// Thread-safe tracker for active scans
#[derive(Clone)]
pub struct ScanTracker {
    /// Map of user_id -> ScanEntry
    active_scans: Arc<RwLock<HashMap<i32, ScanEntry>>>,
    /// Processor stats tracker
    processor_tracker: ProcessorTracker,
}

impl ScanTracker {
    pub fn new() -> Self {
        Self {
            active_scans: Arc::new(RwLock::new(HashMap::new())),
            processor_tracker: ProcessorTracker::new(),
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

    /// Update the total email count for a scan
    pub fn set_total_emails(&self, user_id: i32, total: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.progress.total_emails = total;
        }
    }

    /// Update the processed email count for a scan
    pub fn set_processed_emails(&self, user_id: i32, processed: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.progress.processed_emails = processed;
        }
    }

    /// Increment the processed email count
    pub fn increment_processed(&self, user_id: i32, count: usize) {
        if let Some(entry) = self.active_scans.write().unwrap().get_mut(&user_id) {
            entry.progress.processed_emails += count;
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
    // Processor Tracking Methods
    // ========================================================================

    /// Update processor stats for a user
    pub fn update_processor(
        &self,
        user_email: &str,
        status: ProcessorStatus,
        processed_count: u64,
        queue_count: usize,
    ) {
        self.processor_tracker
            .update_user(user_email, status, processed_count, queue_count);
    }

    /// Remove a processor from tracking
    pub fn remove_processor(&self, user_email: &str) {
        self.processor_tracker.remove_user(user_email);
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

        let headers = ["User", "Type", "Phase", "Job ID", "Progress", "Elapsed"];
        let mut rows: Vec<Vec<String>> = scans
            .values()
            .map(|s| {
                vec![
                    s.user_email.clone(),
                    s.scan_type.to_string(),
                    s.phase.short_name().to_string(),
                    s.phase.job_id().unwrap_or("-").to_string(),
                    s.progress.format_progress(),
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

    /// Get formatted table of processor stats
    pub fn get_processors_table(&self) -> Option<String> {
        self.processor_tracker.get_table()
    }

    /// Get combined status display (both tables)
    pub fn get_current_state(&self) -> Option<String> {
        let scans_table = self.get_scans_table();
        let processors_table = self.get_processors_table();

        match (scans_table, processors_table) {
            (Some(scans), Some(procs)) => Some(format!("{}\n{}", procs, scans)),
            (Some(scans), None) => Some(scans),
            (None, Some(procs)) => Some(procs),
            (None, None) => None,
        }
    }
}

impl Default for ScanTracker {
    fn default() -> Self {
        Self::new()
    }
}
