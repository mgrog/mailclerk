//! Scan Tracker Module
//!
//! Provides observability for batch scans (initial scan, re-categorization, etc.)
//! and batch pipeline tracking with rolling window metrics.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use itertools::{EitherOrBoth, Itertools};
use tracing::info;

use crate::state::email_scanner::scanner_pipeline::queues::PipelineQueues;
use crate::state::email_scanner::scanner_pipeline::types::PipelineStage;

// ============================================================================
// Table Formatting Utilities
// ============================================================================

fn format_row(list: Vec<String>) -> String {
    format!("| {} |\n", list.join(" | "))
}

/// Format a table with headers and rows
fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    // Calculate column widths (max of header and all row values)
    let widths: Vec<usize> = headers
        .iter()
        .zip_longest(rows)
        .map(|e| match e {
            EitherOrBoth::Both(h, r) => h.len().max(r.len()),
            EitherOrBoth::Right(r) => r.len(),
            EitherOrBoth::Left(_) => panic!("Rows must be longer than headers!"),
        })
        .collect();

    let mut output = String::new();

    if !headers.is_empty() {
        // Header row
        let header_line: Vec<String> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
            .collect();
        output.push_str(&format_row(header_line));

        // Separator
        let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        output.push_str(&format!("|-{}-|\n", separator.join("-|-")));
    }

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
        output.push_str(&format_row(cells));
    }

    output
}

/// Format a table with a title, headers, and rows
fn format_table_with_title(title: &str, headers: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    // Calculate column widths (max of header and all row values)
    let mut widths: Vec<usize> = headers
        .iter()
        .zip_longest(rows[0].clone())
        .map(|e| match e {
            EitherOrBoth::Both(h, r) => h.len().max(r.len()),
            EitherOrBoth::Right(r) => r.len(),
            EitherOrBoth::Left(_) => panic!("Rows must be longer than headers!"),
        })
        .collect();

    // Calculate total inner width (content between outer "| " and " |")
    // This is: sum of column widths + " | " separators between columns
    let inner_width: usize = widths.iter().sum::<usize>() + (widths.len() - 1) * 3;

    // Ensure inner width is at least as wide as the title
    let inner_width = inner_width.max(title.len());

    // Recalculate widths if title is wider than the table
    // Distribute extra width to last column
    let total_col_width: usize = widths.iter().sum::<usize>() + (widths.len() - 1) * 3;
    if inner_width > total_col_width {
        let extra = inner_width - total_col_width;
        if let Some(last) = widths.last_mut() {
            *last += extra;
        }
    }

    let mut output = String::new();

    // Title row
    output.push_str(&format!(" {} \n", "=".repeat(inner_width + 2)));
    output.push_str(&format!("| {:<inner_width$} |\n", title));
    output.push_str(&format!("|-{}-|\n", "-".repeat(inner_width)));

    if !headers.is_empty() {
        // Header row
        let header_line: Vec<String> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
            .collect();
        output.push_str(&format_row(header_line));

        // Separator
        let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        output.push_str(&format!("|-{}-|\n", separator.join("-|-")));
    }

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
        output.push_str(&format_row(cells));
    }
    // Close off table
    output.push_str(&format!(" {} \n", "-".repeat(inner_width + 2)));

    output
}

// ============================================================================
// Rolling Window Rate Tracking
// ============================================================================

/// Rolling window size for rate calculations (in seconds)
const ROLLING_WINDOW_SECS: u64 = 10;

/// A timestamped count for rolling window calculations
#[derive(Debug, Clone)]
struct TimestampedCount {
    timestamp: Instant,
    count: u64,
}

/// Reusable rate tracker with rolling window
#[derive(Debug, Default)]
pub struct RateTracker {
    history: VecDeque<TimestampedCount>,
}

impl RateTracker {
    pub fn new() -> Self {
        Self {
            history: VecDeque::new(),
        }
    }

    /// Record a new cumulative count
    pub fn record(&mut self, count: u64) {
        let now = Instant::now();

        // Add new entry
        self.history.push_back(TimestampedCount {
            timestamp: now,
            count,
        });

        // Prune entries older than the window
        let cutoff = now - std::time::Duration::from_secs(ROLLING_WINDOW_SECS);
        while let Some(front) = self.history.front() {
            if front.timestamp < cutoff {
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Calculate rate per second based on rolling window
    pub fn rate_per_sec(&self) -> f64 {
        if self.history.len() < 2 {
            return 0.0;
        }

        let oldest = self.history.front().unwrap();
        let newest = self.history.back().unwrap();

        let count_diff = newest.count.saturating_sub(oldest.count);
        let time_diff = newest.timestamp.duration_since(oldest.timestamp);

        if time_diff.as_secs_f64() < 0.1 {
            return 0.0;
        }

        count_diff as f64 / time_diff.as_secs_f64()
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

/// Progress tracking with current and total counts
#[derive(Debug, Clone, Default)]
pub struct Progress {
    pub current: usize,
    pub total: usize,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        Self { current: 0, total }
    }

    pub fn percentage(&self) -> f32 {
        if self.total == 0 {
            0.0
        } else {
            (self.current as f32 / self.total as f32) * 100.0
        }
    }

    /// Format as "current / total"
    fn format_progress(&self) -> String {
        format!("{} / {}", self.current, self.total)
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

// ============================================================================
// Scanner Pipeline Tracking
// ============================================================================

/// Phase of a pipeline batch job
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchPhase {
    Idle,
    Running { job_id: Option<String> },
    Complete,
    Failed { error: String },
}

impl BatchPhase {
    fn short_name(&self) -> &str {
        match self {
            BatchPhase::Idle => "Idle",
            BatchPhase::Running { .. } => "Running",
            BatchPhase::Complete => "Complete",
            BatchPhase::Failed { .. } => "Failed",
        }
    }

    fn job_id(&self) -> Option<&str> {
        match self {
            BatchPhase::Running { job_id } => job_id.as_deref(),
            _ => None,
        }
    }
}

impl fmt::Display for BatchPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchPhase::Idle => write!(f, "Idle"),
            BatchPhase::Running { job_id } => {
                if let Some(id) = job_id {
                    write!(f, "Running (job: {})", id)
                } else {
                    write!(f, "Running")
                }
            }
            BatchPhase::Complete => write!(f, "Complete"),
            BatchPhase::Failed { error } => write!(f, "Failed: {}", error),
        }
    }
}

/// How long to keep completed jobs visible in the status table
const COMPLETED_JOB_RETENTION_SECS: u64 = 30;

/// Stats for a single pipeline batch job
#[derive(Debug, Clone)]
pub struct BatchJobStats {
    pub stage: PipelineStage,
    pub phase: BatchPhase,
    pub progress: Progress,
    pub estimated_tokens: u64,
    pub actual_tokens: Option<u64>,
    pub started_at: Instant,
    /// When the job completed (for retention before cleanup)
    pub completed_at: Option<Instant>,
}

impl BatchJobStats {
    fn new(stage: PipelineStage, total_items: usize, estimated_tokens: u64) -> Self {
        Self {
            stage,
            phase: BatchPhase::Running { job_id: None },
            progress: Progress::new(total_items),
            estimated_tokens,
            actual_tokens: None,
            started_at: Instant::now(),
            completed_at: None,
        }
    }

    fn elapsed_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    fn format_elapsed(&self) -> String {
        let secs = self.elapsed_secs();
        if secs >= 60 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        }
    }

    fn format_tokens(&self) -> String {
        match self.actual_tokens {
            Some(actual) => format!("{} / {}", self.estimated_tokens, actual),
            None => format!("{} / -", self.estimated_tokens),
        }
    }
}

/// Overall pipeline stats
#[derive(Debug, Clone)]
pub struct PipelineStats {
    /// Counts per queue
    pub pending_fetch_users: usize,
    pub pending_fetch_emails: usize,
    pub main_categorization_queue: usize,
    pub user_defined_categorization_queue: usize,
    pub task_extraction_queue: usize,
    pub done_queue: usize,
    pub failed_queue: usize,
    pub total_in_pipeline: usize,
    pub recently_processed_count: usize,

    /// Rolling window rates
    pub emails_fetched_per_sec: f64,
    pub emails_processed_per_sec: f64,
    pub emails_inserted_per_sec: f64,

    /// Active batch jobs
    pub active_jobs: Vec<BatchJobStats>,
}

impl std::fmt::Display for PipelineStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pend_usr: {} pend_email={} main_cat={} user_cat={} task_ext={} done={} failed={} total={} recent={} fetch/s={:.1} proc/s={:.1} ins/s={:.1} jobs={}",
            self.pending_fetch_users,
            self.pending_fetch_emails,
            self.main_categorization_queue,
            self.user_defined_categorization_queue,
            self.task_extraction_queue,
            self.done_queue,
            self.failed_queue,
            self.total_in_pipeline,
            self.recently_processed_count,
            self.emails_fetched_per_sec,
            self.emails_processed_per_sec,
            self.emails_inserted_per_sec,
            self.active_jobs.len()
        )
    }
}

// ============================================================================
// Per-User Poll/Fetch Tracking
// ============================================================================

/// Status of a user being polled or fetched
#[derive(Debug, Clone)]
pub enum UserActivityStatus {
    Polling,
    PolledFound { new_emails: usize },
    PolledNone,
    PolledSkipped { reason: String },
    Fetching { current: usize, total: usize },
    FetchComplete { fetched: usize, total: usize },
}

impl UserActivityStatus {
    fn short_name(&self) -> &str {
        match self {
            UserActivityStatus::Polling => "Polling",
            UserActivityStatus::PolledFound { .. } => "Found",
            UserActivityStatus::PolledNone => "None",
            UserActivityStatus::PolledSkipped { .. } => "Skipped",
            UserActivityStatus::Fetching { .. } => "Fetching",
            UserActivityStatus::FetchComplete { .. } => "Fetched",
        }
    }

    fn details(&self) -> String {
        match self {
            UserActivityStatus::Polling => "-".to_string(),
            UserActivityStatus::PolledFound { new_emails } => format!("{} new", new_emails),
            UserActivityStatus::PolledNone => "0 new".to_string(),
            UserActivityStatus::PolledSkipped { reason } => reason.clone(),
            UserActivityStatus::Fetching { current, total } => format!("{}/{}", current, total),
            UserActivityStatus::FetchComplete { fetched, total } => {
                format!("{}/{}", fetched, total)
            }
        }
    }
}

/// Entry tracking a user's current activity
#[derive(Debug, Clone)]
pub struct UserActivityEntry {
    pub user_id: i32,
    pub user_email: String,
    pub status: UserActivityStatus,
    pub started_at: Instant,
    pub token_usage: i64,
    pub daily_token_limit: i64,
}

impl UserActivityEntry {
    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn format_elapsed(&self) -> String {
        let ms = self.elapsed_ms();
        if ms >= 1000 {
            format!("{:.1}s", ms as f64 / 1000.0)
        } else {
            format!("{}ms", ms)
        }
    }

    fn format_token_usage(&self) -> String {
        let usage = format_abbreviated_number(self.token_usage);
        if self.daily_token_limit == i64::MAX {
            format!("{}/∞", usage)
        } else {
            let limit = format_abbreviated_number(self.daily_token_limit);
            format!("{}/{}", usage, limit)
        }
    }
}

/// Format a number in abbreviated form with up to 2 significant digits.
/// Drops decimal if it would exceed 3 total digits (i.e., >= 100).
/// Examples: 2000 -> "2.0k", 2100 -> "2.1k", 99000 -> "99.0k", 100000 -> "100k", 2_000_000 -> "2.0M"
fn format_abbreviated_number(n: i64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        let val = n as f64 / 1_000.0;
        if val < 100.0 {
            format!("{:.1}k", val)
        } else {
            format!("{:.0}k", val)
        }
    } else {
        let val = n as f64 / 1_000_000.0;
        if val < 100.0 {
            format!("{:.1}M", val)
        } else {
            format!("{:.0}M", val)
        }
    }
}

/// How long to keep completed user activities visible (in ms)
const USER_ACTIVITY_RETENTION_MS: u64 = 15000;

/// Thread-safe tracker for the batch pipeline
#[derive(Clone)]
pub struct PipelineTracker {
    /// Current batch job stats (one per stage that could be running)
    batch_jobs: Arc<RwLock<HashMap<PipelineStage, BatchJobStats>>>,

    /// Rolling window rate trackers
    fetch_rate: Arc<RwLock<RateTracker>>,
    process_rate: Arc<RwLock<RateTracker>>,
    insert_rate: Arc<RwLock<RateTracker>>,

    /// Cumulative counters
    total_fetched: Arc<RwLock<u64>>,
    total_processed: Arc<RwLock<u64>>,
    total_inserted: Arc<RwLock<u64>>,

    /// Reference to queues for count stats
    queues: Arc<PipelineQueues>,

    /// Per-user activity tracking (user_id -> activity entry)
    user_activities: Arc<RwLock<HashMap<i32, UserActivityEntry>>>,
}

impl PipelineTracker {
    pub fn new(queues: Arc<PipelineQueues>) -> Self {
        Self {
            batch_jobs: Arc::new(RwLock::new(HashMap::new())),
            fetch_rate: Arc::new(RwLock::new(RateTracker::new())),
            process_rate: Arc::new(RwLock::new(RateTracker::new())),
            insert_rate: Arc::new(RwLock::new(RateTracker::new())),
            total_fetched: Arc::new(RwLock::new(0)),
            total_processed: Arc::new(RwLock::new(0)),
            total_inserted: Arc::new(RwLock::new(0)),
            queues,
            user_activities: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ========================================================================
    // Batch Job Tracking
    // ========================================================================

    /// Register a new batch job starting
    pub fn start_batch_job(&self, stage: PipelineStage, total_items: usize, estimated_tokens: u64) {
        let stats = BatchJobStats::new(stage, total_items, estimated_tokens);
        {
            let mut jobs = self.batch_jobs.write().unwrap();
            jobs.insert(stage, stats);
        }
        // Log status
        info!("Scanner Pipeline Status Update:\n{}", self.get_status_table());
    }

    /// Update batch job with job ID once submitted
    pub fn set_job_id(&self, stage: PipelineStage, job_id: String) {
        let mut jobs = self.batch_jobs.write().unwrap();
        if let Some(stats) = jobs.get_mut(&stage) {
            stats.phase = BatchPhase::Running {
                job_id: Some(job_id),
            };
        }
    }

    /// Update progress during batch processing (used as track_fn)
    pub fn update_batch_progress(&self, stage: PipelineStage, completed: usize) {
        let mut jobs = self.batch_jobs.write().unwrap();
        if let Some(stats) = jobs.get_mut(&stage) {
            stats.progress.current = completed;
        }
    }

    /// Complete a batch job with actual token usage
    pub fn complete_batch_job(&self, stage: PipelineStage, actual_tokens: u64) {
        {
            let mut jobs = self.batch_jobs.write().unwrap();
            if let Some(stats) = jobs.get_mut(&stage) {
                let estimated = stats.estimated_tokens;
                stats.phase = BatchPhase::Complete;
                stats.actual_tokens = Some(actual_tokens);
                stats.progress.current = stats.progress.total;
                stats.completed_at = Some(Instant::now());

                // Log token comparison
                let diff = actual_tokens as i64 - estimated as i64;
                info!(
                    "{} batch complete: {} items, estimated {} tokens, actual {} tokens (diff: {:+})",
                    stage,
                    stats.progress.total,
                    estimated,
                    actual_tokens,
                    diff
                );
            }
        }

        // Log status
        info!("Scanner Pipeline Status Update:\n{}", self.get_status_table());
    }

    /// Fail a batch job
    pub fn fail_batch_job(&self, stage: PipelineStage, error: String) {
        {
            let mut jobs = self.batch_jobs.write().unwrap();
            if let Some(stats) = jobs.get_mut(&stage) {
                stats.phase = BatchPhase::Failed {
                    error: error.clone(),
                };
                stats.completed_at = Some(Instant::now());
                tracing::error!("{} batch failed: {}", stage, error);
            }
        }

        // Log status
        info!("Scanner Pipeline Status Update:\n{}", self.get_status_table());
    }

    /// Remove jobs that have been completed for longer than the retention period
    fn cleanup_old_completed_jobs(&self) {
        let mut jobs = self.batch_jobs.write().unwrap();
        let cutoff = Instant::now() - std::time::Duration::from_secs(COMPLETED_JOB_RETENTION_SECS);

        jobs.retain(|_stage, stats| {
            match stats.completed_at {
                Some(completed_at) => completed_at > cutoff,
                None => true, // Keep jobs that haven't completed yet
            }
        });
    }

    // ========================================================================
    // Rate Tracking
    // ========================================================================

    /// Increment emails fetched count and update rate tracker
    pub fn increment_fetched(&self, count: u64) {
        let mut total = self.total_fetched.write().unwrap();
        *total += count;
        let mut rate = self.fetch_rate.write().unwrap();
        rate.record(*total);
    }

    /// Increment emails processed count and update rate tracker
    pub fn increment_processed(&self, count: u64) {
        let mut total = self.total_processed.write().unwrap();
        *total += count;
        let mut rate = self.process_rate.write().unwrap();
        rate.record(*total);
    }

    /// Increment emails inserted count and update rate tracker
    pub fn increment_inserted(&self, count: u64) {
        let mut total = self.total_inserted.write().unwrap();
        *total += count;
        let mut rate = self.insert_rate.write().unwrap();
        rate.record(*total);
    }

    // ========================================================================
    // User Activity Tracking
    // ========================================================================

    /// Record that a user poll has started
    pub fn user_poll_start(
        &self,
        user_id: i32,
        user_email: String,
        token_usage: i64,
        daily_token_limit: i64,
    ) {
        let entry = UserActivityEntry {
            user_id,
            user_email,
            status: UserActivityStatus::Polling,
            started_at: Instant::now(),
            token_usage,
            daily_token_limit,
        };
        self.user_activities.write().unwrap().insert(user_id, entry);
    }

    /// Record that a user poll found new emails
    pub fn user_poll_found(&self, user_id: i32, new_emails: usize) {
        let mut activities = self.user_activities.write().unwrap();
        if let Some(entry) = activities.get_mut(&user_id) {
            entry.status = UserActivityStatus::PolledFound { new_emails };
        }
    }

    /// Record that a user poll found no new emails
    pub fn user_poll_none(&self, user_id: i32) {
        let mut activities = self.user_activities.write().unwrap();
        if let Some(entry) = activities.get_mut(&user_id) {
            entry.status = UserActivityStatus::PolledNone;
        }
    }

    /// Record that a user poll was skipped
    pub fn user_poll_skipped(
        &self,
        user_id: i32,
        user_email: String,
        reason: String,
        token_usage: i64,
        daily_token_limit: i64,
    ) {
        let entry = UserActivityEntry {
            user_id,
            user_email,
            status: UserActivityStatus::PolledSkipped { reason },
            started_at: Instant::now(),
            token_usage,
            daily_token_limit,
        };
        self.user_activities.write().unwrap().insert(user_id, entry);
    }

    /// Record that message fetching has started for a user
    pub fn user_fetch_start(
        &self,
        user_id: i32,
        user_email: String,
        total: usize,
        token_usage: i64,
        daily_token_limit: i64,
    ) {
        let entry = UserActivityEntry {
            user_id,
            user_email,
            status: UserActivityStatus::Fetching { current: 0, total },
            started_at: Instant::now(),
            token_usage,
            daily_token_limit,
        };
        self.user_activities.write().unwrap().insert(user_id, entry);
    }

    /// Update fetch progress for a user
    pub fn user_fetch_progress(&self, user_id: i32, current: usize) {
        let mut activities = self.user_activities.write().unwrap();
        if let Some(entry) = activities.get_mut(&user_id) {
            if let UserActivityStatus::Fetching { total, .. } = entry.status {
                entry.status = UserActivityStatus::Fetching { current, total };
            }
        }
    }

    /// Record that message fetching has completed for a user
    pub fn user_fetch_complete(&self, user_id: i32, fetched: usize, total: usize) {
        let mut activities = self.user_activities.write().unwrap();
        if let Some(entry) = activities.get_mut(&user_id) {
            entry.status = UserActivityStatus::FetchComplete { fetched, total };
        }
    }

    /// Remove stale user activity entries (completed ones older than retention period)
    fn cleanup_old_user_activities(&self) {
        let mut activities = self.user_activities.write().unwrap();
        activities.retain(|_user_id, entry| {
            match &entry.status {
                // Keep active entries (polling, fetching)
                UserActivityStatus::Polling | UserActivityStatus::Fetching { .. } => true,
                // Remove completed/skipped entries older than retention period
                _ => entry.elapsed_ms() < USER_ACTIVITY_RETENTION_MS,
            }
        });
    }

    // ========================================================================
    // Stats
    // ========================================================================

    /// Get current pipeline stats
    pub fn get_stats(&self) -> PipelineStats {
        // Clean up old completed jobs before reporting stats
        self.cleanup_old_completed_jobs();

        let queue_counts = self.queues.get_queue_counts();
        let jobs = self.batch_jobs.read().unwrap();

        PipelineStats {
            pending_fetch_users: queue_counts.pending_fetch_users,
            pending_fetch_emails: queue_counts.pending_fetch_emails,
            main_categorization_queue: queue_counts.main_categorization_queue,
            user_defined_categorization_queue: queue_counts.user_defined_categorization_queue,
            task_extraction_queue: queue_counts.task_extraction_queue,
            done_queue: queue_counts.done_queue,
            failed_queue: queue_counts.failed_queue,
            total_in_pipeline: queue_counts.total_in_pipeline,
            recently_processed_count: queue_counts.recently_processed,
            emails_fetched_per_sec: self.fetch_rate.read().unwrap().rate_per_sec(),
            emails_processed_per_sec: self.process_rate.read().unwrap().rate_per_sec(),
            emails_inserted_per_sec: self.insert_rate.read().unwrap().rate_per_sec(),
            active_jobs: jobs.values().cloned().collect(),
        }
    }

    // ========================================================================
    // Display
    // ========================================================================

    /// Get formatted status table for logging
    pub fn get_status_table(&self) -> String {
        // Clean up old entries first
        self.cleanup_old_user_activities();

        let mut output = String::new();

        output.push_str("Pipeline Status Update \n");
        // General Stats
        let stats = self.get_stats();
        output.push_str(&format!("{}\n", stats));

        // User activities table
        let activities = self.user_activities.read().unwrap();
        if !activities.is_empty() {
            let user_headers = ["User", "Status", "Details", "Token Usage", "Elapsed"];
            let mut user_rows: Vec<Vec<String>> = activities
                .values()
                .map(|entry| {
                    vec![
                        entry.user_email.clone(),
                        entry.status.short_name().to_string(),
                        entry.status.details(),
                        entry.format_token_usage(),
                        entry.format_elapsed(),
                    ]
                })
                .collect();
            // Sort by user email
            user_rows.sort_by(|a, b| a[0].cmp(&b[0]));
            output.push_str(&format_table_with_title(
                "User Poll/Fetch Activity",
                &user_headers,
                &user_rows,
            ));
        }
        drop(activities);

        // Active batch jobs table
        if !stats.active_jobs.is_empty() {
            let job_headers = [
                "Stage",
                "Phase",
                "Job ID",
                "Progress",
                "Tokens (est/actual)",
                "Elapsed",
            ];
            let job_rows: Vec<Vec<String>> = stats
                .active_jobs
                .iter()
                .map(|job| {
                    vec![
                        job.stage.to_string(),
                        job.phase.short_name().to_string(),
                        job.phase.job_id().unwrap_or("-").to_string(),
                        job.progress.format_progress(),
                        job.format_tokens(),
                        job.format_elapsed(),
                    ]
                })
                .collect();
            output.push_str(&format_table_with_title(
                "Active Batch Jobs",
                &job_headers,
                &job_rows,
            ));
        }

        output
    }
}
