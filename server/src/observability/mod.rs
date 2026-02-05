//! Observability Module
//!
//! Provides tools for monitoring and tracking system state including:
//! - Active scans (initial scan, re-categorization, etc.)
//! - Scanner pipeline tracking
//! - Metrics and progress tracking
//! - Critical alert paging via Pushover

mod common;
pub mod pager;
mod pipeline_tracker;
mod scan_tracker;

pub use pipeline_tracker::{PipelineStats, PipelineTracker};
pub use scan_tracker::{ScanPhase, ScanTracker, ScanType};
