//! Observability Module
//!
//! Provides tools for monitoring and tracking system state including:
//! - Active scans (initial scan, re-categorization, etc.)
//! - Scanner pipeline tracking
//! - Metrics and progress tracking

mod common;
mod pipeline_tracker;
mod scan_tracker;

pub use pipeline_tracker::{PipelineStats, PipelineTracker};
pub use scan_tracker::{ScanPhase, ScanTracker, ScanType};
