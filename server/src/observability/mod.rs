//! Observability Module
//!
//! Provides tools for monitoring and tracking system state including:
//! - Active scans (initial scan, re-categorization, etc.)
//! - Scanner pipeline tracking
//! - Metrics and progress tracking

mod scan_tracker;

pub use scan_tracker::{PipelineStats, PipelineTracker, ScanPhase, ScanTracker, ScanType};
