pub mod batch_processor;
pub mod initial_scan;
pub mod scanner_pipeline;
pub mod shared;

// Old queue processor system - replaced by scanner_pipeline
// Keeping module for reference but not exposing publicly
#[allow(dead_code)]
mod queue_processor;

pub use scanner_pipeline::ScannerPipeline;
