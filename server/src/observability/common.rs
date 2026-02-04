//! Common Observability Utilities
//!
//! Shared types and functions used across observability modules.

// ============================================================================
// Progress Tracking
// ============================================================================

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
    pub fn format_progress(&self) -> String {
        format!("{} / {}", self.current, self.total)
    }
}

// ============================================================================
// Table Formatting Utilities
// ============================================================================

fn format_row(list: Vec<String>) -> String {
    format!("| {} |\n", list.join(" | "))
}

/// Calculate column widths based on headers and all row values
fn calculate_column_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let num_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    if num_cols < headers.len() {
        panic!("Rows must be longer than headers!");
    }

    (0..num_cols)
        .map(|i| {
            let header_width = headers.get(i).map(|h| h.len()).unwrap_or(0);
            let max_row_width = rows
                .iter()
                .filter_map(|row| row.get(i))
                .map(|cell| cell.len())
                .max()
                .unwrap_or(0);
            header_width.max(max_row_width)
        })
        .collect()
}

/// Format header row with separator
fn format_headers(headers: &[&str], widths: &[usize], output: &mut String) {
    if headers.is_empty() {
        return;
    }

    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
        .collect();
    output.push_str(&format_row(header_line));

    let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    output.push_str(&format!("|-{}-|\n", separator.join("-|-")));
}

/// Format data rows
fn format_data_rows(rows: &[Vec<String>], widths: &[usize], output: &mut String) {
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
}

/// Format a table with headers and rows
pub fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let widths = calculate_column_widths(headers, rows);
    let mut output = String::new();

    format_headers(headers, &widths, &mut output);
    format_data_rows(rows, &widths, &mut output);

    output
}

/// Format a table with a title, headers, and rows
pub fn format_table_with_title(title: &str, headers: &[&str], rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut widths = calculate_column_widths(headers, rows);

    // Calculate total inner width (content between outer "| " and " |")
    // This is: sum of column widths + " | " separators between columns
    let total_col_width: usize = widths.iter().sum::<usize>() + (widths.len() - 1) * 3;

    // Ensure inner width is at least as wide as the title
    let inner_width = total_col_width.max(title.len());

    // If title is wider than the table, distribute extra width to last column
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

    format_headers(headers, &widths, &mut output);
    format_data_rows(rows, &widths, &mut output);

    // Close off table
    output.push_str(&format!(" {} \n", "-".repeat(inner_width + 2)));

    output
}

/// Format elapsed time as "Xm Ys" or "Ys"
pub fn format_elapsed_secs(secs: u64) -> String {
    if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}
