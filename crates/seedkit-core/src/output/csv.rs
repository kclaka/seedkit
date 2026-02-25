use std::io::Write;

use crate::error::{Result, SeedKitError};
use crate::generate::engine::GeneratedData;
use crate::generate::value::Value;

/// Write generated data as CSV files (one section per table).
/// Tables are separated by a comment header line.
pub fn write_csv<W: Write>(writer: &mut W, data: &GeneratedData) -> Result<()> {
    for (table_name, rows) in &data.tables {
        if rows.is_empty() {
            continue;
        }

        // Table header
        writeln!(writer, "# Table: {}", table_name).map_err(|e| SeedKitError::Output {
            message: format!("writing CSV header for {}", table_name),
            source: e,
        })?;

        // Column headers
        let columns: Vec<&String> = rows[0].keys().collect();
        writeln!(
            writer,
            "{}",
            columns
                .iter()
                .map(|c| csv_escape(c))
                .collect::<Vec<_>>()
                .join(",")
        )
        .map_err(|e| SeedKitError::Output {
            message: format!("writing CSV columns for {}", table_name),
            source: e,
        })?;

        // Data rows
        for row in rows {
            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    row.get(*col)
                        .map(|v| csv_escape(&v.to_csv_string()))
                        .unwrap_or_default()
                })
                .collect();

            writeln!(writer, "{}", values.join(",")).map_err(|e| SeedKitError::Output {
                message: format!("writing CSV row for {}", table_name),
                source: e,
            })?;
        }

        writeln!(writer).map_err(|e| SeedKitError::Output {
            message: "writing newline".to_string(),
            source: e,
        })?;
    }

    Ok(())
}

/// Write CSV for a single table to a writer.
pub fn write_csv_table<W: Write>(
    writer: &mut W,
    table_name: &str,
    rows: &[indexmap::IndexMap<String, Value>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let columns: Vec<&String> = rows[0].keys().collect();
    writeln!(
        writer,
        "{}",
        columns
            .iter()
            .map(|c| csv_escape(c))
            .collect::<Vec<_>>()
            .join(",")
    )
    .map_err(|e| SeedKitError::Output {
        message: format!("writing CSV columns for {}", table_name),
        source: e,
    })?;

    for row in rows {
        let values: Vec<String> = columns
            .iter()
            .map(|col| {
                row.get(*col)
                    .map(|v| csv_escape(&v.to_csv_string()))
                    .unwrap_or_default()
            })
            .collect();

        writeln!(writer, "{}", values.join(",")).map_err(|e| SeedKitError::Output {
            message: format!("writing CSV row for {}", table_name),
            source: e,
        })?;
    }

    Ok(())
}

/// Escape a string for CSV: quote if it contains comma, quote, or newline.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape("hello"), "hello");
        assert_eq!(csv_escape("hello,world"), "\"hello,world\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }
}
