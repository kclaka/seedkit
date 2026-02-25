use std::io::Write;

use base64::Engine;

use crate::error::{Result, SeedKitError};
use crate::generate::engine::GeneratedData;
use crate::generate::value::Value;

/// Write generated data as JSON using streaming serialization.
///
/// Writes directly to the writer table-by-table and row-by-row, avoiding
/// the OOM trap of building the entire JSON tree in memory before serializing.
/// Includes `_deferred_updates` when present (for circular FK resolution).
pub fn write_json<W: Write>(writer: &mut W, data: &GeneratedData) -> Result<()> {
    let table_count = data.tables.len();
    let has_deferred = !data.deferred_updates.is_empty();
    let total_keys = table_count + if has_deferred { 1 } else { 0 };

    write_str(writer, "{\n")?;

    for (table_idx, (table_name, rows)) in data.tables.iter().enumerate() {
        // Use serde_json for RFC 8259-compliant key escaping (not Rust's {:?} Debug format)
        let safe_table_key = json_key(table_name)?;
        write_str(writer, &format!("  {}: [\n", safe_table_key))?;

        for (row_idx, row) in rows.iter().enumerate() {
            write_str(writer, "    {")?;

            let col_count = row.len();
            for (col_idx, (col_name, value)) in row.iter().enumerate() {
                let safe_col_key = json_key(col_name)?;
                let val_str = json_value(value)?;
                write_str(writer, &format!("\n      {}: {}", safe_col_key, val_str))?;
                if col_idx < col_count - 1 {
                    write_str(writer, ",")?;
                }
            }

            write_str(writer, "\n    }")?;
            if row_idx < rows.len() - 1 {
                write_str(writer, ",")?;
            }
            write_str(writer, "\n")?;
        }

        write_str(writer, "  ]")?;
        if table_idx < total_keys - 1 {
            write_str(writer, ",")?;
        }
        write_str(writer, "\n")?;
    }

    // Deferred updates for circular FK resolution
    if has_deferred {
        write_str(writer, "  \"_deferred_updates\": [\n")?;
        for (i, update) in data.deferred_updates.iter().enumerate() {
            let safe_table = json_key(&update.table_name)?;
            let safe_col = json_key(&update.column_name)?;
            let val_str = json_value(&update.value)?;
            write_str(
                writer,
                &format!(
                    "    {{\"table\": {}, \"row_index\": {}, \"column\": {}, \"value\": {}}}",
                    safe_table, update.row_index, safe_col, val_str
                ),
            )?;
            if i < data.deferred_updates.len() - 1 {
                write_str(writer, ",")?;
            }
            write_str(writer, "\n")?;
        }
        write_str(writer, "  ]\n")?;
    }

    write_str(writer, "}\n")?;

    Ok(())
}

/// Helper to write a string slice and map IO errors.
fn write_str<W: Write>(writer: &mut W, s: &str) -> Result<()> {
    writer
        .write_all(s.as_bytes())
        .map_err(|e| SeedKitError::Output {
            message: "writing JSON".to_string(),
            source: e,
        })
}

/// Serialize a string as an RFC 8259-compliant JSON key.
/// Uses serde_json instead of Rust's Debug format (`{:?}`) which does not
/// properly escape unicode control characters per the JSON spec.
fn json_key(s: &str) -> Result<String> {
    serde_json::to_string(s).map_err(|e| SeedKitError::Other(format!("JSON key error: {}", e)))
}

/// Serialize a Value as an RFC 8259-compliant JSON value string.
fn json_value(value: &Value) -> Result<String> {
    let json_val = value_to_json(value);
    serde_json::to_string(&json_val)
        .map_err(|e| SeedKitError::Other(format!("JSON serialization error: {}", e)))
}

/// Convert a Value to its JSON representation.
///
/// Timestamps use ISO 8601 format with milliseconds and trailing 'Z'.
/// Bytes use Base64 encoding (standard alphabet with padding).
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Timestamp(ts) => {
            serde_json::Value::String(format!("{}Z", ts.format("%Y-%m-%dT%H:%M:%S%.3f")))
        }
        Value::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
        Value::Time(t) => serde_json::Value::String(t.format("%H:%M:%S").to_string()),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Json(j) => j.clone(),
        Value::Bytes(b) => {
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::engine::DeferredUpdate;
    use indexmap::IndexMap;
    use std::borrow::Cow;

    fn make_simple_data() -> GeneratedData {
        let mut tables = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert(
            "name".to_string(),
            Value::String(Cow::Owned("Alice".to_string())),
        );
        row.insert("active".to_string(), Value::Bool(true));
        tables.insert("users".to_string(), vec![row]);

        GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        }
    }

    #[test]
    fn test_write_json() {
        let data = make_simple_data();

        let mut output = Vec::new();
        write_json(&mut output, &data).unwrap();

        let json_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert!(parsed["users"].is_array());
        assert_eq!(parsed["users"][0]["name"], "Alice");
    }

    // --- Red/Green TDD tests for new fixes ---

    #[test]
    fn test_timestamp_iso8601_with_z() {
        let mut tables = IndexMap::new();
        let mut row = IndexMap::new();
        let ts = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_milli_opt(12, 30, 45, 123).unwrap(),
        );
        row.insert("created_at".to_string(), Value::Timestamp(ts));
        tables.insert("events".to_string(), vec![row]);

        let data = GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        };
        let mut output = Vec::new();
        write_json(&mut output, &data).unwrap();

        let json_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let ts_str = parsed["events"][0]["created_at"].as_str().unwrap();
        // Must include milliseconds and trailing 'Z'
        assert!(
            ts_str.ends_with('Z'),
            "Timestamp '{}' must end with 'Z'",
            ts_str
        );
        assert!(
            ts_str.contains('.'),
            "Timestamp '{}' must include milliseconds",
            ts_str
        );
        assert_eq!(ts_str, "2025-06-15T12:30:45.123Z");
    }

    #[test]
    fn test_bytes_base64_encoding() {
        let mut tables = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert(
            "data".to_string(),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );
        tables.insert("files".to_string(), vec![row]);

        let data = GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        };
        let mut output = Vec::new();
        write_json(&mut output, &data).unwrap();

        let json_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let encoded = parsed["files"][0]["data"].as_str().unwrap();
        // Should be base64, not hex
        assert_ne!(encoded, "deadbeef", "Bytes should be Base64, not hex");
        // Decode and verify round-trip
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap();
        assert_eq!(decoded, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_deferred_updates_in_output() {
        let mut tables = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), Value::Int(1));
        row.insert("parent_id".to_string(), Value::Null);
        tables.insert("categories".to_string(), vec![row]);

        let data = GeneratedData {
            tables,
            deferred_updates: vec![DeferredUpdate {
                table_name: "categories".to_string(),
                row_index: 0,
                column_name: "parent_id".to_string(),
                value: Value::Int(2),
            }],
        };

        let mut output = Vec::new();
        write_json(&mut output, &data).unwrap();

        let json_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        // Deferred updates must be present in output
        assert!(
            parsed["_deferred_updates"].is_array(),
            "JSON output must include _deferred_updates key"
        );
        let updates = parsed["_deferred_updates"].as_array().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0]["table"], "categories");
        assert_eq!(updates[0]["column"], "parent_id");
        assert_eq!(updates[0]["row_index"], 0);
    }

    #[test]
    fn test_streaming_produces_valid_json() {
        // Generate enough data to verify streaming doesn't corrupt output
        let mut tables = IndexMap::new();
        let mut rows = Vec::new();
        for i in 0..500 {
            let mut row = IndexMap::new();
            row.insert("id".to_string(), Value::Int(i));
            row.insert(
                "name".to_string(),
                Value::String(Cow::Owned(format!("user_{}", i))),
            );
            rows.push(row);
        }
        tables.insert("users".to_string(), rows);

        let data = GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        };
        let mut output = Vec::new();
        write_json(&mut output, &data).unwrap();

        let json_str = String::from_utf8(output).unwrap();
        // Must be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["users"].as_array().unwrap().len(), 500);
    }

    #[test]
    fn test_deterministic_column_order() {
        // Column order must be identical across multiple serializations.
        // With HashMap this would be non-deterministic; IndexMap preserves insertion order.
        let mut tables = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert("alpha".to_string(), Value::Int(1));
        row.insert("beta".to_string(), Value::Int(2));
        row.insert("gamma".to_string(), Value::Int(3));
        row.insert("delta".to_string(), Value::Int(4));
        tables.insert("test".to_string(), vec![row]);

        let data = GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        };

        // Serialize 10 times and assert byte-for-byte identical output
        let mut output1 = Vec::new();
        write_json(&mut output1, &data).unwrap();
        let json1 = String::from_utf8(output1).unwrap();

        for _ in 0..10 {
            let mut output = Vec::new();
            write_json(&mut output, &data).unwrap();
            let json = String::from_utf8(output).unwrap();
            assert_eq!(
                json1, json,
                "JSON output must be byte-for-byte deterministic"
            );
        }

        // Verify insertion order is preserved (alpha before beta before gamma)
        let alpha_pos = json1.find("\"alpha\"").unwrap();
        let beta_pos = json1.find("\"beta\"").unwrap();
        let gamma_pos = json1.find("\"gamma\"").unwrap();
        assert!(alpha_pos < beta_pos, "alpha must appear before beta");
        assert!(beta_pos < gamma_pos, "beta must appear before gamma");
    }
}
