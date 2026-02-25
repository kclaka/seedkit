use std::borrow::Cow;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A generated value for a database column.
///
/// The `String` variant uses `Cow<'static, str>` so that values drawn from
/// static lookup tables (statuses, currencies, departments, etc.) can be held
/// as zero-cost `&'static str` borrows, while dynamically generated values
/// (emails, usernames, formatted strings) are stored as owned `String`s.
/// At 10M+ rows this eliminates millions of unnecessary heap allocations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Cow<'static, str>),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
}

impl Value {
    /// Convert to a SQL literal string suitable for INSERT statements.
    pub fn to_sql_literal(&self, db_type: &crate::schema::types::DatabaseType) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => match db_type {
                crate::schema::types::DatabaseType::MySQL => {
                    if *b {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                }
                _ => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
            },
            Value::Int(i) => i.to_string(),
            Value::Float(f) => {
                if f.is_nan() {
                    "'NaN'".to_string()
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        "'Infinity'".to_string()
                    } else {
                        "'-Infinity'".to_string()
                    }
                } else {
                    format!("{}", f)
                }
            }
            Value::String(s) => format!("'{}'", s.replace('\'', "''")), // Cow<str> derefs to &str
            Value::Timestamp(ts) => format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S")),
            Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("'{}'", t.format("%H:%M:%S")),
            Value::Uuid(u) => format!("'{}'", u),
            Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
            Value::Bytes(b) => match db_type {
                crate::schema::types::DatabaseType::PostgreSQL => {
                    format!("'\\x{}'", hex_encode(b))
                }
                _ => {
                    format!("X'{}'", hex_encode(b))
                }
            },
        }
    }

    /// Convert to a CSV-friendly string.
    pub fn to_csv_string(&self) -> String {
        match self {
            Value::Null => "".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.to_string(),
            Value::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Time(t) => t.format("%H:%M:%S").to_string(),
            Value::Uuid(u) => u.to_string(),
            Value::Json(j) => j.to_string(),
            Value::Bytes(b) => hex_encode(b),
        }
    }

    /// Get a string representation for uniqueness tracking.
    pub fn to_unique_key(&self) -> String {
        match self {
            Value::Null => "__NULL__".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{:.10}", f),
            Value::String(s) => s.to_string(),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::Uuid(u) => u.to_string(),
            Value::Json(j) => j.to_string(),
            Value::Bytes(b) => hex_encode(b),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "{}", s),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::Json(j) => write!(f, "{}", j),
            Value::Bytes(b) => write!(f, "{}", hex_encode(b)),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
