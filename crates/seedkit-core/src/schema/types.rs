use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Top-level representation of a database schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub database_type: DatabaseType,
    pub database_name: String,
    pub tables: IndexMap<String, Table>,
    pub enums: IndexMap<String, Vec<String>>,
}

impl DatabaseSchema {
    pub fn new(database_type: DatabaseType, database_name: String) -> Self {
        Self {
            database_type,
            database_name,
            tables: IndexMap::new(),
            enums: IndexMap::new(),
        }
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn foreign_key_count(&self) -> usize {
        self.tables.values().map(|t| t.foreign_keys.len()).sum()
    }

    pub fn column_count(&self) -> usize {
        self.tables.values().map(|t| t.columns.len()).sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    PostgreSQL,
    MySQL,
    SQLite,
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::PostgreSQL => write!(f, "PostgreSQL"),
            DatabaseType::MySQL => write!(f, "MySQL"),
            DatabaseType::SQLite => write!(f, "SQLite"),
        }
    }
}

/// Represents a database table with its columns, keys, and constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema_name: Option<String>,
    pub columns: IndexMap<String, Column>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub check_constraints: Vec<CheckConstraint>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Self {
            name,
            schema_name: None,
            columns: IndexMap::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
        }
    }
}

/// Represents a single column in a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub raw_type: String,
    pub nullable: bool,
    pub has_default: bool,
    pub is_auto_increment: bool,
    pub max_length: Option<u32>,
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
    pub enum_values: Option<Vec<String>>,
    pub ordinal_position: u32,
}

impl Column {
    pub fn new(name: String, data_type: DataType, raw_type: String) -> Self {
        Self {
            name,
            data_type,
            raw_type,
            nullable: true,
            has_default: false,
            is_auto_increment: false,
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
            enum_values: None,
            ordinal_position: 0,
        }
    }
}

/// Normalized data type enum covering all supported databases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Small integer (int2, smallint)
    SmallInt,
    /// Standard integer (int4, integer, int)
    Integer,
    /// Large integer (int8, bigint)
    BigInt,
    /// Single-precision float (float4, real)
    Float,
    /// Double-precision float (float8, double precision)
    Double,
    /// Exact numeric with precision/scale (numeric, decimal)
    Numeric,
    /// Fixed-length string (char)
    Char,
    /// Variable-length string (varchar, character varying)
    VarChar,
    /// Unbounded text (text)
    Text,
    /// Boolean
    Boolean,
    /// Date only
    Date,
    /// Time only
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// UUID
    Uuid,
    /// JSON (json type)
    Json,
    /// Binary JSON (jsonb)
    Jsonb,
    /// Binary/blob data (bytea, blob)
    Binary,
    /// Array type (PostgreSQL arrays)
    Array(Box<DataType>),
    /// Database-specific enum type
    Enum(String),
    /// IP address (inet)
    Inet,
    /// MAC address
    MacAddr,
    /// XML
    Xml,
    /// Money type
    Money,
    /// Interval/duration
    Interval,
    /// Serial (auto-incrementing integer, PG)
    Serial,
    /// Big serial (auto-incrementing bigint, PG)
    BigSerial,
    /// Unknown or unrecognized type
    Unknown(String),
}

impl DataType {
    /// Parse a raw SQL type string into a normalized DataType.
    pub fn from_raw(raw: &str) -> Self {
        let normalized = raw.trim().to_lowercase();
        let normalized = normalized.as_str();

        // Handle array types first (PostgreSQL)
        if let Some(inner) = normalized.strip_suffix("[]") {
            return DataType::Array(Box::new(DataType::from_raw(inner)));
        }
        if let Some(inner) = normalized.strip_prefix('_') {
            if !inner.is_empty() {
                // PostgreSQL internal array type prefix
                return DataType::Array(Box::new(DataType::from_raw(inner)));
            }
        }

        match normalized {
            // Integer types
            "smallint" | "int2" | "smallserial" | "serial2" | "tinyint" => DataType::SmallInt,
            "integer" | "int" | "int4" | "mediumint" => DataType::Integer,
            "bigint" | "int8" => DataType::BigInt,
            "serial" | "serial4" => DataType::Serial,
            "bigserial" | "serial8" => DataType::BigSerial,

            // Float types
            "real" | "float4" | "float" => DataType::Float,
            "double precision" | "float8" | "double" => DataType::Double,

            // Numeric
            s if s.starts_with("numeric") || s.starts_with("decimal") => DataType::Numeric,

            // String types
            s if s.starts_with("character varying") || s.starts_with("varchar") => {
                DataType::VarChar
            }
            s if s.starts_with("char") || s.starts_with("character(") => DataType::Char,
            "text" | "tinytext" | "mediumtext" | "longtext" | "clob" => DataType::Text,

            // Boolean
            "boolean" | "bool" | "bit" => DataType::Boolean,

            // Date/time
            "date" => DataType::Date,
            "time" | "time without time zone" => DataType::Time,
            "timestamp" | "timestamp without time zone" | "datetime" => DataType::Timestamp,
            "timestamp with time zone" | "timestamptz" => DataType::TimestampTz,

            // UUID
            "uuid" => DataType::Uuid,

            // JSON
            "json" => DataType::Json,
            "jsonb" => DataType::Jsonb,

            // Binary
            "bytea" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
                DataType::Binary
            }

            // Network
            "inet" | "cidr" => DataType::Inet,
            "macaddr" | "macaddr8" => DataType::MacAddr,

            // Other
            "xml" => DataType::Xml,
            "money" => DataType::Money,
            "interval" => DataType::Interval,

            // Catch USER-DEFINED (enums) - handled by caller with enum name
            "user-defined" => DataType::Unknown("USER-DEFINED".to_string()),

            other => DataType::Unknown(other.to_string()),
        }
    }

    /// Returns true if this type represents an auto-incrementing sequence.
    pub fn is_serial(&self) -> bool {
        matches!(self, DataType::Serial | DataType::BigSerial)
    }

    /// Returns true if this type is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Float
                | DataType::Double
                | DataType::Numeric
                | DataType::Serial
                | DataType::BigSerial
                | DataType::Money
        )
    }

    /// Returns true if this type is a string/text type.
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Char | DataType::VarChar | DataType::Text)
    }

    /// Returns true if this type is a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::SmallInt => write!(f, "smallint"),
            DataType::Integer => write!(f, "integer"),
            DataType::BigInt => write!(f, "bigint"),
            DataType::Float => write!(f, "real"),
            DataType::Double => write!(f, "double precision"),
            DataType::Numeric => write!(f, "numeric"),
            DataType::Char => write!(f, "char"),
            DataType::VarChar => write!(f, "varchar"),
            DataType::Text => write!(f, "text"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Date => write!(f, "date"),
            DataType::Time => write!(f, "time"),
            DataType::Timestamp => write!(f, "timestamp"),
            DataType::TimestampTz => write!(f, "timestamptz"),
            DataType::Uuid => write!(f, "uuid"),
            DataType::Json => write!(f, "json"),
            DataType::Jsonb => write!(f, "jsonb"),
            DataType::Binary => write!(f, "bytea"),
            DataType::Array(inner) => write!(f, "{}[]", inner),
            DataType::Enum(name) => write!(f, "enum({})", name),
            DataType::Inet => write!(f, "inet"),
            DataType::MacAddr => write!(f, "macaddr"),
            DataType::Xml => write!(f, "xml"),
            DataType::Money => write!(f, "money"),
            DataType::Interval => write!(f, "interval"),
            DataType::Serial => write!(f, "serial"),
            DataType::BigSerial => write!(f, "bigserial"),
            DataType::Unknown(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: Option<String>,
    pub source_columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
    pub is_deferrable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl ForeignKeyAction {
    pub fn parse_action(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "CASCADE" => ForeignKeyAction::Cascade,
            "SET NULL" => ForeignKeyAction::SetNull,
            "SET DEFAULT" => ForeignKeyAction::SetDefault,
            "RESTRICT" => ForeignKeyAction::Restrict,
            _ => ForeignKeyAction::NoAction,
        }
    }
}

impl fmt::Display for ForeignKeyAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForeignKeyAction::NoAction => write!(f, "NO ACTION"),
            ForeignKeyAction::Restrict => write!(f, "RESTRICT"),
            ForeignKeyAction::Cascade => write!(f, "CASCADE"),
            ForeignKeyAction::SetNull => write!(f, "SET NULL"),
            ForeignKeyAction::SetDefault => write!(f, "SET DEFAULT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expression: String,
    /// Parsed representation of the check constraint, if parseable.
    pub parsed: Option<ParsedCheck>,
}

/// Simple parsed representation of common CHECK constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParsedCheck {
    /// column >= value
    GreaterThanOrEqual { column: String, value: f64 },
    /// column > value
    GreaterThan { column: String, value: f64 },
    /// column <= value
    LessThanOrEqual { column: String, value: f64 },
    /// column < value
    LessThan { column: String, value: f64 },
    /// column BETWEEN low AND high
    Between { column: String, low: f64, high: f64 },
    /// length(column) > 0 or char_length(column) > 0
    MinLength { column: String, min: usize },
    /// column IN (val1, val2, ...)
    InValues { column: String, values: Vec<String> },
    /// column1 < column2 (e.g., start_date < end_date)
    ColumnLessThan { left: String, right: String },
}
