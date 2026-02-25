use std::sync::LazyLock;

use regex::Regex;

use crate::classify::semantic::SemanticType;
use crate::schema::types::DataType;

/// A pre-compiled classification rule with ready-to-use regex patterns.
struct CompiledRule {
    pattern: Regex,
    type_constraint: Option<&'static [DataType]>,
    table_pattern: Option<Regex>,
    semantic_type: SemanticType,
}

/// Pre-compiled table-context rules — built once on first access.
static COMPILED_TABLE_CONTEXT_RULES: LazyLock<Vec<CompiledRule>> = LazyLock::new(|| {
    TABLE_CONTEXT_RULES
        .iter()
        .map(|r| CompiledRule {
            pattern: Regex::new(r.pattern).unwrap(),
            type_constraint: r.type_constraint,
            table_pattern: r.table_pattern.map(|p| Regex::new(p).unwrap()),
            semantic_type: r.semantic_type,
        })
        .collect()
});

/// Pre-compiled general rules — built once on first access.
static COMPILED_GENERAL_RULES: LazyLock<Vec<CompiledRule>> = LazyLock::new(|| {
    GENERAL_RULES
        .iter()
        .map(|r| CompiledRule {
            pattern: Regex::new(r.pattern).unwrap(),
            type_constraint: r.type_constraint,
            table_pattern: r.table_pattern.map(|p| Regex::new(p).unwrap()),
            semantic_type: r.semantic_type,
        })
        .collect()
});

/// A classification rule: regex pattern + optional type constraint + optional table context.
struct ClassificationRule {
    /// Regex to match against the normalized column name
    pattern: &'static str,
    /// Optional data type constraint (column must have one of these types)
    type_constraint: Option<&'static [DataType]>,
    /// Optional table name pattern (restricts rule to matching tables)
    table_pattern: Option<&'static str>,
    /// The semantic type to assign if matched
    semantic_type: SemanticType,
}

/// Classify a column based on its name, data type, and table context.
pub fn classify_column(
    column_name: &str,
    data_type: &DataType,
    table_name: &str,
    is_auto_increment: bool,
    is_primary_key: bool,
    enum_values: Option<&[String]>,
) -> SemanticType {
    // Pre-check: auto-increment PK columns
    if is_auto_increment && is_primary_key {
        return SemanticType::AutoIncrement;
    }

    // Pre-check: serial types as PK
    if is_primary_key && data_type.is_serial() {
        return SemanticType::AutoIncrement;
    }

    // Pre-check: UUID primary key
    if is_primary_key && matches!(data_type, DataType::Uuid) {
        return SemanticType::Uuid;
    }

    // Pre-check: enum values present
    if let Some(values) = enum_values {
        if !values.is_empty() {
            // Try to classify the enum more specifically
            let specific = classify_enum_by_name(column_name, table_name);
            if specific != SemanticType::Unknown {
                return specific;
            }
            return SemanticType::EnumValue;
        }
    }

    let normalized = normalize_column_name(column_name);

    // Pass 1: Table-contextual rules (highest priority)
    for rule in COMPILED_TABLE_CONTEXT_RULES.iter() {
        if let Some(table_re) = &rule.table_pattern {
            if !table_re.is_match(table_name) {
                continue;
            }
        }
        if rule.pattern.is_match(&normalized) {
            if let Some(types) = rule.type_constraint {
                if !types.contains(data_type) {
                    continue;
                }
            }
            return rule.semantic_type;
        }
    }

    // Pass 2: General name-based rules
    for rule in COMPILED_GENERAL_RULES.iter() {
        if rule.pattern.is_match(&normalized) {
            if let Some(types) = rule.type_constraint {
                if !types.contains(data_type) {
                    continue;
                }
            }
            return rule.semantic_type;
        }
    }

    // Pass 3: Type-based fallbacks
    type_based_fallback(data_type)
}

/// Normalize a column name for pattern matching:
/// - CamelCase to snake_case (e.g., firstName → first_name)
/// - lowercase (Unicode-safe)
/// - replace hyphens with underscores
///
/// Uses explicit previous-character tracking instead of byte indexing
/// to avoid UTF-8 multi-byte footguns with non-ASCII column names.
fn normalize_column_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    let mut prev_char: Option<char> = None;

    for ch in name.chars() {
        if ch.is_uppercase() {
            if let Some(p) = prev_char {
                if p.is_lowercase() {
                    result.push('_');
                }
            }
        }
        // ch.to_lowercase() can yield multiple chars for some Unicode (e.g., Turkish İ).
        for lower_ch in ch.to_lowercase() {
            result.push(lower_ch);
        }
        prev_char = Some(ch);
    }
    result.replace('-', "_")
}

/// Try to classify an enum column more specifically by its name.
fn classify_enum_by_name(column_name: &str, _table_name: &str) -> SemanticType {
    let n = normalize_column_name(column_name);
    if n.contains("status") || n.contains("state") {
        SemanticType::Status
    } else if n.contains("role") {
        SemanticType::Role
    } else if n.contains("priority") {
        SemanticType::Priority
    } else if n.contains("category") || n.contains("type") {
        SemanticType::Category
    } else {
        SemanticType::Unknown
    }
}

/// Fallback classification based purely on data type.
fn type_based_fallback(data_type: &DataType) -> SemanticType {
    match data_type {
        DataType::Uuid => SemanticType::Uuid,
        DataType::Boolean => SemanticType::BooleanFlag,
        DataType::Json | DataType::Jsonb => SemanticType::JsonData,
        DataType::Timestamp | DataType::TimestampTz => SemanticType::Timestamp,
        DataType::Date => SemanticType::DateOnly,
        DataType::Time => SemanticType::TimeOnly,
        DataType::Inet => SemanticType::IpAddress,
        DataType::MacAddr => SemanticType::MacAddress,
        _ => SemanticType::Unknown,
    }
}

// === Table-context rules (higher priority, table-specific) ===

static TABLE_CONTEXT_RULES: &[ClassificationRule] = &[
    // "name" in users/people tables → FullName
    ClassificationRule {
        pattern: r"^name$",
        type_constraint: None,
        table_pattern: Some(
            r"(?i)(users?|people|persons?|members?|employees?|staff|contacts?|customers?|accounts?|profiles?)",
        ),
        semantic_type: SemanticType::FullName,
    },
    // "name" in companies/organizations → CompanyName
    ClassificationRule {
        pattern: r"^name$",
        type_constraint: None,
        table_pattern: Some(
            r"(?i)(companies|organizations?|businesses|vendors?|suppliers?|brands?)",
        ),
        semantic_type: SemanticType::CompanyName,
    },
    // "name" in products/items → Title
    ClassificationRule {
        pattern: r"^name$",
        type_constraint: None,
        table_pattern: Some(r"(?i)(products?|items?|goods|catalog|categories|tags?)"),
        semantic_type: SemanticType::Title,
    },
    // "title" in users → JobTitle
    ClassificationRule {
        pattern: r"^title$",
        type_constraint: None,
        table_pattern: Some(r"(?i)(users?|people|persons?|members?|employees?|staff)"),
        semantic_type: SemanticType::JobTitle,
    },
];

/// Integer column types — used as type_constraint for Age, SortOrder, etc.
static INTEGER_TYPES: &[DataType] = &[
    DataType::SmallInt,
    DataType::Integer,
    DataType::BigInt,
    DataType::Serial,
    DataType::BigSerial,
];

/// Numeric column types (integers + floats) — used for Quantity, Rating, Weight, etc.
static NUMERIC_TYPES: &[DataType] = &[
    DataType::SmallInt,
    DataType::Integer,
    DataType::BigInt,
    DataType::Float,
    DataType::Double,
    DataType::Numeric,
    DataType::Serial,
    DataType::BigSerial,
    DataType::Money,
];

// === General name-based rules ===

static GENERAL_RULES: &[ClassificationRule] = &[
    // === Identity ===
    ClassificationRule {
        pattern: r"^(first_?name|given_?name|fname)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::FirstName,
    },
    ClassificationRule {
        pattern: r"^(last_?name|family_?name|surname|lname)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::LastName,
    },
    ClassificationRule {
        pattern: r"^(full_?name|display_?name|real_?name)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::FullName,
    },
    ClassificationRule {
        pattern: r"^(user_?name|login|handle|screen_?name|nick_?name)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Username,
    },
    // === Contact ===
    ClassificationRule {
        pattern: r"^e?_?mail(_?(address|addr))?$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Email,
    },
    ClassificationRule {
        pattern: r"^(phone(_?(number|num))?|telephone|mobile|cell(_?phone)?|fax)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Phone,
    },
    ClassificationRule {
        pattern: r"^(phone_?country_?code|dial_?code|calling_?code)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::PhoneCountryCode,
    },
    // === Address ===
    ClassificationRule {
        pattern: r"^(street(_?(address|addr|line))?|address(_?line)?_?[12]?|addr)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::StreetAddress,
    },
    ClassificationRule {
        pattern: r"^(city|town|municipality)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::City,
    },
    ClassificationRule {
        pattern: r"^(state|province|region|prefecture)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::State,
    },
    ClassificationRule {
        pattern: r"^(zip(_?code)?|postal_?code|post_?code)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ZipCode,
    },
    ClassificationRule {
        pattern: r"^(country)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Country,
    },
    ClassificationRule {
        pattern: r"^(country_?code|country_?iso|iso_?country)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::CountryCode,
    },
    ClassificationRule {
        pattern: r"^(lat(itude)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Latitude,
    },
    ClassificationRule {
        pattern: r"^(lng|lon(gitude)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Longitude,
    },
    // === Company ===
    ClassificationRule {
        pattern: r"^(company(_?name)?|organization(_?name)?|org(_?name)?|employer)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::CompanyName,
    },
    ClassificationRule {
        pattern: r"^(job_?title|position|occupation|role_?title)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::JobTitle,
    },
    ClassificationRule {
        pattern: r"^(department|dept|division|team)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Department,
    },
    ClassificationRule {
        pattern: r"^(industry|sector)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Industry,
    },
    // === Internet ===
    ClassificationRule {
        pattern: r"^(url|link|href|website|web_?url|homepage)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Url,
    },
    ClassificationRule {
        pattern: r"^(domain(_?name)?|host(_?name)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::DomainName,
    },
    ClassificationRule {
        pattern: r"^(ip(_?address)?|ip_?addr|remote_?addr)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::IpAddress,
    },
    ClassificationRule {
        pattern: r"^(mac(_?address)?|mac_?addr|hw_?addr)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::MacAddress,
    },
    ClassificationRule {
        pattern: r"^(user_?agent|ua)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::UserAgent,
    },
    ClassificationRule {
        pattern: r"^slug$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Slug,
    },
    // === Content ===
    ClassificationRule {
        pattern: r"^(title|subject|heading|headline)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Title,
    },
    ClassificationRule {
        pattern: r"^(description|desc|summary|excerpt|blurb|abstract|overview)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Description,
    },
    ClassificationRule {
        pattern: r"^(bio(graphy)?|about(_?me)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Bio,
    },
    ClassificationRule {
        pattern: r"^(body|content|text|message|comment|note|notes|remarks?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Paragraph,
    },
    // === Media ===
    ClassificationRule {
        pattern: r"^(image(_?url)?|img(_?url)?|photo(_?url)?|picture(_?url)?|cover(_?image)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ImageUrl,
    },
    ClassificationRule {
        pattern: r"^(avatar(_?url)?|profile(_?(image|photo|pic|picture))?(_?url)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::AvatarUrl,
    },
    ClassificationRule {
        pattern: r"^(thumbnail(_?url)?|thumb(_?url)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ThumbnailUrl,
    },
    ClassificationRule {
        pattern: r"^(file(_?url)?|attachment(_?url)?|download(_?url)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::FileUrl,
    },
    ClassificationRule {
        pattern: r"^(file_?name|filename|original_?name)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::FileName,
    },
    ClassificationRule {
        pattern: r"^(mime_?type|content_?type|media_?type)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::MimeType,
    },
    ClassificationRule {
        pattern: r"^(file_?size|size_?bytes|content_?length)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::FileSize,
    },
    // === Financial ===
    ClassificationRule {
        pattern: r"^(price|cost|amount|total|subtotal|sub_?total|unit_?price|sale_?price|list_?price|retail_?price|msrp)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Price,
    },
    ClassificationRule {
        pattern: r"^(currency)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Currency,
    },
    ClassificationRule {
        pattern: r"^(currency_?code|currency_?iso)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::CurrencyCode,
    },
    ClassificationRule {
        pattern: r"^(percentage|percent|pct|ratio|rate)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Percentage,
    },
    // === Temporal ===
    ClassificationRule {
        pattern: r"^(created_?(at|on|date|time|timestamp)?|date_?created|insert(ed)?_?(at|on))$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::CreatedAt,
    },
    ClassificationRule {
        pattern: r"^(updated_?(at|on|date|time|timestamp)?|modified_?(at|on|date)?|date_?updated|changed_?(at|on)|last_?modified)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::UpdatedAt,
    },
    ClassificationRule {
        pattern: r"^(deleted_?(at|on|date|time|timestamp)?|removed_?(at|on))$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::DeletedAt,
    },
    ClassificationRule {
        pattern: r"^(start_?(date|time|at)?|begin_?(date|time|at)?|from_?(date|time)|valid_?from)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::StartDate,
    },
    ClassificationRule {
        pattern: r"^(end_?(date|time|at)?|finish_?(date|time|at)?|to_?(date|time)|valid_?(to|until)|expires?_?(at|on|date)?|expir(y|ation)(_?date)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::EndDate,
    },
    ClassificationRule {
        pattern: r"^(birth_?(date|day)?|date_?of_?birth|dob|birthday)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::BirthDate,
    },
    // === Auth/Security ===
    ClassificationRule {
        pattern: r"^(password(_?hash)?|hashed_?password|encrypted_?password|password_?digest|passwd)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::PasswordHash,
    },
    ClassificationRule {
        pattern: r"^(token|access_?token|refresh_?token|auth_?token|session_?token|verification_?token|confirmation_?token|reset_?token)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Token,
    },
    ClassificationRule {
        pattern: r"^(api_?key|app_?key|client_?key|public_?key)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ApiKey,
    },
    ClassificationRule {
        pattern: r"^(secret(_?key)?|client_?secret|private_?key|signing_?key)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::SecretKey,
    },
    // === Identifiers ===
    ClassificationRule {
        pattern: r"^(external_?id|ext_?id|remote_?id|ref(_?id)?|reference(_?id)?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ExternalId,
    },
    ClassificationRule {
        pattern: r"^(sku|product_?code|item_?code|part_?number)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Sku,
    },
    ClassificationRule {
        pattern: r"^(order_?(number|num|no|code|ref))$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::OrderNumber,
    },
    ClassificationRule {
        pattern: r"^(invoice_?(number|num|no|code|ref))$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::InvoiceNumber,
    },
    ClassificationRule {
        pattern: r"^(tracking_?(number|num|no|code|id))$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::TrackingNumber,
    },
    // === Status/Enum ===
    ClassificationRule {
        pattern: r"^(status|state)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Status,
    },
    ClassificationRule {
        pattern: r"^(role|user_?role|account_?type)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Role,
    },
    ClassificationRule {
        pattern: r"^(priority|urgency|severity)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Priority,
    },
    ClassificationRule {
        pattern: r"^(category|type|kind|group|class)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Category,
    },
    ClassificationRule {
        pattern: r"^(tags?|labels?)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Tag,
    },
    ClassificationRule {
        pattern: r"^(is_|has_|can_|should_|was_|allow(s|ed)?_|enable[ds]?_|active|verified|visible|published|featured|archived|locked|blocked|banned|suspended|approved|confirmed|completed|deleted|removed|disabled|hidden|draft|public|private|internal|admin|default|required|optional|primary|secondary|premium|free|trial|test|demo|sample|temp(orary)?)",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::BooleanFlag,
    },
    // === Numeric ===
    // These rules require numeric column types to prevent type mismatches
    // (e.g., generating i64 for a VARCHAR column named "age").
    ClassificationRule {
        pattern: r"^(quantity|qty|count|num|number_?of|total_?count)$",
        type_constraint: Some(NUMERIC_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Quantity,
    },
    ClassificationRule {
        pattern: r"^(rating|stars|score|grade|rank)$",
        type_constraint: Some(NUMERIC_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Rating,
    },
    ClassificationRule {
        pattern: r"^(weight|mass)$",
        type_constraint: Some(NUMERIC_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Weight,
    },
    ClassificationRule {
        pattern: r"^(height|length|width|depth|size)$",
        type_constraint: Some(NUMERIC_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Height,
    },
    ClassificationRule {
        pattern: r"^(age)$",
        type_constraint: Some(INTEGER_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Age,
    },
    ClassificationRule {
        pattern: r"^(duration|elapsed|time_?spent|response_?time|load_?time)$",
        type_constraint: Some(NUMERIC_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::Duration,
    },
    ClassificationRule {
        pattern: r"^(sort_?order|position|display_?order|rank|seq(uence)?|ordinal|index|weight)$",
        type_constraint: Some(INTEGER_TYPES),
        table_pattern: None,
        semantic_type: SemanticType::SortOrder,
    },
    // === Data ===
    ClassificationRule {
        pattern: r"^(color|colour)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Color,
    },
    ClassificationRule {
        pattern: r"^(hex_?color|color_?hex|color_?code)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::HexColor,
    },
    ClassificationRule {
        pattern: r"^(locale|lang(uage)?(_?code)?|i18n)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Locale,
    },
    ClassificationRule {
        pattern: r"^(time_?zone|tz|timezone)$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::Timezone,
    },
    // === Catch-all: _id suffix ===
    // Must be LAST in the list. Columns ending in _id that didn't match any
    // specific rule above are treated as external/foreign identifiers rather
    // than falling through to Unknown (which would generate random words).
    ClassificationRule {
        pattern: r"_id$",
        type_constraint: None,
        table_pattern: None,
        semantic_type: SemanticType::ExternalId,
    },
];

/// Classify all columns in a schema and return a map of (table_name, column_name) -> SemanticType.
pub fn classify_schema(
    schema: &crate::schema::types::DatabaseSchema,
) -> std::collections::HashMap<(String, String), SemanticType> {
    let mut result = std::collections::HashMap::new();

    for (table_name, table) in &schema.tables {
        let pk_columns: Vec<&str> = table
            .primary_key
            .as_ref()
            .map(|pk| pk.columns.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        for (col_name, column) in &table.columns {
            let is_pk = pk_columns.contains(&col_name.as_str());
            let semantic_type = classify_column(
                col_name,
                &column.data_type,
                table_name,
                column.is_auto_increment,
                is_pk,
                column.enum_values.as_deref(),
            );

            result.insert((table_name.clone(), col_name.clone()), semantic_type);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_email() {
        let st = classify_column("email", &DataType::VarChar, "users", false, false, None);
        assert_eq!(st, SemanticType::Email);
    }

    #[test]
    fn test_classify_email_address() {
        let st = classify_column(
            "email_address",
            &DataType::VarChar,
            "users",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::Email);
    }

    #[test]
    fn test_classify_first_name() {
        let st = classify_column(
            "first_name",
            &DataType::VarChar,
            "users",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::FirstName);
    }

    #[test]
    fn test_classify_created_at() {
        let st = classify_column(
            "created_at",
            &DataType::TimestampTz,
            "users",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::CreatedAt);
    }

    #[test]
    fn test_classify_auto_increment_pk() {
        let st = classify_column("id", &DataType::Serial, "users", true, true, None);
        assert_eq!(st, SemanticType::AutoIncrement);
    }

    #[test]
    fn test_classify_uuid_pk() {
        let st = classify_column("id", &DataType::Uuid, "users", false, true, None);
        assert_eq!(st, SemanticType::Uuid);
    }

    #[test]
    fn test_context_name_in_users() {
        let st = classify_column("name", &DataType::VarChar, "users", false, false, None);
        assert_eq!(st, SemanticType::FullName);
    }

    #[test]
    fn test_context_name_in_companies() {
        let st = classify_column("name", &DataType::VarChar, "companies", false, false, None);
        assert_eq!(st, SemanticType::CompanyName);
    }

    #[test]
    fn test_context_name_in_products() {
        let st = classify_column("name", &DataType::VarChar, "products", false, false, None);
        assert_eq!(st, SemanticType::Title);
    }

    #[test]
    fn test_classify_password_hash() {
        let st = classify_column(
            "password_hash",
            &DataType::VarChar,
            "users",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::PasswordHash);
    }

    #[test]
    fn test_classify_price() {
        let st = classify_column("price", &DataType::Numeric, "products", false, false, None);
        assert_eq!(st, SemanticType::Price);
    }

    #[test]
    fn test_classify_boolean_flag() {
        let st = classify_column("is_active", &DataType::Boolean, "users", false, false, None);
        assert_eq!(st, SemanticType::BooleanFlag);
    }

    #[test]
    fn test_classify_jsonb_fallback() {
        let st = classify_column("metadata", &DataType::Jsonb, "users", false, false, None);
        assert_eq!(st, SemanticType::JsonData);
    }

    #[test]
    fn test_classify_zip_code() {
        let st = classify_column(
            "zip_code",
            &DataType::VarChar,
            "addresses",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::ZipCode);
    }

    // --- Red/Green TDD tests for new fixes ---

    #[test]
    fn test_camel_case_first_name() {
        // Prisma-style CamelCase column names must be normalized
        let st = classify_column("firstName", &DataType::VarChar, "users", false, false, None);
        assert_eq!(st, SemanticType::FirstName);
    }

    #[test]
    fn test_camel_case_created_at() {
        let st = classify_column(
            "createdAt",
            &DataType::TimestampTz,
            "users",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::CreatedAt);
    }

    #[test]
    fn test_age_on_varchar_should_not_match() {
        // Age matched on a VARCHAR column should NOT classify as Age
        // (Age generates i64, which breaks VARCHAR columns)
        let st = classify_column("age", &DataType::VarChar, "users", false, false, None);
        assert_ne!(st, SemanticType::Age);
    }

    #[test]
    fn test_age_on_integer_should_match() {
        let st = classify_column("age", &DataType::Integer, "users", false, false, None);
        assert_eq!(st, SemanticType::Age);
    }

    #[test]
    fn test_quantity_on_varchar_should_not_match() {
        let st = classify_column("quantity", &DataType::VarChar, "orders", false, false, None);
        assert_ne!(st, SemanticType::Quantity);
    }

    #[test]
    fn test_unmapped_id_suffix_catches_external_id() {
        // organization_id (not a FK) should fall back to ExternalId, not Unknown
        let st = classify_column(
            "organization_id",
            &DataType::Integer,
            "projects",
            false,
            false,
            None,
        );
        assert_eq!(st, SemanticType::ExternalId);
    }

    #[test]
    fn test_unmapped_id_suffix_uuid_column() {
        let st = classify_column("tenant_id", &DataType::Uuid, "invoices", false, false, None);
        assert_eq!(st, SemanticType::ExternalId);
    }

    #[test]
    fn test_normalize_camel_case() {
        assert_eq!(normalize_column_name("firstName"), "first_name");
        assert_eq!(normalize_column_name("createdAt"), "created_at");
        assert_eq!(normalize_column_name("emailAddress"), "email_address");
        assert_eq!(normalize_column_name("HTMLParser"), "htmlparser"); // consecutive caps stay lowercase
        assert_eq!(normalize_column_name("already_snake"), "already_snake");
    }
}
