use std::borrow::Cow;

use chrono::{Duration as ChronoDuration, NaiveTime};
use fake::faker::address::en::*;
use fake::faker::company::en::*;
use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::faker::phone_number::en::*;
use fake::Fake;
use rand::Rng;
use uuid::Uuid;

use crate::classify::semantic::SemanticType;
use crate::generate::value::Value;
use crate::schema::types::ParsedCheck;

/// Wrap a dynamically generated String into a Value::String.
#[inline]
fn owned(s: String) -> Value {
    Value::String(Cow::Owned(s))
}

/// Wrap a static string literal into a Value::String (zero heap allocation).
#[inline]
fn borrowed(s: &'static str) -> Value {
    Value::String(Cow::Borrowed(s))
}

/// Generate a value for a given semantic type.
///
/// `base_time` is the pinned wall-clock timestamp captured at plan creation.
/// All temporal values are derived from this anchor so that regeneration from a
/// lockfile produces identical output regardless of when it runs.
pub fn generate_value(
    semantic_type: SemanticType,
    rng: &mut impl Rng,
    row_index: usize,
    check_constraints: &[ParsedCheck],
    base_time: chrono::NaiveDateTime,
) -> Value {
    let value = match semantic_type {
        // === Identity ===
        SemanticType::FirstName => owned(FirstName().fake_with_rng(rng)),
        SemanticType::LastName => owned(LastName().fake_with_rng(rng)),
        SemanticType::FullName => owned(Name().fake_with_rng(rng)),
        SemanticType::Username => {
            let user: String = Username().fake_with_rng(rng);
            owned(format!("{}{}", user, row_index))
        }
        SemanticType::DisplayName => owned(Name().fake_with_rng(rng)),

        // === Contact ===
        SemanticType::Email => {
            let email: String = SafeEmail().fake_with_rng(rng);
            let parts: Vec<&str> = email.splitn(2, '@').collect();
            if parts.len() == 2 {
                owned(format!("{}.{}@{}", parts[0], row_index, parts[1]))
            } else {
                owned(format!("user{}@example.com", row_index))
            }
        }
        SemanticType::Phone => owned(PhoneNumber().fake_with_rng(rng)),
        SemanticType::PhoneCountryCode => {
            let codes = ["+1", "+44", "+49", "+33", "+81", "+86", "+91", "+55", "+61"];
            borrowed(codes[rng.random_range(0..codes.len())])
        }

        // === Address ===
        SemanticType::StreetAddress => owned(StreetName().fake_with_rng(rng)),
        SemanticType::City => owned(CityName().fake_with_rng(rng)),
        SemanticType::State => owned(StateName().fake_with_rng(rng)),
        SemanticType::ZipCode => owned(ZipCode().fake_with_rng(rng)),
        SemanticType::PostalCode => owned(ZipCode().fake_with_rng(rng)),
        SemanticType::Country => owned(CountryName().fake_with_rng(rng)),
        SemanticType::CountryCode => owned(CountryCode().fake_with_rng(rng)),
        SemanticType::Latitude => Value::Float(Latitude().fake_with_rng(rng)),
        SemanticType::Longitude => Value::Float(Longitude().fake_with_rng(rng)),

        // === Company ===
        SemanticType::CompanyName => owned(CompanyName().fake_with_rng(rng)),
        SemanticType::JobTitle => owned(Profession().fake_with_rng(rng)),
        SemanticType::Department => {
            let depts = [
                "Engineering",
                "Sales",
                "Marketing",
                "Product",
                "Design",
                "HR",
                "Finance",
                "Legal",
                "Operations",
                "Support",
            ];
            borrowed(depts[rng.random_range(0..depts.len())])
        }
        SemanticType::Industry => owned(Industry().fake_with_rng(rng)),

        // === Internet ===
        SemanticType::Url => {
            let domain: String = DomainSuffix().fake_with_rng(rng);
            owned(format!("https://example-{}.{}", row_index, domain))
        }
        SemanticType::DomainName => owned(FreeEmailProvider().fake_with_rng(rng)),
        SemanticType::IpAddress => owned(IPv4().fake_with_rng(rng)),
        SemanticType::MacAddress => owned(MACAddress().fake_with_rng(rng)),
        SemanticType::UserAgent => owned(UserAgent().fake_with_rng(rng)),
        SemanticType::Slug => {
            let words: Vec<String> = Words(2..4).fake_with_rng(rng);
            owned(words.join("-"))
        }

        // === Content ===
        SemanticType::Title => {
            let words: Vec<String> = Words(3..8).fake_with_rng(rng);
            let title = words.join(" ");
            let mut chars = title.chars();
            let capitalized = match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            };
            owned(capitalized)
        }
        SemanticType::Description => {
            let sentences: Vec<String> = Sentences(2..4).fake_with_rng(rng);
            owned(sentences.join(" "))
        }
        SemanticType::Bio => {
            let sentences: Vec<String> = Sentences(1..3).fake_with_rng(rng);
            owned(sentences.join(" "))
        }
        SemanticType::Paragraph => {
            let paragraphs: Vec<String> = Paragraphs(1..3).fake_with_rng(rng);
            owned(paragraphs.join("\n\n"))
        }
        SemanticType::Sentence => owned(Sentence(5..12).fake_with_rng(rng)),
        SemanticType::HtmlContent => {
            let sentences: Vec<String> = Sentences(2..5).fake_with_rng(rng);
            owned(format!("<p>{}</p>", sentences.join("</p><p>")))
        }
        SemanticType::MarkdownContent => {
            let title_words: Vec<String> = Words(3..6).fake_with_rng(rng);
            let sentences: Vec<String> = Sentences(2..4).fake_with_rng(rng);
            owned(format!(
                "# {}\n\n{}",
                title_words.join(" "),
                sentences.join(" ")
            ))
        }

        // === Media ===
        SemanticType::ImageUrl => {
            owned(format!("https://picsum.photos/seed/{}/800/600", row_index))
        }
        SemanticType::AvatarUrl => owned(format!(
            "https://api.dicebear.com/7.x/avataaars/svg?seed={}",
            row_index
        )),
        SemanticType::ThumbnailUrl => {
            owned(format!("https://picsum.photos/seed/{}/200/200", row_index))
        }
        SemanticType::FileUrl => {
            let exts = ["pdf", "docx", "xlsx", "png", "jpg"];
            owned(format!(
                "https://cdn.example.com/files/file_{}.{}",
                row_index,
                exts[rng.random_range(0..exts.len())]
            ))
        }
        SemanticType::FileName => {
            let words: Vec<String> = Words(1..3).fake_with_rng(rng);
            let exts = ["pdf", "docx", "xlsx", "png", "jpg", "csv", "txt"];
            owned(format!(
                "{}.{}",
                words.join("_"),
                exts[rng.random_range(0..exts.len())]
            ))
        }
        SemanticType::MimeType => {
            let types = [
                "application/pdf",
                "image/png",
                "image/jpeg",
                "text/plain",
                "application/json",
                "text/html",
                "application/xml",
            ];
            borrowed(types[rng.random_range(0..types.len())])
        }
        SemanticType::FileSize => Value::Int(rng.random_range(1024..10_485_760)),

        // === Financial ===
        SemanticType::Price | SemanticType::Amount => {
            let (min, max) = compute_numeric_bounds_f64(0.01, 999.99, check_constraints);
            let val: f64 = rng.random_range(min..=max);
            Value::Float((val * 100.0_f64).round() / 100.0_f64)
        }
        SemanticType::Currency => {
            let currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"];
            borrowed(currencies[rng.random_range(0..currencies.len())])
        }
        SemanticType::CurrencyCode => {
            let codes = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"];
            borrowed(codes[rng.random_range(0..codes.len())])
        }
        SemanticType::Percentage => {
            let val: f64 = rng.random_range(0.0_f64..100.0_f64);
            Value::Float((val * 100.0_f64).round() / 100.0_f64)
        }

        // === Temporal ===
        SemanticType::CreatedAt | SemanticType::Timestamp => {
            let days_ago = rng.random_range(1..365);
            let hours = rng.random_range(0..24);
            let minutes = rng.random_range(0..60);
            let base = base_time - ChronoDuration::days(days_ago)
                + ChronoDuration::hours(hours)
                + ChronoDuration::minutes(minutes);
            Value::Timestamp(base)
        }
        SemanticType::UpdatedAt => {
            let days_ago = rng.random_range(0..30);
            let base = base_time - ChronoDuration::days(days_ago);
            Value::Timestamp(base)
        }
        SemanticType::DeletedAt => {
            let days_ago = rng.random_range(0..7);
            let base = base_time - ChronoDuration::days(days_ago);
            Value::Timestamp(base)
        }
        SemanticType::StartDate => {
            let days_from_now = rng.random_range(-30..90);
            let base = base_time.date() + ChronoDuration::days(days_from_now);
            Value::Date(base)
        }
        SemanticType::EndDate => {
            let days_from_now = rng.random_range(30..180);
            let base = base_time.date() + ChronoDuration::days(days_from_now);
            Value::Date(base)
        }
        SemanticType::BirthDate => {
            let years_ago = rng.random_range(18..80);
            let days_extra = rng.random_range(0..365);
            let base = base_time.date() - ChronoDuration::days(years_ago * 365 + days_extra);
            Value::Date(base)
        }
        SemanticType::DateOnly => {
            let days_offset = rng.random_range(-365..365);
            let base = base_time.date() + ChronoDuration::days(days_offset);
            Value::Date(base)
        }
        SemanticType::TimeOnly => {
            let hour = rng.random_range(0..24) as u32;
            let min = rng.random_range(0..60) as u32;
            let sec = rng.random_range(0..60) as u32;
            Value::Time(NaiveTime::from_hms_opt(hour, min, sec).unwrap_or_default())
        }

        // === Auth/Security ===
        SemanticType::PasswordHash => {
            owned(format!("$2b$12${}", generate_random_alphanumeric(rng, 53)))
        }
        SemanticType::Token | SemanticType::ApiKey | SemanticType::SecretKey => {
            owned(generate_random_alphanumeric(rng, 32))
        }

        // === Identifiers ===
        SemanticType::Uuid => Value::Uuid(Uuid::new_v4()),
        SemanticType::AutoIncrement => Value::Int(row_index as i64 + 1),
        SemanticType::ExternalId => owned(format!("ext_{}", generate_random_alphanumeric(rng, 12))),
        SemanticType::Sku => owned(format!("SKU-{:06}", row_index + 1)),
        SemanticType::OrderNumber => owned(format!("ORD-{:08}", row_index + 1)),
        SemanticType::InvoiceNumber => owned(format!("INV-{:08}", row_index + 1)),
        SemanticType::TrackingNumber => owned(format!(
            "TRK{}",
            generate_random_alphanumeric(rng, 16).to_uppercase()
        )),

        // === Status/Enum (static — zero allocation via Cow::Borrowed) ===
        SemanticType::Status => {
            let statuses = ["active", "inactive", "pending", "suspended"];
            borrowed(statuses[rng.random_range(0..statuses.len())])
        }
        SemanticType::Role => {
            let roles = ["admin", "user", "moderator", "editor", "viewer"];
            borrowed(roles[rng.random_range(0..roles.len())])
        }
        SemanticType::Priority => {
            let priorities = ["low", "medium", "high", "critical"];
            borrowed(priorities[rng.random_range(0..priorities.len())])
        }
        SemanticType::Category => {
            let words: Vec<String> = Words(1..2).fake_with_rng(rng);
            owned(words.join(" "))
        }
        SemanticType::Tag => {
            let words: Vec<String> = Words(1..2).fake_with_rng(rng);
            owned(words.join("-"))
        }
        SemanticType::BooleanFlag => Value::Bool(rng.random_bool(0.7)),
        SemanticType::EnumValue => borrowed("unknown"),

        // === Numeric ===
        SemanticType::Quantity => {
            let (min, max) = compute_numeric_bounds_i64(1, 100, check_constraints);
            Value::Int(rng.random_range(min..=max))
        }
        SemanticType::Rating => {
            let val: f64 = rng.random_range(1.0_f64..5.0_f64);
            Value::Float((val * 10.0_f64).round() / 10.0_f64)
        }
        SemanticType::Score => Value::Int(rng.random_range(0..=100)),
        SemanticType::Weight => {
            let val: f64 = rng.random_range(0.1_f64..100.0_f64);
            Value::Float((val * 100.0_f64).round() / 100.0_f64)
        }
        SemanticType::Height => {
            let val: f64 = rng.random_range(50.0_f64..250.0_f64);
            Value::Float((val * 10.0_f64).round() / 10.0_f64)
        }
        SemanticType::Age => Value::Int(rng.random_range(18..90)),
        SemanticType::Duration => Value::Int(rng.random_range(1..3600)),
        SemanticType::SortOrder => Value::Int(row_index as i64),

        // === Data ===
        SemanticType::JsonData => {
            let keys = ["metadata", "preferences", "flags", "raw_payload"];
            let key = keys[rng.random_range(0..keys.len())];
            Value::Json(serde_json::json!({
                key: generate_random_alphanumeric(rng, 8),
                "processed": rng.random_bool(0.8),
                "retries": rng.random_range(0u32..5u32)
            }))
        }
        SemanticType::Color => {
            let colors = [
                "red", "blue", "green", "yellow", "purple", "orange", "pink", "black", "white",
                "gray", "brown", "cyan", "magenta", "teal",
            ];
            borrowed(colors[rng.random_range(0..colors.len())])
        }
        SemanticType::HexColor => owned(format!(
            "#{:02x}{:02x}{:02x}",
            rng.random_range(0..=255u8),
            rng.random_range(0..=255u8),
            rng.random_range(0..=255u8)
        )),
        SemanticType::Locale => {
            let locales = [
                "en_US", "en_GB", "de_DE", "fr_FR", "es_ES", "ja_JP", "zh_CN", "pt_BR",
            ];
            borrowed(locales[rng.random_range(0..locales.len())])
        }
        SemanticType::Timezone => {
            let tzs = [
                "America/New_York",
                "America/Chicago",
                "America/Los_Angeles",
                "Europe/London",
                "Europe/Berlin",
                "Asia/Tokyo",
                "Asia/Shanghai",
                "Australia/Sydney",
                "America/Sao_Paulo",
            ];
            borrowed(tzs[rng.random_range(0..tzs.len())])
        }

        // === Catch-all ===
        SemanticType::Unknown => {
            let word: String = Word().fake_with_rng(rng);
            owned(word)
        }
    };

    value
}

/// Compute safe f64 bounds from CHECK constraints, clamping if they conflict.
fn compute_numeric_bounds_f64(
    default_min: f64,
    default_max: f64,
    constraints: &[ParsedCheck],
) -> (f64, f64) {
    let mut min = default_min;
    let mut max = default_max;
    for c in constraints {
        match c {
            ParsedCheck::GreaterThanOrEqual { value, .. } => min = min.max(*value),
            ParsedCheck::GreaterThan { value, .. } => min = min.max(*value + 0.01),
            ParsedCheck::LessThanOrEqual { value, .. } => max = max.min(*value),
            ParsedCheck::LessThan { value, .. } => max = max.min(*value - 0.01),
            _ => {}
        }
    }
    if min > max {
        tracing::warn!(
            "Conflicting numeric bounds (min: {}, max: {}). Widening.",
            min,
            max
        );
        max = min + 100.0;
    }
    (min, max)
}

/// Compute safe i64 bounds from CHECK constraints.
fn compute_numeric_bounds_i64(
    default_min: i64,
    default_max: i64,
    constraints: &[ParsedCheck],
) -> (i64, i64) {
    let mut min = default_min;
    let mut max = default_max;
    for c in constraints {
        match c {
            ParsedCheck::GreaterThanOrEqual { value, .. } => min = min.max(*value as i64),
            ParsedCheck::GreaterThan { value, .. } => min = min.max(*value as i64 + 1),
            ParsedCheck::LessThanOrEqual { value, .. } => max = max.min(*value as i64),
            ParsedCheck::LessThan { value, .. } => max = max.min(*value as i64 - 1),
            _ => {}
        }
    }
    if min > max {
        tracing::warn!(
            "Conflicting integer bounds (min: {}, max: {}). Widening.",
            min,
            max
        );
        max = min + 100;
    }
    (min, max)
}

fn generate_random_alphanumeric(rng: &mut impl Rng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..len)
        .map(|_| CHARS[rng.random_range(0..CHARS.len())] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn test_base_time() -> chrono::NaiveDateTime {
        chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )
    }

    #[test]
    fn test_generate_email() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let value = generate_value(SemanticType::Email, &mut rng, 0, &[], test_base_time());
        if let Value::String(s) = value {
            assert!(s.contains('@'));
        } else {
            panic!("Expected string");
        }
    }

    #[test]
    fn test_generate_price_with_constraint() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let constraints = vec![ParsedCheck::GreaterThanOrEqual {
            column: "price".to_string(),
            value: 0.0,
        }];
        let value = generate_value(
            SemanticType::Price,
            &mut rng,
            0,
            &constraints,
            test_base_time(),
        );
        if let Value::Float(f) = value {
            assert!(f >= 0.0);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_generate_uuid() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let value = generate_value(SemanticType::Uuid, &mut rng, 0, &[], test_base_time());
        assert!(matches!(value, Value::Uuid(_)));
    }

    #[test]
    fn test_generate_boolean() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let value = generate_value(
            SemanticType::BooleanFlag,
            &mut rng,
            0,
            &[],
            test_base_time(),
        );
        assert!(matches!(value, Value::Bool(_)));
    }

    #[test]
    fn test_json_data_is_non_empty() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let value = generate_value(SemanticType::JsonData, &mut rng, 0, &[], test_base_time());
        if let Value::Json(j) = value {
            let obj = j.as_object().expect("JsonData should be an object");
            assert!(
                obj.len() >= 2,
                "JsonData should have at least 2 keys, got {}",
                obj.len()
            );
        } else {
            panic!("Expected Json variant");
        }
    }

    #[test]
    fn test_temporal_determinism() {
        let bt = test_base_time();
        let mut rng1 = rand::rngs::StdRng::seed_from_u64(99);
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(99);

        let v1 = generate_value(SemanticType::CreatedAt, &mut rng1, 0, &[], bt);
        let v2 = generate_value(SemanticType::CreatedAt, &mut rng2, 0, &[], bt);
        assert_eq!(
            v1, v2,
            "Same seed + same base_time must produce identical timestamps"
        );
    }
}
