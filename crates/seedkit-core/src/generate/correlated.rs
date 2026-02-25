use std::borrow::Cow;

use chrono::Duration as ChronoDuration;
use rand::Rng;

use crate::classify::semantic::{CorrelationGroup, SemanticType};
use crate::generate::plan::CorrelationGroupPlan;
use crate::generate::providers::generate_value;
use crate::generate::value::Value;

// TODO: Phase 3 — Replace this 50-entry slice with a compressed ~40,000 US zip
// code dataset loaded via `include_bytes!`. At 50 entries, every 200th generated
// user lands in the exact same city/zip, which produces unrealistic distributions
// for high-volume performance tests.
/// Built-in US city/state/zip data for correlated address generation.
static US_LOCATIONS: &[(&str, &str, &str)] = &[
    ("New York", "New York", "10001"),
    ("Los Angeles", "California", "90001"),
    ("Chicago", "Illinois", "60601"),
    ("Houston", "Texas", "77001"),
    ("Phoenix", "Arizona", "85001"),
    ("Philadelphia", "Pennsylvania", "19101"),
    ("San Antonio", "Texas", "78201"),
    ("San Diego", "California", "92101"),
    ("Dallas", "Texas", "75201"),
    ("San Jose", "California", "95101"),
    ("Austin", "Texas", "73301"),
    ("Jacksonville", "Florida", "32099"),
    ("Fort Worth", "Texas", "76101"),
    ("Columbus", "Ohio", "43085"),
    ("Charlotte", "North Carolina", "28201"),
    ("San Francisco", "California", "94101"),
    ("Indianapolis", "Indiana", "46201"),
    ("Seattle", "Washington", "98101"),
    ("Denver", "Colorado", "80201"),
    ("Nashville", "Tennessee", "37201"),
    ("Portland", "Oregon", "97201"),
    ("Las Vegas", "Nevada", "89101"),
    ("Memphis", "Tennessee", "38101"),
    ("Louisville", "Kentucky", "40201"),
    ("Baltimore", "Maryland", "21201"),
    ("Milwaukee", "Wisconsin", "53201"),
    ("Albuquerque", "New Mexico", "87101"),
    ("Tucson", "Arizona", "85701"),
    ("Fresno", "California", "93650"),
    ("Sacramento", "California", "95814"),
    ("Mesa", "Arizona", "85201"),
    ("Atlanta", "Georgia", "30301"),
    ("Kansas City", "Missouri", "64101"),
    ("Omaha", "Nebraska", "68101"),
    ("Miami", "Florida", "33101"),
    ("Minneapolis", "Minnesota", "55401"),
    ("Cleveland", "Ohio", "44101"),
    ("Raleigh", "North Carolina", "27601"),
    ("Tampa", "Florida", "33601"),
    ("New Orleans", "Louisiana", "70112"),
    ("Pittsburgh", "Pennsylvania", "15201"),
    ("Cincinnati", "Ohio", "45201"),
    ("St. Louis", "Missouri", "63101"),
    ("Orlando", "Florida", "32801"),
    ("Boston", "Massachusetts", "02101"),
    ("Detroit", "Michigan", "48201"),
    ("Honolulu", "Hawaii", "96801"),
    ("Salt Lake City", "Utah", "84101"),
    ("Anchorage", "Alaska", "99501"),
    ("Richmond", "Virginia", "23219"),
];

/// Generate values for a correlation group.
///
/// `base_time` is the pinned wall-clock timestamp from the generation plan.
/// Temporal groups derive all dates relative to this anchor so regeneration
/// from a lockfile produces identical output regardless of when it runs.
pub fn generate_correlated_group(
    plan: &CorrelationGroupPlan,
    row_index: usize,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    match plan.group {
        CorrelationGroup::Address => generate_address(plan, rng, base_time),
        CorrelationGroup::GeoCoordinates => generate_geo(plan, rng, base_time),
        CorrelationGroup::PersonIdentity => generate_person(plan, row_index, rng, base_time),
        CorrelationGroup::Temporal => generate_temporal(plan, rng, base_time),
        CorrelationGroup::TemporalRange => generate_temporal_range(plan, rng, base_time),
    }
}

fn generate_address(
    plan: &CorrelationGroupPlan,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    let loc = US_LOCATIONS[rng.random_range(0..US_LOCATIONS.len())];
    let street_num = rng.random_range(100..9999);
    let streets = [
        "Main St",
        "Oak Ave",
        "Elm St",
        "Park Blvd",
        "Cedar Ln",
        "Maple Dr",
        "Pine St",
        "Washington Ave",
        "Lake Rd",
        "Hill St",
    ];
    let street = streets[rng.random_range(0..streets.len())];

    let mut values = Vec::new();
    for (col_name, st) in &plan.columns {
        let value = match st {
            SemanticType::StreetAddress => {
                Value::String(Cow::Owned(format!("{} {}", street_num, street)))
            }
            SemanticType::City => Value::String(Cow::Borrowed(loc.0)),
            SemanticType::State => Value::String(Cow::Borrowed(loc.1)),
            SemanticType::ZipCode | SemanticType::PostalCode => Value::String(Cow::Borrowed(loc.2)),
            SemanticType::Country => Value::String(Cow::Borrowed("United States")),
            SemanticType::CountryCode => Value::String(Cow::Borrowed("US")),
            // Column was swept into this group by classification but doesn't
            // match any address sub-type. Fall back to standalone generation
            // so NOT NULL columns get a real value instead of NULL.
            _ => generate_value(*st, rng, 0, &[], base_time),
        };
        values.push((col_name.clone(), value));
    }
    values
}

fn generate_geo(
    plan: &CorrelationGroupPlan,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    // Generate realistic US coordinates
    let lat: f64 = rng.random_range(25.0_f64..48.0_f64);
    let lng: f64 = rng.random_range(-125.0_f64..-70.0_f64);

    let mut values = Vec::new();
    for (col_name, st) in &plan.columns {
        let value = match st {
            SemanticType::Latitude => Value::Float((lat * 1_000_000.0).round() / 1_000_000.0),
            SemanticType::Longitude => Value::Float((lng * 1_000_000.0).round() / 1_000_000.0),
            _ => generate_value(*st, rng, 0, &[], base_time),
        };
        values.push((col_name.clone(), value));
    }
    values
}

fn generate_person(
    plan: &CorrelationGroupPlan,
    row_index: usize,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    use fake::faker::name::en::*;
    use fake::Fake;

    let first: String = FirstName().fake_with_rng(rng);
    let last: String = LastName().fake_with_rng(rng);
    let full_name = format!("{} {}", first, last);
    let email = format!(
        "{}.{}{}@example.com",
        first.to_lowercase(),
        last.to_lowercase(),
        if row_index > 0 {
            format!(".{}", row_index)
        } else {
            String::new()
        }
    );
    // Use first.last.index format to safely clear common minimum-length
    // constraints (many apps require LENGTH(username) >= 5).
    let username = format!(
        "{}.{}{}",
        first.to_lowercase(),
        last.to_lowercase(),
        row_index
    );

    let mut values = Vec::new();
    for (col_name, st) in &plan.columns {
        let value = match st {
            SemanticType::FirstName => Value::String(Cow::Owned(first.clone())),
            SemanticType::LastName => Value::String(Cow::Owned(last.clone())),
            SemanticType::FullName | SemanticType::DisplayName => {
                Value::String(Cow::Owned(full_name.clone()))
            }
            SemanticType::Email => Value::String(Cow::Owned(email.clone())),
            SemanticType::Username => Value::String(Cow::Owned(username.clone())),
            _ => generate_value(*st, rng, row_index, &[], base_time),
        };
        values.push((col_name.clone(), value));
    }
    values
}

fn generate_temporal(
    plan: &CorrelationGroupPlan,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    let created_days_ago = rng.random_range(30..365);
    let created = base_time - ChronoDuration::days(created_days_ago);
    let updated = created + ChronoDuration::days(rng.random_range(1..created_days_ago));
    let deleted = if rng.random_bool(0.1) {
        Some(updated + ChronoDuration::days(rng.random_range(1..30)))
    } else {
        None
    };

    let mut values = Vec::new();
    for (col_name, st) in &plan.columns {
        let value = match st {
            SemanticType::CreatedAt => Value::Timestamp(created),
            SemanticType::UpdatedAt => Value::Timestamp(updated),
            SemanticType::DeletedAt => match deleted {
                Some(d) => Value::Timestamp(d),
                None => Value::Null,
            },
            _ => generate_value(*st, rng, 0, &[], base_time),
        };
        values.push((col_name.clone(), value));
    }
    values
}

fn generate_temporal_range(
    plan: &CorrelationGroupPlan,
    rng: &mut impl Rng,
    base_time: chrono::NaiveDateTime,
) -> Vec<(String, Value)> {
    let today = base_time.date();
    let start_offset = rng.random_range(-30..60);
    let start = today + ChronoDuration::days(start_offset);
    let duration = rng.random_range(1..90);
    let end = start + ChronoDuration::days(duration);

    let mut values = Vec::new();
    for (col_name, st) in &plan.columns {
        let value = match st {
            SemanticType::StartDate => Value::Date(start),
            SemanticType::EndDate => Value::Date(end),
            _ => generate_value(*st, rng, 0, &[], base_time),
        };
        values.push((col_name.clone(), value));
    }
    values
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
    fn test_address_correlation() {
        let plan = CorrelationGroupPlan {
            group: CorrelationGroup::Address,
            columns: vec![
                ("city".to_string(), SemanticType::City),
                ("state".to_string(), SemanticType::State),
                ("zip".to_string(), SemanticType::ZipCode),
            ],
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let values = generate_correlated_group(&plan, 0, &mut rng, test_base_time());

        assert_eq!(values.len(), 3);
        // All values should be non-null strings
        for (_, v) in &values {
            assert!(matches!(v, Value::String(_)));
        }
    }

    #[test]
    fn test_person_correlation() {
        let plan = CorrelationGroupPlan {
            group: CorrelationGroup::PersonIdentity,
            columns: vec![
                ("first_name".to_string(), SemanticType::FirstName),
                ("last_name".to_string(), SemanticType::LastName),
                ("email".to_string(), SemanticType::Email),
            ],
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let values = generate_correlated_group(&plan, 0, &mut rng, test_base_time());

        // Email should contain first.last
        let email = values
            .iter()
            .find(|(n, _)| n == "email")
            .map(|(_, v)| v.clone())
            .unwrap();
        if let Value::String(e) = email {
            assert!(e.contains('@'));
            assert!(e.contains('.'));
        }
    }

    #[test]
    fn test_temporal_determinism() {
        let plan = CorrelationGroupPlan {
            group: CorrelationGroup::Temporal,
            columns: vec![
                ("created_at".to_string(), SemanticType::CreatedAt),
                ("updated_at".to_string(), SemanticType::UpdatedAt),
            ],
        };
        let bt = test_base_time();
        let mut rng1 = rand::rngs::StdRng::seed_from_u64(42);
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(42);

        let v1 = generate_correlated_group(&plan, 0, &mut rng1, bt);
        let v2 = generate_correlated_group(&plan, 0, &mut rng2, bt);

        // Same seed + same base_time must produce identical timestamps.
        for ((_, a), (_, b)) in v1.iter().zip(v2.iter()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_unmatched_column_falls_back_to_provider() {
        let plan = CorrelationGroupPlan {
            group: CorrelationGroup::Address,
            columns: vec![
                ("city".to_string(), SemanticType::City),
                ("notes".to_string(), SemanticType::Paragraph),
            ],
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let values = generate_correlated_group(&plan, 0, &mut rng, test_base_time());

        // "notes" doesn't match any address sub-type but should NOT be Null.
        let notes = values.iter().find(|(n, _)| n == "notes").unwrap();
        assert!(
            !matches!(notes.1, Value::Null),
            "Unmatched column in correlation group should fall back to provider, not Null"
        );
    }

    #[test]
    fn test_username_minimum_length() {
        let plan = CorrelationGroupPlan {
            group: CorrelationGroup::PersonIdentity,
            columns: vec![
                ("first_name".to_string(), SemanticType::FirstName),
                ("last_name".to_string(), SemanticType::LastName),
                ("username".to_string(), SemanticType::Username),
            ],
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let values = generate_correlated_group(&plan, 0, &mut rng, test_base_time());

        let username = values.iter().find(|(n, _)| n == "username").unwrap();
        if let Value::String(u) = &username.1 {
            assert!(u.len() >= 5, "Username '{}' should be >= 5 chars", u);
        }
    }
}
