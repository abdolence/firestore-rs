//! Automatic support for `std::time::SystemTime`.
//!
//! Serde models `SystemTime` as a two field structure named `SystemTime`,
//! carrying `secs_since_epoch` and `nanos_since_epoch`. Both the Firestore
//! serializer and the deserializer recognise that shape and map it to a native
//! Firestore timestamp instead, so a plain `SystemTime` field needs neither an
//! attribute nor a wrapping type.
//!
//! Reading is deliberately lenient: a native timestamp is preferred, but the two
//! field map written by the earlier versions of this library is still accepted,
//! so the existing documents keep working without a migration.

use std::collections::HashMap;

use gcloud_sdk::google::firestore::v1::value;
use gcloud_sdk::google::firestore::v1::Value;

use crate::errors::{FirestoreError, FirestoreSerializationError};
use crate::timestamp_utils::to_timestamp;
use crate::FirestoreInstant;

/// The structure name serde uses for `std::time::SystemTime`.
pub(crate) const FIRESTORE_SYSTEM_TIME_TYPE_TAG_TYPE: &str = "SystemTime";

/// The field names serde uses for `std::time::SystemTime`.
const FIRESTORE_SYSTEM_TIME_SECS_FIELD: &str = "secs_since_epoch";
const FIRESTORE_SYSTEM_TIME_NANOS_FIELD: &str = "nanos_since_epoch";

/// Folds the fields collected for a structure named `SystemTime` into a native
/// Firestore timestamp.
///
/// Returns `Ok(None)` when the fields do not have the exact shape serde produces
/// for `std::time::SystemTime`, in which case the caller keeps the previous
/// behaviour and writes an ordinary map. That keeps a user defined structure
/// that merely happens to have the same name working unchanged.
pub(crate) fn system_time_fields_to_timestamp_value(
    fields: &HashMap<String, Value>,
) -> Result<Option<Value>, FirestoreError> {
    if fields.len() != 2 {
        return Ok(None);
    }

    let (secs, nanos) = match (
        fields
            .get(FIRESTORE_SYSTEM_TIME_SECS_FIELD)
            .and_then(|v| v.value_type.as_ref()),
        fields
            .get(FIRESTORE_SYSTEM_TIME_NANOS_FIELD)
            .and_then(|v| v.value_type.as_ref()),
    ) {
        (
            Some(value::ValueType::IntegerValue(secs)),
            Some(value::ValueType::IntegerValue(nanos)),
        ) => (*secs, *nanos),
        _ => return Ok(None),
    };

    // Serde widens both fields from unsigned values, so anything negative here
    // cannot have come from a real `SystemTime` and is left alone as a map.
    let (Ok(secs), Ok(nanos)) = (u64::try_from(secs), u32::try_from(nanos)) else {
        return Ok(None);
    };

    Ok(Some(Value {
        value_type: Some(value::ValueType::TimestampValue(epoch_offset_to_timestamp(
            secs, nanos,
        )?)),
    }))
}

/// Converts an offset from the Unix epoch, as serde reports it for
/// `std::time::SystemTime`, into a Google `prost_types::Timestamp`.
fn epoch_offset_to_timestamp(
    secs_since_epoch: u64,
    nanos_since_epoch: u32,
) -> Result<gcloud_sdk::prost_types::Timestamp, FirestoreError> {
    // Subsecond nanoseconds never reach a full second, but normalise defensively
    // so that the protobuf range is always respected.
    let carry_secs = u64::from(nanos_since_epoch / 1_000_000_000);
    let nanos = (nanos_since_epoch % 1_000_000_000) as i32;

    let seconds = secs_since_epoch
        .checked_add(carry_secs)
        .and_then(|secs| i64::try_from(secs).ok())
        .ok_or_else(|| {
            FirestoreError::SerializeError(FirestoreSerializationError::from_message(format!(
                "SystemTime is too far in the future to be a Firestore timestamp: \
                 {secs_since_epoch}s {nanos_since_epoch}ns since the Unix epoch"
            )))
        })?;

    // Going through `FirestoreInstant` keeps the accepted range identical to
    // every other timestamp of this library, and reports the out of range values
    // here instead of failing on the server side.
    let instant = FirestoreInstant::new(seconds, nanos).map_err(|err| {
        FirestoreError::SerializeError(FirestoreSerializationError::from_message(format!(
            "Invalid or out-of-range SystemTime: {seconds}s {nanos}ns since the Unix epoch. {err}"
        )))
    })?;

    Ok(to_timestamp(instant))
}

/// Expands a Firestore timestamp into the sequence of the seconds and the
/// nanoseconds since the Unix epoch that serde expects for
/// `std::time::SystemTime`.
pub(crate) fn timestamp_to_system_time_parts(
    ts: gcloud_sdk::prost_types::Timestamp,
) -> Result<Vec<Value>, FirestoreError> {
    // Normalise a negative subsecond part back into the protobuf range first,
    // mirroring `timestamp_utils::to_timestamp`.
    let (seconds, nanos) = if ts.nanos < 0 {
        (ts.seconds - 1, ts.nanos + 1_000_000_000)
    } else {
        (ts.seconds, ts.nanos)
    };

    if seconds < 0 {
        return Err(FirestoreError::DeserializeError(
            FirestoreSerializationError::from_message(format!(
                "`std::time::SystemTime` cannot represent the timestamp {seconds}s {nanos}ns, \
                 since serde only supports the instants at or after the Unix epoch. \
                 Use `FirestoreTimestamp` or `FirestoreInstant` for the earlier dates."
            )),
        ));
    }

    Ok(vec![
        Value {
            value_type: Some(value::ValueType::IntegerValue(seconds)),
        },
        Value {
            value_type: Some(value::ValueType::IntegerValue(i64::from(nanos))),
        },
    ])
}

#[cfg(test)]
mod tests {
    use crate::{firestore_document_from_serializable, firestore_document_to_serializable};
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use gcloud_sdk::google::firestore::v1::{Document, MapValue, Value};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct NestedStructure {
        touched_at: SystemTime,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestStructure {
        created_at: SystemTime,
        updated_at: Option<SystemTime>,
        deleted_at: Option<SystemTime>,
        history: Vec<SystemTime>,
        nested: NestedStructure,
    }

    fn sample_time() -> SystemTime {
        UNIX_EPOCH + Duration::new(1_670_000_000, 123_456_789)
    }

    #[test]
    fn test_system_time_serde_roundtrip() {
        let test_structure = TestStructure {
            created_at: sample_time(),
            updated_at: Some(sample_time() + Duration::from_secs(60)),
            deleted_at: None,
            history: vec![sample_time(), UNIX_EPOCH],
            nested: NestedStructure {
                touched_at: sample_time(),
            },
        };

        let document =
            firestore_document_from_serializable("test/doc-id", &test_structure).unwrap();

        // A plain `SystemTime` must land as a native timestamp, not as a map
        match document.fields["created_at"].value_type {
            Some(ValueType::TimestampValue(ts)) => {
                assert_eq!(ts.seconds, 1_670_000_000);
                assert_eq!(ts.nanos, 123_456_789);
            }
            ref other => panic!("created_at must be a timestamp value, got {other:?}"),
        }

        // Optionals, sequences and the nested structures go the same way
        assert!(matches!(
            document.fields["updated_at"].value_type,
            Some(ValueType::TimestampValue(_))
        ));
        assert!(!document.fields.contains_key("deleted_at"));

        match document.fields["history"].value_type {
            Some(ValueType::ArrayValue(ref array)) => {
                assert_eq!(array.values.len(), 2);
                for element in &array.values {
                    assert!(matches!(
                        element.value_type,
                        Some(ValueType::TimestampValue(_))
                    ));
                }
            }
            ref other => panic!("history must be an array value, got {other:?}"),
        }

        match document.fields["nested"].value_type {
            Some(ValueType::MapValue(ref map)) => assert!(matches!(
                map.fields["touched_at"].value_type,
                Some(ValueType::TimestampValue(_))
            )),
            ref other => panic!("nested must be a map value, got {other:?}"),
        }

        let deserialized: TestStructure =
            firestore_document_to_serializable(&document).expect("Unable to deserialize");
        assert_eq!(deserialized, test_structure);
    }

    #[test]
    fn test_legacy_system_time_map_still_deserializes() {
        // The documents written by the earlier versions carry the two field map
        // serde produces by default, and must keep working without a migration.
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct LegacyStructure {
            created_at: SystemTime,
            updated_at: Option<SystemTime>,
        }

        let legacy_fields: HashMap<String, Value> = vec![
            (
                "secs_since_epoch".to_string(),
                Value {
                    value_type: Some(ValueType::IntegerValue(1_670_000_000)),
                },
            ),
            (
                "nanos_since_epoch".to_string(),
                Value {
                    value_type: Some(ValueType::IntegerValue(123_456_789)),
                },
            ),
        ]
        .into_iter()
        .collect();

        let document = Document {
            name: "test/doc-id".to_string(),
            fields: vec![(
                "created_at".to_string(),
                Value {
                    value_type: Some(ValueType::MapValue(MapValue {
                        fields: legacy_fields,
                    })),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let deserialized: LegacyStructure =
            firestore_document_to_serializable(&document).expect("Unable to deserialize");
        assert_eq!(deserialized.created_at, sample_time());
        assert_eq!(deserialized.updated_at, None);
    }

    #[test]
    fn test_user_structure_named_system_time_is_not_intercepted() {
        // Only the shape serde produces is folded, so a structure that merely
        // shares the name keeps serializing as an ordinary map.
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename = "SystemTime")]
        struct MachineTime {
            host: String,
            uptime_secs: u64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct Wrapper {
            machine: MachineTime,
        }

        let wrapper = Wrapper {
            machine: MachineTime {
                host: "localhost".to_string(),
                uptime_secs: 42,
            },
        };

        let document = firestore_document_from_serializable("test/doc-id", &wrapper).unwrap();
        assert!(matches!(
            document.fields["machine"].value_type,
            Some(ValueType::MapValue(_))
        ));

        let deserialized: Wrapper = firestore_document_to_serializable(&document).unwrap();
        assert_eq!(deserialized, wrapper);
    }

    #[test]
    fn test_pre_epoch_system_time_is_rejected() {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Wrapper {
            created_at: SystemTime,
        }

        // Serde itself refuses to serialize the instants before the Unix epoch
        let before_epoch = Wrapper {
            created_at: UNIX_EPOCH - Duration::from_secs(1),
        };
        assert!(firestore_document_from_serializable("test/doc-id", &before_epoch).is_err());

        // And reading one back reports a descriptive error instead of overflowing
        let document = Document {
            name: "test/doc-id".to_string(),
            fields: vec![(
                "created_at".to_string(),
                Value {
                    value_type: Some(ValueType::TimestampValue(
                        gcloud_sdk::prost_types::Timestamp {
                            seconds: -1,
                            nanos: 0,
                        },
                    )),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert!(firestore_document_to_serializable::<Wrapper>(&document).is_err());
    }
}
