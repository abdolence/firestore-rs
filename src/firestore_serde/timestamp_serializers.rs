use crate::FirestoreInstant;
use gcloud_sdk::google::firestore::v1::value;
use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize, Serializer};

use crate::{
    errors::FirestoreSerializationError, timestamp_utils::to_timestamp, FirestoreError,
    FirestoreValue,
};

/// A wrapper around [`FirestoreInstant`] that is always serialized as a
/// Firestore timestamp value, without needing a `#[serde(with)]` attribute.
///
/// Use [`FirestoreInstant`] directly when you prefer the attributes, and this
/// wrapper when you would rather carry the behaviour in the type. It still
/// serializes as a string to JSON, so the same model can be reused for both
/// JSON and Firestore.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Default, ValueStruct,
)]
pub struct FirestoreTimestamp(pub FirestoreInstant);

impl FirestoreTimestamp {
    /// Returns the current instant.
    #[inline]
    pub fn now() -> Self {
        FirestoreTimestamp(FirestoreInstant::now())
    }
}

impl std::fmt::Display for FirestoreTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for FirestoreTimestamp {
    type Err = crate::errors::FirestoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(FirestoreTimestamp(s.parse::<FirestoreInstant>()?))
    }
}

impl From<FirestoreTimestamp> for FirestoreInstant {
    fn from(ts: FirestoreTimestamp) -> Self {
        ts.0
    }
}

/// Converting to a `SystemTime` is infallible, since every representable
/// timestamp fits into it.
impl From<FirestoreTimestamp> for std::time::SystemTime {
    fn from(ts: FirestoreTimestamp) -> Self {
        ts.0.into()
    }
}

/// Converting from a `SystemTime` is fallible, since it may be outside of the
/// range supported by Firestore timestamps.
impl TryFrom<std::time::SystemTime> for FirestoreTimestamp {
    type Error = crate::errors::FirestoreError;

    fn try_from(system_time: std::time::SystemTime) -> Result<Self, Self::Error> {
        Ok(FirestoreTimestamp(FirestoreInstant::try_from(system_time)?))
    }
}

/// Converts from a Google `prost_types::Timestamp`, as received from Firestore.
impl TryFrom<gcloud_sdk::prost_types::Timestamp> for FirestoreTimestamp {
    type Error = crate::errors::FirestoreError;

    fn try_from(ts: gcloud_sdk::prost_types::Timestamp) -> Result<Self, Self::Error> {
        Ok(FirestoreTimestamp(crate::timestamp_utils::from_timestamp(
            ts,
        )?))
    }
}

/// Converts into a Google `prost_types::Timestamp`, as sent to Firestore.
impl From<FirestoreTimestamp> for gcloud_sdk::prost_types::Timestamp {
    fn from(ts: FirestoreTimestamp) -> Self {
        crate::timestamp_utils::to_timestamp(ts.0)
    }
}

pub(crate) const FIRESTORE_TS_TYPE_TAG_TYPE: &str = "FirestoreTimestamp";

pub(crate) const FIRESTORE_TS_NULL_TYPE_TAG_TYPE: &str = "FirestoreTimestampAsNull";

pub mod serialize_as_timestamp {
    use crate::FirestoreInstant;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &FirestoreInstant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_TYPE_TAG_TYPE, &ts)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<FirestoreInstant, D::Error>
    where
        D: Deserializer<'de>,
    {
        FirestoreInstant::deserialize(deserializer)
    }
}

pub mod serialize_as_optional_timestamp {
    use crate::FirestoreInstant;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &Option<FirestoreInstant>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match ts {
            Some(v) => serializer
                .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_TYPE_TAG_TYPE, v),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FirestoreInstant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<FirestoreInstant>::deserialize(deserializer)
    }
}

pub mod serialize_as_null_timestamp {
    use crate::FirestoreInstant;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &Option<FirestoreInstant>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer
            .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_NULL_TYPE_TAG_TYPE, ts)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FirestoreInstant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<FirestoreInstant>::deserialize(deserializer)
    }
}

pub fn serialize_timestamp_for_firestore<T: ?Sized + Serialize>(
    value: &T,
    none_as_null: bool,
) -> Result<FirestoreValue, FirestoreError> {
    struct TimestampSerializer {
        none_as_null: bool,
    }

    impl Serializer for TimestampSerializer {
        type Ok = FirestoreValue;
        type Error = FirestoreError;
        type SerializeSeq = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTuple = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleStruct = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleVariant = crate::firestore_serde::serializer::SerializeTupleVariant;
        type SerializeMap = crate::firestore_serde::serializer::SerializeMap;
        type SerializeStruct = crate::firestore_serde::serializer::SerializeMap;
        type SerializeStructVariant = crate::firestore_serde::serializer::SerializeStructVariant;

        fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
            let ts = v.parse::<FirestoreInstant>()?;
            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value {
                    value_type: Some(value::ValueType::TimestampValue(to_timestamp(ts))),
                },
            ))
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            if self.none_as_null {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::NullValue(0)),
                    },
                ))
            } else {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value { value_type: None },
                ))
            }
        }

        fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value { value_type: None },
            ))
        }

        fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
            self.serialize_unit()
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            self.serialize_str(variant)
        }

        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<Self::Ok, Self::Error> {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Timestamp serializer doesn't support this type",
                ),
            ))
        }
    }

    value.serialize(TimestampSerializer { none_as_null })
}

#[cfg(test)]
mod tests {
    use crate::{
        firestore_document_from_serializable, firestore_document_to_serializable, FirestoreInstant,
        FirestoreTimestamp,
    };
    use rvstruct::ValueStruct;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestStructure {
        #[serde(with = "crate::serialize_as_timestamp")]
        created_at: FirestoreInstant,
        #[serde(default)]
        #[serde(with = "crate::serialize_as_optional_timestamp")]
        updated_at: Option<FirestoreInstant>,
        wrapped_at: FirestoreTimestamp,
    }

    #[test]
    fn test_timestamp_serde_roundtrip() {
        let created_at: FirestoreInstant = "2022-12-02T16:53:20.123456789Z".parse().unwrap();
        let wrapped_at: FirestoreInstant = "2018-01-01T00:00:00Z".parse().unwrap();

        let test_structure = TestStructure {
            created_at,
            updated_at: Some(created_at),
            wrapped_at: FirestoreTimestamp(wrapped_at),
        };

        let document =
            firestore_document_from_serializable("test/doc-id", &test_structure).unwrap();

        // The timestamps must land as Firestore timestamp values, not as strings
        assert!(matches!(
            document.fields["created_at"].value_type,
            Some(gcloud_sdk::google::firestore::v1::value::ValueType::TimestampValue(_))
        ));

        let deserialized: TestStructure =
            firestore_document_to_serializable(&document).expect("Unable to deserialize");

        assert_eq!(deserialized, test_structure);
        // Firestore timestamps keep nanosecond precision
        assert_eq!(deserialized.created_at.subsec_nanosecond(), 123_456_789);
    }

    #[test]
    fn test_timestamp_conversions() {
        use std::str::FromStr;
        use std::time::SystemTime;

        let ts = FirestoreTimestamp::from_str("2022-12-02T16:53:20.123456789Z").unwrap();

        // Display round trips through FromStr
        assert_eq!(FirestoreTimestamp::from_str(&ts.to_string()).unwrap(), ts);

        // SystemTime, infallible one way and fallible back
        let system_time: SystemTime = ts.into();
        assert_eq!(FirestoreTimestamp::try_from(system_time).unwrap(), ts);

        // Google protobuf timestamps
        let proto: gcloud_sdk::prost_types::Timestamp = ts.into();
        assert_eq!(proto.seconds, 1670000000);
        assert_eq!(proto.nanos, 123_456_789);
        assert_eq!(FirestoreTimestamp::try_from(proto).unwrap(), ts);

        // The inner instant, both ways
        let instant: FirestoreInstant = ts.into();
        assert_eq!(FirestoreTimestamp::from(instant), ts);
        assert_eq!(ts.value(), &instant);

        assert_eq!(
            FirestoreTimestamp::try_from(SystemTime::UNIX_EPOCH).unwrap(),
            FirestoreTimestamp::default()
        );
    }

    /// Mirrors the structure used by `examples/timestamp.rs`, so that all three
    /// documented ways of writing a Firestore timestamp stay verified.
    #[test]
    fn test_all_documented_timestamp_forms() {
        use gcloud_sdk::google::firestore::v1::value::ValueType;
        use std::time::SystemTime;

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct ExampleStructure {
            some_id: String,
            created_at: FirestoreTimestamp,
            updated_at: Option<FirestoreTimestamp>,
            updated_at_always_none: Option<FirestoreTimestamp>,
            #[serde(with = "crate::serialize_as_timestamp")]
            created_at_attr: FirestoreInstant,
            #[serde(default)]
            #[serde(with = "crate::serialize_as_optional_timestamp")]
            updated_at_attr: Option<FirestoreInstant>,
            #[serde(default)]
            #[serde(with = "crate::serialize_as_optional_timestamp")]
            updated_at_attr_always_none: Option<FirestoreInstant>,
        }

        let from_system_time: FirestoreTimestamp = SystemTime::now().try_into().unwrap();

        let example = ExampleStructure {
            some_id: "test-1".to_string(),
            created_at: FirestoreTimestamp::now(),
            updated_at: Some(from_system_time),
            updated_at_always_none: None,
            created_at_attr: FirestoreInstant::now(),
            updated_at_attr: Some(FirestoreInstant::now()),
            updated_at_attr_always_none: None,
        };

        let document = firestore_document_from_serializable("test/doc-id", &example).unwrap();

        // Every populated form must land as a Firestore timestamp value, not a string
        for field in [
            "created_at",
            "updated_at",
            "created_at_attr",
            "updated_at_attr",
        ] {
            assert!(
                matches!(
                    document.fields[field].value_type,
                    Some(ValueType::TimestampValue(_))
                ),
                "{field} must serialize as a Firestore timestamp"
            );
        }

        let deserialized: ExampleStructure =
            firestore_document_to_serializable(&document).expect("Unable to deserialize");
        assert_eq!(deserialized, example);
    }

    #[test]
    fn test_optional_timestamp_none_roundtrip() {
        let created_at: FirestoreInstant = "2022-12-02T16:53:20Z".parse().unwrap();

        let test_structure = TestStructure {
            created_at,
            updated_at: None,
            wrapped_at: FirestoreTimestamp(created_at),
        };

        let document =
            firestore_document_from_serializable("test/doc-id", &test_structure).unwrap();

        let deserialized: TestStructure =
            firestore_document_to_serializable(&document).expect("Unable to deserialize");

        assert_eq!(deserialized, test_structure);
    }
}
