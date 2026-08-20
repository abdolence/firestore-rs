use crate::FirestoreDateTime;
use gcloud_sdk::google::firestore::v1::value;
use serde::{Deserialize, Serialize, Serializer};

use crate::{
    errors::FirestoreSerializationError, timestamp_utils::to_timestamp, FirestoreError,
    FirestoreValue,
};

/// A wrapper around [`FirestoreDateTime`] that is always serialized as a
/// Firestore timestamp value, without needing a `#[serde(with)]` attribute.
///
/// It still serializes as a string to JSON, so the same model can be reused for
/// both JSON and Firestore.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, PartialOrd, Default)]
pub struct FirestoreTimestamp(pub FirestoreDateTime);

impl From<FirestoreDateTime> for FirestoreTimestamp {
    fn from(ts: FirestoreDateTime) -> Self {
        FirestoreTimestamp(ts)
    }
}

pub(crate) const FIRESTORE_TS_TYPE_TAG_TYPE: &str = "FirestoreTimestamp";

pub(crate) const FIRESTORE_TS_NULL_TYPE_TAG_TYPE: &str = "FirestoreTimestampAsNull";

pub mod serialize_as_timestamp {
    use crate::FirestoreDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &FirestoreDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_TYPE_TAG_TYPE, &ts)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<FirestoreDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        FirestoreDateTime::deserialize(deserializer)
    }
}

pub mod serialize_as_optional_timestamp {
    use crate::FirestoreDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &Option<FirestoreDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match ts {
            Some(v) => serializer
                .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_TYPE_TAG_TYPE, v),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FirestoreDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<FirestoreDateTime>::deserialize(deserializer)
    }
}

pub mod serialize_as_null_timestamp {
    use crate::FirestoreDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &Option<FirestoreDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer
            .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_TS_NULL_TYPE_TAG_TYPE, ts)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FirestoreDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<FirestoreDateTime>::deserialize(deserializer)
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
            let ts = v.parse::<FirestoreDateTime>()?;
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
        firestore_document_from_serializable, firestore_document_to_serializable,
        FirestoreDateTime, FirestoreTimestamp,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestStructure {
        #[serde(with = "crate::serialize_as_timestamp")]
        created_at: FirestoreDateTime,
        #[serde(default)]
        #[serde(with = "crate::serialize_as_optional_timestamp")]
        updated_at: Option<FirestoreDateTime>,
        wrapped_at: FirestoreTimestamp,
    }

    #[test]
    fn test_timestamp_serde_roundtrip() {
        let created_at: FirestoreDateTime = "2022-12-02T16:53:20.123456789Z".parse().unwrap();
        let wrapped_at: FirestoreDateTime = "2018-01-01T00:00:00Z".parse().unwrap();

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
    fn test_optional_timestamp_none_roundtrip() {
        let created_at: FirestoreDateTime = "2022-12-02T16:53:20Z".parse().unwrap();

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
