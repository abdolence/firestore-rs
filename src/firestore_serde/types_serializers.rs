use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FirestoreTimestamp(pub DateTime<Utc>);

pub mod serialize_as_timestamp {

    pub(crate) const NEWTYPE_TAG_TYPE: &str = "FirestoreTimestamp";

    use crate::errors::*;
    use crate::timestamp_utils::to_timestamp;
    use crate::{FirestoreError, FirestoreValue};
    use chrono::prelude::*;
    use gcloud_sdk::google::firestore::v1::value;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct(NEWTYPE_TAG_TYPE, &date)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        DateTime::<Utc>::deserialize(deserializer)
    }

    pub(crate) fn serialize_for_firestore<T: ?Sized + Serialize>(
        value: &T,
    ) -> Result<FirestoreValue, FirestoreError> {
        struct TimestampSerializer;

        impl Serializer for TimestampSerializer {
            type Ok = FirestoreValue;
            type Error = FirestoreError;
            type SerializeSeq = crate::firestore_serde::serializer::SerializeVec;
            type SerializeTuple = crate::firestore_serde::serializer::SerializeVec;
            type SerializeTupleStruct = crate::firestore_serde::serializer::SerializeVec;
            type SerializeTupleVariant = crate::firestore_serde::serializer::SerializeTupleVariant;
            type SerializeMap = crate::firestore_serde::serializer::SerializeMap;
            type SerializeStruct = crate::firestore_serde::serializer::SerializeMap;
            type SerializeStructVariant =
                crate::firestore_serde::serializer::SerializeStructVariant;

            fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
                let dt = v.parse::<DateTime<Utc>>()?;
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::TimestampValue(to_timestamp(dt))),
                    },
                ))
            }

            fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
            where
                T: Serialize,
            {
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

            fn serialize_newtype_struct<T: ?Sized>(
                self,
                _name: &'static str,
                value: &T,
            ) -> Result<Self::Ok, Self::Error>
            where
                T: Serialize,
            {
                value.serialize(self)
            }

            fn serialize_newtype_variant<T: ?Sized>(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _value: &T,
            ) -> Result<Self::Ok, Self::Error>
            where
                T: Serialize,
            {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
                Err(FirestoreError::DeserializeError(
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
                Err(FirestoreError::DeserializeError(
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
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }

            fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
                Err(FirestoreError::DeserializeError(
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
                Err(FirestoreError::DeserializeError(
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
                Err(FirestoreError::DeserializeError(
                    FirestoreSerializationError::from_message(
                        "Timestamp serializer doesn't support this type",
                    ),
                ))
            }
        }

        value.serialize(TimestampSerializer {})
    }
}
