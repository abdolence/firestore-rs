use crate::errors::{FirestoreError, FirestoreSerializationError};
use crate::firestore_serde::serializer::SerializeVec;
use crate::FirestoreValue;
use serde::de::{MapAccess, Visitor};
use serde::{Deserializer, Serialize, Serializer};

pub(crate) const FIRESTORE_VECTOR_TYPE_TAG_TYPE: &str = "FirestoreVector";

#[derive(Serialize, Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct FirestoreVector(pub Vec<f64>);

impl FirestoreVector {
    pub fn new(vec: Vec<f64>) -> Self {
        FirestoreVector(vec)
    }
}

impl<I> From<I> for FirestoreVector
where
    I: IntoIterator<Item = f64>,
{
    fn from(vec: I) -> Self {
        FirestoreVector(vec.into_iter().collect())
    }
}

pub fn serialize_vector_for_firestore<T: ?Sized + Serialize>(
    value: &T,
) -> Result<FirestoreValue, FirestoreError> {
    struct VectorSerializer;

    impl Serializer for VectorSerializer {
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
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value { value_type: None },
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
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type: newtype_variant",
                ),
            ))
        }

        fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            Ok(SerializeVec {
                none_as_null: false,
                vec: Vec::with_capacity(len.unwrap_or(0)),
            })
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type: tuple",
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
                    "Vector serializer doesn't support this type: tuple_struct",
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
                    "Vector serializer doesn't support this type: tuple_variant",
                ),
            ))
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "Vector serializer doesn't support this type: map",
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
                    "Vector serializer doesn't support this type: struct",
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
                    "Vector serializer doesn't support this type: struct_variant",
                ),
            ))
        }
    }

    let value_with_array = value.serialize(VectorSerializer {})?;

    Ok(FirestoreValue::from(
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue {
                    fields: vec![
                        (
                            "__type__".to_string(),
                            gcloud_sdk::google::firestore::v1::Value {
                                value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::StringValue(
                                    "__vector__".to_string()
                                )),
                            }
                        ),
                        (
                            "value".to_string(),
                            value_with_array.value
                        )].into_iter().collect()
                }
            ))
        }),
    )
}

struct FirestoreVectorVisitor;

impl<'de> Visitor<'de> for FirestoreVectorVisitor {
    type Value = FirestoreVector;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a FirestoreVector")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut vec = Vec::new();

        while let Some(value) = seq.next_element()? {
            vec.push(value);
        }

        Ok(FirestoreVector(vec))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "__type__" => {
                    let value = map.next_value::<String>()?;
                    if value != "__vector__" {
                        return Err(serde::de::Error::custom(
                            "Expected __vector__  for FirestoreVector",
                        ));
                    }
                }
                "value" => {
                    let value = map.next_value::<Vec<f64>>()?;
                    return Ok(FirestoreVector(value));
                }
                _ => {
                    return Err(serde::de::Error::custom(
                        "Unknown field for FirestoreVector",
                    ));
                }
            }
        }
        Err(serde::de::Error::custom(
            "Unknown structure for FirestoreVector",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for FirestoreVector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FirestoreVectorVisitor)
    }
}
