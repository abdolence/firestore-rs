use gcloud_sdk::google::firestore::v1::value;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;

use crate::errors::*;
use crate::firestore_serde::serializer::FirestoreValueSerializer;
use crate::FirestoreValue;

pub(crate) const FIRESTORE_LATLNG_TYPE_TAG_TYPE: &str = "FirestoreLatLng";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct FirestoreGeoPoint {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct FirestoreLatLng(pub FirestoreGeoPoint);

pub fn serialize_latlng_for_firestore<T: ?Sized + Serialize>(
    value: &T,
) -> Result<FirestoreValue, FirestoreError> {
    struct LatLngSerializer;

    struct SerializeLatLngStruct {
        fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
    }

    impl serde::ser::SerializeStruct for SerializeLatLngStruct {
        type Ok = FirestoreValue;
        type Error = FirestoreError;

        fn serialize_field<T: ?Sized>(
            &mut self,
            key: &'static str,
            value: &T,
        ) -> Result<(), Self::Error>
        where
            T: Serialize,
        {
            let serializer = FirestoreValueSerializer {
                none_as_null: false,
            };
            let serialized_value = value.serialize(serializer)?.value;
            if serialized_value.value_type.is_some() {
                self.fields.insert(key.to_string(), serialized_value);
            }
            Ok(())
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            let (lat, lng) = match (self.fields.get("latitude"), self.fields.get("longitude")) {
                (
                    Some(gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::DoubleValue(lat)),
                    }),
                    Some(gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::DoubleValue(lng)),
                    }),
                ) => (*lat, *lng),
                _ => {
                    return Err(FirestoreError::SerializeError(
                        FirestoreSerializationError::from_message(
                            "LatLng serializer doesn't recognize the structure of the object",
                        ),
                    ))
                }
            };

            Ok(FirestoreValue::from(
                gcloud_sdk::google::firestore::v1::Value {
                    value_type: Some(value::ValueType::GeoPointValue(
                        gcloud_sdk::google::r#type::LatLng {
                            latitude: lat,
                            longitude: lng,
                        },
                    )),
                },
            ))
        }
    }

    impl Serializer for LatLngSerializer {
        type Ok = FirestoreValue;
        type Error = FirestoreError;
        type SerializeSeq = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTuple = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleStruct = crate::firestore_serde::serializer::SerializeVec;
        type SerializeTupleVariant = crate::firestore_serde::serializer::SerializeTupleVariant;
        type SerializeMap = crate::firestore_serde::serializer::SerializeMap;
        type SerializeStruct = SerializeLatLngStruct;
        type SerializeStructVariant = crate::firestore_serde::serializer::SerializeStructVariant;

        fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
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
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
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
                    "LatLng serializer doesn't support this type",
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
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message(
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }

        fn serialize_struct(
            self,
            _name: &'static str,
            len: usize,
        ) -> Result<Self::SerializeStruct, Self::Error> {
            Ok(SerializeLatLngStruct {
                fields: HashMap::with_capacity(len),
            })
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
                    "LatLng serializer doesn't support this type",
                ),
            ))
        }
    }

    value.serialize(LatLngSerializer {})
}
