use crate::errors::*;
use crate::{FirestoreError, FirestoreValue};
use gcloud_sdk::google::firestore::v1::value;
use serde::Serialize;
use std::collections::HashMap;

pub struct FirestoreValueSerializer {
    pub none_as_null: bool,
}

impl FirestoreValueSerializer {
    pub fn new() -> Self {
        Self {
            none_as_null: false,
        }
    }
}

pub struct SerializeVec {
    pub none_as_null: bool,
    pub vec: Vec<gcloud_sdk::google::firestore::v1::Value>,
}

pub struct SerializeTupleVariant {
    none_as_null: bool,
    name: String,
    vec: Vec<gcloud_sdk::google::firestore::v1::Value>,
}

pub struct SerializeMap {
    none_as_null: bool,
    fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
    next_key: Option<String>,
}

pub struct SerializeStructVariant {
    none_as_null: bool,
    name: String,
    fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
}

impl serde::Serializer for FirestoreValueSerializer {
    type Ok = FirestoreValue;
    type Error = FirestoreError;
    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap;
    type SerializeStruct = SerializeMap;
    type SerializeStructVariant = SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::BooleanValue(v)),
            },
        ))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v)),
            },
        ))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v.into())),
            },
        ))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::IntegerValue(v as i64)),
            },
        ))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::DoubleValue(v.into())),
            },
        ))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::DoubleValue(v)),
            },
        ))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::StringValue(v.to_string())),
            },
        ))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::StringValue(v.to_string())),
            },
        ))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::BytesValue(v.into())),
            },
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
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        match name {
            crate::firestore_serde::timestamp_serializers::FIRESTORE_TS_TYPE_TAG_TYPE => {
                crate::firestore_serde::timestamp_serializers::serialize_timestamp_for_firestore(
                    value, false,
                )
            }
            crate::firestore_serde::timestamp_serializers::FIRESTORE_TS_NULL_TYPE_TAG_TYPE => {
                crate::firestore_serde::timestamp_serializers::serialize_timestamp_for_firestore(
                    value, true,
                )
            }
            crate::firestore_serde::null_serializers::FIRESTORE_NULL_TYPE_TAG_TYPE => {
                value.serialize(Self { none_as_null: true })
            }
            crate::firestore_serde::latlng_serializers::FIRESTORE_LATLNG_TYPE_TAG_TYPE => {
                crate::firestore_serde::latlng_serializers::serialize_latlng_for_firestore(value)
            }
            crate::firestore_serde::reference_serializers::FIRESTORE_REFERENCE_TYPE_TAG_TYPE => {
                crate::firestore_serde::reference_serializers::serialize_reference_for_firestore(
                    value, false,
                )
            }
            crate::firestore_serde::vector_serializers::FIRESTORE_VECTOR_TYPE_TAG_TYPE => {
                crate::firestore_serde::vector_serializers::serialize_vector_for_firestore(
                    self, value,
                )
            }
            _ => value.serialize(self),
        }
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        let mut fields = HashMap::new();
        fields.insert(String::from(variant), value.serialize(self)?.value);
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue { fields },
                )),
            },
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeVec {
            none_as_null: self.none_as_null,
            vec: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SerializeTupleVariant {
            none_as_null: self.none_as_null,
            name: String::from(variant),
            vec: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeMap {
            none_as_null: self.none_as_null,
            fields: HashMap::with_capacity(len.unwrap_or(0)),
            next_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SerializeStructVariant {
            none_as_null: self.none_as_null,
            name: String::from(variant),
            fields: HashMap::with_capacity(len),
        })
    }
}

impl serde::ser::SerializeSeq for SerializeVec {
    type Ok = FirestoreValue;
    type Error = FirestoreError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        let serialized_value = value
            .serialize(FirestoreValueSerializer {
                none_as_null: self.none_as_null,
            })?
            .value;
        if serialized_value.value_type.is_some() {
            self.vec.push(serialized_value);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::ArrayValue(
                    gcloud_sdk::google::firestore::v1::ArrayValue { values: self.vec },
                )),
            },
        ))
    }
}

impl serde::ser::SerializeTuple for SerializeVec {
    type Ok = FirestoreValue;
    type Error = FirestoreError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleStruct for SerializeVec {
    type Ok = FirestoreValue;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = FirestoreValue;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        let serialized_value = value
            .serialize(FirestoreValueSerializer {
                none_as_null: self.none_as_null,
            })?
            .value;
        if serialized_value.value_type.is_some() {
            self.vec.push(serialized_value)
        };
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut fields = HashMap::new();
        fields.insert(
            self.name,
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::ArrayValue(
                    gcloud_sdk::google::firestore::v1::ArrayValue { values: self.vec },
                )),
            },
        );

        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue { fields },
                )),
            },
        ))
    }
}

impl serde::ser::SerializeMap for SerializeMap {
    type Ok = FirestoreValue;
    type Error = FirestoreError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        let serializer = FirestoreValueSerializer {
            none_as_null: self.none_as_null,
        };
        match key.serialize(serializer)?.value.value_type {
            Some(value::ValueType::StringValue(str)) => {
                self.next_key = Some(str);
                Ok(())
            }
            Some(value::ValueType::IntegerValue(num)) => {
                self.next_key = Some(num.to_string());
                Ok(())
            }
            _ => Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message("Map key should be a string format"),
            )),
        }
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        match self.next_key.take() {
            Some(key) => {
                let serializer = FirestoreValueSerializer {
                    none_as_null: self.none_as_null,
                };
                let serialized_value = value.serialize(serializer)?.value;
                if serialized_value.value_type.is_some() {
                    self.fields.insert(key, serialized_value);
                }
                Ok(())
            }
            None => Err(FirestoreError::SerializeError(
                FirestoreSerializationError::from_message("Unexpected map value without key"),
            )),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue {
                        fields: self.fields,
                    },
                )),
            },
        ))
    }
}

impl serde::ser::SerializeStruct for SerializeMap {
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
            none_as_null: self.none_as_null,
        };
        let serialized_value = value.serialize(serializer)?.value;
        if serialized_value.value_type.is_some() {
            self.fields.insert(key.to_string(), serialized_value);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue {
                        fields: self.fields,
                    },
                )),
            },
        ))
    }
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
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
            none_as_null: self.none_as_null,
        };
        let serialized_value = value.serialize(serializer)?.value;
        if serialized_value.value_type.is_some() {
            self.fields.insert(key.to_string(), serialized_value);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut object = HashMap::new();
        object.insert(
            self.name,
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue {
                        fields: self.fields,
                    },
                )),
            },
        );

        Ok(FirestoreValue::from(
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue { fields: object },
                )),
            },
        ))
    }
}

pub fn firestore_document_from_serializable<S, T>(
    document_path: S,
    object: &T,
) -> Result<gcloud_sdk::google::firestore::v1::Document, FirestoreError>
where
    S: AsRef<str>,
    T: Serialize,
{
    let serializer = crate::firestore_serde::serializer::FirestoreValueSerializer {
        none_as_null: false,
    };
    let document_value = object.serialize(serializer)?;

    match document_value.value.value_type {
        Some(value::ValueType::MapValue(mv)) => Ok(gcloud_sdk::google::firestore::v1::Document {
            fields: mv.fields,
            name: document_path.as_ref().to_string(),
            ..Default::default()
        }),
        _ => Err(FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new("SystemError".into()),
            "Unable to create document from value. No object found".into(),
        ))),
    }
}

pub fn firestore_document_from_map<S, I, IS>(
    document_path: S,
    fields: I,
) -> Result<gcloud_sdk::google::firestore::v1::Document, FirestoreError>
where
    S: AsRef<str>,
    I: IntoIterator<Item = (IS, FirestoreValue)>,
    IS: AsRef<str>,
{
    let fields_map: HashMap<String, gcloud_sdk::google::firestore::v1::Value> = fields
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_string(), v.value))
        .collect();

    Ok(gcloud_sdk::google::firestore::v1::Document {
        fields: fields_map,
        name: document_path.as_ref().to_string(),
        ..Default::default()
    })
}
