use crate::errors::FirestoreSerializeError;
use crate::FirestoreError;
use gcloud_sdk::google::firestore::v1::value;
use serde::Serialize;
use std::collections::HashMap;

pub struct FirestoreValueSerializer;

pub struct SerializeVec {
    vec: Vec<gcloud_sdk::google::firestore::v1::Value>,
}

pub struct SerializeTupleVariant {
    name: String,
    vec: Vec<gcloud_sdk::google::firestore::v1::Value>,
}

pub struct SerializeMap {
    fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
    next_key: Option<String>,
}

pub struct SerializeStructVariant {
    name: String,
    fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value>,
}

impl serde::Serializer for FirestoreValueSerializer {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;
    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap;
    type SerializeStruct = SerializeMap;
    type SerializeStructVariant = SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::BooleanValue(v)),
        })
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v.into())),
        })
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::IntegerValue(v as i64)),
        })
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::DoubleValue(v.into())),
        })
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::DoubleValue(v.into())),
        })
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::StringValue(v.to_string())),
        })
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::StringValue(v.to_string())),
        })
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::BytesValue(v.into())),
        })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok { value_type: None })
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok { value_type: None })
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
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        let mut fields = HashMap::new();
        fields.insert(String::from(variant), value.serialize(self)?);
        Ok(Self::Ok {
            value_type: Some(value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue { fields },
            )),
        })
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeVec {
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
            name: String::from(variant),
            vec: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeMap {
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
            name: String::from(variant),
            fields: HashMap::with_capacity(len),
        })
    }
}

impl serde::ser::SerializeSeq for SerializeVec {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.vec.push(value.serialize(FirestoreValueSerializer {})?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::ArrayValue(
                gcloud_sdk::google::firestore::v1::ArrayValue { values: self.vec },
            )),
        })
    }
}

impl serde::ser::SerializeTuple for SerializeVec {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleStruct for SerializeVec {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.vec.push(value.serialize(FirestoreValueSerializer {})?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut fields = HashMap::new();
        fields.insert(
            self.name,
            Self::Ok {
                value_type: Some(value::ValueType::ArrayValue(
                    gcloud_sdk::google::firestore::v1::ArrayValue { values: self.vec },
                )),
            },
        );

        Ok(Self::Ok {
            value_type: Some(value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue { fields },
            )),
        })
    }
}

impl serde::ser::SerializeMap for SerializeMap {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let serializer = FirestoreValueSerializer {};
        match key.serialize(serializer)?.value_type {
            Some(value::ValueType::StringValue(str)) => {
                self.next_key = Some(str);
                Ok(())
            }
            Some(value::ValueType::IntegerValue(num)) => {
                self.next_key = Some(num.to_string());
                Ok(())
            }
            _ => Err(FirestoreSerializeError::from_message(
                "Map key should be a string format",
            )),
        }
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        match self.next_key.take() {
            Some(key) => {
                let serializer = FirestoreValueSerializer {};
                self.fields.insert(key, value.serialize(serializer)?);
                Ok(())
            }
            None => Err(FirestoreSerializeError::from_message(
                "Unexpected map value without key",
            )),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue {
                    fields: self.fields,
                },
            )),
        })
    }
}

impl serde::ser::SerializeStruct for SerializeMap {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let serializer = FirestoreValueSerializer {};
        self.fields
            .insert(key.to_string(), value.serialize(serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Self::Ok {
            value_type: Some(value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue {
                    fields: self.fields,
                },
            )),
        })
    }
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = gcloud_sdk::google::firestore::v1::Value;
    type Error = FirestoreError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let serializer = FirestoreValueSerializer {};
        self.fields
            .insert(key.to_string(), value.serialize(serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut object = HashMap::new();
        object.insert(
            self.name,
            Self::Ok {
                value_type: Some(value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue {
                        fields: self.fields,
                    },
                )),
            },
        );

        Ok(Self::Ok {
            value_type: Some(value::ValueType::MapValue(
                gcloud_sdk::google::firestore::v1::MapValue { fields: object },
            )),
        })
    }
}
