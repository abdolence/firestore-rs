use crate::errors::FirestoreSerializationError;
use crate::{FirestoreError, FirestoreValue};
use chrono::{DateTime, Utc};
use gcloud_sdk::google::firestore::v1::value;
use serde::de::{DeserializeSeed, Visitor};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Formatter;

impl<'de> Deserialize<'de> for FirestoreValue {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<FirestoreValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = FirestoreValue;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("any valid value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::BooleanValue(value)),
                    },
                ))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::IntegerValue(value)),
                    },
                ))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::IntegerValue(value as i64)),
                    },
                ))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::DoubleValue(value)),
                    },
                ))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::StringValue(value)),
                    },
                ))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value { value_type: None },
                ))
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value { value_type: None },
                ))
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::SeqAccess<'de>,
            {
                let mut vec: Vec<gcloud_sdk::google::firestore::v1::Value> = Vec::new();

                while let Some(elem) = visitor.next_element::<Self::Value>()? {
                    vec.push(elem.value);
                }

                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::ArrayValue(
                            gcloud_sdk::google::firestore::v1::ArrayValue { values: vec },
                        )),
                    },
                ))
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut values = HashMap::new();

                while let Some((key, value)) = visitor.next_entry::<String, Self::Value>()? {
                    values.insert(key, value.value);
                }

                Ok(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::MapValue(
                            gcloud_sdk::google::firestore::v1::MapValue { fields: values },
                        )),
                    },
                ))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

struct FirestoreValueSeqAccess {
    iter: std::vec::IntoIter<FirestoreValue>,
}

impl FirestoreValueSeqAccess {
    fn new(vec: Vec<gcloud_sdk::google::firestore::v1::Value>) -> Self {
        FirestoreValueSeqAccess {
            iter: vec
                .into_iter()
                .map(FirestoreValue::from)
                .collect::<Vec<FirestoreValue>>()
                .into_iter(),
        }
    }
}

impl<'de> serde::de::SeqAccess<'de> for FirestoreValueSeqAccess {
    type Error = FirestoreError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(value) => seed.deserialize(value).map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.iter.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

struct FirestoreValueMapAccess {
    iter: <HashMap<String, FirestoreValue> as IntoIterator>::IntoIter,
    value: Option<FirestoreValue>,
}

impl FirestoreValueMapAccess {
    fn new(map: HashMap<String, gcloud_sdk::google::firestore::v1::Value>) -> Self {
        FirestoreValueMapAccess {
            iter: map
                .into_iter()
                .map(|(k, v)| (k, FirestoreValue::from(v)))
                .collect::<HashMap<String, FirestoreValue>>()
                .into_iter(),
            value: None,
        }
    }
}

impl<'de> serde::de::MapAccess<'de> for FirestoreValueMapAccess {
    type Error = FirestoreError;

    fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key, value)) => {
                self.value = Some(value);
                seed.deserialize(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::StringValue(key)),
                    },
                ))
                .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.value.take() {
            Some(value) => seed.deserialize(value),
            None => Err(serde::de::Error::custom("value is missing")),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.iter.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

impl<'de> serde::Deserializer<'de> for FirestoreValue {
    type Error = FirestoreError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.value_type {
            Some(value::ValueType::NullValue(_)) => visitor.visit_unit(),
            Some(value::ValueType::BooleanValue(v)) => visitor.visit_bool(v),
            Some(value::ValueType::IntegerValue(v)) => visitor.visit_i64(v),
            Some(value::ValueType::StringValue(v)) => visitor.visit_string(v),
            Some(value::ValueType::ArrayValue(v)) => {
                visitor.visit_seq(FirestoreValueSeqAccess::new(v.values))
            }
            Some(value::ValueType::MapValue(v)) => {
                visitor.visit_map(FirestoreValueMapAccess::new(v.fields))
            }
            Some(value::ValueType::DoubleValue(v)) => visitor.visit_f64(v),
            Some(value::ValueType::BytesValue(ref v)) => visitor.visit_bytes(v),
            Some(value::ValueType::ReferenceValue(v)) => visitor.visit_string(v),
            Some(value::ValueType::GeoPointValue(_)) => Err(FirestoreError::DeserializeError(
                FirestoreSerializationError::from_message("LatLng not supported yet"),
            )),
            Some(value::ValueType::TimestampValue(ts)) => {
                let dt = DateTime::<Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp(ts.seconds, ts.nanos as u32),
                    Utc,
                );
                visitor.visit_string(dt.to_rfc3339())
            }
            None => visitor.visit_unit(),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.value_type {
            Some(value::ValueType::NullValue(_)) => visitor.visit_none(),
            None => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

pub fn firestore_document_to_serializable<T>(
    document: &gcloud_sdk::google::firestore::v1::Document,
) -> Result<T, FirestoreError>
where
    for<'de> T: Deserialize<'de>,
{
    let mut fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value> =
        HashMap::with_capacity(document.fields.len());

    for (k, v) in document.fields.iter() {
        fields.insert(k.to_owned(), v.to_owned());
    }

    let firestore_value = FirestoreValue::from(gcloud_sdk::google::firestore::v1::Value {
        value_type: Some(value::ValueType::MapValue(
            gcloud_sdk::google::firestore::v1::MapValue { fields },
        )),
    });

    T::deserialize(firestore_value)
}
