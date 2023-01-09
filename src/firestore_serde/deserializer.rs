use crate::errors::FirestoreSerializationError;
use crate::timestamp_utils::from_timestamp;
use crate::{FirestoreError, FirestoreValue};
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

#[derive(Debug, PartialEq, Clone)]
struct FirestoreVariantAccess {
    de: FirestoreValue,
}

impl FirestoreVariantAccess {
    fn new(de: FirestoreValue) -> Self {
        Self { de }
    }
}

impl<'de> serde::de::EnumAccess<'de> for FirestoreVariantAccess {
    type Error = FirestoreError;
    type Variant = FirestoreValue;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.de.value.value_type.clone() {
            Some(value::ValueType::MapValue(v)) => {
                if let Some((k, v)) = v.fields.iter().next() {
                    let variant = seed.deserialize(FirestoreValue::from(
                        gcloud_sdk::google::firestore::v1::Value {
                            value_type: Some(value::ValueType::StringValue(k.clone())),
                        },
                    ))?;
                    Ok((variant, FirestoreValue::from(v.clone())))
                } else {
                    Err(FirestoreError::DeserializeError(
                        FirestoreSerializationError::from_message(format!(
                            "Unexpected enum empty map type: {:?}",
                            self.de.value.value_type
                        )),
                    ))
                }
            }
            Some(value::ValueType::StringValue(v)) => {
                let variant = seed.deserialize(FirestoreValue::from(
                    gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(value::ValueType::StringValue(v)),
                    },
                ))?;
                Ok((variant, self.de))
            }
            _ => Err(FirestoreError::DeserializeError(
                FirestoreSerializationError::from_message(format!(
                    "Unexpected enum type: {:?}",
                    self.de.value.value_type
                )),
            )),
        }
    }
}

impl<'de> serde::de::VariantAccess<'de> for FirestoreValue {
    type Error = FirestoreError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value.value_type {
            Some(value::ValueType::ArrayValue(v)) => {
                visitor.visit_seq(FirestoreValueSeqAccess::new(v.values))
            }
            _ => Err(FirestoreError::DeserializeError(
                FirestoreSerializationError::from_message(
                    "Unexpected tuple_variant for variant access",
                ),
            )),
        }
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(FirestoreError::DeserializeError(
            FirestoreSerializationError::from_message(
                "Unexpected struct_variant for variant access",
            ),
        ))
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
            Some(value::ValueType::GeoPointValue(v)) => {
                let lat_lng_fields: HashMap<String, gcloud_sdk::google::firestore::v1::Value> =
                    vec![
                        ("latitude".to_string(), gcloud_sdk::google::firestore::v1::Value {
                            value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(v.latitude))
                        }),
                        ("longitude".to_string(),
                         gcloud_sdk::google::firestore::v1::Value {
                             value_type: Some(gcloud_sdk::google::firestore::v1::value::ValueType::DoubleValue(v.longitude))
                         }),
                    ]
                    .into_iter()
                    .collect();
                visitor.visit_map(FirestoreValueMapAccess::new(lat_lng_fields))
            }
            Some(value::ValueType::TimestampValue(ts)) => {
                visitor.visit_string(from_timestamp(ts)?.to_rfc3339())
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
        visitor.visit_unit()
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
        visitor.visit_enum(FirestoreVariantAccess::new(self))
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

    let doc_name = document.name.clone();

    let doc_id = doc_name
        .split('/')
        .last()
        .map(|s| s.to_string())
        .unwrap_or_else(|| doc_name.clone());

    fields.insert(
        "_firestore_id".to_string(),
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(value::ValueType::StringValue(doc_id)),
        },
    );

    fields.insert(
        "_firestore_full_id".to_string(),
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(value::ValueType::StringValue(doc_name)),
        },
    );

    if let Some(created_time) = &document.create_time {
        fields.insert(
            "_firestore_created".to_string(),
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::TimestampValue(created_time.clone())),
            },
        );
    }

    if let Some(updated_time) = &document.update_time {
        fields.insert(
            "_firestore_updated".to_string(),
            gcloud_sdk::google::firestore::v1::Value {
                value_type: Some(value::ValueType::TimestampValue(updated_time.clone())),
            },
        );
    }

    let firestore_value = FirestoreValue::from(gcloud_sdk::google::firestore::v1::Value {
        value_type: Some(value::ValueType::MapValue(
            gcloud_sdk::google::firestore::v1::MapValue { fields },
        )),
    });

    T::deserialize(firestore_value)
}
