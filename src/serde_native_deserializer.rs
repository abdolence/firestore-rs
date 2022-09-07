use crate::FirestoreValue;
use serde::de::Visitor;
use serde::Deserialize;
use std::fmt::Formatter;

impl<'de> Deserialize<'de> for FirestoreValue {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<FirestoreValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = gcloud_sdk::google::firestore::v1::Value;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("any valid value")
            }

            // #[inline]
            // fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
            //     Ok(Value::Bool(value))
            // }
            //
            // #[inline]
            // fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
            //     Ok(Value::Number(value.into()))
            // }
            //
            // #[inline]
            // fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
            //     Ok(Value::Number(value.into()))
            // }
            //
            // #[inline]
            // fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
            //     Ok(Number::from_f64(value).map_or(Value::Null, Value::Number))
            // }
            //
            // #[inline]
            // fn visit_str<E>(self, value: &str) -> Result<Value, E>
            //     where
            //         E: serde::de::Error,
            // {
            //     self.visit_string(String::from(value))
            // }
            //
            // #[inline]
            // fn visit_string<E>(self, value: String) -> Result<Value, E> {
            //     Ok(Value::String(value))
            // }
            //
            // #[inline]
            // fn visit_none<E>(self) -> Result<Value, E> {
            //     Ok(Value::Null)
            // }
            //
            // #[inline]
            // fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
            //     where
            //         D: serde::Deserializer<'de>,
            // {
            //     Deserialize::deserialize(deserializer)
            // }
            //
            // #[inline]
            // fn visit_unit<E>(self) -> Result<Value, E> {
            //     Ok(Value::Null)
            // }
            //
            // #[inline]
            // fn visit_seq<V>(self, mut visitor: V) -> Result<Value, V::Error>
            //     where
            //         V: SeqAccess<'de>,
            // {
            //     let mut vec = Vec::new();
            //
            //     while let Some(elem) = tri!(visitor.next_element()) {
            //         vec.push(elem);
            //     }
            //
            //     Ok(Value::Array(vec))
            // }
            //
            // fn visit_map<V>(self, mut visitor: V) -> Result<Value, V::Error>
            //     where
            //         V: MapAccess<'de>,
            // {
            //     match visitor.next_key_seed(KeyClassifier)? {
            //         Some(KeyClass::Map(first_key)) => {
            //             let mut values = Map::new();
            //
            //             values.insert(first_key, tri!(visitor.next_value()));
            //             while let Some((key, value)) = tri!(visitor.next_entry()) {
            //                 values.insert(key, value);
            //             }
            //
            //             Ok(Value::Object(values))
            //         }
            //         None => Ok(Value::Object(Map::new())),
            //     }
            // }
        }

        Ok(FirestoreValue::from(
            deserializer.deserialize_any(ValueVisitor)?,
        ))
    }
}
