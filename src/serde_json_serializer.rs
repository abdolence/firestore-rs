use std::collections::HashMap;

use chrono::prelude::*;
use chrono::serde::ts_seconds;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};

use crate::errors::*;
use crate::FirestoreQueryValue;

fn firestore_value_to_serde_json_value(v: &Value) -> serde_json::Value {
    match v.value_type.as_ref() {
        Some(value::ValueType::StringValue(sv)) => serde_json::Value::String(sv.into()),
        Some(value::ValueType::IntegerValue(iv)) => serde_json::Value::Number((*iv).into()),
        Some(value::ValueType::DoubleValue(dv)) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*dv).unwrap())
        }
        Some(value::ValueType::BooleanValue(bv)) => serde_json::Value::Bool(*bv),
        Some(value::ValueType::TimestampValue(ts)) => {
            let dt = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(ts.seconds, ts.nanos as u32),
                Utc,
            );

            #[derive(Serialize)]
            struct DtWrapper(#[serde(with = "ts_seconds")] DateTime<Utc>);

            serde_json::Value::String(serde_json::to_string(&DtWrapper(dt)).unwrap())
        }
        Some(value::ValueType::ArrayValue(av)) => {
            let serde_value_array: Vec<serde_json::Value> = av
                .values
                .iter()
                .map(firestore_value_to_serde_json_value)
                .collect();
            serde_json::Value::Array(serde_value_array)
        }
        Some(value::ValueType::MapValue(mv)) => {
            let mut serde_map = serde_json::Map::new();

            for (k, v) in mv.fields.iter() {
                serde_map.insert(k.clone(), firestore_value_to_serde_json_value(v));
            }
            serde_json::Value::Object(serde_map)
        }
        _ => serde_json::Value::Null,
    }
}

fn serde_json_value_to_firestore_value(v: &serde_json::Value) -> Value {
    let value_type = match v {
        serde_json::Value::String(str) => Some(value::ValueType::StringValue(str.clone())),
        serde_json::Value::Number(numb) if numb.is_i64() => {
            Some(value::ValueType::IntegerValue(numb.as_i64().unwrap()))
        }
        serde_json::Value::Number(numb) if numb.is_u64() => {
            Some(value::ValueType::IntegerValue(numb.as_u64().unwrap() as i64))
        }
        serde_json::Value::Number(numb) if numb.is_f64() => {
            Some(value::ValueType::DoubleValue(numb.as_f64().unwrap()))
        }
        serde_json::Value::Bool(bv) => Some(value::ValueType::BooleanValue(*bv)),
        serde_json::Value::Array(av) => Some(value::ValueType::ArrayValue(ArrayValue {
            values: av.iter().map(serde_json_value_to_firestore_value).collect(),
        })),
        serde_json::Value::Object(mv) => Some(value::ValueType::MapValue(MapValue {
            fields: mv
                .iter()
                .map(|(k, v)| (k.clone(), serde_json_value_to_firestore_value(v)))
                .collect(),
        })),
        _ => None,
    };

    Value { value_type }
}

pub fn firestore_value_from_serializable<T>(object: &T) -> Result<Value, FirestoreError>
where
    T: Serialize,
{
    let serde_object_value = serde_json::to_value(object)?;
    Ok(serde_json_value_to_firestore_value(&serde_object_value))
}

pub fn firestore_document_from_serializable<T>(
    document_path: &str,
    object: &T,
) -> Result<Document, FirestoreError>
where
    T: Serialize,
{
    let serde_object_value = serde_json::to_value(object)?;
    match serde_json_value_to_firestore_value(&serde_object_value)
        .value_type
        .as_ref()
    {
        Some(value::ValueType::MapValue(mv)) => Ok(Document {
            fields: mv.fields.clone(),
            name: document_path.into(),
            create_time: None,
            update_time: None,
        }),
        _ => Err(FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new("SystemError".into()),
            "Unable to create document from serde value. No object found".into(),
        ))),
    }
}

pub fn firestore_document_to_serializable<T>(document: &Document) -> Result<T, FirestoreError>
where
    for<'de> T: Deserialize<'de>,
{
    #[derive(Serialize, Deserialize)]
    struct DeserializeWrapper {
        #[serde(flatten)]
        fields: HashMap<String, serde_json::Value>,
    }
    let wrapper = DeserializeWrapper {
        fields: document
            .fields
            .iter()
            .map(|(k, v)| (k.to_owned(), firestore_value_to_serde_json_value(v)))
            .collect(),
    };
    let serde_value = serde_json::to_value(wrapper)?;
    let result_object: T = serde_json::from_value(serde_value)?;
    Ok(result_object)
}

impl<T> std::convert::From<T> for FirestoreQueryValue
where
    T: Serialize,
{
    fn from(value: T) -> Self {
        FirestoreQueryValue {
            value: firestore_value_from_serializable(&value).unwrap_or(Value { value_type: None }),
        }
    }
}
