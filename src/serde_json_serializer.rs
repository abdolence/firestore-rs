use std::collections::HashMap;

use chrono::prelude::*;
use chrono::serde::ts_seconds;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};

use crate::errors::*;

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

pub fn firestore_document_from_serializable<T>(
    document_path: &str,
    object: &T,
) -> Result<Document, FirestoreError>
where
    T: Serialize,
{
    let serializer = crate::serde_native_serializer::FirestoreValueSerializer {};
    let document_value = object.serialize(serializer)?;

    match document_value.value.value_type {
        Some(value::ValueType::MapValue(mv)) => Ok(Document {
            fields: mv.fields.clone(),
            name: document_path.into(),
            create_time: None,
            update_time: None,
        }),
        _ => Err(FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new("SystemError".into()),
            "Unable to create document from value. No object found".into(),
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
