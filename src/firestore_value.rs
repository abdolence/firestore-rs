use gcloud_sdk::google::firestore::v1::Value;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreValue {
    pub value: Value,
}

impl FirestoreValue {
    pub fn from(value: Value) -> Self {
        Self { value }
    }

    pub fn from_map<I, IS>(fields: I) -> Self
    where
        I: IntoIterator<Item = (IS, FirestoreValue)>,
        IS: AsRef<str>,
    {
        let fields: HashMap<String, Value> = fields
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.value))
            .collect();
        Self::from(Value {
            value_type: Some(
                gcloud_sdk::google::firestore::v1::value::ValueType::MapValue(
                    gcloud_sdk::google::firestore::v1::MapValue { fields },
                ),
            ),
        })
    }
}
