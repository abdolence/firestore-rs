use gcloud_sdk::google::firestore::v1::Value;

#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreValue {
    pub value: Value,
}

impl FirestoreValue {
    pub fn from(value: Value) -> Self {
        Self { value }
    }
}
