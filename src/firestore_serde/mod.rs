mod deserializer;
mod serializer;

mod types_serializers;
use crate::FirestoreValue;
use gcloud_sdk::google::firestore::v1::Value;
pub use types_serializers::*;

pub use deserializer::firestore_document_to_serializable;
pub use serializer::firestore_document_from_serializable;

impl<T> std::convert::From<T> for FirestoreValue
where
    T: serde::Serialize,
{
    fn from(value: T) -> Self {
        let serializer = crate::firestore_serde::serializer::FirestoreValueSerializer {};
        value
            .serialize(serializer)
            .unwrap_or_else(|_| FirestoreValue::from(Value { value_type: None }))
    }
}
