use gcloud_sdk::google::firestore::v1::Value;
use std::collections::HashMap;

/// Represents a Firestore value, wrapping the underlying gRPC `Value` type.
///
/// This struct provides a convenient way to work with Firestore's native data types
/// and is used extensively throughout the crate, especially in serialization and
/// deserialization, query filters, and field transformations.
///
/// It can represent various types such as null, boolean, integer, double, timestamp,
/// string, bytes, geo point, array, and map.
///
/// Conversions from common Rust types to `FirestoreValue` are typically handled by
/// the `From` trait implementations in the `firestore_serde` module (though not directly
/// visible in this file, they are a core part of how `FirestoreValue` is used).
///
/// # Examples
///
/// ```rust
/// use firestore::FirestoreValue;
///
/// // Or, for direct construction of a map value:
/// let fv_map = FirestoreValue::from_map(vec![
///     ("name", "Alice".into()), // .into() relies on From<T> for FirestoreValue
///     ("age", 30.into()),
/// ]);
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreValue {
    /// The underlying gRPC `Value` protobuf message.
    pub value: Value,
}

impl FirestoreValue {
    /// Creates a `FirestoreValue` directly from a `gcloud_sdk::google::firestore::v1::Value`.
    pub fn from(value: Value) -> Self {
        Self { value }
    }

    /// Creates a `FirestoreValue` representing a Firestore map from an iterator of key-value pairs.
    ///
    /// # Type Parameters
    /// * `I`: An iterator type yielding pairs of field names and their `FirestoreValue`s.
    /// * `IS`: A type that can be converted into a string for field names.
    ///
    /// # Arguments
    /// * `fields`: An iterator providing the map's fields.
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
