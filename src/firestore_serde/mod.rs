//! Serde bridge between Rust values and Firestore's `Value` protobuf representation.
//!
//! [`firestore_document_from_serializable`] and [`firestore_document_to_serializable`] convert a
//! whole struct to and from a Firestore document. Most callers only meet this module through the
//! `#[serde(with = "...")]` attributes it exports - [`serialize_as_timestamp`],
//! [`serialize_as_null`], [`serialize_as_reference`], and their variants - for the handful of
//! fields that need a Firestore-specific wire representation; everything else serializes through
//! the blanket [`From`] impl below with no attribute needed.

mod deserializer;
mod serializer;

mod timestamp_serializers;
pub use timestamp_serializers::*;

mod null_serializers;
pub use null_serializers::*;

mod latlng_serializers;
pub use latlng_serializers::*;

mod reference_serializers;
pub use reference_serializers::*;

mod vector_serializers;
pub use vector_serializers::*;

mod system_time_serializers;

use crate::FirestoreValue;
use gcloud_sdk::google::firestore::v1::Value;

pub use deserializer::firestore_document_to_serializable;
pub use serializer::firestore_document_from_map;
pub use serializer::firestore_document_from_serializable;

/// Converts any [`serde::Serialize`] value into a [`FirestoreValue`].
///
/// Serialization failure - which a well-behaved `Serialize` impl should never trigger - falls
/// back to an empty value rather than panicking, since `From` has no way to return a `Result`.
/// Prefer [`firestore_document_from_serializable`] for a top level struct, so a real failure
/// surfaces as an error instead of a silently empty document field.
///
/// ```rust
/// use firestore::FirestoreValue;
///
/// let fv_string: FirestoreValue = "hello".into();
/// let fv_int: FirestoreValue = 42.into();
/// let fv_bool: FirestoreValue = true.into();
/// ```
impl<T> std::convert::From<T> for FirestoreValue
where
    T: serde::Serialize,
{
    fn from(value: T) -> Self {
        let serializer = crate::firestore_serde::serializer::FirestoreValueSerializer::new();
        value
            .serialize(serializer)
            .unwrap_or_else(|_err| FirestoreValue::from(Value { value_type: None }))
    }
}
