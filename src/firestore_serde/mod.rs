//! Provides custom Serde serializers and deserializers for converting between
//! Rust types and Firestore's native data representation.
//!
//! This module is central to enabling idiomatic Rust struct usage with Firestore.
//! It handles the mapping of Rust types (like `String`, `i64`, `bool`, `Vec`, `HashMap`,
//! and custom structs) to Firestore's `Value` protobuf message, and vice-versa.
//!
//! Key components:
//! - [`FirestoreValueSerializer`](serializer::FirestoreValueSerializer): Implements `serde::Serializer`
//!   to convert Rust types into [`FirestoreValue`](crate::FirestoreValue).
//! - [`FirestoreValueDeserializer`](deserializer::FirestoreValueDeserializer): Implements `serde::Deserializer`
//!   to convert [`FirestoreValue`](crate::FirestoreValue) back into Rust types.
//! - Helper modules (e.g., `timestamp_serializers`, `latlng_serializers`) provide
//!   `#[serde(with = "...")]` compatible functions for specific Firestore types like
//!   Timestamps and GeoPoints.
//!
//! The primary public functions re-exported here are:
//! - [`firestore_document_to_serializable`]: Deserializes a Firestore document into a Rust struct.
//! - [`firestore_document_from_serializable`]: Serializes a Rust struct into a Firestore document.
//! - [`firestore_document_from_map`]: Creates a Firestore document from a map of field names to `FirestoreValue`s.
//!
//! Additionally, a generic `From<T> for FirestoreValue where T: serde::Serialize`
//! implementation is provided, allowing easy conversion of any serializable Rust type
//! into a `FirestoreValue`.

mod deserializer;
mod serializer;

/// Provides `#[serde(with = "...")]` serializers and deserializers for Firestore Timestamps
/// (converting between `chrono::DateTime<Utc>` and `google::protobuf::Timestamp`).
mod timestamp_serializers;
pub use timestamp_serializers::*;

/// Provides `#[serde(with = "...")]` serializers and deserializers for Firestore Null values,
/// particularly for handling `Option<T>` where `None` maps to a NullValue.
mod null_serializers;
pub use null_serializers::*;

/// Provides `#[serde(with = "...")]` serializers and deserializers for Firestore GeoPoint values
/// (converting between a suitable Rust type like a struct with `latitude` and `longitude`
/// fields and `google::type::LatLng`).
mod latlng_serializers;
pub use latlng_serializers::*;

/// Provides `#[serde(with = "...")]` serializers and deserializers for Firestore DocumentReference values
/// (converting between `String` or a custom `FirestoreReference` type and Firestore's reference format).
mod reference_serializers;
pub use reference_serializers::*;

/// Provides `#[serde(with = "...")]` serializers and deserializers for Firestore Vector values.
mod vector_serializers;
pub use vector_serializers::*;

use crate::FirestoreValue;
use gcloud_sdk::google::firestore::v1::Value;

pub use deserializer::firestore_document_to_serializable;
pub use serializer::firestore_document_from_map;
pub use serializer::firestore_document_from_serializable;

/// Generic conversion from any `serde::Serialize` type into a [`FirestoreValue`].
///
/// This allows for convenient creation of `FirestoreValue` instances from various Rust types
/// using `.into()`. If serialization fails (which is rare for well-behaved `Serialize`
/// implementations), it defaults to a `FirestoreValue` representing a "null" or empty value.
///
/// # Examples
/// ```rust
/// use firestore::FirestoreValue;
///
/// let fv_string: FirestoreValue = "hello".into();
/// let fv_int: FirestoreValue = 42.into();
/// let fv_bool: FirestoreValue = true.into();
///
/// // Assuming MyStruct implements serde::Serialize
/// // struct MyStruct { field: String }
/// // let my_struct = MyStruct { field: "test".to_string() };
/// // let fv_struct: FirestoreValue = my_struct.into();
/// ```
impl<T> std::convert::From<T> for FirestoreValue
where
    T: serde::Serialize,
{
    fn from(value: T) -> Self {
        let serializer = crate::firestore_serde::serializer::FirestoreValueSerializer::new();
        value.serialize(serializer).unwrap_or_else(|err| {
            // It's generally better to panic or return a Result here if serialization
            // is critical and failure indicates a programming error.
            // However, matching existing behavior of defaulting to None/Null.
            // Consider logging the error: eprintln!("Failed to serialize to FirestoreValue: {}", err);
            FirestoreValue::from(Value { value_type: None })
        })
    }
}
