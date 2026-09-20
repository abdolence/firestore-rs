//! The `#[serde(with = "firestore::serialize_as_null")]` attribute for writing an explicit
//! Firestore null instead of the default of omitting a `None` field.

pub(crate) const FIRESTORE_NULL_TYPE_TAG_TYPE: &str = "FirestoreNull";

/// Stores an `Option<T>` field as `T`'s ordinary Firestore value when present, or an explicit
/// Firestore null when `None`.
///
/// By default a `None` field is left out of the document entirely; use this attribute wherever a
/// document must instead carry a visible null, for example to clear a field a reader treats as
/// present-but-empty. Pair it with `#[serde(default)]` so a document that omits the field still
/// deserializes to `None`. [`serialize_as_null_timestamp`](crate::serialize_as_null_timestamp)
/// is the same idea specialised for `FirestoreInstant`.
///
/// ```rust
/// use firestore::firestore_document_from_serializable;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Profile {
///     #[serde(default)]
///     #[serde(with = "firestore::serialize_as_null")]
///     nickname: Option<String>,
/// }
///
/// let profile = Profile { nickname: None };
/// let document = firestore_document_from_serializable("profiles/p1", &profile).unwrap();
///
/// assert!(matches!(
///     document.fields["nickname"].value_type,
///     Some(gcloud_sdk::google::firestore::v1::value::ValueType::NullValue(_))
/// ));
/// ```
pub mod serialize_as_null {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes `date` as its ordinary Firestore value when `Some`, or an explicit null.
    pub fn serialize<S, T>(date: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        serializer
            .serialize_newtype_struct(crate::firestore_serde::FIRESTORE_NULL_TYPE_TAG_TYPE, &date)
    }

    /// Deserializes an ordinary value, a null value, or an absent field.
    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: for<'tde> Deserialize<'tde>,
    {
        Option::<T>::deserialize(deserializer)
    }
}
