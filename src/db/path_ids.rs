//! Validated newtypes for Firestore document and collection IDs.
//!
//! See [`FirestoreDocumentId`] for the full rationale; [`FirestoreCollectionId`] is the same
//! shape applied to collection names. This module is private — both types are re-exported at the
//! crate root.

use crate::errors::{
    FirestoreError, FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails,
};
use crate::FirestoreResult;
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Which Firestore path segment [`validate_path_segment`] is checking.
///
/// Firestore's documented limits (<https://firebase.google.com/docs/firestore/quotas>) are
/// identical for document IDs and collection IDs; this only selects the `field` name reported in
/// the resulting error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum FirestorePathSegmentKind {
    DocumentId,
    CollectionId,
}

impl FirestorePathSegmentKind {
    const fn field_name(self) -> &'static str {
        match self {
            FirestorePathSegmentKind::DocumentId => "document_id",
            FirestorePathSegmentKind::CollectionId => "collection_id",
        }
    }
}

fn path_segment_error(field: &str, error: String) -> FirestoreError {
    FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
        FirestoreInvalidParametersPublicDetails::new(field.to_string(), error),
    ))
}

/// Validates a single Firestore path segment (a document ID or a collection ID).
///
/// UTF-8 validity needs no check of its own — `segment: &str` is UTF-8 by construction. The
/// reserved `__*__` namespace is deliberately not checked; see the module docs.
pub(crate) fn validate_path_segment(
    segment: &str,
    kind: FirestorePathSegmentKind,
) -> FirestoreResult<()> {
    let field = kind.field_name();

    if segment.is_empty() {
        return Err(path_segment_error(field, "must not be empty".to_string()));
    }

    // This runs before the '/' and '.'/'..' checks below so that every value reaching those
    // messages is already bounded to 1500 bytes; only this message must avoid echoing the value,
    // since it is the one case where the value itself can be unbounded.
    if segment.len() > 1500 {
        return Err(path_segment_error(
            field,
            format!("must be at most 1500 bytes, was {} bytes", segment.len()),
        ));
    }

    if segment.contains('/') {
        return Err(path_segment_error(
            field,
            format!(
                "must not contain '/': \"{}\" - a slash-delimited path such as \"users/123/posts\" is not a single ID; \
                 build it with `.parent(db.parent_path(\"users\", \"123\")?)` and pass \"posts\" as the {field}",
                segment.escape_debug(),
            ),
        ));
    }

    if segment == "." || segment == ".." {
        return Err(path_segment_error(
            field,
            format!("must not be \".\" or \"..\", got \"{segment}\""),
        ));
    }

    Ok(())
}

/// A Firestore document ID that has been checked against Firestore's ID rules.
///
/// Construct one with [`FirestoreDocumentId::new`] at the point an ID enters your system, then
/// pass it (by value or by reference) anywhere this crate accepts a document ID, or store it in
/// your own structs.
///
/// ```rust
/// use firestore::FirestoreDocumentId;
///
/// let id = FirestoreDocumentId::new("user-42").expect("valid document id");
/// assert_eq!(id.as_str(), "user-42");
///
/// let err = FirestoreDocumentId::new("a/b").unwrap_err();
/// assert!(err.to_string().contains("document_id"));
///
/// let err = FirestoreDocumentId::new("").unwrap_err();
/// assert!(err.to_string().contains("document_id"));
/// ```
///
/// It implements `AsRef<str>` (and `&FirestoreDocumentId` does too, via the standard library's
/// blanket impl), so it drops into every document-ID parameter in this crate's fluent API without
/// a signature change:
///
/// ```rust,no_run
/// use firestore::{FirestoreDb, FirestoreDocumentId, FirestoreResult};
///
/// # async fn run() -> FirestoreResult<()> {
/// let db = FirestoreDb::new("my-project").await?;
/// let id = FirestoreDocumentId::new("user-42")?;
///
/// let doc = db.fluent().select().by_id_in("users").one(&id).await?;
/// # let _ = doc;
/// # Ok(())
/// # }
/// ```
///
/// A field typed as [`FirestoreDocumentId`] carries the same proof, and validates the same way
/// when deserialized — the usual arrival point for an untrusted ID:
///
/// ```rust
/// use firestore::FirestoreDocumentId;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Deserialize, Serialize)]
/// struct CreateSession {
///     user_id: FirestoreDocumentId,
/// }
///
/// let session = CreateSession {
///     user_id: FirestoreDocumentId::new("user-42")?,
/// };
/// assert_eq!(session.user_id.as_str(), "user-42");
/// # Ok::<(), firestore::errors::FirestoreError>(())
/// ```
///
/// This departs from [`FirestoreReference`](crate::FirestoreReference), which derives
/// `Deserialize` unconditionally and only validates lazily, on `parse()`. If a field must
/// round-trip an ID without validating it (for example, one already known to be safe, or logged
/// verbatim for diagnostics), keep it typed as `String` — that is the documented escape hatch,
/// not a gap in this type.
///
/// Neither this type nor [`FirestoreCollectionId`] implements [`rvstruct::ValueStruct`]. That
/// trait requires an `into_value(self) -> String` alongside `value()`, and a consuming conversion
/// back to a bare `String` is exactly what these types exist to discourage: it launders a
/// validated ID back into a value that can be mutated or concatenated while still looking like it
/// came from a checked source. For the same reason there is no `into_string()` and no
/// `From<FirestoreDocumentId> for String` — only the read-only [`value()`](Self::value) and
/// [`as_str()`](Self::as_str) accessors. An owned `String` is still reachable, deliberately
/// explicitly, via `id.as_str().to_string()`:
///
/// ```compile_fail
/// use firestore::FirestoreDocumentId;
/// let id = FirestoreDocumentId::new("user-42").unwrap();
/// let _owned: String = id.into();
/// ```
///
/// Firestore also exposes Datastore-mode entities with integer IDs as documents literally named
/// `__id7__`, and such documents are read today through the same path-building code these types
/// back. Validation here therefore does not reject the reserved `__*__` namespace: doing so would
/// turn an existing, working read into a client-side error for a case the server only rejects on
/// write. `"__id7__"` and `"__name__"` are accepted by [`FirestoreDocumentId::new`] and
/// [`FirestoreCollectionId::new`] on purpose.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct FirestoreDocumentId(String);

impl FirestoreDocumentId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if `id` is empty, longer than 1500
    /// bytes, contains `/`, or is exactly `"."` or `".."`.
    pub fn new<S: Into<String>>(id: S) -> FirestoreResult<Self> {
        let id = id.into();
        validate_path_segment(&id, FirestorePathSegmentKind::DocumentId)?;
        Ok(Self(id))
    }

    /// Validates `id` without allocating or constructing a value.
    ///
    /// Useful to check a borrowed `&str` before deciding whether to take ownership of it.
    ///
    /// # Errors
    /// Same conditions as [`FirestoreDocumentId::new`].
    pub fn validate_str(id: &str) -> FirestoreResult<()> {
        validate_path_segment(id, FirestorePathSegmentKind::DocumentId)
    }

    /// Re-validates an already-constructed ID.
    ///
    /// A valid [`FirestoreDocumentId`] cannot become invalid after construction, so this always
    /// succeeds; it exists to mirror [`FirestoreListenerTarget::validate`](crate::FirestoreListenerTarget::validate)
    /// for callers that validate every ID-like value the same way.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<()>` for symmetry with [`FirestoreDocumentId::validate_str`].
    pub fn validate(&self) -> FirestoreResult<()> {
        validate_path_segment(&self.0, FirestorePathSegmentKind::DocumentId)
    }

    /// Returns the wrapped ID.
    ///
    /// There is no consuming equivalent that returns an owned `String` (see the
    /// [`FirestoreDocumentId`] type docs for why). Use `id.as_str().to_string()` when an owned
    /// copy is genuinely needed.
    pub fn value(&self) -> &String {
        &self.0
    }

    /// Returns the wrapped ID as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for FirestoreDocumentId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for FirestoreDocumentId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for FirestoreDocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TryFrom<&str> for FirestoreDocumentId {
    type Error = FirestoreError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for FirestoreDocumentId {
    type Error = FirestoreError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for FirestoreDocumentId {
    type Err = FirestoreError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl PartialEq<str> for FirestoreDocumentId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<FirestoreDocumentId> for str {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        self == other.0
    }
}

impl PartialEq<&str> for FirestoreDocumentId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<FirestoreDocumentId> for &str {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        *self == other.0
    }
}

impl PartialEq<String> for FirestoreDocumentId {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl PartialEq<FirestoreDocumentId> for String {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        self == &other.0
    }
}

impl Serialize for FirestoreDocumentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

/// A Firestore collection ID that has been checked against Firestore's ID rules.
///
/// Construct one with [`FirestoreCollectionId::new`] at the point a collection name enters your
/// system — the natural case is a collection chosen at runtime, where an unchecked `/` would
/// silently re-target the operation at a different collection. It has the same API shape, and the
/// same design rationale (no consuming unwrap, no reserved-namespace check, the `String` escape
/// hatch), as [`FirestoreDocumentId`]; see that type's docs for the full explanation.
///
/// ```rust
/// use firestore::FirestoreCollectionId;
///
/// let id = FirestoreCollectionId::new("users").expect("valid collection id");
/// assert_eq!(id.as_str(), "users");
///
/// let err = FirestoreCollectionId::new("a/b").unwrap_err();
/// assert!(err.to_string().contains("collection_id"));
/// assert!(err.to_string().contains(".parent("));
///
/// let err = FirestoreCollectionId::new("").unwrap_err();
/// assert!(err.to_string().contains("collection_id"));
/// ```
///
/// Drop-in by reference into the fluent API, exactly like [`FirestoreDocumentId`]:
///
/// ```rust,no_run
/// use firestore::{FirestoreCollectionId, FirestoreDb, FirestoreResult};
///
/// # async fn run() -> FirestoreResult<()> {
/// let db = FirestoreDb::new("my-project").await?;
/// let collection = FirestoreCollectionId::new("users")?;
///
/// let documents = db.fluent().list().from(&collection).stream_all().await?;
/// # let _ = documents;
/// # Ok(())
/// # }
/// ```
///
/// Stored in a caller's own struct:
///
/// ```rust
/// use firestore::FirestoreCollectionId;
///
/// struct DynamicCollectionRequest {
///     collection: FirestoreCollectionId,
/// }
///
/// let request = DynamicCollectionRequest {
///     collection: FirestoreCollectionId::new("users")?,
/// };
/// assert_eq!(request.collection.as_str(), "users");
/// # Ok::<(), firestore::errors::FirestoreError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct FirestoreCollectionId(String);

impl FirestoreCollectionId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if `id` is empty, longer than 1500
    /// bytes, contains `/`, or is exactly `"."` or `".."`.
    pub fn new<S: Into<String>>(id: S) -> FirestoreResult<Self> {
        let id = id.into();
        validate_path_segment(&id, FirestorePathSegmentKind::CollectionId)?;
        Ok(Self(id))
    }

    /// Validates `id` without allocating or constructing a value.
    ///
    /// # Errors
    /// Same conditions as [`FirestoreCollectionId::new`].
    pub fn validate_str(id: &str) -> FirestoreResult<()> {
        validate_path_segment(id, FirestorePathSegmentKind::CollectionId)
    }

    /// Re-validates an already-constructed ID.
    ///
    /// A valid [`FirestoreCollectionId`] cannot become invalid after construction, so this always
    /// succeeds; it exists to mirror [`FirestoreListenerTarget::validate`](crate::FirestoreListenerTarget::validate)
    /// for callers that validate every ID-like value the same way.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<()>` for symmetry with [`FirestoreCollectionId::validate_str`].
    pub fn validate(&self) -> FirestoreResult<()> {
        validate_path_segment(&self.0, FirestorePathSegmentKind::CollectionId)
    }

    /// Returns the wrapped ID.
    ///
    /// There is no consuming equivalent that returns an owned `String` (see the
    /// [`FirestoreDocumentId`] type docs for why). Use `id.as_str().to_string()` when an owned
    /// copy is genuinely needed.
    pub fn value(&self) -> &String {
        &self.0
    }

    /// Returns the wrapped ID as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for FirestoreCollectionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for FirestoreCollectionId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for FirestoreCollectionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TryFrom<&str> for FirestoreCollectionId {
    type Error = FirestoreError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for FirestoreCollectionId {
    type Error = FirestoreError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for FirestoreCollectionId {
    type Err = FirestoreError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl PartialEq<str> for FirestoreCollectionId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<FirestoreCollectionId> for str {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        self == other.0
    }
}

impl PartialEq<&str> for FirestoreCollectionId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<FirestoreCollectionId> for &str {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        *self == other.0
    }
}

impl PartialEq<String> for FirestoreCollectionId {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl PartialEq<FirestoreCollectionId> for String {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        self == &other.0
    }
}

impl Serialize for FirestoreCollectionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_id_accepts_valid_ids() {
        assert!(FirestoreDocumentId::new("user-42").is_ok());
        assert!(FirestoreDocumentId::new("...").is_ok());
        assert!(FirestoreDocumentId::new(".x").is_ok());
        assert!(FirestoreDocumentId::new("__id7__").is_ok());
        assert!(FirestoreDocumentId::new("__name__").is_ok());
    }

    #[test]
    fn collection_id_accepts_valid_ids() {
        assert!(FirestoreCollectionId::new("users").is_ok());
        assert!(FirestoreCollectionId::new("...").is_ok());
        assert!(FirestoreCollectionId::new(".x").is_ok());
        assert!(FirestoreCollectionId::new("__id7__").is_ok());
        assert!(FirestoreCollectionId::new("__name__").is_ok());
    }

    #[test]
    fn rejects_empty_slash_and_dot_segments() {
        for bad in ["", ".", "..", "a/b"] {
            assert!(
                FirestoreDocumentId::new(bad).is_err(),
                "expected {bad:?} to be rejected as a document id"
            );
            assert!(
                FirestoreCollectionId::new(bad).is_err(),
                "expected {bad:?} to be rejected as a collection id"
            );
        }
    }

    #[test]
    fn length_limit_is_measured_in_bytes_not_chars() {
        let at_limit = "é".repeat(750);
        assert_eq!(at_limit.len(), 1500);
        assert!(FirestoreDocumentId::new(at_limit.clone()).is_ok());
        assert!(FirestoreCollectionId::new(at_limit).is_ok());

        let over_limit = "é".repeat(751);
        assert_eq!(over_limit.len(), 1502);
        assert!(FirestoreDocumentId::new(over_limit.clone()).is_err());
        assert!(FirestoreCollectionId::new(over_limit).is_err());
    }

    #[test]
    fn oversized_id_error_reports_byte_count_not_the_value() {
        let huge = "x".repeat(10_000);
        let err = FirestoreDocumentId::new(huge).unwrap_err();
        let message = err.to_string();
        assert!(
            message.len() < 200,
            "message should be short, was {} bytes: {message}",
            message.len()
        );
        assert!(
            !message.contains(&"x".repeat(10)),
            "message must not echo the value"
        );
        assert!(message.contains("10000"));
    }

    #[test]
    fn slash_error_escapes_embedded_newlines() {
        let err = FirestoreCollectionId::new("a\nb/c").unwrap_err();
        let message = err.to_string();
        assert!(
            !message.contains('\n'),
            "message must not contain a raw newline: {message:?}"
        );
        assert!(
            message.contains("\\n"),
            "message should show the escaped newline: {message:?}"
        );
    }

    #[test]
    fn slash_error_names_the_parent_builder() {
        let err = FirestoreCollectionId::new("users/123").unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains(".parent("),
            "message should point at `.parent()`: {message}"
        );
        assert!(message.contains("collection_id"));
    }

    #[test]
    fn value_returns_reference_to_string() {
        let id = FirestoreDocumentId::new("user-42").unwrap();
        let v: &String = id.value();
        assert_eq!(v, "user-42");
        assert_eq!(id.as_str(), "user-42");
    }

    #[test]
    fn validate_str_and_validate_agree_with_new() {
        assert!(FirestoreDocumentId::validate_str("user-42").is_ok());
        assert!(FirestoreDocumentId::validate_str("a/b").is_err());

        let id = FirestoreCollectionId::new("users").unwrap();
        assert!(id.validate().is_ok());
    }

    #[test]
    fn partial_eq_with_str_and_string_both_directions() {
        let id = FirestoreDocumentId::new("user-42").unwrap();
        assert_eq!(id, *"user-42");
        assert_eq!(*"user-42", id);
        assert_eq!(id, "user-42");
        assert_eq!("user-42", id);
        assert_eq!(id, "user-42".to_string());
        assert_eq!("user-42".to_string(), id);
    }

    #[test]
    fn try_from_and_from_str_round_trip() {
        let via_try_from: FirestoreDocumentId = "user-42".try_into().unwrap();
        let via_from_str: FirestoreDocumentId = "user-42".parse().unwrap();
        assert_eq!(via_try_from, via_from_str);

        assert!(FirestoreDocumentId::try_from("a/b").is_err());
        assert!("a/b".parse::<FirestoreDocumentId>().is_err());
    }

    #[test]
    fn deserialize_validates_via_try_from_string() {
        use serde::de::value::{Error as ValueError, StringDeserializer};
        use serde::de::IntoDeserializer;

        let deserializer: StringDeserializer<ValueError> =
            "user-42".to_string().into_deserializer();
        let id = FirestoreDocumentId::deserialize(deserializer).unwrap();
        assert_eq!(id.as_str(), "user-42");

        let bad_deserializer: StringDeserializer<ValueError> =
            "a/b".to_string().into_deserializer();
        assert!(FirestoreDocumentId::deserialize(bad_deserializer).is_err());
    }

    #[test]
    fn drop_in_via_as_ref_str_send() {
        fn takes<S: AsRef<str> + Send>(_: S) {}

        let doc_id = FirestoreDocumentId::new("doc-1").unwrap();
        takes(doc_id.clone());
        takes(&doc_id);

        let coll_id = FirestoreCollectionId::new("col-1").unwrap();
        takes(coll_id.clone());
        takes(&coll_id);
    }
}
