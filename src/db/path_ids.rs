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
use std::borrow::{Borrow, Cow};
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

/// The one rule a path segment fails, if any.
///
/// This is the single source of truth for what makes a Firestore path segment valid, shared by
/// the runtime path ([`validate_path_segment`], which turns a violation into an error message)
/// and the compile-time path (`from_static` on both ID types, which turns one into a `panic!` that
/// aborts `const` evaluation). Add or change a rule here, not in either caller.
enum PathSegmentViolation {
    Empty,
    TooLong { len: usize },
    ContainsSlash,
    DotOrDotDot,
}

impl PathSegmentViolation {
    /// The message `from_static` panics with.
    ///
    /// A plain literal, not the detailed message [`validate_path_segment`] builds: `Display::fmt`
    /// is not a `const fn` on stable Rust, so there is no const-compatible way to interpolate the
    /// segment or its length here the way the runtime path does. `panic!("{}", ...)` with a single
    /// `&'static str` argument is const-evaluable even though general formatting is not, which is
    /// what makes returning one from here workable inside a `const fn`.
    const fn panic_message(self, kind: FirestorePathSegmentKind) -> &'static str {
        use FirestorePathSegmentKind::{CollectionId, DocumentId};
        match (kind, self) {
            (DocumentId, Self::Empty) => "Firestore document id must not be empty",
            (DocumentId, Self::TooLong { .. }) => {
                "Firestore document id must be at most 1500 bytes"
            }
            (DocumentId, Self::ContainsSlash) => "Firestore document id must not contain '/'",
            (DocumentId, Self::DotOrDotDot) => "Firestore document id must not be \".\" or \"..\"",
            (CollectionId, Self::Empty) => "Firestore collection id must not be empty",
            (CollectionId, Self::TooLong { .. }) => {
                "Firestore collection id must be at most 1500 bytes"
            }
            (CollectionId, Self::ContainsSlash) => "Firestore collection id must not contain '/'",
            (CollectionId, Self::DotOrDotDot) => {
                "Firestore collection id must not be \".\" or \"..\""
            }
        }
    }
}

/// Checks `segment` against Firestore's ID rules without allocating.
///
/// `str::contains` and `str::eq` are not `const fn`, so this cannot reuse the ordinary string
/// methods and instead walks `segment.as_bytes()` by hand. That byte loop is what lets
/// `from_static` run at compile time; do not replace it with the `str`-method equivalent even
/// though it reads simpler; doing so would stop `from_static` from being a `const fn`.
const fn check_path_segment(segment: &str) -> Result<(), PathSegmentViolation> {
    let bytes = segment.as_bytes();
    let len = bytes.len();

    if len == 0 {
        return Err(PathSegmentViolation::Empty);
    }

    // Checked before the '/' and '.'/'..' checks below so that every other rule sees a value
    // already bounded to 1500 bytes.
    if len > 1500 {
        return Err(PathSegmentViolation::TooLong { len });
    }

    let mut i = 0;
    while i < len {
        if bytes[i] == b'/' {
            return Err(PathSegmentViolation::ContainsSlash);
        }
        i += 1;
    }

    if (len == 1 && bytes[0] == b'.') || (len == 2 && bytes[0] == b'.' && bytes[1] == b'.') {
        return Err(PathSegmentViolation::DotOrDotDot);
    }

    Ok(())
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

    match check_path_segment(segment) {
        Ok(()) => Ok(()),
        Err(PathSegmentViolation::Empty) => {
            Err(path_segment_error(field, "must not be empty".to_string()))
        }
        // The value itself is unbounded here, unlike every other violation below, so this
        // message reports the length rather than echoing it.
        Err(PathSegmentViolation::TooLong { len }) => Err(path_segment_error(
            field,
            format!("must be at most 1500 bytes, was {len} bytes"),
        )),
        Err(PathSegmentViolation::ContainsSlash) => Err(path_segment_error(
            field,
            format!(
                "must not contain '/': \"{}\" - a slash-delimited path such as \"users/123/posts\" is not a single ID; \
                 build it with `.parent(db.parent_path(\"users\", \"123\")?)` and pass \"posts\" as the {field}",
                segment.escape_debug(),
            ),
        )),
        Err(PathSegmentViolation::DotOrDotDot) => Err(path_segment_error(
            field,
            format!("must not be \".\" or \"..\", got \"{segment}\""),
        )),
    }
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
pub struct FirestoreDocumentId(Cow<'static, str>);

impl FirestoreDocumentId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if `id` is empty, longer than 1500
    /// bytes, contains `/`, or is exactly `"."` or `".."`.
    pub fn new<S: Into<String>>(id: S) -> FirestoreResult<Self> {
        let id = id.into();
        validate_path_segment(&id, FirestorePathSegmentKind::DocumentId)?;
        Ok(Self(Cow::Owned(id)))
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

    /// Validates `id` at compile time and wraps it without allocating, for a name known up front.
    ///
    /// Declare validated names the way you would declare `const NAME: &str = "..."` today:
    ///
    /// ```rust
    /// use firestore::FirestoreDocumentId;
    ///
    /// const WELCOME_DOC: FirestoreDocumentId = FirestoreDocumentId::from_static("welcome");
    /// assert_eq!(WELCOME_DOC.as_str(), "welcome");
    /// ```
    ///
    /// Used in a `const` or `static` item, an invalid literal is a compile error: evaluating the
    /// item panics, which `rustc` reports at the definition site rather than letting the invalid
    /// value reach a binary. Called from inside a function body, where the result is only a
    /// runtime value, the same invalid literal panics instead, exactly like an out-of-bounds
    /// `const fn` array index would.
    ///
    /// ```compile_fail
    /// use firestore::FirestoreDocumentId;
    /// const BAD: FirestoreDocumentId = FirestoreDocumentId::from_static("a/b");
    /// ```
    ///
    /// # Panics
    /// Panics if `id` is empty, longer than 1500 bytes, contains `/`, or is exactly `"."` or
    /// `".."`, and the call is not evaluated at compile time.
    pub const fn from_static(id: &'static str) -> Self {
        match check_path_segment(id) {
            Ok(()) => Self(Cow::Borrowed(id)),
            Err(violation) => {
                panic!(
                    "{}",
                    violation.panic_message(FirestorePathSegmentKind::DocumentId)
                )
            }
        }
    }

    /// Returns the wrapped ID.
    ///
    /// Equivalent to [`as_str`](Self::as_str); kept as a separate method so a validated ID reads
    /// the same as [`rvstruct::ValueStruct::value`] elsewhere in this crate's API. There is no
    /// consuming equivalent that returns an owned `String` (see the [`FirestoreDocumentId`] type
    /// docs for why). Use `id.as_str().to_string()` when an owned copy is genuinely needed.
    pub fn value(&self) -> &str {
        &self.0
    }

    /// Returns the wrapped ID as a `&str`. Equivalent to [`value`](Self::value).
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
        Display::fmt(self.0.as_ref(), f)
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
        self.0.as_ref() == other
    }
}

impl PartialEq<FirestoreDocumentId> for str {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        self == other.0.as_ref()
    }
}

impl PartialEq<&str> for FirestoreDocumentId {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<FirestoreDocumentId> for &str {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        *self == other.0.as_ref()
    }
}

impl PartialEq<String> for FirestoreDocumentId {
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other.as_str()
    }
}

impl PartialEq<FirestoreDocumentId> for String {
    fn eq(&self, other: &FirestoreDocumentId) -> bool {
        self.as_str() == other.0.as_ref()
    }
}

impl Serialize for FirestoreDocumentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_ref())
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
pub struct FirestoreCollectionId(Cow<'static, str>);

impl FirestoreCollectionId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`] if `id` is empty, longer than 1500
    /// bytes, contains `/`, or is exactly `"."` or `".."`.
    pub fn new<S: Into<String>>(id: S) -> FirestoreResult<Self> {
        let id = id.into();
        validate_path_segment(&id, FirestorePathSegmentKind::CollectionId)?;
        Ok(Self(Cow::Owned(id)))
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

    /// Validates `id` at compile time and wraps it without allocating, for a name known up front.
    ///
    /// Declare validated names the way you would declare `const NAME: &str = "..."` today:
    ///
    /// ```rust
    /// use firestore::FirestoreCollectionId;
    ///
    /// const USERS: FirestoreCollectionId = FirestoreCollectionId::from_static("users");
    /// assert_eq!(USERS.as_str(), "users");
    /// ```
    ///
    /// Used in a `const` or `static` item, an invalid literal is a compile error: evaluating the
    /// item panics, which `rustc` reports at the definition site rather than letting the invalid
    /// value reach a binary. Called from inside a function body, where the result is only a
    /// runtime value, the same invalid literal panics instead, exactly like an out-of-bounds
    /// `const fn` array index would.
    ///
    /// ```compile_fail
    /// use firestore::FirestoreCollectionId;
    /// const BAD: FirestoreCollectionId = FirestoreCollectionId::from_static("a/b");
    /// ```
    ///
    /// # Panics
    /// Panics if `id` is empty, longer than 1500 bytes, contains `/`, or is exactly `"."` or
    /// `".."`, and the call is not evaluated at compile time.
    pub const fn from_static(id: &'static str) -> Self {
        match check_path_segment(id) {
            Ok(()) => Self(Cow::Borrowed(id)),
            Err(violation) => {
                panic!(
                    "{}",
                    violation.panic_message(FirestorePathSegmentKind::CollectionId)
                )
            }
        }
    }

    /// Returns the wrapped ID.
    ///
    /// Equivalent to [`as_str`](Self::as_str); kept as a separate method so a validated ID reads
    /// the same as [`rvstruct::ValueStruct::value`] elsewhere in this crate's API. There is no
    /// consuming equivalent that returns an owned `String` (see the [`FirestoreDocumentId`] type
    /// docs for why). Use `id.as_str().to_string()` when an owned copy is genuinely needed.
    pub fn value(&self) -> &str {
        &self.0
    }

    /// Returns the wrapped ID as a `&str`. Equivalent to [`value`](Self::value).
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
        Display::fmt(self.0.as_ref(), f)
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
        self.0.as_ref() == other
    }
}

impl PartialEq<FirestoreCollectionId> for str {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        self == other.0.as_ref()
    }
}

impl PartialEq<&str> for FirestoreCollectionId {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<FirestoreCollectionId> for &str {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        *self == other.0.as_ref()
    }
}

impl PartialEq<String> for FirestoreCollectionId {
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other.as_str()
    }
}

impl PartialEq<FirestoreCollectionId> for String {
    fn eq(&self, other: &FirestoreCollectionId) -> bool {
        self.as_str() == other.0.as_ref()
    }
}

impl Serialize for FirestoreCollectionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_ref())
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
    fn value_and_as_str_agree() {
        let id = FirestoreDocumentId::new("user-42").unwrap();
        let v: &str = id.value();
        assert_eq!(v, "user-42");
        assert_eq!(id.value(), id.as_str());
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

    const CONST_DOC_ID: FirestoreDocumentId = FirestoreDocumentId::from_static("const-doc");
    static STATIC_COLLECTION_ID: FirestoreCollectionId =
        FirestoreCollectionId::from_static("static-collection");

    #[test]
    fn from_static_builds_const_and_static_items() {
        assert_eq!(CONST_DOC_ID.as_str(), "const-doc");
        assert_eq!(STATIC_COLLECTION_ID.as_str(), "static-collection");
    }

    #[test]
    fn from_static_agrees_with_new_on_valid_input() {
        let via_new = FirestoreDocumentId::new("const-doc").unwrap();
        assert_eq!(CONST_DOC_ID, via_new);

        let via_new = FirestoreCollectionId::new("static-collection").unwrap();
        assert_eq!(STATIC_COLLECTION_ID, via_new);
    }

    #[test]
    fn borrowed_and_owned_are_equal_and_hash_the_same() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let borrowed = FirestoreDocumentId::from_static("user-42");
        let owned = FirestoreDocumentId::new("user-42").unwrap();
        assert_eq!(borrowed, owned);

        let hash_of = |id: &FirestoreDocumentId| {
            let mut hasher = DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash_of(&borrowed), hash_of(&owned));
    }

    #[test]
    fn hashmap_lookup_by_str_finds_a_from_static_key() {
        use std::collections::HashMap;

        const KEY: FirestoreCollectionId = FirestoreCollectionId::from_static("users");
        let mut map: HashMap<FirestoreCollectionId, u32> = HashMap::new();
        map.insert(KEY, 1);

        assert_eq!(map.get("users"), Some(&1));
    }
}
