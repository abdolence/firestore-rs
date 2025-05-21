use crate::db::safe_document_path;
use crate::{FirestoreReference, FirestoreResult};
use std::fmt::{Display, Formatter};

/// A builder for constructing Firestore document paths, typically for parent documents
/// when dealing with sub-collections.
///
/// `ParentPathBuilder` allows for fluently creating nested document paths.
/// It starts with an initial document path and allows appending further
/// collection and document ID segments.
///
/// This is often used to specify the parent document when performing operations
/// on a sub-collection.
///
/// # Examples
///
/// ```rust
/// use firestore::{FirestoreDb, FirestoreResult, ParentPathBuilder};
///
/// # async fn run() -> FirestoreResult<()> {
/// let db = FirestoreDb::new("my-project").await?;
///
/// // Path to "my-collection/my-doc"
/// let parent_path = db.parent_path("my-collection", "my-doc")?;
/// assert_eq!(parent_path.to_string(), "projects/my-project/databases/(default)/documents/my-collection/my-doc");
///
/// // Path to "my-collection/my-doc/sub-collection/sub-doc"
/// let sub_collection_path = parent_path.at("sub-collection", "sub-doc")?;
/// assert_eq!(sub_collection_path.to_string(), "projects/my-project/databases/(default)/documents/my-collection/my-doc/sub-collection/sub-doc");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ParentPathBuilder {
    value: String,
}

impl ParentPathBuilder {
    /// Creates a new `ParentPathBuilder` with an initial path.
    /// This is typically called internally by [`FirestoreDb::parent_path()`](crate::FirestoreDb::parent_path).
    #[inline]
    pub(crate) fn new(initial: String) -> Self {
        Self { value: initial }
    }

    /// Appends a collection name and document ID to the current path.
    ///
    /// This method extends the existing path with `/collection_name/document_id`.
    ///
    /// # Arguments
    /// * `collection_name`: The name of the collection to append.
    /// * `document_id`: The ID of the document within that collection.
    ///
    /// # Errors
    /// Returns [`FirestoreError::InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError)
    /// if the `document_id` is invalid (e.g., contains `/`).
    #[inline]
    pub fn at<S>(self, collection_name: &str, document_id: S) -> FirestoreResult<Self>
    where
        S: AsRef<str>,
    {
        Ok(Self::new(safe_document_path(
            self.value.as_str(),
            collection_name,
            document_id.as_ref(),
        )?))
    }
}

impl Display for ParentPathBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl AsRef<str> for ParentPathBuilder {
    fn as_ref(&self) -> &str {
        self.value.as_str()
    }
}

impl From<ParentPathBuilder> for String {
    fn from(pb: ParentPathBuilder) -> Self {
        pb.value
    }
}

impl<'a> From<&'a ParentPathBuilder> for &'a str {
    fn from(pb: &'a ParentPathBuilder) -> &'a str {
        pb.value.as_str()
    }
}

impl From<ParentPathBuilder> for FirestoreReference {
    fn from(pb: ParentPathBuilder) -> Self {
        FirestoreReference(pb.value)
    }
}
