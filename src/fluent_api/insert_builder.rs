//! Builder for constructing Firestore insert (create) operations.
//!
//! This module provides a fluent API to specify the collection, document ID (optional),
//! and the data to be inserted into Firestore. It supports inserting both raw
//! [`Document`](gcloud_sdk::google::firestore::v1::Document) types and serializable Rust objects.

use crate::{FirestoreCreateSupport, FirestoreResult};
use gcloud_sdk::google::firestore::v1::Document;
use serde::{Deserialize, Serialize};

/// The initial builder for a Firestore insert operation.
///
/// Created by calling [`FirestoreExprBuilder::insert()`](crate::FirestoreExprBuilder::insert).
#[derive(Clone, Debug)]
pub struct FirestoreInsertInitialBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreInsertInitialBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Specifies the collection ID into which the document will be inserted.
    #[inline]
    pub fn into<S: AsRef<str>>(self, collection_id: S) -> FirestoreInsertDocIdBuilder<'a, D> {
        FirestoreInsertDocIdBuilder::new(self.db, collection_id.as_ref().to_string())
    }
}

/// A builder for specifying the document ID for an insert operation.
///
/// This stage allows either providing a specific document ID or opting for
/// Firestore to auto-generate one.
#[derive(Clone, Debug)]
pub struct FirestoreInsertDocIdBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
}

impl<'a, D> FirestoreInsertDocIdBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String) -> Self {
        Self { db, collection_id }
    }

    /// Specifies a user-defined ID for the new document.
    ///
    /// If this ID already exists in the collection, the operation will fail.
    #[inline]
    pub fn document_id<S>(self, document_id: S) -> FirestoreInsertDocObjBuilder<'a, D>
    where
        S: AsRef<str> + Send,
    {
        FirestoreInsertDocObjBuilder::new(
            self.db,
            self.collection_id,
            Some(document_id.as_ref().to_string()),
        )
    }

    /// Configures the operation to let Firestore auto-generate the document ID.
    #[inline]
    pub fn generate_document_id(self) -> FirestoreInsertDocObjBuilder<'a, D> {
        FirestoreInsertDocObjBuilder::new(self.db, self.collection_id, None)
    }
}

/// A builder for specifying the object or document data for an insert operation.
///
/// This stage also allows specifying a parent path (for sub-collections) and
/// which fields to return from the operation.
#[derive(Clone, Debug)]
pub struct FirestoreInsertDocObjBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: Option<String>,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreInsertDocObjBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String, document_id: Option<String>) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent: None,
            return_only_fields: None,
        }
    }

    /// Specifies the parent document path for inserting a document into a sub-collection.
    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    /// Specifies which fields of the newly created document should be returned.
    ///
    /// If not set, the entire document is typically returned (behavior may depend on the server).
    #[inline]
    pub fn return_only_fields<I>(self, return_only_fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            return_only_fields: Some(
                return_only_fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    /// Specifies the document data to insert as a raw [`Document`].
    #[inline]
    pub fn document(self, document: Document) -> FirestoreInsertDocExecuteBuilder<'a, D> {
        FirestoreInsertDocExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.document_id,
            self.parent,
            document,
            self.return_only_fields,
        )
    }

    /// Specifies the document data to insert as a serializable Rust object.
    ///
    /// The object `T` must implement `serde::Serialize`.
    #[inline]
    pub fn object<T>(self, object: &'a T) -> FirestoreInsertObjExecuteBuilder<'a, D, T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>, // Bound for potential return type deserialization
    {
        FirestoreInsertObjExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.parent,
            self.document_id,
            object,
            self.return_only_fields,
        )
    }
}

/// A builder for executing an insert operation with raw [`Document`] data.
#[derive(Clone, Debug)]
pub struct FirestoreInsertDocExecuteBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: Option<String>,
    parent: Option<String>,
    document: Document,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreInsertDocExecuteBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        document_id: Option<String>,
        parent: Option<String>,
        document: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent,
            document,
            return_only_fields,
        }
    }

    /// Sends the document to Firestore and returns it as stored.
    ///
    /// Returns an error if the request fails, or if `document_id` was given and a document with
    /// that ID already exists in the collection.
    ///
    /// ```rust,no_run
    /// use firestore::FirestoreDb;
    /// use gcloud_sdk::google::firestore::v1::Document;
    ///
    /// # async fn example(
    /// #     db: FirestoreDb,
    /// #     document: Document,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let created = db
    ///     .fluent()
    ///     .insert()
    ///     .into("users")
    ///     .generate_document_id()
    ///     .document(document)
    ///     .execute()
    ///     .await?;
    /// # let _ = created;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(self) -> FirestoreResult<Document> {
        if let Some(parent) = self.parent {
            self.db
                .create_doc_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.document,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .create_doc(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.document,
                    self.return_only_fields,
                )
                .await
        }
    }
}

/// A builder for executing an insert operation with a serializable Rust object.
#[derive(Clone, Debug)]
pub struct FirestoreInsertObjExecuteBuilder<'a, D, T>
where
    D: FirestoreCreateSupport,
    T: Serialize + Sync + Send,
{
    db: &'a D,
    collection_id: String,
    parent: Option<String>,
    document_id: Option<String>,
    object: &'a T,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D, T> FirestoreInsertObjExecuteBuilder<'a, D, T>
where
    D: FirestoreCreateSupport,
    T: Serialize + Sync + Send,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        parent: Option<String>,
        document_id: Option<String>,
        object: &'a T,
        return_only_fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            db,
            collection_id,
            parent,
            document_id,
            object,
            return_only_fields,
        }
    }

    /// Serializes the object, sends it to Firestore, and deserializes the stored result into `O`.
    ///
    /// Returns an error if serialization or the request fails, if `document_id` was given and a
    /// document with that ID already exists, or if the result does not deserialize into `O`.
    pub async fn execute<O>(self) -> FirestoreResult<O>
    where
        for<'de> O: Deserialize<'de>,
    {
        if let Some(parent) = self.parent {
            self.db
                .create_obj_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .create_obj(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.return_only_fields,
                )
                .await
        }
    }
}
