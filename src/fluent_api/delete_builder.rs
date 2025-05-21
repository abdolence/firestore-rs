//! Builder for constructing Firestore delete operations.
//!
//! This module provides a fluent API to specify the document to be deleted,
//! optionally including a parent path for sub-collections and preconditions
//! for the delete operation.

use crate::{
    FirestoreBatch, FirestoreBatchWriter, FirestoreDeleteSupport, FirestoreResult,
    FirestoreTransaction, FirestoreWritePrecondition,
};

/// The initial builder for a Firestore delete operation.
///
/// Created by calling [`FirestoreExprBuilder::delete()`](crate::FirestoreExprBuilder::delete).
#[derive(Clone, Debug)]
pub struct FirestoreDeleteInitialBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreDeleteInitialBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    /// Creates a new `FirestoreDeleteInitialBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Specifies the collection ID from which to delete the document.
    ///
    /// # Arguments
    /// * `collection_id`: The ID of the collection.
    ///
    /// # Returns
    /// A [`FirestoreDeleteDocIdBuilder`] to specify the document ID and other options.
    #[inline]
    pub fn from(self, collection_id: &str) -> FirestoreDeleteDocIdBuilder<'a, D> {
        FirestoreDeleteDocIdBuilder::new(self.db, collection_id.to_string())
    }
}

/// A builder for specifying the document ID and options for a delete operation.
#[derive(Clone, Debug)]
pub struct FirestoreDeleteDocIdBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
    collection_id: String,
    parent: Option<String>,
    precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, D> FirestoreDeleteDocIdBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    /// Creates a new `FirestoreDeleteDocIdBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String) -> Self {
        Self {
            db,
            collection_id,
            parent: None,
            precondition: None,
        }
    }

    /// Specifies the parent document path for deleting a document in a sub-collection.
    ///
    /// # Arguments
    /// * `parent`: The full path to the parent document.
    ///
    /// # Returns
    /// The builder instance with the parent path set.
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

    /// Specifies a precondition for the delete operation.
    ///
    /// The delete will only be executed if the precondition is met.
    ///
    /// # Arguments
    /// * `precondition`: The [`FirestoreWritePrecondition`] to apply.
    ///
    /// # Returns
    /// The builder instance with the precondition set.
    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    /// Specifies the ID of the document to delete.
    ///
    /// # Arguments
    /// * `document_id`: The ID of the document.
    ///
    /// # Returns
    /// A [`FirestoreDeleteExecuteBuilder`] to execute the delete operation or add it to a batch/transaction.
    #[inline]
    pub fn document_id<S>(self, document_id: S) -> FirestoreDeleteExecuteBuilder<'a, D>
    where
        S: AsRef<str> + Send,
    {
        FirestoreDeleteExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            document_id.as_ref().to_string(),
            self.parent,
            self.precondition,
        )
    }
}

/// A builder for executing a Firestore delete operation or adding it to a batch/transaction.
#[derive(Clone, Debug)]
pub struct FirestoreDeleteExecuteBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: String,
    parent: Option<String>,
    precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, D> FirestoreDeleteExecuteBuilder<'a, D>
where
    D: FirestoreDeleteSupport,
{
    /// Creates a new `FirestoreDeleteExecuteBuilder`.
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        document_id: String,
        parent: Option<String>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent,
            precondition,
        }
    }

    /// Specifies the parent document path. This is an alternative way to set the parent
    /// if not already set in the previous builder step.
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

    /// Specifies a precondition for the delete operation. This is an alternative way to set
    /// the precondition if not already set.
    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    /// Executes the configured delete operation.
    ///
    /// # Returns
    /// A `FirestoreResult` indicating success or failure.
    pub async fn execute(self) -> FirestoreResult<()> {
        if let Some(parent) = self.parent {
            self.db
                .delete_by_id_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.precondition,
                )
                .await
        } else {
            self.db
                .delete_by_id(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.precondition,
                )
                .await
        }
    }

    /// Adds this delete operation to a [`FirestoreTransaction`].
    ///
    /// # Arguments
    /// * `transaction`: A mutable reference to the transaction to add this operation to.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the transaction, allowing for chaining.
    #[inline]
    pub fn add_to_transaction<'t>(
        self,
        transaction: &'a mut FirestoreTransaction<'t>,
    ) -> FirestoreResult<&'a mut FirestoreTransaction<'t>> {
        if let Some(parent) = self.parent {
            transaction.delete_by_id_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        } else {
            transaction.delete_by_id(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        }
    }

    /// Adds this delete operation to a [`FirestoreBatch`].
    ///
    /// # Arguments
    /// * `batch`: A mutable reference to the batch writer to add this operation to.
    ///
    /// # Type Parameters
    /// * `W`: The type of the batch writer, implementing [`FirestoreBatchWriter`].
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the batch, allowing for chaining.
    #[inline]
    pub fn add_to_batch<'t, W>(
        self,
        batch: &'a mut FirestoreBatch<'t, W>,
    ) -> FirestoreResult<&'a mut FirestoreBatch<'t, W>>
    where
        W: FirestoreBatchWriter,
    {
        if let Some(parent) = self.parent {
            batch.delete_by_id_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        } else {
            batch.delete_by_id(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
            )
        }
    }
}
