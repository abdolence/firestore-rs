//! Builder for constructing Firestore delete operations.
//!
//! This module provides a fluent API to specify the document to be deleted,
//! optionally including a parent path for sub-collections and preconditions
//! for the delete operation.

use crate::{
    FirestoreBatch, FirestoreBatchWriter, FirestoreDeleteSupport, FirestoreResult,
    FirestoreTransactionOps, FirestoreWritePrecondition,
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
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Specifies the collection ID from which to delete the document.
    #[inline]
    pub fn from<S: AsRef<str>>(self, collection_id: S) -> FirestoreDeleteDocIdBuilder<'a, D> {
        FirestoreDeleteDocIdBuilder::new(self.db, collection_id.as_ref().to_string())
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
    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    /// Specifies the ID of the document to delete.
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

    /// Sets or overrides the parent document path.
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

    /// Sets or overrides the precondition for the delete.
    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    /// Sends the delete to Firestore.
    ///
    /// Returns an error if the request fails, or if a precondition was set and does not hold.
    ///
    /// ```rust,no_run
    /// use firestore::FirestoreDb;
    ///
    /// # async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error>> {
    /// db.fluent()
    ///     .delete()
    ///     .from("users")
    ///     .document_id("user-42")
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Queues this delete on `transaction`, to be sent when the transaction commits.
    ///
    /// Returns an error if the delete cannot be added to the transaction.
    #[inline]
    pub fn add_to_transaction<'t, TO>(self, transaction: &'t mut TO) -> FirestoreResult<&'t mut TO>
    where
        TO: FirestoreTransactionOps,
    {
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

    /// Queues this delete on `batch`, to be sent when the batch is written.
    ///
    /// Returns an error if the delete cannot be added to the batch.
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
