//! Builder for constructing Firestore delete operations.
//!
//! This module provides a fluent API to specify the document to be deleted,
//! optionally including a parent path for sub-collections and preconditions
//! for the delete operation.

use crate::{
    FirestoreBatch, FirestoreBatchWriter, FirestoreDeleteSupport, FirestoreResult,
    FirestoreTransactionOps, FirestoreWritePrecondition,
};
#[cfg(feature = "admin")]
use crate::{
    FirestoreBulkDeleteParams, FirestoreBulkDeleteResult, FirestoreBulkDeleteSupport,
    FirestoreCollectionId, FirestoreOperationWaitOptions,
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

// A separate impl block, bounded only by `FirestoreBulkDeleteSupport`, so that this method never
// widens the bounds a plain single-document delete pays for.
#[cfg(feature = "admin")]
impl<'a, D> FirestoreDeleteInitialBuilder<'a, D>
where
    D: FirestoreDeleteSupport + FirestoreBulkDeleteSupport + Clone + Send + Sync + 'static,
{
    /// Switches to Google's `BulkDeleteDocuments` operation: every document in the named
    /// collection groups, at any depth, across the whole database, deleted in the background.
    /// Requires the `admin` feature.
    ///
    /// See <https://cloud.google.com/firestore/docs/manage-data/bulk-delete>. In particular:
    ///
    /// - it reaches every document in each named collection group at any depth, anywhere under
    ///   the database, not only documents under one parent - there is no `.parent()` on the
    ///   builder this returns, because the underlying API cannot express one;
    /// - it runs in the background and is not transactional;
    /// - documents written after the operation starts processing are not touched, so they
    ///   survive;
    /// - a collection with a different ID is never touched, even if nested under a deleted group.
    ///
    /// Continue with [`FirestoreBulkDeleteBuilder::collection_groups`].
    #[inline]
    pub fn bulk(self) -> FirestoreBulkDeleteBuilder<'a, D> {
        FirestoreBulkDeleteBuilder::new(self.db)
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

/// The bulk-delete operation Google calls `BulkDeleteDocuments`: deletes every document in the
/// named collection groups, at any depth, across the whole database. Requires the `admin`
/// feature.
///
/// Reach this from [`FirestoreDeleteInitialBuilder::bulk`]. There is no `.parent()` or
/// `.document_id()` here: a parent-scoped bulk delete is not an operation Firestore offers.
#[cfg(feature = "admin")]
#[derive(Clone, Debug)]
pub struct FirestoreBulkDeleteBuilder<'a, D>
where
    D: FirestoreBulkDeleteSupport,
{
    db: &'a D,
    collection_groups: Vec<String>,
    wait: Option<FirestoreOperationWaitOptions>,
}

#[cfg(feature = "admin")]
impl<'a, D> FirestoreBulkDeleteBuilder<'a, D>
where
    D: FirestoreBulkDeleteSupport + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            collection_groups: Vec::new(),
            wait: None,
        }
    }

    /// Names the collection groups to delete. Every document in each group, at any depth across
    /// the whole database, is removed - see [`FirestoreDeleteInitialBuilder::bulk`] for the full
    /// semantics. Each call replaces any groups set by a previous call.
    ///
    /// Validated into [`FirestoreCollectionId`]s at [`execute`](Self::execute): an empty list, or
    /// a group named twice, is rejected there rather than sent, since Firestore reads an empty
    /// `collection_ids` as "the whole database".
    #[inline]
    pub fn collection_groups<I>(self, collection_groups: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            collection_groups: collection_groups
                .into_iter()
                .map(|group| group.as_ref().to_string())
                .collect(),
            ..self
        }
    }

    /// Waits for the bulk delete to reach a terminal state before [`execute`](Self::execute)
    /// returns, up to `timeout`, polling at the default interval. Without this, `execute` returns
    /// once the operation is requested.
    #[inline]
    pub fn wait_until_done(self, timeout: std::time::Duration) -> Self {
        self.wait_until_done_with_options(FirestoreOperationWaitOptions::new(timeout))
    }

    /// Waits for the bulk delete to reach a terminal state before [`execute`](Self::execute)
    /// returns, per `options`. Without this, `execute` returns once the operation is requested.
    #[inline]
    pub fn wait_until_done_with_options(self, options: FirestoreOperationWaitOptions) -> Self {
        Self {
            wait: Some(options),
            ..self
        }
    }

    /// Sends the bulk delete to Firestore.
    ///
    /// Returns [`FirestoreError::InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError)
    /// if no collection groups were named, or the same group was named twice: an empty list is
    /// never sent, since Firestore reads it as "delete the whole database".
    pub async fn execute(self) -> FirestoreResult<FirestoreBulkDeleteResult> {
        let collection_groups = self
            .collection_groups
            .into_iter()
            .map(FirestoreCollectionId::new)
            .collect::<FirestoreResult<Vec<_>>>()?;
        let params = FirestoreBulkDeleteParams::new(collection_groups);
        let params = match self.wait {
            Some(wait) => params.with_wait(wait),
            None => params,
        };
        crate::validate_bulk_delete_params(&params)?;
        self.db.bulk_delete_documents(params).await
    }
}

#[cfg(all(test, feature = "admin"))]
mod tests {
    use super::FirestoreDeleteInitialBuilder;
    use crate::fluent_api::tests::mockdb::MockBulkDeleteDatabase;
    use crate::{FirestoreCollectionId, FirestoreOperationWaitOptions};
    use std::time::Duration;

    #[tokio::test]
    async fn bulk_chain_produces_the_expected_params() {
        let mock = MockBulkDeleteDatabase::default();
        FirestoreDeleteInitialBuilder::new(&mock)
            .bulk()
            .collection_groups(["users", "orders"])
            .wait_until_done(Duration::from_secs(600))
            .execute()
            .await
            .expect("capturing mock always succeeds");

        let params = mock.captured().expect("bulk_delete_documents was called");
        assert_eq!(
            params.collection_groups,
            vec![
                FirestoreCollectionId::new("users").unwrap(),
                FirestoreCollectionId::new("orders").unwrap(),
            ]
        );
        assert_eq!(
            params.wait,
            Some(FirestoreOperationWaitOptions::new(Duration::from_secs(600)))
        );
    }

    #[tokio::test]
    async fn without_wait_defaults_to_no_wait() {
        let mock = MockBulkDeleteDatabase::default();
        FirestoreDeleteInitialBuilder::new(&mock)
            .bulk()
            .collection_groups(["users"])
            .execute()
            .await
            .unwrap();

        let params = mock.captured().unwrap();
        assert!(params.wait.is_none());
    }

    #[tokio::test]
    async fn empty_collection_groups_is_rejected_before_the_call() {
        let mock = MockBulkDeleteDatabase::default();
        let err = FirestoreDeleteInitialBuilder::new(&mock)
            .bulk()
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("collection_groups"));
        assert!(mock.captured().is_none());
    }

    #[tokio::test]
    async fn duplicate_collection_group_is_rejected_before_the_call() {
        let mock = MockBulkDeleteDatabase::default();
        let err = FirestoreDeleteInitialBuilder::new(&mock)
            .bulk()
            .collection_groups(["users", "users"])
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("more than once"));
        assert!(mock.captured().is_none());
    }

    #[tokio::test]
    async fn invalid_collection_group_is_rejected_before_the_call() {
        let mock = MockBulkDeleteDatabase::default();
        let err = FirestoreDeleteInitialBuilder::new(&mock)
            .bulk()
            .collection_groups(["a/b"])
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("collection_id"));
        assert!(mock.captured().is_none());
    }
}
