use crate::db::transaction_ops::{TransformObjectOperation, UpdateObjectOperation};
use crate::db::DeleteOperation;
use crate::errors::FirestoreError;
use crate::FirestoreInstant;
use crate::{
    FirestoreDb, FirestoreFieldTransform, FirestoreResult, FirestoreWritePrecondition,
    FirestoreWriteResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::Write;
use gcloud_sdk::google::rpc::Status;
use rsb_derive::*;
use serde::Serialize;

/// Sends a queued batch of writes to Firestore. Implemented by
/// [`FirestoreSimpleBatchWriter`](crate::FirestoreSimpleBatchWriter) and
/// [`FirestoreStreamingBatchWriter`](crate::FirestoreStreamingBatchWriter), which differ in how
/// they send the writes and what `WriteResult` they return; see [`FirestoreBatch::write`] for the
/// call callers actually make.
#[async_trait]
pub trait FirestoreBatchWriter {
    /// What a completed write reports back. The simple writer returns Firestore's response;
    /// the streaming one returns nothing, since it acknowledges writes as they drain.
    type WriteResult;

    /// Sends `writes` to Firestore. Called by [`FirestoreBatch::write`]; build writes through a
    /// [`FirestoreBatch`] rather than calling this directly.
    async fn write(&self, writes: Vec<Write>) -> FirestoreResult<Self::WriteResult>;
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreBatchWriteResponse {
    pub position: u64,
    pub write_results: Vec<FirestoreWriteResult>,
    pub statuses: Vec<Status>,
    pub commit_time: Option<FirestoreInstant>,
}

pub struct FirestoreBatch<'a, W>
where
    W: FirestoreBatchWriter,
{
    pub db: &'a FirestoreDb,
    pub writer: &'a W,
    pub writes: Vec<Write>,
}

impl<'a, W> FirestoreBatch<'a, W>
where
    W: FirestoreBatchWriter,
{
    pub(crate) fn new(db: &'a FirestoreDb, writer: &'a W) -> Self {
        Self {
            db,
            writer,
            writes: Vec::new(),
        }
    }

    /// Queues a single write, converting `write` via its `TryInto<Write>` impl.
    ///
    /// This is the primitive every other method on `FirestoreBatch` queues through; call it
    /// directly only when building a write Firestore's higher-level helpers do not cover.
    ///
    /// Returns an error if the conversion fails.
    #[inline]
    pub fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>,
    {
        self.writes.push(write.try_into()?);
        Ok(self)
    }

    /// Sends every queued write through the batch's [`FirestoreBatchWriter`], consuming it.
    ///
    /// Unlike a [`FirestoreTransaction`](crate::FirestoreTransaction), a batch does not commit
    /// atomically: each queued write can succeed or fail independently of the others. What
    /// happens on the wire depends on which writer created this batch - see
    /// [`FirestoreSimpleBatchWriter`](crate::FirestoreSimpleBatchWriter) and
    /// [`FirestoreStreamingBatchWriter`](crate::FirestoreStreamingBatchWriter).
    ///
    /// Returns an error if the underlying writer's request fails.
    #[inline]
    pub async fn write(self) -> FirestoreResult<W::WriteResult> {
        self.writer.write(self.writes).await
    }

    /// Queues a create-or-replace of `obj` at `document_id` in `collection_id`, under this
    /// batch's own documents path.
    ///
    /// A Firestore batch write cannot create a document with a server-generated ID, so this -
    /// with an explicit `document_id` - is how a batch creates one. `update_only` restricts the
    /// write to those field paths, leaving the rest of an existing document untouched;
    /// `precondition` fails that one write if the document's current state does not match it;
    /// `update_transforms` runs additional server-side transforms atomically with the write.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation, or
    /// if `obj` cannot be serialized.
    pub fn update_object<T, S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        update_transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.update_object_at(
            self.db.get_documents_path(),
            collection_id,
            document_id,
            obj,
            update_only,
            precondition,
            update_transforms,
        )
    }

    /// Same as [`update_object`](Self::update_object), at an explicit `parent` path instead of
    /// this batch's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation, or
    /// if `obj` cannot be serialized.
    pub fn update_object_at<T, S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        update_transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.add(UpdateObjectOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            obj,
            update_only,
            precondition,
            update_transforms,
        })
    }

    /// Queues a delete of `document_id` in `collection_id`, under this batch's own documents
    /// path. `precondition` fails that one write if the document's current state does not match
    /// it.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    pub fn delete_by_id<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.delete_by_id_at(
            self.db.get_documents_path(),
            collection_id,
            document_id,
            precondition,
        )
    }

    /// Same as [`delete_by_id`](Self::delete_by_id), at an explicit `parent` path instead of this
    /// batch's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    pub fn delete_by_id_at<S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.add(DeleteOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            precondition,
        })
    }

    /// Queues one or more atomic field transforms (for example `serverTimestamp()` or a numeric
    /// increment) on `document_id` in `collection_id`, under this batch's own documents path,
    /// without reading or resending the rest of the document. `precondition` fails that one
    /// write if the document's current state does not match it.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    pub fn transform<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.transform_at(
            self.db.get_documents_path(),
            collection_id,
            document_id,
            precondition,
            transforms,
        )
    }

    /// Same as [`transform`](Self::transform), at an explicit `parent` path instead of this
    /// batch's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    pub fn transform_at<S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.add(TransformObjectOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            precondition,
            transforms,
        })
    }
}
