use crate::db::transaction_ops::{TransformObjectOperation, UpdateObjectOperation};
use crate::db::DeleteOperation;
use crate::errors::FirestoreError;
use crate::{
    FirestoreDb, FirestoreFieldTransform, FirestoreResult, FirestoreWritePrecondition,
    FirestoreWriteResult,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcloud_sdk::google::firestore::v1::Write;
use gcloud_sdk::google::rpc::Status;
use rsb_derive::*;
use serde::Serialize;

#[async_trait]
pub trait FirestoreBatchWriter {
    type WriteResult;

    async fn write(&self, writes: Vec<Write>) -> FirestoreResult<Self::WriteResult>;
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreBatchWriteResponse {
    pub position: u64,
    pub write_results: Vec<FirestoreWriteResult>,
    pub statuses: Vec<Status>,
    pub commit_time: Option<DateTime<Utc>>,
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

    #[inline]
    pub fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>,
    {
        self.writes.push(write.try_into()?);
        Ok(self)
    }

    #[inline]
    pub async fn write(self) -> FirestoreResult<W::WriteResult> {
        self.writer.write(self.writes).await
    }

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
