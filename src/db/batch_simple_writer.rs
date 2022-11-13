use crate::{
    FirestoreBatch, FirestoreBatchWriteResponse, FirestoreBatchWriter, FirestoreDb, FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::{BatchWriteRequest, Write};
use rsb_derive::*;
use std::collections::HashMap;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreSimpleBatchWriteOptions {}

pub struct FirestoreSimpleBatchWriter {
    pub db: FirestoreDb,
    pub options: FirestoreSimpleBatchWriteOptions,
    pub batch_span: Span,
}

impl FirestoreSimpleBatchWriter {
    pub async fn new<'b>(
        db: FirestoreDb,
        options: FirestoreSimpleBatchWriteOptions,
    ) -> FirestoreResult<FirestoreSimpleBatchWriter> {
        let batch_span = span!(Level::DEBUG, "Firestore Batch Write");

        Ok(Self {
            db,
            options,
            batch_span,
        })
    }

    pub fn new_batch(&self) -> FirestoreBatch<FirestoreSimpleBatchWriter> {
        FirestoreBatch::new(&self.db, self)
    }
}

#[async_trait]
impl FirestoreBatchWriter for FirestoreSimpleBatchWriter {
    type WriteResult = FirestoreBatchWriteResponse;

    async fn write(&self, writes: Vec<Write>) -> FirestoreResult<FirestoreBatchWriteResponse> {
        let response = self
            .db
            .client()
            .get()
            .batch_write(BatchWriteRequest {
                database: self.db.get_database_path().to_string(),
                writes,
                labels: HashMap::new(),
            })
            .await?;

        let batch_response = response.into_inner();

        Ok(FirestoreBatchWriteResponse::new(
            0,
            batch_response.write_results,
            batch_response.status,
        ))
    }
}

impl FirestoreDb {
    pub async fn create_simple_batch_writer(&self) -> FirestoreResult<FirestoreSimpleBatchWriter> {
        self.create_simple_batch_writer_with_options(FirestoreSimpleBatchWriteOptions::new())
            .await
    }

    pub async fn create_simple_batch_writer_with_options(
        &self,
        options: FirestoreSimpleBatchWriteOptions,
    ) -> FirestoreResult<FirestoreSimpleBatchWriter> {
        FirestoreSimpleBatchWriter::new(self.clone(), options).await
    }
}
