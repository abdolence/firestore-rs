use crate::errors::*;
use crate::{
    FirestoreBatch, FirestoreBatchWriteResponse, FirestoreBatchWriter, FirestoreDb,
    FirestoreResult, FirestoreWriteResult,
};
use async_trait::async_trait;
use futures::TryFutureExt;
use gcloud_sdk::google::firestore::v1::{BatchWriteRequest, Write};
use rsb_derive::*;
use std::collections::HashMap;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreSimpleBatchWriteOptions {
    retry_max_elapsed_time: Option<chrono::Duration>,
}

pub struct FirestoreSimpleBatchWriter {
    pub db: FirestoreDb,
    pub options: FirestoreSimpleBatchWriteOptions,
    pub batch_span: Span,
}

impl FirestoreSimpleBatchWriter {
    pub async fn new(
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
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(
                self.options
                    .retry_max_elapsed_time
                    .map(|v| v.to_std())
                    .transpose()?,
            )
            .build();

        let request = BatchWriteRequest {
            database: self.db.get_database_path().to_string(),
            writes,
            labels: HashMap::new(),
        };

        backoff::future::retry(backoff, || {
            async {
                let response = self
                    .db
                    .client()
                    .get()
                    .batch_write(request.clone())
                    .await
                    .map_err(FirestoreError::from)?;

                let batch_response = response.into_inner();

                let write_results: FirestoreResult<Vec<FirestoreWriteResult>> = batch_response
                    .write_results
                    .into_iter()
                    .map(|s| s.try_into())
                    .collect();

                Ok(FirestoreBatchWriteResponse::new(
                    0,
                    write_results?,
                    batch_response.status,
                ))
            }
            .map_err(firestore_err_to_backoff)
        })
        .await
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
