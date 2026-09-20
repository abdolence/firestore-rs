use crate::errors::*;
use crate::FirestoreDuration;
use crate::{
    FirestoreBatch, FirestoreBatchWriteResponse, FirestoreBatchWriter, FirestoreDb,
    FirestoreRequestOptions, FirestoreResult, FirestoreWriteResult,
};
use async_trait::async_trait;
use futures::TryFutureExt;
use gcloud_sdk::google::firestore::v1::{BatchWriteRequest, Write};
use rsb_derive::*;
use std::collections::HashMap;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreSimpleBatchWriteOptions {
    retry_max_elapsed_time: Option<FirestoreDuration>,

    /// Request options (e.g. request tags) for the batch write requests.
    pub request_options: Option<FirestoreRequestOptions>,
}

pub struct FirestoreSimpleBatchWriter {
    pub db: FirestoreDb,
    pub options: FirestoreSimpleBatchWriteOptions,
    pub batch_span: Span,
}

impl FirestoreSimpleBatchWriter {
    /// Wraps `db` with `options` to write batches through Firestore's non-streaming `BatchWrite`
    /// RPC.
    ///
    /// Most callers get one from [`FirestoreDb::create_simple_batch_writer`] or
    /// [`FirestoreDb::create_simple_batch_writer_with_options`] rather than calling this
    /// directly.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<Self>` for symmetry with
    /// [`FirestoreStreamingBatchWriter::new`](crate::FirestoreStreamingBatchWriter::new), which
    /// can.
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

    /// Starts a new batch of writes against this writer.
    ///
    /// Queue writes on it with [`FirestoreBatch`] methods such as `update_object`, then send it
    /// with [`FirestoreBatch::write`].
    pub fn new_batch(&self) -> FirestoreBatch<'_, FirestoreSimpleBatchWriter> {
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
                    .map(std::time::Duration::try_from)
                    .transpose()?,
            )
            .build();

        let request = BatchWriteRequest {
            database: self.db.get_database_path().to_string(),
            writes,
            labels: HashMap::new(),
            request_options: self
                .db
                .resolve_request_options(self.options.request_options.as_ref()),
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
    /// Opens a batch writer that sends each batch as one Firestore `BatchWrite` request, with
    /// default options.
    ///
    /// Prefer this over [`create_streaming_batch_writer`](Self::create_streaming_batch_writer)
    /// for occasional or moderate-sized batches; reach for the streaming writer only once enough
    /// batches are being sent back-to-back that one request per batch becomes the bottleneck.
    /// Equivalent to
    /// `create_simple_batch_writer_with_options(FirestoreSimpleBatchWriteOptions::new())`.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<FirestoreSimpleBatchWriter>` for symmetry with
    /// [`create_streaming_batch_writer`](Self::create_streaming_batch_writer), which can.
    pub async fn create_simple_batch_writer(&self) -> FirestoreResult<FirestoreSimpleBatchWriter> {
        self.create_simple_batch_writer_with_options(FirestoreSimpleBatchWriteOptions::new())
            .await
    }

    /// Same as [`create_simple_batch_writer`](Self::create_simple_batch_writer), with explicit
    /// `options`.
    ///
    /// Most usefully `options.retry_max_elapsed_time`, which bounds how long a `BatchWrite`
    /// request keeps retrying on a transient failure before giving up.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<FirestoreSimpleBatchWriter>` for symmetry with
    /// [the streaming writer's equivalent](Self::create_streaming_batch_writer_with_options),
    /// which can.
    pub async fn create_simple_batch_writer_with_options(
        &self,
        options: FirestoreSimpleBatchWriteOptions,
    ) -> FirestoreResult<FirestoreSimpleBatchWriter> {
        FirestoreSimpleBatchWriter::new(self.clone(), options).await
    }
}
