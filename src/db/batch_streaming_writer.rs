use crate::{
    FirestoreBatch, FirestoreBatchWriteResponse, FirestoreBatchWriter, FirestoreDb,
    FirestoreRequestOptions, FirestoreResult, FirestoreWriteResult,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::{Write, WriteRequest};
use rsb_derive::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use crate::timestamp_utils::from_timestamp;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreStreamingBatchWriteOptions {
    #[default = "Duration::from_millis(500)"]
    pub throttle_batch_duration: Duration,

    /// Request options (e.g. request tags) for the streaming write requests.
    pub request_options: Option<FirestoreRequestOptions>,
}

pub struct FirestoreStreamingBatchWriter {
    pub db: FirestoreDb,
    pub options: FirestoreStreamingBatchWriteOptions,
    pub batch_span: Span,
    finished: Arc<AtomicBool>,
    writer: UnboundedSender<WriteRequest>,
    thread: Option<JoinHandle<()>>,
    last_token: Arc<RwLock<Vec<u8>>>,
    sent_counter: Arc<AtomicU64>,
    received_counter: Arc<AtomicU64>,
    init_wait_reader: UnboundedReceiver<()>,
}

impl Drop for FirestoreStreamingBatchWriter {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.batch_span
                .in_scope(|| warn!("Batch was not finished."));
        }
    }
}

impl FirestoreStreamingBatchWriter {
    /// Opens a streaming batch writer against `db` and returns it with the stream of per-batch
    /// responses.
    ///
    /// Most callers get one from [`FirestoreDb::create_streaming_batch_writer`] or
    /// [`FirestoreDb::create_streaming_batch_writer_with_options`] rather than calling this
    /// directly. This backs Firestore's streaming `Write` RPC: writes queued through
    /// [`new_batch`](Self::new_batch) and [`FirestoreBatch::write`] go over one long-lived stream
    /// instead of one request per batch, and `options.throttle_batch_duration` - 500ms by
    /// default - paces how often a batch is flushed onto the wire, which is what keeps a fast
    /// producer under Firestore's per-stream write-rate limit; a
    /// [`FirestoreSimpleBatchWriter`](crate::FirestoreSimpleBatchWriter) has no such limit to
    /// manage and is the simpler choice unless batches are being sent back-to-back. The returned
    /// stream yields one [`FirestoreBatchWriteResponse`] per batch, in send order; consume it
    /// (for example from a spawned task) or responses back up unread. Call
    /// [`finish`](Self::finish) when done - dropping the writer instead only logs a warning and
    /// leaves the stream's background task running until it notices.
    ///
    /// Returns an error if the initial handshake with the `Write` RPC fails.
    pub async fn new<'b>(
        db: FirestoreDb,
        options: FirestoreStreamingBatchWriteOptions,
    ) -> FirestoreResult<(
        FirestoreStreamingBatchWriter,
        BoxStream<'b, FirestoreResult<FirestoreBatchWriteResponse>>,
    )> {
        let batch_span = span!(Level::DEBUG, "Firestore Batch Write");

        let (requests_writer, requests_receiver) = mpsc::unbounded_channel::<WriteRequest>();
        let (responses_writer, responses_receiver) =
            mpsc::unbounded_channel::<FirestoreResult<FirestoreBatchWriteResponse>>();
        let (init_wait_sender, mut init_wait_reader) = mpsc::unbounded_channel::<()>();

        let finished = Arc::new(AtomicBool::new(false));
        let thread_finished = finished.clone();

        let sent_counter = Arc::new(AtomicU64::new(0));
        let thread_sent_counter = sent_counter.clone();

        let received_counter = Arc::new(AtomicU64::new(0));
        let thread_received_counter = received_counter.clone();

        let last_token: Arc<RwLock<Vec<u8>>> = Arc::new(RwLock::new(vec![]));
        let thread_last_token = last_token.clone();

        let mut thread_db_client = db.client().get();
        let thread_options = options.clone();

        let thread = tokio::spawn(async move {
            let stream = {
                use tokio_stream::StreamExt;
                tokio_stream::wrappers::UnboundedReceiverStream::new(requests_receiver)
                    .throttle(thread_options.throttle_batch_duration)
            };
            match thread_db_client.write(stream).await {
                Ok(response) => {
                    let mut response_stream = response.into_inner().boxed();
                    loop {
                        let response_result = response_stream.try_next().await;
                        let received_counter = thread_received_counter.load(Ordering::Relaxed);

                        match response_result {
                            Ok(Some(response)) => {
                                {
                                    let mut locked = thread_last_token.write().await;
                                    *locked = response.stream_token;
                                }

                                if received_counter == 0 {
                                    init_wait_sender.send(()).ok();
                                } else {
                                    let write_results: FirestoreResult<Vec<FirestoreWriteResult>> =
                                        response
                                            .write_results
                                            .into_iter()
                                            .map(|s| s.try_into())
                                            .collect();

                                    match write_results {
                                        Ok(write_results) => {
                                            responses_writer
                                                .send(Ok(FirestoreBatchWriteResponse::new(
                                                    received_counter - 1,
                                                    write_results,
                                                    vec![],
                                                )
                                                .opt_commit_time(
                                                    response
                                                        .commit_time
                                                        .and_then(|ts| from_timestamp(ts).ok()),
                                                )))
                                                .ok();
                                        }
                                        Err(err) => {
                                            error!(
                                                %err,
                                                received_counter,
                                                "Batch write operation failed.",
                                            );
                                            responses_writer.send(Err(err)).ok();
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                responses_writer
                                    .send(Ok(FirestoreBatchWriteResponse::new(
                                        received_counter - 1,
                                        vec![],
                                        vec![],
                                    )))
                                    .ok();
                                break;
                            }
                            Err(err) if err.code() == gcloud_sdk::tonic::Code::Cancelled => {
                                debug!(received_counter, "Batch write operation finished.");
                                responses_writer
                                    .send(Ok(FirestoreBatchWriteResponse::new(
                                        received_counter - 1,
                                        vec![],
                                        vec![],
                                    )))
                                    .ok();
                                break;
                            }
                            Err(err) => {
                                error!(
                                    %err,
                                    received_counter,
                                    "Batch write operation failed.",
                                );
                                responses_writer.send(Err(err.into())).ok();
                                break;
                            }
                        }

                        {
                            let _locked = thread_last_token.read().await;
                            if thread_finished.load(Ordering::Relaxed)
                                && thread_sent_counter.load(Ordering::Relaxed) == received_counter
                            {
                                init_wait_sender.send(()).ok();
                                break;
                            }
                        }

                        thread_received_counter.fetch_add(1, Ordering::Relaxed);
                    }

                    {
                        let _locked = thread_last_token.write().await;
                        thread_finished.store(true, Ordering::Relaxed);
                        init_wait_sender.send(()).ok();
                    }
                }
                Err(err) => {
                    error!(
                        %err,
                        "Batch write operation failed.",
                    );
                    responses_writer.send(Err(err.into())).ok();
                }
            }
        });

        requests_writer.send(WriteRequest {
            database: db.get_database_path().to_string(),
            stream_id: "".to_string(),
            writes: vec![],
            stream_token: vec![],
            labels: HashMap::new(),
            request_options: db.resolve_request_options(options.request_options.as_ref()),
        })?;

        init_wait_reader.recv().await;

        let responses_stream =
            tokio_stream::wrappers::UnboundedReceiverStream::new(responses_receiver).boxed();

        Ok((
            Self {
                db,
                options,
                batch_span,
                finished,
                writer: requests_writer,
                thread: Some(thread),
                last_token,
                sent_counter,
                received_counter,
                init_wait_reader,
            },
            responses_stream,
        ))
    }

    /// Waits for every sent write's response, then closes the stream and joins its background
    /// task.
    ///
    /// Always call this instead of letting the writer drop. It waits for pending responses to
    /// arrive, so the response stream returned by [`new`](Self::new) must already be consumed
    /// (for example from a spawned task) before calling this, or it blocks forever waiting for
    /// responses nobody is reading.
    pub async fn finish(mut self) {
        let locked = self.last_token.write().await;

        if !self.finished.load(Ordering::Relaxed) {
            self.finished.store(true, Ordering::Relaxed);

            if self.sent_counter.load(Ordering::Relaxed)
                > self.received_counter.load(Ordering::Relaxed) - 1
            {
                drop(locked);
                debug!("Still waiting to receive responses for batch writes.");
                self.init_wait_reader.recv().await;
            } else {
                drop(locked);
            }

            self.writer
                .send(WriteRequest {
                    database: self.db.get_database_path().to_string(),
                    stream_id: "".to_string(),
                    writes: vec![],
                    stream_token: {
                        let locked = self.last_token.read().await;
                        locked.clone()
                    },
                    labels: HashMap::new(),
                    request_options: self
                        .db
                        .resolve_request_options(self.options.request_options.as_ref()),
                })
                .ok();
        } else {
            drop(locked);
        }

        if let Some(thread) = self.thread.take() {
            let _ = tokio::join!(thread);
        }
    }

    async fn write_iterator<I>(&self, writes: I) -> FirestoreResult<()>
    where
        I: IntoIterator,
        I::Item: Into<Write>,
    {
        self.sent_counter.fetch_add(1, Ordering::Relaxed);

        Ok(self.writer.send(WriteRequest {
            database: self.db.get_database_path().to_string(),
            stream_id: "".to_string(),
            writes: writes.into_iter().map(|write| write.into()).collect(),
            stream_token: {
                let locked = self.last_token.read().await;
                locked.clone()
            },
            labels: HashMap::new(),
            request_options: self
                .db
                .resolve_request_options(self.options.request_options.as_ref()),
        })?)
    }

    /// Starts a new batch of writes against this writer.
    ///
    /// Queue writes on it with [`FirestoreBatch`] methods such as `update_object`, then send it
    /// with [`FirestoreBatch::write`].
    pub fn new_batch(&self) -> FirestoreBatch<'_, FirestoreStreamingBatchWriter> {
        FirestoreBatch::new(&self.db, self)
    }
}

#[async_trait]
impl FirestoreBatchWriter for FirestoreStreamingBatchWriter {
    type WriteResult = ();

    async fn write(&self, writes: Vec<Write>) -> FirestoreResult<()> {
        self.write_iterator(writes).await
    }
}

impl FirestoreDb {
    /// Opens a streaming batch writer with default options.
    ///
    /// Equivalent to
    /// `create_streaming_batch_writer_with_options(FirestoreStreamingBatchWriteOptions::new())`.
    /// See [`FirestoreStreamingBatchWriter::new`] for the throttling behavior, the response
    /// stream and the failure mode, and prefer
    /// [`create_simple_batch_writer`](Self::create_simple_batch_writer) unless enough batches are
    /// sent back-to-back that one `BatchWrite` request per batch becomes the bottleneck.
    ///
    /// # Examples
    /// ```rust,no_run
    /// use firestore::*;
    /// use futures::TryStreamExt;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Clone, Deserialize, Serialize)]
    /// struct MyTestStructure {
    ///     some_id: String,
    /// }
    ///
    /// # async fn example(db: FirestoreDb) -> FirestoreResult<()> {
    /// let (batch_writer, mut responses) = db.create_streaming_batch_writer().await?;
    ///
    /// let responses_task = tokio::spawn(async move {
    ///     while let Ok(Some(response)) = responses.try_next().await {
    ///         println!("{response:?}");
    ///     }
    /// });
    ///
    /// let mut current_batch = batch_writer.new_batch();
    /// for idx in 0..1000 {
    ///     let doc = MyTestStructure {
    ///         some_id: format!("doc-{idx}"),
    ///     };
    ///
    ///     db.fluent()
    ///         .update()
    ///         .in_col("my-collection")
    ///         .document_id(&doc.some_id)
    ///         .object(&doc)
    ///         .add_to_batch(&mut current_batch)?;
    ///
    ///     if idx % 100 == 0 {
    ///         current_batch.write().await?;
    ///         current_batch = batch_writer.new_batch();
    ///     }
    /// }
    ///
    /// batch_writer.finish().await;
    /// let _ = tokio::join!(responses_task);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_streaming_batch_writer<'b>(
        &self,
    ) -> FirestoreResult<(
        FirestoreStreamingBatchWriter,
        BoxStream<'b, FirestoreResult<FirestoreBatchWriteResponse>>,
    )> {
        self.create_streaming_batch_writer_with_options(FirestoreStreamingBatchWriteOptions::new())
            .await
    }

    /// Opens a streaming batch writer with explicit `options`.
    ///
    /// Most usefully `options.throttle_batch_duration`, which paces how often queued writes are
    /// flushed onto Firestore's streaming `Write` RPC to stay under its per-stream write-rate
    /// limit; see [`FirestoreStreamingBatchWriter::new`] for the full behavior.
    ///
    /// Returns an error if the initial handshake with the `Write` RPC fails.
    pub async fn create_streaming_batch_writer_with_options<'b>(
        &self,
        options: FirestoreStreamingBatchWriteOptions,
    ) -> FirestoreResult<(
        FirestoreStreamingBatchWriter,
        BoxStream<'b, FirestoreResult<FirestoreBatchWriteResponse>>,
    )> {
        FirestoreStreamingBatchWriter::new(self.clone(), options).await
    }
}
