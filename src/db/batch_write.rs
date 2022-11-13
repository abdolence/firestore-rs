use crate::db::transaction_ops::UpdateObjectOperation;
use crate::db::DeleteOperation;
use crate::errors::FirestoreError;
use crate::{FirestoreDb, FirestoreResult, FirestoreWritePrecondition};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::{Write, WriteRequest};
use rsb_derive::*;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use tonic::Code;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreBatchWriteOptions {
    #[default = "Duration::from_millis(500)"]
    throttle_batch_duration: Duration,
}

pub struct FirestoreBatchWriter {
    pub db: FirestoreDb,
    pub options: FirestoreBatchWriteOptions,
    pub batch_span: Span,
    finished: Arc<AtomicBool>,
    writer: UnboundedSender<WriteRequest>,
    thread: Option<JoinHandle<()>>,
    last_token: Arc<RwLock<Vec<u8>>>,
    sent_counter: Arc<AtomicU64>,
    received_counter: Arc<AtomicU64>,
    init_wait_reader: UnboundedReceiver<()>,
}

impl Drop for FirestoreBatchWriter {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.batch_span.in_scope(|| warn!("Batch was not finished"));
        }
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreBatchWriteResponse {
    pub position: u64,
    pub write_results: Vec<gcloud_sdk::google::firestore::v1::WriteResult>,
}

impl FirestoreBatchWriter {
    pub async fn new<'b>(
        db: FirestoreDb,
        options: FirestoreBatchWriteOptions,
    ) -> FirestoreResult<(
        FirestoreBatchWriter,
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
                                    responses_writer
                                        .send(Ok(FirestoreBatchWriteResponse::new(
                                            received_counter - 1,
                                            response.write_results,
                                        )))
                                        .ok();
                                }
                            }
                            Ok(None) => {
                                responses_writer
                                    .send(Ok(FirestoreBatchWriteResponse::new(
                                        received_counter - 1,
                                        vec![],
                                    )))
                                    .ok();
                                break;
                            }
                            Err(err) if err.code() == Code::Cancelled => {
                                debug!("Batch write operation finished on: {}", received_counter);
                                responses_writer
                                    .send(Ok(FirestoreBatchWriteResponse::new(
                                        received_counter - 1,
                                        vec![],
                                    )))
                                    .ok();
                                break;
                            }
                            Err(err) => {
                                error!(
                                    "Batch write operation {} failed: {}",
                                    received_counter, err
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
                    error!("Batch write operation failed: {}", err);
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

    pub async fn write<I>(&self, writes: I) -> FirestoreResult<()>
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
        })?)
    }

    pub async fn finish(mut self) {
        let locked = self.last_token.write().await;

        if !self.finished.load(Ordering::Relaxed) {
            self.finished.store(true, Ordering::Relaxed);

            if self.sent_counter.load(Ordering::Relaxed)
                > self.received_counter.load(Ordering::Relaxed) - 1
            {
                drop(locked);
                debug!("Still waiting receiving responses for batch writes");
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
                })
                .ok();
        } else {
            drop(locked);
        }

        if let Some(thread) = self.thread.take() {
            let _ = tokio::join!(thread);
        }
    }

    pub fn new_batch(&self) -> FirestoreBatch {
        FirestoreBatch::new(self)
    }
}

impl FirestoreDb {
    pub async fn create_batch_writer<'a, 'b>(
        &'a self,
    ) -> FirestoreResult<(
        FirestoreBatchWriter,
        BoxStream<'b, FirestoreResult<FirestoreBatchWriteResponse>>,
    )> {
        Self::create_batch_writer_with_options(self, FirestoreBatchWriteOptions::new()).await
    }

    pub async fn create_batch_writer_with_options<'a, 'b>(
        &'a self,
        options: FirestoreBatchWriteOptions,
    ) -> FirestoreResult<(
        FirestoreBatchWriter,
        BoxStream<'b, FirestoreResult<FirestoreBatchWriteResponse>>,
    )> {
        FirestoreBatchWriter::new(self.clone(), options).await
    }
}

pub struct FirestoreBatch<'a> {
    pub writer: &'a FirestoreBatchWriter,
    pub writes: Vec<Write>,
}

impl<'a> FirestoreBatch<'a> {
    pub(crate) fn new(writer: &'a FirestoreBatchWriter) -> Self {
        Self {
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
    pub async fn write(self) -> FirestoreResult<()> {
        self.writer.write(self.writes).await
    }

    pub fn update_object<T, S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.update_object_at(
            self.writer.db.get_documents_path(),
            collection_id,
            document_id,
            obj,
            update_only,
            precondition,
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
            self.writer.db.get_documents_path(),
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
}
