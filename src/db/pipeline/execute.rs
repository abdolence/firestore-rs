use crate::db::end_at_first_error;
use crate::{
    FirestoreConsistencySelector, FirestoreDb, FirestoreDocument, FirestoreError, FirestoreInstant,
    FirestorePipelineParams, FirestorePipelineSupport, FirestorePipelineWriteParams,
    FirestorePipelineWriteResult, FirestorePipelineWriteTransaction, FirestoreResult,
    FirestoreTransaction, FirestoreTransactionOptions, FirestoreWithPipelineMetadata,
};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{future, StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::execute_pipeline_request::{
    ConsistencySelector, PipelineType,
};
use gcloud_sdk::google::firestore::v1::{ExecutePipelineRequest, Pipeline, StructuredPipeline};
use gcloud_sdk::RequestTimeouts;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::*;

impl FirestoreDb {
    fn create_pipeline_request(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<ExecutePipelineRequest> {
        Ok(ExecutePipelineRequest {
            database: self.get_database_path().clone(),
            auto_commit_transaction: false,
            request_options: self.resolve_request_options(params.request_options.as_ref()),
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(TryInto::try_into)
                .transpose()?,
            pipeline_type: Some(PipelineType::StructuredPipeline(StructuredPipeline {
                options: params
                    .explain_options
                    .as_ref()
                    .map(|options| ("explain_options".to_string(), options.into()))
                    .into_iter()
                    .collect(),
                pipeline: Some(params.pipeline.try_into()?),
            })),
        })
    }

    fn create_pipeline_write_request(
        &self,
        params: FirestorePipelineWriteParams,
    ) -> FirestoreResult<ExecutePipelineRequest> {
        let (consistency_selector, auto_commit_transaction) = match (
            params.transaction,
            &self.session_params.consistency_selector,
        ) {
            (Some(FirestorePipelineWriteTransaction::Existing(transaction_id)), _) => (
                Some(ConsistencySelector::Transaction(transaction_id)),
                false,
            ),
            (_, Some(FirestoreConsistencySelector::Transaction(_))) => {
                return Err(FirestoreError::invalid_parameters(
                    "consistency_selector",
                    "a pipeline write in a transaction runs with execute_in_transaction",
                ))
            }
            (_, Some(FirestoreConsistencySelector::ReadTime(_))) => {
                return Err(FirestoreError::invalid_parameters(
                    "consistency_selector",
                    "Firestore runs no pipeline write at a read time",
                ))
            }
            (Some(FirestorePipelineWriteTransaction::Atomic), None) => (
                Some(ConsistencySelector::NewTransaction(
                    FirestoreTransactionOptions::new().try_into()?,
                )),
                true,
            ),
            (None, None) => (None, false),
        };
        let mut pipeline = Pipeline::try_from(params.pipeline)?;
        pipeline.stages.push(params.write.try_into()?);
        Ok(ExecutePipelineRequest {
            database: self.get_database_path().clone(),
            auto_commit_transaction,
            request_options: self.resolve_request_options(params.request_options.as_ref()),
            consistency_selector,
            pipeline_type: Some(PipelineType::StructuredPipeline(StructuredPipeline {
                options: HashMap::new(),
                pipeline: Some(pipeline),
            })),
        })
    }
}

#[async_trait]
impl FirestorePipelineSupport for FirestoreDb {
    async fn query_pipeline_doc(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<Vec<FirestoreDocument>> {
        self.stream_pipeline_doc_with_errors(params)
            .await?
            .try_collect()
            .await
    }

    async fn stream_pipeline_doc<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreDocument>> {
        Ok(without_errors(
            self.stream_pipeline_doc_with_errors(params).await?,
        ))
    }

    async fn stream_pipeline_doc_with_errors<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        Ok(self
            .stream_pipeline_doc_with_metadata(params)
            .await?
            .try_filter_map(|item| future::ready(Ok(item.document)))
            .boxed())
    }

    async fn stream_pipeline_doc_with_metadata<'b>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<
        BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<FirestoreDocument>>>,
    > {
        let span = span!(
            Level::DEBUG,
            "Firestore Pipeline",
            "/firestore/response_time" = field::Empty
        );
        let request = self.create_pipeline_request(params)?;
        let begin_utc = FirestoreInstant::now();

        let responses = end_at_first_error(
            self.retry_read(
                &span,
                "execute pipeline",
                &request,
                RequestTimeouts::with_idle,
                |mut client, request| async move { client.execute_pipeline(request).await },
            )
            .await?,
        );

        let duration = FirestoreInstant::now().duration_since(begin_utc);
        span.record("/firestore/response_time", duration.as_millis());
        span.in_scope(|| {
            debug!(
                duration_milliseconds = duration.as_millis(),
                "Started a pipeline stream."
            );
        });

        Ok(responses
            .map_err(FirestoreError::from)
            .and_then(|response| {
                future::ready(FirestoreWithPipelineMetadata::from_response(response))
            })
            .map_ok(|items| stream::iter(items.into_iter().map(Ok)))
            .try_flatten()
            .boxed())
    }

    async fn query_pipeline_obj<T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        self.query_pipeline_doc(params)
            .await?
            .iter()
            .map(Self::deserialize_doc_to)
            .collect()
    }

    async fn stream_pipeline_obj<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        Ok(without_errors(
            self.stream_pipeline_obj_with_errors(params).await?,
        ))
    }

    async fn stream_pipeline_obj_with_errors<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        Ok(self
            .stream_pipeline_doc_with_errors(params)
            .await?
            .and_then(|document| future::ready(Self::deserialize_doc_to(&document)))
            .boxed())
    }

    async fn stream_pipeline_obj_with_metadata<'b, T>(
        &self,
        params: FirestorePipelineParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<T>>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        Ok(self
            .stream_pipeline_doc_with_metadata(params)
            .await?
            .and_then(|item| {
                future::ready(
                    item.document
                        .as_ref()
                        .map(Self::deserialize_doc_to)
                        .transpose()
                        .map(|document| FirestoreWithPipelineMetadata {
                            document,
                            metadata: item.metadata,
                        }),
                )
            })
            .boxed())
    }

    async fn execute_pipeline_write(
        &self,
        params: FirestorePipelineWriteParams,
    ) -> FirestoreResult<FirestorePipelineWriteResult> {
        let standalone = params.transaction.is_none();
        let map_status = match params.transaction {
            Some(FirestorePipelineWriteTransaction::Atomic) => FirestoreError::from_commit_status,
            _ => FirestoreError::from,
        };
        let request = self.create_pipeline_write_request(params)?;
        let sent = self.send_pipeline_write(request, map_status).await;
        if standalone {
            sent.map_err(FirestoreError::without_retry)
        } else {
            sent
        }
    }

    async fn execute_pipeline_write_in_transaction(
        &self,
        params: FirestorePipelineWriteParams,
        transaction: &mut FirestoreTransaction<'_>,
    ) -> FirestoreResult<FirestorePipelineWriteResult> {
        let transaction_id = transaction.transaction_data().transaction_id().clone();
        let request = self.create_pipeline_write_request(
            params.with_transaction(FirestorePipelineWriteTransaction::Existing(transaction_id)),
        )?;
        self.send_pipeline_write(request, FirestoreError::from)
            .await
    }
}

impl FirestoreDb {
    /// Sends a pipeline write once, never retrying it, and converts a failed status, whether of
    /// the call or of a response in its stream, with `map_status`.
    async fn send_pipeline_write(
        &self,
        request: ExecutePipelineRequest,
        map_status: fn(gcloud_sdk::tonic::Status) -> FirestoreError,
    ) -> FirestoreResult<FirestorePipelineWriteResult> {
        let span = span!(
            Level::DEBUG,
            "Firestore Pipeline Write",
            "/firestore/response_time" = field::Empty
        );
        let begin_utc = FirestoreInstant::now();

        let responses = self
            .client()
            .get()
            .execute_pipeline(self.request_with_timeout(request, RequestTimeouts::with_idle))
            .await
            .map_err(map_status)?
            .into_inner();
        let result = FirestorePipelineWriteResult::try_from(
            end_at_first_error(responses)
                .try_collect::<Vec<_>>()
                .await
                .map_err(map_status)?,
        )?;

        let duration = FirestoreInstant::now().duration_since(begin_utc);
        span.record("/firestore/response_time", duration.as_millis());
        span.in_scope(|| {
            debug!(
                duration_milliseconds = duration.as_millis(),
                documents_modified = result.documents_modified,
                "Ran a pipeline write."
            );
        });
        Ok(result)
    }
}

/// `stream` with every error logged and left out, for the streams that yield bare values.
fn without_errors<'b, T: Send + 'b>(stream: BoxStream<'b, FirestoreResult<T>>) -> BoxStream<'b, T> {
    stream
        .filter_map(|item| {
            future::ready(match item {
                Ok(value) => Some(value),
                Err(err) => {
                    error!(%err, "Error occurred while consuming a pipeline stream.");
                    None
                }
            })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::db::fake_firestore::{begin_response, FakeFirestore, FakeResponse};
    use crate::db::pipeline::fixtures::{fixture_case, request, responses};
    use crate::timestamp_utils::{from_timestamp, to_timestamp};
    use crate::{
        FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreExplainOptions,
        FirestoreInstant, FirestorePipelineWriteResult, FirestoreReference, FirestoreResult,
        FirestoreTransactionId,
    };
    use futures::TryStreamExt;
    use gcloud_sdk::google::firestore::v1::execute_pipeline_request::ConsistencySelector;
    use gcloud_sdk::google::firestore::v1::{ExecutePipelineRequest, RequestOptions};
    use gcloud_sdk::prost::Message;
    use gcloud_sdk::tonic::Code;
    use serde::Serialize;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// A server that answers every pipeline with the responses captured in `fixture`, and the
    /// requests it received. It also opens, commits and rolls back transactions.
    async fn replaying(
        fixture: &serde_json::Value,
    ) -> (FakeFirestore, Arc<Mutex<Vec<ExecutePipelineRequest>>>) {
        let messages: Vec<Vec<u8>> = responses(fixture)
            .iter()
            .map(Message::encode_to_vec)
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let begins = AtomicU8::new(0);
        let server = FakeFirestore::start(move |method, body| {
            if method.ends_with("/BeginTransaction") {
                return begin_response(&begins);
            }
            if method.ends_with("/Commit") {
                return ("Commit".to_string(), FakeResponse::committed());
            }
            if method.ends_with("/Rollback") {
                return ("Rollback".to_string(), FakeResponse::empty());
            }
            received
                .lock()
                .unwrap()
                .push(ExecutePipelineRequest::decode(body).unwrap());
            (
                "ExecutePipeline".to_string(),
                FakeResponse::Paced(messages.clone(), Duration::ZERO),
            )
        })
        .await;
        (server, requests)
    }

    /// A row of `dml-insert.json#by_name`.
    #[derive(Serialize)]
    struct DmlRow {
        #[serde(rename = "__name__")]
        name: FirestoreReference,
        title: &'static str,
        n: i64,
    }

    /// How a write runs.
    #[derive(Clone, Copy, Debug)]
    enum WriteRun {
        Execute,
        Atomic,
        InTransaction,
    }

    /// Runs the insert of `dml-insert.json#by_name` on `db` as `run` says, and returns its result
    /// with the ID of the transaction it ran in.
    async fn insert_by_name(
        db: &FirestoreDb,
        run: WriteRun,
    ) -> (
        FirestoreResult<FirestorePipelineWriteResult>,
        Option<FirestoreTransactionId>,
    ) {
        let documents = "projects/latestbit/databases/firestore-rs-enterprise/documents";
        let insert = db.fluent().pipeline().insert().literals([
            DmlRow {
                name: FirestoreReference(format!("{documents}/pipeline-dml/doc-1")),
                title: "one",
                n: 1,
            },
            DmlRow {
                name: FirestoreReference(format!("{documents}/pipeline-dml/doc-2")),
                title: "two",
                n: 2,
            },
        ]);
        match run {
            WriteRun::Execute => (insert.execute().await, None),
            WriteRun::Atomic => (insert.atomic().execute().await, None),
            WriteRun::InTransaction => {
                let mut transaction = db.begin_transaction().await.unwrap();
                let result = insert.execute_in_transaction(&mut transaction).await;
                let transaction_id = transaction.transaction_id().clone();
                transaction.rollback().await.unwrap();
                (result, Some(transaction_id))
            }
        }
    }

    #[tokio::test]
    async fn a_write_request_follows_the_session_selector_and_the_run() {
        let by_name = fixture_case("dml-insert", Some("by_name"));
        let auto_commit = request(&fixture_case(
            "dml-transactions",
            Some("auto_commit_new_read_write"),
        ));
        let (server, requests) = replaying(&by_name).await;
        let read_time: FirestoreInstant = "2026-11-04T18:30:00Z".parse().unwrap();
        let in_transaction = server
            .db
            .clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(vec![42]));
        let at_read_time = server
            .db
            .clone_with_consistency_selector(FirestoreConsistencySelector::ReadTime(read_time));

        for (session, db, run, accepted) in [
            ("no selector", &server.db, WriteRun::Execute, true),
            ("no selector", &server.db, WriteRun::Atomic, true),
            ("no selector", &server.db, WriteRun::InTransaction, true),
            ("transaction", &in_transaction, WriteRun::Execute, false),
            ("transaction", &in_transaction, WriteRun::Atomic, false),
            ("read time", &at_read_time, WriteRun::Execute, false),
            ("read time", &at_read_time, WriteRun::Atomic, false),
            ("read time", &at_read_time, WriteRun::InTransaction, true),
        ] {
            let (result, transaction_id) = insert_by_name(db, run).await;
            let sent: Vec<_> = requests.lock().unwrap().drain(..).collect();

            if !accepted {
                assert!(
                    matches!(result, Err(FirestoreError::InvalidParametersError(_))),
                    "{session}, {run:?}: {result:?}"
                );
                assert!(sent.is_empty(), "{session}, {run:?}: {sent:?}");
                continue;
            }
            let (consistency_selector, auto_commit_transaction) = match (run, transaction_id) {
                (_, Some(transaction_id)) => (
                    Some(ConsistencySelector::Transaction(transaction_id)),
                    false,
                ),
                (WriteRun::Atomic, None) => (
                    auto_commit.consistency_selector.clone(),
                    auto_commit.auto_commit_transaction,
                ),
                _ => (None, false),
            };
            let expected = ExecutePipelineRequest {
                database: "projects/fake-firestore/databases/(default)".to_string(),
                consistency_selector,
                auto_commit_transaction,
                ..request(&by_name)
            };
            assert_eq!(
                result.unwrap(),
                FirestorePipelineWriteResult::new(2),
                "{session}, {run:?}"
            );
            assert_eq!(sent, [expected], "{session}, {run:?}");
        }
    }

    /// A server that opens and rolls back transactions and fails every pipeline with `code`.
    async fn failing_pipelines(code: Code) -> FakeFirestore {
        let begins = AtomicU8::new(0);
        FakeFirestore::start(move |method, _| {
            if method.ends_with("/BeginTransaction") {
                return begin_response(&begins);
            }
            if method.ends_with("/Rollback") {
                return ("Rollback".to_string(), FakeResponse::empty());
            }
            ("ExecutePipeline".to_string(), FakeResponse::Status(code))
        })
        .await
    }

    #[tokio::test]
    async fn a_failed_write_is_sent_once_and_flags_retry_by_its_run() {
        for (run, code, retryable) in [
            (WriteRun::Execute, Code::Unavailable, false),
            (WriteRun::Execute, Code::ResourceExhausted, false),
            (WriteRun::Atomic, Code::Unavailable, false),
            (WriteRun::Atomic, Code::ResourceExhausted, false),
            (WriteRun::Atomic, Code::Aborted, true),
            (WriteRun::InTransaction, Code::Unavailable, true),
        ] {
            let server = failing_pipelines(code).await;
            let delete = server.db.fluent().pipeline().delete().from("pipeline-dml");

            let result = match run {
                WriteRun::Execute => delete.execute().await,
                WriteRun::Atomic => delete.atomic().execute().await,
                WriteRun::InTransaction => {
                    let mut transaction = server.db.begin_transaction().await.unwrap();
                    let result = delete.execute_in_transaction(&mut transaction).await;
                    transaction.rollback().await.unwrap();
                    result
                }
            };

            assert!(
                matches!(&result, Err(FirestoreError::DatabaseError(error)) if error.retry_possible == retryable),
                "{run:?} {code:?}: {result:?}"
            );
            let sent = server
                .calls()
                .iter()
                .filter(|call| *call == "ExecutePipeline")
                .count();
            assert_eq!(sent, 1, "{run:?} {code:?}");
        }
    }

    #[test]
    fn firestore_refuses_a_commit_that_mixes_pipeline_and_buffered_writes() {
        let captured = fixture_case("core-rpc/commit-mixing-pipeline-and-classic-writes", None);

        assert_eq!(captured["response"]["error"]["status"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn a_request_carries_the_session_selector_and_the_pipeline_options() {
        let read_time: FirestoreInstant = "2026-11-04T18:00:00Z".parse().unwrap();
        for (explain_options, case) in [
            (
                FirestoreExplainOptions::new().with_analyze(true),
                "analyze-text",
            ),
            (FirestoreExplainOptions::new(), "explain-text"),
        ] {
            let fixture = fixture_case("explain-modes", Some(case));
            let (server, requests) = replaying(&fixture).await;
            let db = server
                .db
                .clone_with_consistency_selector(FirestoreConsistencySelector::ReadTime(read_time));

            db.fluent()
                .pipeline()
                .select()
                .from("pipeline-books")
                .filter(|q| q.field("genre").eq("Dystopian"))
                .sort(|e| [e.field("rating").desc()])
                .explain_with_options(explain_options)
                .request_tags(["pipelines"])
                .query()
                .await
                .unwrap();

            let expected = ExecutePipelineRequest {
                database: "projects/fake-firestore/databases/(default)".to_string(),
                consistency_selector: Some(ConsistencySelector::ReadTime(to_timestamp(read_time))),
                request_options: Some(RequestOptions {
                    request_tags: vec!["pipelines".to_string()],
                }),
                ..request(&fixture)
            };
            assert_eq!(*requests.lock().unwrap(), [expected], "{case}");
        }
    }

    #[tokio::test]
    async fn responses_flatten_into_documents_and_a_closing_metadata_item() {
        for (fixture_name, documents, closing_stats) in [
            ("stream-batches", 200, None),
            ("explain-multi-message", 200, Some(true)),
            ("stage-empty-result", 0, Some(false)),
        ] {
            let fixture = fixture_case(fixture_name, None);
            let execution_time = responses(&fixture)[0]
                .execution_time
                .map(from_timestamp)
                .transpose()
                .unwrap();
            let (server, _) = replaying(&fixture).await;

            let mut items: Vec<_> = server
                .db
                .fluent()
                .pipeline()
                .select()
                .from("pipeline-books")
                .stream_query_with_metadata()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();

            let closing = closing_stats.map(|stats| {
                let item = items
                    .pop()
                    .expect("a document-less item must close the stream");
                (stats, item)
            });
            assert_eq!(items.len(), documents, "{fixture_name}");
            assert!(
                items.iter().all(|item| item.document.is_some()
                    && item.metadata.execution_time == execution_time
                    && item.metadata.explain_stats.is_none()),
                "{fixture_name}"
            );
            if let Some((stats, item)) = closing {
                assert!(item.document.is_none(), "{fixture_name}");
                assert_eq!(
                    item.metadata.execution_time, execution_time,
                    "{fixture_name}"
                );
                assert_eq!(
                    item.metadata.explain_stats.is_some(),
                    stats,
                    "{fixture_name}"
                );
            }
        }
    }
}
