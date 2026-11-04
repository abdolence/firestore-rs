use crate::db::end_at_first_error;
use crate::{
    FirestoreDb, FirestoreDocument, FirestoreError, FirestoreInstant, FirestorePipelineParams,
    FirestorePipelineSupport, FirestoreResult, FirestoreWithPipelineMetadata,
};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{future, StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::execute_pipeline_request::PipelineType;
use gcloud_sdk::google::firestore::v1::{ExecutePipelineRequest, StructuredPipeline};
use gcloud_sdk::RequestTimeouts;
use serde::Deserialize;
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
    use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
    use crate::db::pipeline::fixtures::{fixture_case, request, responses};
    use crate::timestamp_utils::{from_timestamp, to_timestamp};
    use crate::{FirestoreConsistencySelector, FirestoreExplainOptions, FirestoreInstant};
    use futures::TryStreamExt;
    use gcloud_sdk::google::firestore::v1::execute_pipeline_request::ConsistencySelector;
    use gcloud_sdk::google::firestore::v1::{ExecutePipelineRequest, RequestOptions};
    use gcloud_sdk::prost::Message;
    use serde::Deserialize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// A server that answers every pipeline with the responses captured in `fixture`, and the
    /// requests it received.
    async fn replaying(
        fixture: &serde_json::Value,
    ) -> (FakeFirestore, Arc<Mutex<Vec<ExecutePipelineRequest>>>) {
        let messages: Vec<Vec<u8>> = responses(fixture)
            .iter()
            .map(Message::encode_to_vec)
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let server = FakeFirestore::start(move |_, body| {
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

    #[derive(Debug, PartialEq, Deserialize)]
    struct SelectedBook {
        #[serde(alias = "_firestore_id")]
        id: Option<String>,
        title: String,
        author_name: String,
    }

    #[tokio::test]
    async fn a_selected_result_has_no_document_id() {
        let (server, _) = replaying(&fixture_case("stage-select", None)).await;

        let books: Vec<SelectedBook> = server
            .db
            .fluent()
            .pipeline()
            .select()
            .from("pipeline-books")
            .obj()
            .query()
            .await
            .unwrap();

        assert_eq!(
            books,
            [
                SelectedBook {
                    id: None,
                    title: "Untitled draft".to_string(),
                    author_name: "Anonymous".to_string(),
                },
                SelectedBook {
                    id: None,
                    title: "The Hitchhiker's Guide to the Galaxy".to_string(),
                    author_name: "Douglas Adams".to_string(),
                },
            ]
        );
    }
}
