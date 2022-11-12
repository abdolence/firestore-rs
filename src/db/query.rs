use crate::{
    FirestoreDb, FirestoreError, FirestorePartitionQueryParams, FirestoreQueryCursor,
    FirestoreQueryParams, FirestoreResult,
};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::FutureExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use serde::Deserialize;
use tracing::*;

#[async_trait]
pub trait FirestoreQuerySupport {
    async fn query_doc(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<Document>>;

    async fn stream_query_doc<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_query_doc_with_errors<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn query_obj<T>(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>;
    async fn stream_query_obj<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>;
    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;

    fn stream_partition_cursors_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<FirestoreResult<BoxStream<FirestoreResult<FirestoreQueryCursor>>>>;

    fn stream_partition_docs_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<FirestoreResult<BoxStream<Vec<FirestoreResult<FirestoreQueryCursor>>>>>;
}

impl FirestoreDb {
    fn create_query_request(
        &self,
        params: &FirestoreQueryParams,
    ) -> FirestoreResult<tonic::Request<RunQueryRequest>> {
        Ok(tonic::Request::new(RunQueryRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
            query_type: Some(run_query_request::QueryType::StructuredQuery(params.into())),
        }))
    }

    fn stream_query_doc_with_retries<'a, 'b>(
        &'a self,
        params: FirestoreQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<BoxStream<'b, FirestoreResult<Option<Document>>>>> {
        async move {
            let query_request = self.create_query_request(&params)?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client
                .get()
                .run_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream = query_response
                        .into_inner()
                        .map_ok(|r| r.document)
                        .map_err(|e| e.into())
                        .boxed();

                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

                    span.record(
                        "/firestore/response_time",
                        query_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            "[DB]: Querying stream of documents in {:?} took {}ms",
                            params.collection_id,
                            query_duration.num_milliseconds()
                        );
                    });

                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );

                        self.stream_query_doc_with_retries(params, retries + 1, span)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    fn query_doc_with_retries<'a>(
        &'a self,
        params: FirestoreQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<Vec<Document>>> {
        async move {
            let query_request = self.create_query_request(&params)?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client
                .get()
                .run_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream = query_response
                        .into_inner()
                        .map_ok(|rs| rs.document)
                        .try_collect::<Vec<Option<Document>>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect();
                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

                    span.record(
                        "/firestore/response_time",
                        query_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            "[DB]: Querying documents in {:?} took {}ms",
                            params.collection_id,
                            query_duration.num_milliseconds()
                        );
                    });

                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );
                        self.query_doc_with_retries(params, retries + 1, span).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }
}

#[async_trait]
impl FirestoreQuerySupport for FirestoreDb {
    async fn query_doc(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<Document>> {
        let collection_str = params.collection_id.to_string();
        let span = span!(
            Level::DEBUG,
            "Firestore Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );
        self.query_doc_with_retries(params, 0, &span).await
    }

    async fn stream_query_doc<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        let collection_str = params.collection_id.to_string();

        let span = span!(
            Level::DEBUG,
            "Firestore Streaming Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );

        let doc_stream = self.stream_query_doc_with_retries(params, 0, &span).await?;

        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(err) => {
                    error!("[DB] Error occurred while consuming query: {}", err);
                    None
                }
            })
        })))
    }

    async fn stream_query_doc_with_errors<'b>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        let collection_str = params.collection_id.to_string();

        let span = span!(
            Level::DEBUG,
            "Firestore Streaming Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );

        let doc_stream = self.stream_query_doc_with_retries(params, 0, &span).await?;

        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(Some(doc)) => Some(Ok(doc)),
                Ok(None) => None,
                Err(err) => {
                    error!("[DB] Error occurred while consuming query: {}", err);
                    Some(Err(err))
                }
            })
        })))
    }

    async fn query_obj<T>(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_vec = self.query_doc(params).await?;
        doc_vec
            .iter()
            .map(|doc| Self::deserialize_doc_to(doc))
            .collect()
    }

    async fn stream_query_obj<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_stream = self.stream_query_doc(params).await?;
        Ok(Box::pin(doc_stream.filter_map(|doc| async move {
            match Self::deserialize_doc_to::<T>(&doc) {
                Ok(obj) => Some(obj),
                Err(err) => {
                    error!(
                        "[DB] Error occurred while consuming query document as a stream: {}",
                        err
                    );
                    None
                }
            }
        })))
    }

    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        let doc_stream = self.stream_query_doc_with_errors(params).await?;
        Ok(Box::pin(doc_stream.and_then(|doc| {
            future::ready(Self::deserialize_doc_to::<T>(&doc))
        })))
    }

    fn stream_partition_cursors_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<FirestoreResult<BoxStream<FirestoreResult<FirestoreQueryCursor>>>> {
        Box::pin(async move {
            let consistency_selector: Option<
                gcloud_sdk::google::firestore::v1::partition_query_request::ConsistencySelector,
            > = self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?;

            let stream: BoxStream<FirestoreResult<FirestoreQueryCursor>> = futures::stream::unfold(
                Some((params, consistency_selector)),
                move |maybe_params| async move {
                    if let Some((params, maybe_consistency_selector)) = maybe_params {
                        let request = tonic::Request::new(PartitionQueryRequest {
                            page_size: params.page_size as i32,
                            partition_count: params.partition_count as i64,
                            parent: params
                                .query_params
                                .parent
                                .as_ref()
                                .unwrap_or_else(|| self.get_documents_path())
                                .clone(),
                            consistency_selector: maybe_consistency_selector.clone(),
                            query_type: Some(partition_query_request::QueryType::StructuredQuery(
                                params.query_params.to_structured_query(),
                            )),
                            page_token: params.page_token.clone().unwrap_or_default(),
                        });

                        match self.client().get().partition_query(request).await {
                            Ok(response) => {
                                let partition_response = response.into_inner();
                                let firestore_cursors: Vec<FirestoreQueryCursor> =
                                    partition_response
                                        .partitions
                                        .into_iter()
                                        .map(|e| e.into())
                                        .collect();

                                if !partition_response.next_page_token.is_empty() {
                                    Some((
                                        Ok(firestore_cursors),
                                        Some((
                                            params.with_page_token(
                                                partition_response.next_page_token.clone(),
                                            ),
                                            maybe_consistency_selector,
                                        )),
                                    ))
                                } else {
                                    Some((Ok(firestore_cursors), None))
                                }
                            }
                            Err(err) => Some((Err(FirestoreError::from(err)), None)),
                        }
                    } else {
                        None
                    }
                },
            )
            .flat_map(|s| {
                futures::stream::iter(match s {
                    Ok(results) => results
                        .into_iter()
                        .map(|r| Ok::<FirestoreQueryCursor, FirestoreError>(r))
                        .collect(),
                    Err(err) => vec![Err(err)],
                })
            })
            .boxed();

            Ok(stream)
        })
    }

    fn stream_partition_docs_with_errors(
        &self,
        params: FirestorePartitionQueryParams,
    ) -> BoxFuture<FirestoreResult<BoxStream<Vec<FirestoreResult<FirestoreQueryCursor>>>>> {
        todo!()
    }
}
