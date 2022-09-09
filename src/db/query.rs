use crate::{FirestoreDb, FirestoreError, FirestoreQueryParams, FirestoreResult};
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

impl FirestoreDb {
    pub async fn query_doc(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<Document>> {
        let collection_str = params.collection_id.to_string();
        let span = span!(
            Level::DEBUG,
            "Firestore Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );
        self.query_doc_with_retries(params, 0, &span).await
    }

    pub async fn stream_query_doc<'b>(
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

    pub async fn stream_query_doc_with_errors<'b>(
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

    pub async fn query_obj<T>(&self, params: FirestoreQueryParams) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_vec = self.query_doc(params).await?;
        doc_vec
            .iter()
            .map(|doc| Self::deserialize_doc_to(doc))
            .collect()
    }

    pub async fn stream_query_obj<'b, T>(
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

    pub async fn stream_query_obj_with_errors<'b, T>(
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

    fn create_query_request(
        &self,
        params: &FirestoreQueryParams,
    ) -> tonic::Request<RunQueryRequest> {
        tonic::Request::new(RunQueryRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            consistency_selector: None,
            query_type: Some(run_query_request::QueryType::StructuredQuery(
                params.to_structured_query(),
            )),
        })
    }

    fn stream_query_doc_with_retries<'a, 'b>(
        &'a self,
        params: FirestoreQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<BoxStream<'b, FirestoreResult<Option<Document>>>>> {
        let query_request = self.create_query_request(&params);
        async move {
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
                        &query_duration.num_milliseconds(),
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
        let query_request = self.create_query_request(&params);
        async move {
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
                        &query_duration.num_milliseconds(),
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
