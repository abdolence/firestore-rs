#![allow(clippy::derive_partial_eq_without_eq)] // Since we may not be able to implement Eq for the changes coming from Firestore protos

use crate::{FirestoreDb, FirestoreError, FirestoreQueryParams, FirestoreResult};
use chrono::prelude::*;
use futures::FutureExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use serde::Deserialize;
use tracing::*;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreAggregatedQueryParams {
    pub query_params: FirestoreQueryParams,
    pub aggregations: Vec<FirestoreAggregation>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreAggregation {
    pub alias: String,
    pub operator: Option<FirestoreAggregationOperator>,
}

impl From<&FirestoreAggregation> for structured_aggregation_query::Aggregation {
    fn from(aggregation: &FirestoreAggregation) -> Self {
        structured_aggregation_query::Aggregation {
            alias: aggregation.alias.clone(),
            operator: aggregation.operator.as_ref().map(|agg| agg.into()),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreAggregationOperator {
    Count(FirestoreAggregationOperatorCount),
}

impl From<&FirestoreAggregationOperator> for structured_aggregation_query::aggregation::Operator {
    fn from(op: &FirestoreAggregationOperator) -> Self {
        match op {
            FirestoreAggregationOperator::Count(cnt) => {
                structured_aggregation_query::aggregation::Operator::Count(cnt.into())
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreAggregationOperatorCount {
    pub up_to: Option<usize>,
}

impl From<&FirestoreAggregationOperatorCount> for structured_aggregation_query::aggregation::Count {
    fn from(cnt: &FirestoreAggregationOperatorCount) -> Self {
        structured_aggregation_query::aggregation::Count {
            up_to: cnt.up_to.map(|v| v as i64),
        }
    }
}

impl FirestoreDb {
    pub async fn aggregated_query_doc(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<Document>> {
        let collection_str = params.query_params.collection_id.to_string();

        let span = span!(
            Level::DEBUG,
            "Firestore Aggregated Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );
        self.aggregated_query_doc_with_retries(params, 0, &span)
            .await
    }

    pub async fn stream_aggregated_query_doc<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        let collection_str = params.query_params.collection_id.to_string();

        let span = span!(
            Level::DEBUG,
            "Firestore Streaming Aggregated Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );

        let doc_stream = self
            .stream_aggregated_query_doc_with_retries(params, 0, &span)
            .await?;

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

    pub async fn stream_aggregated_query_doc_with_errors<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        let collection_str = params.query_params.collection_id.to_string();

        let span = span!(
            Level::DEBUG,
            "Firestore Streaming Aggregated Query",
            "/firestore/collection_name" = collection_str.as_str(),
            "/firestore/response_time" = field::Empty
        );

        let doc_stream = self
            .stream_aggregated_query_doc_with_retries(params, 0, &span)
            .await?;

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

    pub async fn aggregated_query_obj<T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_vec = self.aggregated_query_doc(params).await?;
        doc_vec
            .iter()
            .map(|doc| Self::deserialize_doc_to(doc))
            .collect()
    }

    pub async fn stream_aggregated_query_obj<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_stream = self.stream_aggregated_query_doc(params).await?;
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

    pub async fn stream_aggregated_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b,
    {
        let doc_stream = self.stream_aggregated_query_doc_with_errors(params).await?;
        Ok(Box::pin(doc_stream.and_then(|doc| {
            future::ready(Self::deserialize_doc_to::<T>(&doc))
        })))
    }

    fn create_aggregated_query_request(
        &self,
        params: &FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<tonic::Request<RunAggregationQueryRequest>> {
        Ok(tonic::Request::new(RunAggregationQueryRequest {
            parent: params
                .query_params
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
            query_type: Some(run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                StructuredAggregationQuery {
                    aggregations: params.aggregations.iter().map(|agg| agg.into()).collect(),
                    query_type: Some(gcloud_sdk::google::firestore::v1::structured_aggregation_query::QueryType::StructuredQuery(params.query_params.to_structured_query())),
                }
            )),
        }))
    }

    fn stream_aggregated_query_doc_with_retries<'a, 'b>(
        &'a self,
        params: FirestoreAggregatedQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<BoxStream<'b, FirestoreResult<Option<Document>>>>> {
        async move {
            let query_request = self.create_aggregated_query_request(&params)?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client
                .get()
                .run_aggregation_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream = query_response
                        .into_inner()
                        .map_ok(Self::aggregated_response_to_doc)
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
                            params.query_params.collection_id,
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

                        self.stream_aggregated_query_doc_with_retries(params, retries + 1, span)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    fn aggregated_query_doc_with_retries<'a>(
        &'a self,
        params: FirestoreAggregatedQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<Vec<Document>>> {
        async move {
            let query_request = self.create_aggregated_query_request(&params)?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client
                .get()
                .run_aggregation_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream = query_response
                        .into_inner()
                        .map_ok(Self::aggregated_response_to_doc)
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
                            params.query_params.collection_id,
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
                        self.aggregated_query_doc_with_retries(params, retries + 1, span)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    fn aggregated_response_to_doc(mut agg_res: RunAggregationQueryResponse) -> Option<Document> {
        agg_res.result.take().map(|agg_res_doc| Document {
            name: "".to_string(),
            fields: agg_res_doc.aggregate_fields,
            create_time: None,
            update_time: None,
        })
    }
}
