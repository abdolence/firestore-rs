#![allow(clippy::derive_partial_eq_without_eq)] // Since we may not be able to implement Eq for the changes coming from Firestore protos

use crate::{FirestoreDb, FirestoreError, FirestoreQueryParams, FirestoreResult};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use rand::Rng;
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
    Sum(FirestoreAggregationOperatorSum),
    Avg(FirestoreAggregationOperatorAvg),
}

impl From<&FirestoreAggregationOperator> for structured_aggregation_query::aggregation::Operator {
    fn from(op: &FirestoreAggregationOperator) -> Self {
        match op {
            FirestoreAggregationOperator::Count(opts) => {
                structured_aggregation_query::aggregation::Operator::Count(opts.into())
            }
            FirestoreAggregationOperator::Sum(opts) => {
                structured_aggregation_query::aggregation::Operator::Sum(opts.into())
            }
            FirestoreAggregationOperator::Avg(opts) => {
                structured_aggregation_query::aggregation::Operator::Avg(opts.into())
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

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreAggregationOperatorSum {
    pub field_name: String,
}

impl From<&FirestoreAggregationOperatorSum> for structured_aggregation_query::aggregation::Sum {
    fn from(operator: &FirestoreAggregationOperatorSum) -> Self {
        structured_aggregation_query::aggregation::Sum {
            field: Some(structured_query::FieldReference {
                field_path: operator.field_name.clone(),
            }),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreAggregationOperatorAvg {
    pub field_name: String,
}

impl From<&FirestoreAggregationOperatorAvg> for structured_aggregation_query::aggregation::Avg {
    fn from(operator: &FirestoreAggregationOperatorAvg) -> Self {
        structured_aggregation_query::aggregation::Avg {
            field: Some(structured_query::FieldReference {
                field_path: operator.field_name.clone(),
            }),
        }
    }
}

#[async_trait]
pub trait FirestoreAggregatedQuerySupport {
    async fn aggregated_query_doc(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<Document>>;

    async fn stream_aggregated_query_doc<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_aggregated_query_doc_with_errors<'b>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn aggregated_query_obj<T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<Vec<T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_aggregated_query_obj<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_aggregated_query_obj_with_errors<'b, T>(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
        T: Send + 'b;
}

#[async_trait]
impl FirestoreAggregatedQuerySupport for FirestoreDb {
    async fn aggregated_query_doc(
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

    async fn stream_aggregated_query_doc<'b>(
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
                    error!(%err, "Error occurred while consuming query.");
                    None
                }
            })
        })))
    }

    async fn stream_aggregated_query_doc_with_errors<'b>(
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
                    error!(%err, "Error occurred while consuming query.");
                    Some(Err(err))
                }
            })
        })))
    }

    async fn aggregated_query_obj<T>(
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

    async fn stream_aggregated_query_obj<'b, T>(
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
                        %err,
                        "Error occurred while consuming query document as a stream.",
                    );
                    None
                }
            }
        })))
    }

    async fn stream_aggregated_query_obj_with_errors<'b, T>(
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
}

impl FirestoreDb {
    fn create_aggregated_query_request(
        &self,
        params: FirestoreAggregatedQueryParams,
    ) -> FirestoreResult<gcloud_sdk::tonic::Request<RunAggregationQueryRequest>> {
        Ok(gcloud_sdk::tonic::Request::new(RunAggregationQueryRequest {
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
                    query_type: Some(gcloud_sdk::google::firestore::v1::structured_aggregation_query::QueryType::StructuredQuery(params.query_params.try_into()?)),
                }
            )),
            explain_options: None,
        }))
    }

    fn stream_aggregated_query_doc_with_retries<'a, 'b>(
        &'a self,
        params: FirestoreAggregatedQueryParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<BoxStream<'b, FirestoreResult<Option<Document>>>>> {
        async move {
            let query_request = self.create_aggregated_query_request(params.clone())?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client()
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
                        query_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            collection_id = ?params.query_params.collection_id,
                            duration_milliseconds = query_duration.num_milliseconds(),
                            "Querying stream of documents in specified collection.",
                        );
                    });

                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                    if db_err.retry_possible && retries < self.inner.options.max_retries =>
                        {
                            let sleep_duration = tokio::time::Duration::from_millis(
                                rand::rng().random_range(0..2u64.pow(retries as u32) * 1000 + 1),
                            );
                            warn!(
                                err = %db_err,
                                current_retry = retries + 1,
                                max_retries = self.inner.options.max_retries,
                                delay = sleep_duration.as_millis(),
                                "Failed to run aggregation query. Retrying up to the specified number of times.",
                            );

                            tokio::time::sleep(sleep_duration).await;

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
            let query_request = self.create_aggregated_query_request(params.clone())?;
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .client()
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
                        query_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            collection_id = ?params.query_params.collection_id,
                            duration_milliseconds = query_duration.num_milliseconds(),
                            "Querying documents in specified collection.",
                        );
                    });

                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                    if db_err.retry_possible && retries < self.inner.options.max_retries =>
                        {
                            let sleep_duration = tokio::time::Duration::from_millis(
                                rand::rng().random_range(0..2u64.pow(retries as u32) * 1000 + 1),
                            );
                            warn!(
                                err = %db_err,
                                current_retry = retries + 1,
                                max_retries = self.inner.options.max_retries,
                                delay = sleep_duration.as_millis(),
                                "Failed to run aggregation query. Retrying up to the specified number of times.",
                            );

                            tokio::time::sleep(sleep_duration).await;

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
