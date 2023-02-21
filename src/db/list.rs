use crate::{FirestoreDb, FirestoreError, FirestoreQueryOrder, FirestoreResult};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use serde::Deserialize;
use std::future;
use tracing::*;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreListDocParams {
    pub collection_id: String,

    pub parent: Option<String>,

    #[default = "100"]
    pub page_size: usize,

    pub page_token: Option<String>,
    pub order_by: Option<Vec<FirestoreQueryOrder>>,
    pub return_only_fields: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreListDocResult {
    pub documents: Vec<Document>,
    pub page_token: Option<String>,
}

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreListCollectionIdsParams {
    pub parent: Option<String>,

    #[default = "100"]
    pub page_size: usize,
    pub page_token: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreListCollectionIdsResult {
    pub collection_ids: Vec<String>,
    pub page_token: Option<String>,
}

#[async_trait]
pub trait FirestoreListingSupport {
    async fn list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<FirestoreListDocResult>;

    async fn stream_list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<Document>>;

    async fn stream_list_doc_with_errors(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<Document>>>;

    async fn stream_list_obj<T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<T>>
    where
        for<'de> T: Deserialize<'de>;

    async fn stream_list_obj_with_errors<T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>;

    async fn list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<FirestoreListCollectionIdsResult>;

    async fn stream_list_collection_ids_with_errors(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<String>>>;

    async fn stream_list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<String>>;
}

#[async_trait]
impl FirestoreListingSupport for FirestoreDb {
    async fn list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<FirestoreListDocResult> {
        let span = span!(
            Level::DEBUG,
            "Firestore ListDocs",
            "/firestore/collection_name" = params.collection_id.as_str(),
            "/firestore/response_time" = field::Empty
        );

        self.list_doc_with_retries(params, 0, &span).await
    }

    async fn stream_list_doc_with_errors(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<Document>>> {
        let stream: BoxStream<FirestoreResult<Document>> = Box::pin(
            futures::stream::unfold(Some(params), move |maybe_params| async move {
                if let Some(params) = maybe_params {
                    let collection_str = params.collection_id.to_string();

                    let span = span!(
                        Level::DEBUG,
                        "Firestore Streaming ListDocs",
                        "/firestore/collection_name" = collection_str.as_str(),
                        "/firestore/response_time" = field::Empty
                    );

                    match self.list_doc_with_retries(params.clone(), 0, &span).await {
                        Ok(results) => {
                            if let Some(next_page_token) = results.page_token.clone() {
                                Some((Ok(results), Some(params.with_page_token(next_page_token))))
                            } else {
                                Some((Ok(results), None))
                            }
                        }
                        Err(err) => {
                            error!("[DB] Error occurred while consuming documents: {}", err);
                            Some((Err(err), None))
                        }
                    }
                } else {
                    None
                }
            })
            .flat_map(|doc_res| {
                futures::stream::iter(match doc_res {
                    Ok(results) => results
                        .documents
                        .into_iter()
                        .map(Ok::<Document, FirestoreError>)
                        .collect(),
                    Err(err) => vec![Err(err)],
                })
            }),
        );

        Ok(stream)
    }

    async fn stream_list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<Document>> {
        let doc_stream = self.stream_list_doc_with_errors(params).await?;
        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(doc) => Some(doc),
                Err(err) => {
                    error!("[DB] Error occurred while consuming documents: {}", err);
                    None
                }
            })
        })))
    }

    async fn stream_list_obj<T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<T>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_stream = self.stream_list_doc(params).await?;

        Ok(Box::pin(doc_stream.filter_map(|doc| async move {
            match Self::deserialize_doc_to::<T>(&doc) {
                Ok(obj) => Some(obj),
                Err(err) => {
                    error!(
                        "[DB] Error occurred while consuming list document as a stream: {}",
                        err
                    );
                    None
                }
            }
        })))
    }

    async fn stream_list_obj_with_errors<T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_stream = self.stream_list_doc_with_errors(params).await?;

        Ok(Box::pin(doc_stream.and_then(|doc| async move {
            Self::deserialize_doc_to::<T>(&doc)
        })))
    }

    async fn list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<FirestoreListCollectionIdsResult> {
        let span = span!(
            Level::DEBUG,
            "Firestore ListCollectionIds",
            "/firestore/response_time" = field::Empty
        );

        self.list_collection_ids_with_retries(params, 0, &span)
            .await
    }

    async fn stream_list_collection_ids(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<String>> {
        let stream = self.stream_list_collection_ids_with_errors(params).await?;
        Ok(Box::pin(stream.filter_map(|col_res| {
            future::ready(match col_res {
                Ok(col) => Some(col),
                Err(err) => {
                    error!(
                        "[DB] Error occurred while consuming collection IDs: {}",
                        err
                    );
                    None
                }
            })
        })))
    }

    async fn stream_list_collection_ids_with_errors(
        &self,
        params: FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<BoxStream<FirestoreResult<String>>> {
        let stream: BoxStream<FirestoreResult<String>> = Box::pin(
            futures::stream::unfold(Some(params), move |maybe_params| async move {
                if let Some(params) = maybe_params {
                    let span = span!(
                        Level::DEBUG,
                        "Firestore Streaming ListCollections",
                        "/firestore/response_time" = field::Empty
                    );

                    match self
                        .list_collection_ids_with_retries(params.clone(), 0, &span)
                        .await
                    {
                        Ok(results) => {
                            if let Some(next_page_token) = results.page_token.clone() {
                                Some((Ok(results), Some(params.with_page_token(next_page_token))))
                            } else {
                                Some((Ok(results), None))
                            }
                        }
                        Err(err) => {
                            error!("[DB] Error occurred while consuming documents: {}", err);
                            Some((Err(err), None))
                        }
                    }
                } else {
                    None
                }
            })
            .flat_map(|doc_res| {
                futures::stream::iter(match doc_res {
                    Ok(results) => results
                        .collection_ids
                        .into_iter()
                        .map(Ok::<String, FirestoreError>)
                        .collect(),
                    Err(err) => vec![Err(err)],
                })
            }),
        );

        Ok(stream)
    }
}

impl FirestoreDb {
    fn create_list_doc_request(
        &self,
        params: &FirestoreListDocParams,
    ) -> FirestoreResult<tonic::Request<ListDocumentsRequest>> {
        Ok(tonic::Request::new(ListDocumentsRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            collection_id: params.collection_id.clone(),
            page_size: params.page_size as i32,
            page_token: params.page_token.clone().unwrap_or_default(),
            order_by: params
                .order_by
                .as_ref()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| field.to_string_format())
                        .collect::<Vec<String>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "".to_string()),
            mask: params
                .return_only_fields
                .as_ref()
                .map(|masks| DocumentMask {
                    field_paths: masks.clone(),
                }),
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
            show_missing: false,
        }))
    }

    fn list_doc_with_retries<'a>(
        &'a self,
        params: FirestoreListDocParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<FirestoreListDocResult>> {
        async move {
            let list_request = self.create_list_doc_request(&params)?;
            let begin_utc: DateTime<Utc> = Utc::now();

            match self
                .client()
                .get()
                .list_documents(list_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(listing_response) => {
                    let list_inner = listing_response.into_inner();
                    let result = FirestoreListDocResult::new(list_inner.documents).opt_page_token(
                        if !list_inner.next_page_token.is_empty() {
                            Some(list_inner.next_page_token)
                        } else {
                            None
                        },
                    );
                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let listing_duration = end_query_utc.signed_duration_since(begin_utc);

                    span.record(
                        "/firestore/response_time",
                        listing_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            "[DB]: Listing documents in {:?} took {}ms",
                            params.collection_id,
                            listing_duration.num_milliseconds()
                        );
                    });

                    Ok(result)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.inner.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Listing failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.inner.options.max_retries
                        );
                        self.list_doc_with_retries(params, retries + 1, span).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    fn create_list_collection_ids_request(
        &self,
        params: &FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<tonic::Request<ListCollectionIdsRequest>> {
        Ok(tonic::Request::new(ListCollectionIdsRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            page_size: params.page_size as i32,
            page_token: params.page_token.clone().unwrap_or_default(),
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
        }))
    }

    fn list_collection_ids_with_retries<'a>(
        &'a self,
        params: FirestoreListCollectionIdsParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<FirestoreListCollectionIdsResult>> {
        async move {
            let list_request = self.create_list_collection_ids_request(&params)?;
            let begin_utc: DateTime<Utc> = Utc::now();

            match self
                .client()
                .get()
                .list_collection_ids(list_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(listing_response) => {
                    let list_inner = listing_response.into_inner();
                    let result = FirestoreListCollectionIdsResult::new(list_inner.collection_ids)
                        .opt_page_token(if !list_inner.next_page_token.is_empty() {
                            Some(list_inner.next_page_token)
                        } else {
                            None
                        });
                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let listing_duration = end_query_utc.signed_duration_since(begin_utc);

                    span.record(
                        "/firestore/response_time",
                        listing_duration.num_milliseconds(),
                    );
                    span.in_scope(|| {
                        debug!(
                            "[DB]: Listing collections took {}ms",
                            listing_duration.num_milliseconds()
                        );
                    });

                    Ok(result)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.inner.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Listing failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.inner.options.max_retries
                        );
                        self.list_collection_ids_with_retries(params, retries + 1, span)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }
}
