use crate::db::FirestoreDbInner;
use crate::*;
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
use std::sync::Arc;
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

    async fn stream_list_doc<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, Document>>;

    async fn stream_list_doc_with_errors<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>>;

    async fn stream_list_obj<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de> + 'b;

    async fn stream_list_obj_with_errors<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de> + 'b;

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

        self.list_doc_with_retries(params, 0, span).await
    }

    async fn stream_list_doc_with_errors<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.stream_list_doc_with_retries(params).await
    }

    async fn stream_list_doc<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, Document>> {
        let doc_stream = self.stream_list_doc_with_errors(params).await?;
        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(doc) => Some(doc),
                Err(err) => {
                    error!(%err, "Error occurred while consuming documents.");
                    None
                }
            })
        })))
    }

    async fn stream_list_obj<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, T>>
    where
        for<'de> T: Deserialize<'de> + 'b,
    {
        let doc_stream = self.stream_list_doc(params).await?;

        Ok(Box::pin(doc_stream.filter_map(|doc| async move {
            match Self::deserialize_doc_to::<T>(&doc) {
                Ok(obj) => Some(obj),
                Err(err) => {
                    error!(
                        %err,
                        "Error occurred while consuming list document as a stream.",
                    );
                    None
                }
            }
        })))
    }

    async fn stream_list_obj_with_errors<'b, T>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        for<'de> T: Deserialize<'de> + 'b,
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
                    error!(%err, "Error occurred while consuming collection IDs.");
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
                            error!(%err, "Error occurred while consuming documents.");
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
        params: FirestoreListDocParams,
    ) -> FirestoreResult<ListDocumentsRequest> {
        Ok(ListDocumentsRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            collection_id: params.collection_id,
            page_size: params.page_size as i32,
            page_token: params.page_token.unwrap_or_default(),
            order_by: params
                .order_by
                .map(|fields| {
                    fields
                        .into_iter()
                        .map(|field| field.to_string_format())
                        .collect::<Vec<String>>()
                        .join(", ")
                })
                .unwrap_or_default(),
            mask: params
                .return_only_fields
                .map(|masks| DocumentMask { field_paths: masks }),
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
            show_missing: false,
        })
    }

    fn list_doc_with_retries<'b>(
        &self,
        params: FirestoreListDocParams,
        retries: usize,
        span: Span,
    ) -> BoxFuture<'b, FirestoreResult<FirestoreListDocResult>> {
        match self.create_list_doc_request(params) {
            Ok(list_request) => {
                Self::list_doc_with_retries_inner(self.inner.clone(), list_request, retries, span)
                    .boxed()
            }
            Err(err) => futures::future::err(err).boxed(),
        }
    }

    fn list_doc_with_retries_inner<'b>(
        db_inner: Arc<FirestoreDbInner>,
        list_request: ListDocumentsRequest,
        retries: usize,
        span: Span,
    ) -> BoxFuture<'b, FirestoreResult<FirestoreListDocResult>> {
        async move {
            let begin_utc: DateTime<Utc> = Utc::now();

            match db_inner.client.get()
                .list_documents(
                    gcloud_sdk::tonic::Request::new(list_request.clone())
                )
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
                            collection_id = list_request.collection_id.as_str(),
                            duration_milliseconds = listing_duration.num_milliseconds(),
                            num_documents = result.documents.len(),
                            "Listed documents.",
                        );
                    });

                    Ok(result)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < db_inner.options.max_retries =>
                    {
                        warn!(
                            err = %db_err,
                            current_retry = retries + 1,
                            max_retries = db_inner.options.max_retries,
                            "Failed to list documents. Retrying up to the specified number of times.",
                        );

                        Self::list_doc_with_retries_inner(db_inner, list_request, retries + 1, span).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    async fn stream_list_doc_with_retries<'b>(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        #[cfg(feature = "caching")]
        {
            if let FirestoreCachedValue::UseCached(stream) =
                self.list_docs_from_cache(&params).await?
            {
                return Ok(stream);
            }
        }
        let list_request = self.create_list_doc_request(params.clone())?;
        Self::stream_list_doc_with_retries_inner(self.inner.clone(), list_request)
    }

    fn stream_list_doc_with_retries_inner<'b>(
        db_inner: Arc<FirestoreDbInner>,
        list_request: ListDocumentsRequest,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        let stream: BoxStream<FirestoreResult<Document>> = Box::pin(
            futures::stream::unfold(
                (db_inner, Some(list_request)),
                move |(db_inner, list_request)| async move {
                    if let Some(mut list_request) = list_request {
                        let span = span!(
                            Level::DEBUG,
                            "Firestore Streaming ListDocs",
                            "/firestore/collection_name" = list_request.collection_id.as_str(),
                            "/firestore/response_time" = field::Empty
                        );
                        match Self::list_doc_with_retries_inner(
                            db_inner.clone(),
                            list_request.clone(),
                            0,
                            span,
                        )
                        .await
                        {
                            Ok(results) => {
                                if let Some(next_page_token) = results.page_token.clone() {
                                    list_request.page_token = next_page_token;
                                    Some((Ok(results), (db_inner, Some(list_request))))
                                } else {
                                    Some((Ok(results), (db_inner, None)))
                                }
                            }
                            Err(err) => {
                                error!(%err, "Error occurred while consuming documents.");
                                Some((Err(err), (db_inner, None)))
                            }
                        }
                    } else {
                        None
                    }
                },
            )
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

    fn create_list_collection_ids_request(
        &self,
        params: &FirestoreListCollectionIdsParams,
    ) -> FirestoreResult<gcloud_sdk::tonic::Request<ListCollectionIdsRequest>> {
        Ok(gcloud_sdk::tonic::Request::new(ListCollectionIdsRequest {
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
                            duration_milliseconds = listing_duration.num_milliseconds(),
                            "Listed collections.",
                        );
                    });

                    Ok(result)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.inner.options.max_retries =>
                    {
                        warn!(
                            err = %db_err,
                            current_retry = retries + 1,
                            max_retries = self.inner.options.max_retries,
                            "Failed to list collection IDs. Retrying up to the specified number of times.",
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

    #[cfg(feature = "caching")]
    #[inline]
    pub async fn list_docs_from_cache<'b>(
        &self,
        params: &FirestoreListDocParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>
    {
        if let FirestoreDbSessionCacheMode::ReadCachedOnly(ref cache) =
            self.session_params.cache_mode
        {
            let span = span!(
                Level::DEBUG,
                "Firestore List Cached",
                "/firestore/collection_name" = params.collection_id,
                "/firestore/cache_result" = field::Empty,
                "/firestore/response_time" = field::Empty
            );

            let begin_query_utc: DateTime<Utc> = Utc::now();

            let collection_path = if let Some(parent) = params.parent.as_ref() {
                format!("{}/{}", parent, params.collection_id.as_str())
            } else {
                format!(
                    "{}/{}",
                    self.get_documents_path(),
                    params.collection_id.as_str()
                )
            };

            let cached_result = cache.list_all_docs(&collection_path).await?;

            let end_query_utc: DateTime<Utc> = Utc::now();
            let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

            span.record(
                "/firestore/response_time",
                query_duration.num_milliseconds(),
            );

            match cached_result {
                FirestoreCachedValue::UseCached(stream) => {
                    span.record("/firestore/cache_result", "hit");
                    span.in_scope(|| {
                        debug!(
                            collection_id = params.collection_id,
                            "Reading all documents from cache."
                        );
                    });

                    Ok(FirestoreCachedValue::UseCached(stream))
                }
                FirestoreCachedValue::SkipCache => {
                    span.record("/firestore/cache_result", "miss");
                    if matches!(
                        self.session_params.cache_mode,
                        FirestoreDbSessionCacheMode::ReadCachedOnly(_)
                    ) {
                        span.in_scope(|| {
                            debug!(
                                collection_id = params.collection_id,
                                "Cache doesn't have suitable documents for specified collection, but cache mode is ReadCachedOnly so returning empty stream.",
                            );
                        });
                        Ok(FirestoreCachedValue::UseCached(Box::pin(
                            futures::stream::empty(),
                        )))
                    } else {
                        span.in_scope(|| {
                            debug!(
                                collection_id = params.collection_id,
                                "Cache doesn't have suitable documents for specified collection, so skipping cache and reading from Firestore.",
                            );
                        });
                        Ok(FirestoreCachedValue::SkipCache)
                    }
                }
            }
        } else {
            Ok(FirestoreCachedValue::SkipCache)
        }
    }
}
