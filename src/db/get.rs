use crate::db::safe_document_path;
use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use chrono::prelude::*;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::BoxStream;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use serde::Deserialize;
use tracing::*;

#[async_trait]
pub trait FirestoreGetByIdSupport {
    async fn get_doc<S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn get_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn get_obj<T, S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_return_fields<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at_return_fields<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_if_exists<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn get_obj_at_if_exists<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn batch_stream_get_docs<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_with_errors<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_at<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_docs_at_with_errors<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_with_errors<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_at<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;

    async fn batch_stream_get_objects_at_with_errors<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send;
}

#[async_trait]
impl FirestoreGetByIdSupport for FirestoreDb {
    async fn get_doc_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        let document_path = safe_document_path(parent, collection_id, document_id.as_ref())?;
        self.get_doc_by_path(
            collection_id.to_string(),
            document_path,
            return_only_fields,
            0,
        )
        .await
    }

    async fn get_doc<S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        self.get_doc_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            return_only_fields,
        )
        .await
    }

    async fn get_obj<T, S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.get_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
        )
        .await
    }

    async fn get_obj_return_fields<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.get_obj_at_return_fields(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            return_only_fields,
        )
        .await
    }

    async fn get_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let doc: Document = self
            .get_doc_at(parent, collection_id, document_id, None)
            .await?;

        let obj: T = Self::deserialize_doc_to(&doc)?;
        Ok(obj)
    }

    async fn get_obj_at_return_fields<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let doc: Document = self
            .get_doc_at(parent, collection_id, document_id, return_only_fields)
            .await?;

        let obj: T = Self::deserialize_doc_to(&doc)?;
        Ok(obj)
    }

    async fn get_obj_if_exists<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.get_obj_at_if_exists(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            return_only_fields,
        )
        .await
    }

    async fn get_obj_at_if_exists<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        match self
            .get_obj_at_return_fields::<T, S>(
                parent,
                collection_id,
                document_id,
                return_only_fields,
            )
            .await
        {
            Ok(obj) => Ok(Some(obj)),
            Err(err) => match err {
                FirestoreError::DataNotFoundError(_) => Ok(None),
                _ => Err(err),
            },
        }
    }

    async fn batch_stream_get_docs_at_with_errors<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        let full_doc_ids: Vec<String> = document_ids
            .into_iter()
            .map(|document_id| safe_document_path(parent, collection_id, document_id.as_ref()))
            .collect::<FirestoreResult<Vec<String>>>()?;

        self.get_docs_by_ids(collection_id.to_string(), full_doc_ids, return_only_fields)
            .await
    }

    async fn batch_stream_get_docs_at<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        let doc_stream = self
            .batch_stream_get_docs_at_with_errors(
                parent,
                collection_id,
                document_ids,
                return_only_fields,
            )
            .await?;

        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(doc_pair) => Some(doc_pair),
                Err(err) => {
                    error!(
                        "Error occurred while consuming batch get as a stream: {}",
                        err
                    );
                    None
                }
            })
        })))
    }

    async fn batch_stream_get_docs<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        self.batch_stream_get_docs_at(
            self.get_documents_path(),
            collection_id,
            document_ids,
            return_only_fields,
        )
        .await
    }

    async fn batch_stream_get_docs_with_errors<S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        self.batch_stream_get_docs_at_with_errors(
            self.get_documents_path(),
            collection_id,
            document_ids,
            return_only_fields,
        )
        .await
    }

    async fn batch_stream_get_objects<'b, T, S, I>(
        &'b self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'b, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'b,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        self.batch_stream_get_objects_at(
            self.get_documents_path(),
            collection_id,
            document_ids,
            return_only_fields,
        )
        .await
    }

    async fn batch_stream_get_objects_at<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        let doc_stream = self
            .batch_stream_get_docs_at(parent, collection_id, document_ids, return_only_fields)
            .await?;

        Ok(Box::pin(doc_stream.filter_map(
            |(doc_id, maybe_doc)| async move {
                match maybe_doc {
                    Some(doc) => match Self::deserialize_doc_to(&doc) {
                        Ok(obj) => Some((doc_id, Some(obj))),
                        Err(err) => {
                            error!(
                                "Error occurred while consuming batch documents as a stream: {}",
                                err
                            );
                            None
                        }
                    },
                    None => Some((doc_id, None)),
                }
            },
        )))
    }

    async fn batch_stream_get_objects_at_with_errors<'a, T, S, I>(
        &'a self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        let doc_stream = self
            .batch_stream_get_docs_at_with_errors(
                parent,
                collection_id,
                document_ids,
                return_only_fields,
            )
            .await?;

        Ok(Box::pin(doc_stream.and_then(|(doc_id, maybe_doc)| {
            future::ready({
                maybe_doc
                    .map(|doc| Self::deserialize_doc_to::<T>(&doc))
                    .transpose()
                    .map(|obj| (doc_id, obj))
            })
        })))
    }

    async fn batch_stream_get_objects_with_errors<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        let doc_stream = self
            .batch_stream_get_docs_at_with_errors(
                self.get_documents_path(),
                collection_id,
                document_ids,
                return_only_fields,
            )
            .await?;

        Ok(Box::pin(doc_stream.and_then(|(doc_id, maybe_doc)| {
            future::ready({
                maybe_doc
                    .map(|doc| Self::deserialize_doc_to::<T>(&doc))
                    .transpose()
                    .map(|obj| (doc_id, obj))
            })
        })))
    }
}

impl FirestoreDb {
    pub(crate) fn get_doc_by_path(
        &self,
        collection_id: String,
        document_path: String,
        return_only_fields: Option<Vec<String>>,
        retries: usize,
    ) -> BoxFuture<FirestoreResult<Document>> {
        async move {
            #[cfg(feature = "caching")]
            {
                if let FirestoreCachedValue::UseCached(doc) = self
                    .get_doc_from_cache(
                        collection_id.as_str(),
                        document_path.as_str(),
                        &return_only_fields,
                    )
                    .await?
                {
                    return Ok(doc);
                }
            }

            let _return_only_fields_empty = return_only_fields.is_none();

            let span = span!(
                Level::DEBUG,
                "Firestore Get Doc",
                "/firestore/collection_name" = collection_id,
                "/firestore/response_time" = field::Empty,
                "/firestore/document_name" = document_path.as_str()
            );
            let begin_query_utc: DateTime<Utc> = Utc::now();

            let request = gcloud_sdk::tonic::Request::new(GetDocumentRequest {
                name: document_path.clone(),
                consistency_selector: self
                    .session_params
                    .consistency_selector
                    .as_ref()
                    .map(|selector| selector.try_into())
                    .transpose()?,
                mask: return_only_fields.map({
                    |vf| gcloud_sdk::google::firestore::v1::DocumentMask {
                        field_paths: vf.iter().map(|f| f.to_string()).collect(),
                    }
                }),
            });

            let response = self
                .client()
                .get()
                .get_document(request)
                .map_err(|e| e.into())
                .await;

            let end_query_utc: DateTime<Utc> = Utc::now();
            let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

            span.record(
                "/firestore/response_time",
                query_duration.num_milliseconds(),
            );

            match response {
                Ok(doc_response) => {
                    span.in_scope(|| {
                        debug!(
                            "Reading document {} took {}ms",
                            document_path,
                            query_duration.num_milliseconds()
                        );
                    });

                    let doc = doc_response.into_inner();
                    #[cfg(feature = "caching")]
                    if _return_only_fields_empty {
                        self.offer_doc_update_to_cache(&doc).await?;
                    }
                    Ok(doc)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.get_options().max_retries =>
                    {
                        span.in_scope(|| {
                            warn!(
                                "Failed with {}. Retrying: {}/{}",
                                db_err,
                                retries + 1,
                                self.get_options().max_retries
                            );
                        });
                        self.get_doc_by_path(collection_id, document_path, None, retries + 1)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    pub(crate) async fn get_docs_by_ids(
        &self,
        collection_id: String,
        full_doc_ids: Vec<String>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>> {
        #[cfg(feature = "caching")]
        {
            if let FirestoreCachedValue::UseCached(stream) = self
                .get_docs_by_ids_from_cache(
                    collection_id.as_str(),
                    &full_doc_ids,
                    &return_only_fields,
                )
                .await?
            {
                return Ok(stream);
            }
        }

        let span = span!(
            Level::DEBUG,
            "Firestore Batch Get",
            "/firestore/collection_name" = collection_id.as_str(),
            "/firestore/ids_count" = full_doc_ids.len()
        );

        let request = gcloud_sdk::tonic::Request::new(BatchGetDocumentsRequest {
            database: self.get_database_path().clone(),
            documents: full_doc_ids,
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
            mask: return_only_fields.map({
                |vf| gcloud_sdk::google::firestore::v1::DocumentMask {
                    field_paths: vf.iter().map(|f| f.to_string()).collect(),
                }
            }),
        });

        match self.client().get().batch_get_documents(request).await {
            Ok(response) => {
                span.in_scope(|| debug!("Start consuming a batch of documents by ids"));
                let stream = response
                    .into_inner()
                    .filter_map(move |r| async move {
                        match r {
                            Ok(doc_response) => match doc_response.result {
                                Some(batch_get_documents_response::Result::Found(document)) => {
                                    let doc_id = document
                                        .name
                                        .split('/')
                                        .last()
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| document.name.clone());
                                    #[cfg(feature = "caching")]
                                    {
                                        self.offer_doc_update_to_cache(&document).await.ok();

                                        Some(Ok((doc_id, Some(document))))
                                    }
                                    #[cfg(not(feature = "caching"))]
                                    {
                                        Some(Ok((doc_id, Some(document))))
                                    }
                                }
                                Some(batch_get_documents_response::Result::Missing(
                                    full_doc_id,
                                )) => {
                                    let doc_id = full_doc_id
                                        .split('/')
                                        .last()
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| full_doc_id);
                                    Some(Ok((doc_id, None)))
                                }
                                None => None,
                            },
                            Err(err) => Some(Err(err.into())),
                        }
                    })
                    .boxed();
                Ok(stream)
            }
            Err(err) => Err(err.into()),
        }
    }

    #[cfg(feature = "caching")]
    pub(crate) async fn get_doc_from_cache(
        &self,
        collection_id: &str,
        document_path: &str,
        _return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<FirestoreCachedValue<FirestoreDocument>> {
        if let FirestoreDbSessionCacheMode::ReadThroughCache(ref cache)
        | FirestoreDbSessionCacheMode::ReadCachedOnly(ref cache) = self.session_params.cache_mode
        {
            let begin_query_utc: DateTime<Utc> = Utc::now();

            let cache_response = cache.get_doc_by_path(document_path).await?;

            let end_query_utc: DateTime<Utc> = Utc::now();
            let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

            let span = span!(
                Level::DEBUG,
                "Firestore Get Cache",
                "/firestore/collection_name" = collection_id,
                "/firestore/response_time" = query_duration.num_milliseconds(),
                "/firestore/document_name" = document_path,
                "/firestore/cache_result" = field::Empty,
            );

            if let Some(doc) = cache_response {
                span.record("/firestore/cache_result", "hit");
                span.in_scope(|| {
                    debug!(
                        "Reading document {} from cache took {}ms",
                        document_path,
                        query_duration.num_milliseconds()
                    );
                });

                return Ok(FirestoreCachedValue::UseCached(doc));
            } else {
                span.record("/firestore/cache_result", "miss");
                span.in_scope(|| {
                    debug!("Missing document {} in cache", document_path);
                });
                if let FirestoreDbSessionCacheMode::ReadCachedOnly(_) =
                    self.session_params.cache_mode
                {
                    return Err(FirestoreError::DataNotFoundError(
                        FirestoreDataNotFoundError::new(
                            FirestoreErrorPublicGenericDetails::new("CACHE_MISS".to_string()),
                            format!("Document {} not found in cache", document_path),
                        ),
                    ));
                }
            }
        }
        Ok(FirestoreCachedValue::SkipCache)
    }

    #[cfg(feature = "caching")]
    #[inline]
    pub(crate) async fn get_docs_by_ids_from_cache(
        &self,
        collection_id: &str,
        full_doc_ids: &Vec<String>,
        _return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<FirestoreResult<(String, Option<Document>)>>>>
    {
        if let FirestoreDbSessionCacheMode::ReadThroughCache(ref cache)
        | FirestoreDbSessionCacheMode::ReadCachedOnly(ref cache) = self.session_params.cache_mode
        {
            let span = span!(
                Level::DEBUG,
                "Firestore Batch Get Cached",
                "/firestore/collection_name" = collection_id,
                "/firestore/ids_count" = full_doc_ids.len(),
                "/firestore/cache_result" = field::Empty,
            );

            let cached_stream: BoxStream<FirestoreResult<(String, Option<FirestoreDocument>)>> =
                cache.get_docs_by_paths(full_doc_ids).await?;

            let cached_vec: Vec<(String, Option<FirestoreDocument>)> =
                cached_stream.try_collect::<Vec<_>>().await?;

            if cached_vec.len() == full_doc_ids.len()
                || matches!(
                    self.session_params.cache_mode,
                    FirestoreDbSessionCacheMode::ReadCachedOnly(_)
                )
            {
                span.record("/firestore/cache_result", "hit");
                span.in_scope(|| {
                    debug!("Reading {} documents from cache", full_doc_ids.len());
                });
                return Ok(FirestoreCachedValue::UseCached(Box::pin(
                    futures::stream::iter(cached_vec)
                        .map(|(doc_id, maybe_doc)| Ok((doc_id, maybe_doc))),
                )));
            } else {
                span.record("/firestore/cache_result", "miss");
                span.in_scope(|| {
                    debug!("Not all documents were found in cache. Reading from Firestore.")
                });
                return Ok(FirestoreCachedValue::SkipCache);
            }
        }
        Ok(FirestoreCachedValue::SkipCache)
    }

    #[cfg(feature = "caching")]
    #[inline]
    pub(crate) async fn offer_doc_update_to_cache(
        &self,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        if let FirestoreDbSessionCacheMode::ReadThroughCache(ref cache) =
            self.session_params.cache_mode
        {
            cache.update_doc_by_path(document).await?;
        }
        Ok(())
    }
}
