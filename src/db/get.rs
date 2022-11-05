use crate::{FirestoreDb, FirestoreError, FirestoreResult};
use chrono::prelude::*;
use futures::future::{BoxFuture, FutureExt};
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures_util::stream::BoxStream;
use futures_util::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use serde::Deserialize;
use tracing::*;

impl FirestoreDb {
    pub(crate) fn get_doc_by_path(
        &self,
        document_path: String,
        retries: usize,
    ) -> BoxFuture<FirestoreResult<Document>> {
        async move {
            let request = tonic::Request::new(GetDocumentRequest {
                name: document_path.clone(),
                consistency_selector: self
                    .session_params
                    .consistency_selector
                    .as_ref()
                    .map(|selector| selector.try_into())
                    .transpose()?,
                mask: None,
            });

            match self
                .client()
                .get()
                .get_document(request)
                .map_err(|e| e.into())
                .await
            {
                Ok(doc_response) => Ok(doc_response.into_inner()),
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.get_options().max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.get_options().max_retries
                        );
                        self.get_doc_by_path(document_path, retries + 1).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    pub async fn get_doc_by_id<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str>,
    {
        let document_path = format!("{}/{}/{}", parent, collection_id, document_id.as_ref());
        self.get_doc_by_path(document_path, 0).await
    }

    pub async fn get_obj<T, S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        self.get_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            &document_id.as_ref().to_string(),
        )
        .await
    }

    pub async fn get_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<T>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        let begin_query_utc: DateTime<Utc> = Utc::now();
        let doc: Document = self
            .get_doc_by_id(parent, collection_id, document_id.as_ref())
            .await?;
        let end_query_utc: DateTime<Utc> = Utc::now();
        let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

        debug!(
            "[DB]: Reading document by id: {}/{} took {}ms",
            collection_id,
            document_id.as_ref(),
            query_duration.num_milliseconds()
        );

        let obj: T = Self::deserialize_doc_to(&doc)?;
        Ok(obj)
    }

    pub async fn get_obj_if_exists<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        self.get_obj_at_if_exists(
            self.get_documents_path().as_str(),
            collection_id,
            &document_id.as_ref().to_string(),
        )
        .await
    }

    pub async fn get_obj_at_if_exists<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<Option<T>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        match self
            .get_obj_at::<T, S>(parent, collection_id, document_id)
            .await
        {
            Ok(obj) => Ok(Some(obj)),
            Err(err) => match err {
                FirestoreError::DataNotFoundError(_) => Ok(None),
                _ => Err(err),
            },
        }
    }

    pub async fn batch_stream_get_docs_by_ids<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<(String, Option<Document>)>>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let doc_stream = self
            .batch_stream_get_docs_by_ids_with_errors(parent, collection_id, document_ids)
            .await?;

        Ok(Box::pin(doc_stream.filter_map(|doc_res| {
            future::ready(match doc_res {
                Ok(doc_pair) => Some(doc_pair),
                Err(err) => {
                    error!(
                        "[DB] Error occurred while consuming batch get as a stream: {}",
                        err
                    );
                    None
                }
            })
        })))
    }

    pub async fn batch_stream_get_objects_by_ids<T, S, I>(
        &self,
        collection_id: &str,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<(String, Option<T>)>>
    where
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let doc_stream = self
            .batch_stream_get_docs_by_ids(self.get_documents_path(), collection_id, document_ids)
            .await?;

        Ok(Box::pin(doc_stream.filter_map(|(doc_id,maybe_doc)| async move {
            match maybe_doc {
                Some(doc) => {
                    match Self::deserialize_doc_to(&doc) {
                        Ok(obj) => Some((doc_id, Some(obj))),
                        Err(err) => {
                            error!(
                                "[DB] Error occurred while consuming batch documents as a stream: {}",
                                err
                            );
                            None
                        }
                    }
                },
                None => Some((doc_id, None))
            }
        })))
    }

    pub async fn batch_stream_get_docs_by_ids_with_errors<S, I>(
        &self,
        parent: &str,
        collection_id: &str,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let full_doc_ids: Vec<String> = document_ids
            .into_iter()
            .map(|document_id| format!("{}/{}/{}", parent, collection_id, document_id.as_ref()))
            .collect();

        let span = span!(
            Level::DEBUG,
            "Firestore Batch Get",
            "/firestore/collection_name" = collection_id,
            "/firestore/ids_count" = full_doc_ids.len()
        );

        let request = tonic::Request::new(BatchGetDocumentsRequest {
            database: self.get_database_path().clone(),
            documents: full_doc_ids,
            consistency_selector: self
                .session_params
                .consistency_selector
                .as_ref()
                .map(|selector| selector.try_into())
                .transpose()?,
            mask: None,
        });
        match self.client().get().batch_get_documents(request).await {
            Ok(response) => {
                span.in_scope(|| debug!("Start consuming a batch of documents by ids"));
                let stream = response
                    .into_inner()
                    .filter_map(|r| {
                        future::ready(match r {
                            Ok(doc_response) => doc_response.result.map(|doc_res| match doc_res {
                                batch_get_documents_response::Result::Found(document) => {
                                    let doc_id = document
                                        .name
                                        .split('/')
                                        .last()
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| document.name.clone());
                                    Ok((doc_id, Some(document)))
                                }
                                batch_get_documents_response::Result::Missing(full_doc_id) => {
                                    let doc_id = full_doc_id
                                        .split('/')
                                        .last()
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| full_doc_id);
                                    Ok((doc_id, None))
                                }
                            }),
                            Err(err) => Some(Err(err.into())),
                        })
                    })
                    .boxed();
                Ok(stream)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn batch_stream_get_objects_by_ids_with_errors<'a, T, S, I>(
        &'a self,
        collection_id: &str,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        for<'de> T: Deserialize<'de> + Send + 'a,
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let doc_stream = self
            .batch_stream_get_docs_by_ids_with_errors(
                self.get_documents_path(),
                collection_id,
                document_ids,
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
