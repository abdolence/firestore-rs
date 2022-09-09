use crate::{FirestoreDb, FirestoreError, FirestoreQueryOrder, FirestoreResult};
use chrono::prelude::*;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use serde::Deserialize;
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

impl FirestoreDb {
    pub async fn list_doc(
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

    pub async fn stream_list_doc(
        &self,
        params: FirestoreListDocParams,
    ) -> FirestoreResult<BoxStream<Document>> {
        let stream: BoxStream<Document> = Box::pin(
            futures_util::stream::unfold(Some(params), move |maybe_params| async move {
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
                                Some((results, Some(params.with_page_token(next_page_token))))
                            } else {
                                Some((results, None))
                            }
                        }
                        Err(err) => {
                            error!("[DB] Error occurred while consuming documents: {}", err);
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .flat_map(|doc_result| futures_util::stream::iter(doc_result.documents)),
        );

        Ok(stream)
    }

    pub async fn stream_list_obj<T>(
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

    fn create_list_request(
        &self,
        params: &FirestoreListDocParams,
    ) -> tonic::Request<ListDocumentsRequest> {
        tonic::Request::new(ListDocumentsRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            collection_id: params.collection_id.clone(),
            page_size: params.page_size as i32,
            page_token: params.page_token.clone().unwrap_or_else(|| "".to_string()),
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
            consistency_selector: None,
            show_missing: false,
        })
    }

    fn list_doc_with_retries<'a>(
        &'a self,
        params: FirestoreListDocParams,
        retries: usize,
        span: &'a Span,
    ) -> BoxFuture<'a, FirestoreResult<FirestoreListDocResult>> {
        let list_request = self.create_list_request(&params);
        async move {
            let begin_utc: DateTime<Utc> = Utc::now();

            match self
                .client
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
                        &listing_duration.num_milliseconds(),
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
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Listing failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );
                        self.list_doc_with_retries(params, retries + 1, span).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }
}
