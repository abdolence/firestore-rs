use crate::db::safe_document_path;
use crate::db::support::FirestoreListenSupport;
use crate::timestamp_utils::to_timestamp;
use crate::{
    FirestoreDb, FirestoreListenerTarget, FirestoreListenerTargetParams,
    FirestoreListenerTargetResumeType, FirestoreRequestOptions, FirestoreResult,
    FirestoreTargetType,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use gcloud_sdk::google::firestore::v1::*;
use rvstruct::ValueStruct;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;

#[async_trait]
impl FirestoreListenSupport for FirestoreDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        mut requests: UnboundedReceiver<ListenRequest>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        // Closing the response must also end the request producer owned by the HTTP/2 transport.
        let (keep_request_open, response_closed) = tokio::sync::oneshot::channel::<()>();
        let request = gcloud_sdk::tonic::Request::new(
            futures::stream::poll_fn(move |cx| requests.poll_recv(cx))
                .chain(futures::stream::pending())
                .take_until(response_closed),
        );

        let response = self.client().get().listen(request).await?;

        Ok(futures::stream::unfold(
            (
                crate::db::end_at_first_error(response.into_inner()),
                keep_request_open,
            ),
            |(mut response, keep_request_open)| async move {
                response
                    .next()
                    .await
                    .map(|item| (item.map_err(Into::into), (response, keep_request_open)))
            },
        )
        .fuse()
        .boxed())
    }

    fn add_target_request(
        &self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            database: self.get_database_path().to_string(),
            labels: target_params.labels,
            request_options: self.resolve_request_options(target_params.request_options.as_ref()),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: target_params.target.try_into()?,
                once: target_params.add_target_once.unwrap_or(false),
                target_type: Some(match target_params.target_type {
                    FirestoreTargetType::Query(query_params) => {
                        target::TargetType::Query(target::QueryTarget {
                            parent: query_params
                                .parent
                                .as_ref()
                                .unwrap_or_else(|| self.get_documents_path())
                                .clone(),
                            query_type: Some(target::query_target::QueryType::StructuredQuery(
                                query_params.try_into()?,
                            )),
                        })
                    }
                    FirestoreTargetType::Documents(collection_documents) => {
                        target::TargetType::Documents(target::DocumentsTarget {
                            documents: collection_documents
                                .documents
                                .into_iter()
                                .map(|doc_id| {
                                    safe_document_path(
                                        collection_documents
                                            .parent
                                            .as_deref()
                                            .unwrap_or_else(|| self.get_documents_path()),
                                        collection_documents.collection.as_str(),
                                        doc_id,
                                    )
                                })
                                .collect::<FirestoreResult<Vec<String>>>()?,
                        })
                    }
                }),
                resume_type: target_params
                    .resume_type
                    .map(|resume_type| match resume_type {
                        FirestoreListenerTargetResumeType::Token(token) => {
                            target::ResumeType::ResumeToken(token.into_value())
                        }
                        FirestoreListenerTargetResumeType::ReadTime(dt) => {
                            target::ResumeType::ReadTime(to_timestamp(dt))
                        }
                    }),
                ..Default::default()
            })),
        })
    }

    fn remove_target_request(
        &self,
        target: &FirestoreListenerTarget,
        request_options: Option<&FirestoreRequestOptions>,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            database: self.get_database_path().to_string(),
            labels: HashMap::new(),
            request_options: self.resolve_request_options(request_options),
            target_change: Some(listen_request::TargetChange::RemoveTarget(
                target.clone().try_into()?,
            )),
        })
    }
}
