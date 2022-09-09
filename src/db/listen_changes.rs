use crate::{FirestoreDb, FirestoreQueryParams, FirestoreResult};
use futures::StreamExt;
use futures::TryStreamExt;
use futures_util::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::*;
use std::collections::HashMap;

impl FirestoreDb {
    pub async fn listen_doc_changes<'a, 'b>(
        &'a self,
        database_path: &'a str,
        params: &'a FirestoreQueryParams,
        labels: HashMap<String, String>,
        since_token_value: Option<Vec<u8>>,
        target_id: i32,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        use futures::stream;

        let query_request = params.to_structured_query();
        let listen_request = ListenRequest {
            database: database_path.into(),
            labels,
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id,
                once: false,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent: params
                        .parent
                        .as_ref()
                        .unwrap_or_else(|| self.get_documents_path())
                        .clone(),
                    query_type: Some(target::query_target::QueryType::StructuredQuery(
                        query_request,
                    )),
                })),
                resume_type: since_token_value.map(target::ResumeType::ResumeToken),
            })),
        };

        let request = tonic::Request::new(
            futures::stream::iter(vec![listen_request]).chain(stream::pending()),
        );

        let response = self.client.get().listen(request).await?;

        Ok(response.into_inner().map_err(|e| e.into()).boxed())
    }
}
