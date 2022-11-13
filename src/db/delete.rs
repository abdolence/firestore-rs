use crate::db::safe_document_path;
use crate::{FirestoreDb, FirestoreResult, FirestoreWritePrecondition};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcloud_sdk::google::firestore::v1::*;
use tracing::*;

#[async_trait]
pub trait FirestoreDeleteSupport {
    async fn delete_by_id<S>(
        &self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl FirestoreDeleteSupport for FirestoreDb {
    async fn delete_by_id<S>(
        &self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        self.delete_by_id_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            precondition,
        )
        .await
    }

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        let document_path = safe_document_path(parent, collection_id, document_id.as_ref())?;

        let span = span!(
            Level::DEBUG,
            "Firestore Delete Document",
            "/firestore/collection_name" = collection_id,
            "/firestore/response_time" = field::Empty
        );

        let request = tonic::Request::new(DeleteDocumentRequest {
            name: document_path,
            current_document: precondition.map(|cond| cond.try_into()).transpose()?,
        });

        let begin_query_utc: DateTime<Utc> = Utc::now();
        self.client().get().delete_document(request).await?;
        let end_query_utc: DateTime<Utc> = Utc::now();
        let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

        span.record(
            "/firestore/response_time",
            query_duration.num_milliseconds(),
        );

        span.in_scope(|| {
            debug!(
                "[DB]: Deleted a document: {}/{}",
                collection_id,
                document_id.as_ref()
            );
        });

        Ok(())
    }
}
