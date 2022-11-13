use crate::db::safe_document_path;
use crate::{FirestoreDb, FirestoreResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

#[async_trait]
pub trait FirestoreUpdateSupport {
    async fn update_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn update_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>;
}

#[async_trait]
impl FirestoreUpdateSupport for FirestoreDb {
    async fn update_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.update_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            update_only,
            return_only_fields,
        )
        .await
    }

    async fn update_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let firestore_doc = Self::serialize_to_doc(
            safe_document_path(parent, collection_id, document_id.as_ref())?.as_str(),
            obj,
        )?;

        let doc = self
            .update_doc(
                collection_id,
                firestore_doc,
                update_only,
                return_only_fields,
            )
            .await?;

        Self::deserialize_doc_to(&doc)
    }

    async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document> {
        let span = span!(
            Level::DEBUG,
            "Firestore Update Document",
            "/firestore/collection_name" = collection_id,
            "/firestore/response_time" = field::Empty
        );

        let document_id = firestore_doc.name.clone();

        let update_document_request = tonic::Request::new(UpdateDocumentRequest {
            update_mask: update_only.map({
                |vf| DocumentMask {
                    field_paths: vf.iter().map(|f| f.to_string()).collect(),
                }
            }),
            document: Some(firestore_doc),
            mask: return_only_fields.as_ref().map(|masks| DocumentMask {
                field_paths: masks.clone(),
            }),
            current_document: None,
        });

        let begin_query_utc: DateTime<Utc> = Utc::now();
        let update_response = self
            .client()
            .get()
            .update_document(update_document_request)
            .await?;
        let end_query_utc: DateTime<Utc> = Utc::now();
        let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

        span.record(
            "/firestore/response_time",
            query_duration.num_milliseconds(),
        );

        span.in_scope(|| {
            debug!(
                "[DB]: Updated the document: {}/{}",
                collection_id, document_id
            );
        });

        Ok(update_response.into_inner())
    }
}
