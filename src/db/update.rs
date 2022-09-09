use crate::{FirestoreDb, FirestoreResult};
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

impl FirestoreDb {
    pub async fn update_obj<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        self.update_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            update_only,
        )
        .await
    }

    pub async fn update_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        let firestore_doc = Self::serialize_to_doc(
            format!("{}/{}/{}", parent, collection_id, document_id.as_ref()).as_str(),
            obj,
        )?;

        let doc = self
            .update_doc(collection_id, firestore_doc, update_only, None)
            .await?;
        Self::deserialize_doc_to(&doc)
    }

    pub async fn update_doc(
        &self,
        collection_id: &str,
        firestore_doc: Document,
        update_only: Option<Vec<String>>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document> {
        let _span = span!(
            Level::DEBUG,
            "Firestore Update Document",
            "/firestore/collection_name" = collection_id
        );

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

        let update_response = self
            .client()
            .get()
            .update_document(update_document_request)
            .await?;

        Ok(update_response.into_inner())
    }
}
