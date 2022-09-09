use crate::{FirestoreDb, FirestoreResult};
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

impl FirestoreDb {
    pub async fn create_obj<T, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &T,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        self.create_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
        )
        .await
    }

    pub async fn create_obj_at<T, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
    ) -> FirestoreResult<T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
        S: AsRef<str>,
    {
        let input_doc = Self::serialize_to_doc("", obj)?;

        let doc = self
            .create_doc(parent, collection_id, document_id, input_doc, None)
            .await?;

        Self::deserialize_doc_to(&doc)
    }

    pub async fn create_doc<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str>,
    {
        let _span = span!(
            Level::DEBUG,
            "Firestore Create Document",
            "/firestore/collection_name" = collection_id
        );

        let create_document_request = tonic::Request::new(CreateDocumentRequest {
            parent: parent.into(),
            document_id: document_id.as_ref().to_string(),
            mask: return_only_fields.as_ref().map(|masks| DocumentMask {
                field_paths: masks.clone(),
            }),
            collection_id: collection_id.into(),
            document: Some(input_doc),
        });

        let create_response = self
            .client()
            .get()
            .create_document(create_document_request)
            .await?;

        Ok(create_response.into_inner())
    }
}
