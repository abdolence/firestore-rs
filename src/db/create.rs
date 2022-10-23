use crate::{FirestoreDb, FirestoreResult};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::*;
use serde::{Deserialize, Serialize};
use tracing::*;

#[async_trait]
pub trait FirestoreCreateSupport {
    async fn create_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn create_obj_return_fields<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn create_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn create_obj_at_return_fields<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send;

    async fn create_doc_root<S>(
        &self,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;

    async fn create_doc<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl FirestoreCreateSupport for FirestoreDb {
    async fn create_obj<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.create_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
        )
        .await
    }

    async fn create_obj_return_fields<I, O, S>(
        &self,
        collection_id: &str,
        document_id: S,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.create_obj_at_return_fields(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            return_only_fields,
        )
        .await
    }

    async fn create_obj_at<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        self.create_obj_at_return_fields(parent, collection_id, document_id, obj, None)
            .await
    }

    async fn create_obj_at_return_fields<I, O, S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &I,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<O>
    where
        I: Serialize + Sync + Send,
        for<'de> O: Deserialize<'de>,
        S: AsRef<str> + Send,
    {
        let input_doc = Self::serialize_to_doc("", obj)?;

        let doc = self
            .create_doc(
                parent,
                collection_id,
                document_id,
                input_doc,
                return_only_fields,
            )
            .await?;

        Self::deserialize_doc_to(&doc)
    }

    async fn create_doc_root<S>(
        &self,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
    {
        self.create_doc(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            input_doc,
            return_only_fields,
        )
        .await
    }

    async fn create_doc<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        input_doc: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreResult<Document>
    where
        S: AsRef<str> + Send,
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
