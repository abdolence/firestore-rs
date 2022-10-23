use crate::{FirestoreDb, FirestoreResult};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::v1::*;

#[async_trait]
pub trait FirestoreDeleteSupport {
    async fn delete_by_id<S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl FirestoreDeleteSupport for FirestoreDb {
    async fn delete_by_id<S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        self.delete_by_id_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
        )
        .await
    }

    async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str> + Send,
    {
        let document_path = format!("{}/{}/{}", parent, collection_id, document_id.as_ref());

        let request = tonic::Request::new(DeleteDocumentRequest {
            name: document_path,
            current_document: None,
        });

        self.client().get().delete_document(request).await?;

        Ok(())
    }
}
