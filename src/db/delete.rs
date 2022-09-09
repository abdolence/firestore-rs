use crate::{FirestoreDb, FirestoreResult};
use gcloud_sdk::google::firestore::v1::*;

impl FirestoreDb {
    pub async fn delete_by_id<S>(&self, collection_id: &str, document_id: S) -> FirestoreResult<()>
    where
        S: AsRef<str>,
    {
        self.delete_by_id_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
        )
        .await
    }

    pub async fn delete_by_id_at<S>(
        &self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<()>
    where
        S: AsRef<str>,
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
