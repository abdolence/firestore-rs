use crate::*;
use async_trait::async_trait;

pub struct FirestoreMemOnlyOnDemandCacheBackend {}
impl FirestoreMemOnlyOnDemandCacheBackend {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemOnlyOnDemandCacheBackend {
    async fn load(
        &mut self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
        allocated_target_id: FirestoreListenerTarget,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        Ok(Vec::new())
    }

    async fn shutdown(&mut self) -> Result<(), FirestoreError> {
        Ok(())
    }
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestoreMemOnlyOnDemandCacheBackend {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
        return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        Ok(None)
    }

    async fn update_doc_by_path(
        &self,
        collection_id: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        Ok(())
    }
}
