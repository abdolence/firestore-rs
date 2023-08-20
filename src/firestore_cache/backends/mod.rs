use crate::errors::FirestoreError;
use crate::*;
use async_trait::async_trait;

pub struct FirestoreMemoryOnlyByIdCacheBackend {}

impl FirestoreMemoryOnlyByIdCacheBackend {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryOnlyByIdCacheBackend {
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
    ) -> Result<(), FirestoreError> {
        Ok(())
    }
}
