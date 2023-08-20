use crate::errors::FirestoreError;
use crate::*;
use async_trait::async_trait;

pub struct FirestoreMemoryOnlyCacheBackend {}

impl FirestoreMemoryOnlyCacheBackend {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryOnlyCacheBackend {
    async fn load(
        &mut self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
    ) -> Result<(), FirestoreError> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), FirestoreError> {
        Ok(())
    }
}
