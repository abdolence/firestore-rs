use crate::errors::FirestoreError;
use crate::*;
use async_trait::async_trait;
use rsb_derive::Builder;

#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct FirestoreMemoryOnlyCacheBackendOptions {
    pub mode: FirestoreMemoryOnlyCacheBackendMode,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum FirestoreMemoryOnlyCacheBackendMode {
    LoadOnDemandOnly,
    LoadAll,
}

pub struct FirestoreMemoryOnlyCacheBackend {
    options: FirestoreMemoryOnlyCacheBackendOptions,
}

impl FirestoreMemoryOnlyCacheBackend {
    pub fn new(mode: FirestoreMemoryOnlyCacheBackendMode) -> Self {
        Self {
            options: FirestoreMemoryOnlyCacheBackendOptions::new(mode),
        }
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

#[async_trait]
impl FirestoreCacheGetDocsSupport for FirestoreMemoryOnlyCacheBackend {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
        return_only_fields: &Option<Vec<String>>,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        Ok(None)
    }
}

#[async_trait]
impl FirestoreCacheDocUpdateSupport for FirestoreMemoryOnlyCacheBackend {
    async fn update_doc_by_path(
        &mut self,
        collection_id: &str,
        document_path: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        Ok(())
    }
}
