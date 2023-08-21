use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;

pub type FirestoreMemCacheOptions = CacheBuilder<
    String,
    FirestoreDocument,
    Cache<String, gcloud_sdk::google::firestore::v1::Document>,
>;

pub type FirestoreMemCache = Cache<String, FirestoreDocument, RandomState>;

pub struct FirestoreMemoryCacheBackend {
    mem_cache: FirestoreMemCache,
}
impl FirestoreMemoryCacheBackend {
    pub fn new() -> Self {
        Self::with_options(FirestoreMemCache::builder().max_capacity(10000))
    }

    pub fn with_options(cache_options: FirestoreMemCacheOptions) -> Self {
        Self {
            mem_cache: cache_options.build(),
        }
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
        _db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        Ok(config
            .collections
            .iter()
            .map(|(collection, collection_config)| {
                FirestoreListenerTargetParams::new(
                    collection_config.listener_target.clone(),
                    FirestoreTargetType::Query(FirestoreQueryParams::new(
                        collection.as_str().into(),
                    )),
                    HashMap::new(),
                )
                .with_resume_type(FirestoreListenerTargetResumeType::ReadTime(Utc::now()))
            })
            .collect())
    }

    async fn shutdown(&self) -> Result<(), FirestoreError> {
        Ok(())
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        match event {
            FirestoreListenEvent::DocumentChange(doc_change) => {
                if let Some(doc) = doc_change.document {
                    self.mem_cache.insert(doc.name.clone(), doc).await
                }
                Ok(())
            }
            FirestoreListenEvent::DocumentDelete(doc_deleted) => {
                self.mem_cache.remove(&doc_deleted.document).await;
                Ok(())
            }
            FirestoreListenEvent::DocumentRemove(doc_removed) => {
                self.mem_cache.remove(&doc_removed.document).await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestoreMemoryCacheBackend {
    async fn get_doc_by_path(
        &self,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        Ok(self.mem_cache.get(document_path))
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        self.mem_cache
            .insert(document.name.clone(), document.clone())
            .await;
        Ok(())
    }

    async fn list_all_docs(
        &self,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        Ok(Box::pin(futures::stream::iter(
            self.mem_cache.iter().map(|(_, doc)| Ok(doc)),
        )))
    }
}
