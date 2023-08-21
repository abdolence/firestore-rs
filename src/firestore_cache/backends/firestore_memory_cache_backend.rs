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
    pub config: FirestoreCacheConfiguration,
    collection_caches: HashMap<String, FirestoreMemCache>,
    collection_targets: HashMap<FirestoreListenerTarget, String>,
}

impl FirestoreMemoryCacheBackend {
    pub fn new(config: FirestoreCacheConfiguration) -> Self {
        Self::with_collection_options(config, HashMap::new())
    }

    pub fn with_collection_options(
        config: FirestoreCacheConfiguration,
        mut mem_options_by_collection: HashMap<String, FirestoreMemCacheOptions>,
    ) -> Self {
        let collection_caches = config
            .collections
            .keys()
            .map(|collection| {
                mem_options_by_collection
                    .remove(collection.as_str())
                    .map_or_else(
                        || (collection.clone(), FirestoreMemCache::builder().build()),
                        |mem_options| (collection.clone(), mem_options.build()),
                    )
            })
            .collect();

        let collection_targets = config
            .collections
            .iter()
            .map(|(collection, collection_config)| {
                (
                    collection_config.listener_target.clone(),
                    collection.clone(),
                )
            })
            .collect();
        Self {
            config,
            collection_caches,
            collection_targets,
        }
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        _db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        Ok(self
            .config
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
                    if let Some(target_id) = doc_change.target_ids.first() {
                        if let Some(mem_cache_name) =
                            self.collection_targets.get(&(*target_id as u32).into())
                        {
                            if let Some(mem_cache) = self.collection_caches.get(mem_cache_name) {
                                mem_cache.insert(doc.name.clone(), doc).await;
                            }
                        }
                    }
                }
                Ok(())
            }
            FirestoreListenEvent::DocumentDelete(doc_deleted) => {
                if let Some(target_id) = doc_deleted.removed_target_ids.first() {
                    if let Some(mem_cache_name) =
                        self.collection_targets.get(&(*target_id as u32).into())
                    {
                        if let Some(mem_cache) = self.collection_caches.get(mem_cache_name) {
                            mem_cache.remove(&doc_deleted.document).await;
                        }
                    }
                }
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
        collection_id: &str,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        match self.collection_caches.get(collection_id) {
            Some(mem_cache) => Ok(mem_cache.get(document_path)),
            None => Ok(None),
        }
    }

    async fn update_doc_by_path(
        &self,
        collection_id: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        match self.collection_caches.get(collection_id) {
            Some(mem_cache) => {
                mem_cache
                    .insert(document.name.clone(), document.clone())
                    .await;
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn list_all_docs(
        &self,
        collection_id: &str,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        match self.collection_caches.get(collection_id) {
            Some(mem_cache) => Ok(Box::pin(futures::stream::iter(
                mem_cache.iter().map(|(_, doc)| Ok(doc)),
            ))),
            None => Ok(Box::pin(futures::stream::empty())),
        }
    }
}
