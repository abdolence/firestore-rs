use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder};

use futures::TryStreamExt;
use std::collections::HashMap;
use tracing::*;

pub type FirestoreMemCache = Cache<String, FirestoreDocument>;

pub type FirestoreMemCacheOptions = CacheBuilder<String, FirestoreDocument, FirestoreMemCache>;

pub struct FirestoreMemoryCacheBackend {
    pub config: FirestoreCacheConfiguration,
    collection_caches: HashMap<String, FirestoreMemCache>,
    collection_targets: HashMap<FirestoreListenerTarget, String>,
}

impl FirestoreMemoryCacheBackend {
    pub fn new(config: FirestoreCacheConfiguration) -> FirestoreResult<Self> {
        Self::with_collection_options(config, |_| FirestoreMemCache::builder().max_capacity(10000))
    }

    pub fn with_collection_options(
        config: FirestoreCacheConfiguration,
        collection_mem_options: fn(&str) -> FirestoreMemCacheOptions,
    ) -> FirestoreResult<Self> {
        let collection_caches = config
            .collections
            .keys()
            .map(|collection| {
                (
                    collection.clone(),
                    collection_mem_options(collection.as_str()).build(),
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
        Ok(Self {
            config,
            collection_caches,
            collection_targets,
        })
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection, config) in &self.config.collections {
            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllDocs => {
                    if let Some(mem_cache) = self.collection_caches.get(collection.as_str()) {
                        debug!("Preloading {}", collection.as_str());
                        let stream = db
                            .fluent()
                            .list()
                            .from(collection.as_str())
                            .page_size(1000)
                            .stream_all_with_errors()
                            .await?;

                        stream
                            .try_for_each_concurrent(2, |doc| async move {
                                mem_cache.insert(doc.name.clone(), doc).await;
                                Ok(())
                            })
                            .await?;

                        info!(
                            "Preloading collection `{}` has been finished. Loaded: {} entries",
                            collection.as_str(),
                            mem_cache.entry_count()
                        );
                    }
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestoreMemoryCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        let read_from_time = Utc::now();

        self.preload_collections(db).await?;

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
                .with_resume_type(FirestoreListenerTargetResumeType::ReadTime(read_from_time))
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
