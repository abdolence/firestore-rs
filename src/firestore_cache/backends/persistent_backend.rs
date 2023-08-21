use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;

use futures::TryStreamExt;
use rocksdb::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::*;

pub struct FirestorePersistentCacheBackend {
    pub config: FirestoreCacheConfiguration,
    collection_targets: HashMap<FirestoreListenerTarget, String>,
    db: DBWithThreadMode<MultiThreaded>,
}

impl FirestorePersistentCacheBackend {
    pub fn new(config: FirestoreCacheConfiguration) -> FirestoreResult<Self> {
        let temp_dir = std::env::temp_dir();
        let firestore_cache_dir = temp_dir.join("firestore_cache");
        let db_dir = firestore_cache_dir.join("persistent");

        if !db_dir.exists() {
            debug!(
                "Creating a temp directory to store persistent cache: {}",
                db_dir.display()
            );
            std::fs::create_dir_all(&db_dir)?;
        } else {
            debug!(
                "Using a temp directory to store persistent cache: {}",
                db_dir.display()
            );
        }
        Self::with_options(config, db_dir)
    }

    pub fn with_options(
        config: FirestoreCacheConfiguration,
        data_dir: PathBuf,
    ) -> FirestoreResult<Self> {
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

        debug!("Opening database for persistent cache {:?}...", data_dir);

        let cfs: Vec<String> = config.collections.keys().into_iter().cloned().collect();

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        db_opts.set_compression_type(DBCompressionType::Snappy);

        let db = DBWithThreadMode::open_cf(&db_opts, data_dir, cfs)?;
        info!("Successfully opened database for persistent cache");

        Ok(Self {
            config,
            collection_targets,
            db,
        })
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection, config) in &self.config.collections {
            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllDocs => {
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
                            unimplemented!();
                            Ok(())
                        })
                        .await?;

                    info!(
                        "Preloading collection `{}` has been finished. Loaded: {} entries",
                        collection.as_str(),
                        unimplemented!()
                    );
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestorePersistentCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
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
                            unimplemented!()
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
                        unimplemented!()
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl FirestoreCacheDocsByPathSupport for FirestorePersistentCacheBackend {
    async fn get_doc_by_path(
        &self,
        collection_id: &str,
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        unimplemented!()
    }

    async fn update_doc_by_path(
        &self,
        collection_id: &str,
        document: &FirestoreDocument,
    ) -> FirestoreResult<()> {
        unimplemented!()
    }

    async fn list_all_docs(
        &self,
        collection_id: &str,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        unimplemented!()
    }
}
