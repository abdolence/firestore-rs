use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;

use chrono::Utc;
use futures::StreamExt;
use gcloud_sdk::google::firestore::v1::Document;
use gcloud_sdk::prost::Message;
use redb::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::*;

pub struct FirestorePersistentCacheBackend {
    pub config: FirestoreCacheConfiguration,
    redb: Database,
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
        Self::with_options(config, db_dir.join("redb"))
    }

    pub fn with_options(
        config: FirestoreCacheConfiguration,
        data_file_path: PathBuf,
    ) -> FirestoreResult<Self> {
        if data_file_path.exists() {
            debug!(
                "Opening database for persistent cache {:?}...",
                data_file_path
            );
        } else {
            debug!(
                "Creating database for persistent cache {:?}...",
                data_file_path
            );
        }

        let mut db = Database::create(data_file_path)?;

        db.compact()?;
        info!("Successfully opened database for persistent cache");

        Ok(Self { config, redb: db })
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection_path, config) in &self.config.collections {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path.as_str());

            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllDocs
                | FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty => {
                    let existing_records = {
                        let read_tx = self.redb.begin_read()?;
                        if read_tx
                            .list_tables()?
                            .any(|t| t.name() == collection_path.as_str())
                        {
                            read_tx.open_table(td)?.len()?
                        } else {
                            0
                        }
                    };

                    if matches!(
                        config.collection_load_mode,
                        FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty
                    ) && existing_records > 0
                    {
                        info!(
                                "Preloading collection `{}` has been skipped. Already loaded: {} entries",
                                collection_path.as_str(),
                                existing_records
                            );
                        continue;
                    }

                    debug!("Preloading {}", collection_path.as_str());
                    let params = if let Some(parent) = &config.parent {
                        db.fluent()
                            .list()
                            .from(&config.collection_name)
                            .parent(parent)
                    } else {
                        db.fluent().list().from(&config.collection_name)
                    };

                    let stream = params.page_size(1000).stream_all().await?;

                    stream
                        .ready_chunks(100)
                        .for_each(|docs| async move {
                            if let Err(err) = self.write_batch_docs(collection_path, docs) {
                                error!("Error while preloading collection: {}", err);
                            }
                        })
                        .await;

                    let updated_records = if matches!(
                        config.collection_load_mode,
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs
                    ) || existing_records == 0
                    {
                        let read_tx = self.redb.begin_read()?;
                        let table = read_tx.open_table(td)?;
                        table.len()?
                    } else {
                        existing_records
                    };

                    info!(
                        "Preloading collection `{}` has been finished. Loaded: {} entries",
                        collection_path.as_str(),
                        updated_records
                    );
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {
                    let tx = self.redb.begin_write()?;
                    debug!(
                        "Creating corresponding collection table `{}`",
                        collection_path
                    );
                    tx.open_table(td)?;
                    tx.commit()?;
                }
            }
        }
        Ok(())
    }

    fn write_batch_docs(&self, collection_path: &str, docs: Vec<Document>) -> FirestoreResult<()> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let write_txn = self.redb.begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;

            for doc in docs {
                let (_, document_id) = split_document_path(&doc.name);
                let doc_bytes = Self::document_to_buf(&doc)?;
                table.insert(document_id, doc_bytes.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    fn document_to_buf(doc: &FirestoreDocument) -> FirestoreResult<Vec<u8>> {
        let mut proto_output_buf = Vec::new();
        doc.encode(&mut proto_output_buf)?;
        Ok(proto_output_buf)
    }

    fn buf_to_document<B>(buf: B) -> FirestoreResult<FirestoreDocument>
    where
        B: AsRef<[u8]>,
    {
        let doc = FirestoreDocument::decode(buf.as_ref())?;
        Ok(doc)
    }

    fn write_document(&self, doc: &Document) -> FirestoreResult<()> {
        let (collection_path, document_id) = split_document_path(&doc.name);

        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let write_txn = self.redb.begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;
            let doc_bytes = Self::document_to_buf(doc)?;
            table.insert(document_id, doc_bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn table_len(&self, collection_id: &str) -> FirestoreResult<u64> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_id);
        let read_tx = self.redb.begin_read()?;
        let len = read_tx.open_table(td)?.len()?;
        Ok(len)
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestorePersistentCacheBackend {
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
            .map(|(collection_path, collection_config)| {
                let collection_table_len = self.table_len(collection_path).ok().unwrap_or(0);
                let resume_type = if collection_table_len == 0 {
                    Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time))
                } else {
                    None
                };
                FirestoreListenerTargetParams::new(
                    collection_config.listener_target.clone(),
                    FirestoreTargetType::Query(
                        FirestoreQueryParams::new(
                            collection_config.collection_name.as_str().into(),
                        )
                        .opt_parent(collection_config.parent.clone()),
                    ),
                    HashMap::new(),
                )
                .opt_resume_type(resume_type)
            })
            .collect())
    }

    async fn invalidate_all(&self) -> FirestoreResult<()> {
        for collection_path in self.config.collections.keys() {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path.as_str());

            let write_txn = self.redb.begin_write()?;
            {
                debug!(
                    "Invalidating {} and draining the corresponding table",
                    collection_path
                );
                let mut table = write_txn.open_table(td)?;
                table.drain::<&str>(..)?;
            }
            write_txn.commit()?;
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), FirestoreError> {
        Ok(())
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        match event {
            FirestoreListenEvent::DocumentChange(doc_change) => {
                if let Some(doc) = doc_change.document {
                    trace!(
                        "Writing document to cache due to listener event: {:?}",
                        doc.name
                    );
                    self.write_document(&doc)?;
                }
                Ok(())
            }
            FirestoreListenEvent::DocumentDelete(doc_deleted) => {
                let (collection_path, document_id) = split_document_path(&doc_deleted.document);
                let write_txn = self.redb.begin_write()?;
                let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
                let mut table = write_txn.open_table(td)?;
                trace!(
                    "Removing document from cache due to listener event: {:?}",
                    doc_deleted.document.as_str()
                );
                table.remove(document_id)?;
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
        document_path: &str,
    ) -> FirestoreResult<Option<FirestoreDocument>> {
        let (collection_path, document_id) = split_document_path(document_path);

        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let read_tx = self.redb.begin_read()?;
        let table = read_tx.open_table(td)?;
        let value = table.get(document_id)?;
        value.map(|v| Self::buf_to_document(v.value())).transpose()
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        self.write_document(document)?;
        Ok(())
    }

    async fn list_all_docs(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<BoxStream<FirestoreResult<FirestoreDocument>>> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let read_tx = self.redb.begin_read()?;
        let table = read_tx.open_table(td)?;
        let iter = table.iter()?;

        // It seems there is no way to work with streaming for redb, so this is not efficient
        let mut docs: Vec<FirestoreResult<FirestoreDocument>> = Vec::new();
        for record in iter {
            let (_, v) = record?;
            let doc = Self::buf_to_document(v.value())?;
            docs.push(Ok(doc));
        }

        Ok(Box::pin(futures::stream::iter(docs)))
    }

    async fn query_docs(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<FirestoreResult<FirestoreDocument>>>> {
        // For now only basic/simple query all supported
        if query.all_descendants.iter().all(|x| !*x)
            && query.order_by.is_none()
            && query.filter.is_none()
            && query.start_at.is_none()
            && query.end_at.is_none()
            && query.offset.is_none()
            && query.limit.is_none()
            && query.return_only_fields.is_none()
        {
            Ok(FirestoreCachedValue::UseCached(
                self.list_all_docs(collection_path).await?,
            ))
        } else {
            Ok(FirestoreCachedValue::SkipCache)
        }
    }
}

impl From<redb::Error> for FirestoreError {
    fn from(db_err: redb::Error) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::DatabaseError> for FirestoreError {
    fn from(db_err: redb::DatabaseError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbDatabaseError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::TransactionError> for FirestoreError {
    fn from(db_err: redb::TransactionError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbTransactionError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::TableError> for FirestoreError {
    fn from(db_err: redb::TableError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbTableError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::CommitError> for FirestoreError {
    fn from(db_err: redb::CommitError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbCommitError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::StorageError> for FirestoreError {
    fn from(db_err: redb::StorageError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbStorageError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}

impl From<redb::CompactionError> for FirestoreError {
    fn from(db_err: redb::CompactionError) -> Self {
        FirestoreError::CacheError(FirestoreCacheError::new(
            FirestoreErrorPublicGenericDetails::new("RedbCompactionError".into()),
            format!("Cache error: {db_err}"),
        ))
    }
}
