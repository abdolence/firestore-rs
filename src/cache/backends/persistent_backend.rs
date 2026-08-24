use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use crate::FirestoreInstant;
use futures::StreamExt;
use gcloud_sdk::google::firestore::v1::Document;
use gcloud_sdk::prost::Message;
use redb::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::*;

/// A disk-backed cache, storing documents as protobuf in a
/// [redb](https://github.com/cberner/redb) database with one table per collection.
///
/// Because the cache survives restarts, a collection configured with
/// [`PreloadAllIfEmpty`](crate::FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty) is only
/// downloaded once and then kept current by the listener.
///
/// Create one through the builder rather than directly:
///
/// ```rust,no_run
/// # use firestore::*;
/// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let cache = FirestoreCache::persistent(db)
///     .data_dir("/var/cache/my-app")
///     .preloaded_collection("countries")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct FirestorePersistentCacheBackend {
    /// Behind a lock so that collections can be added and removed while the cache runs. Readers
    /// clone the `Arc` out and drop the guard, so no guard is ever held across an await.
    config: std::sync::RwLock<Arc<FirestoreCacheConfiguration>>,
    /// `None` once the cache has been shut down.
    ///
    /// The database holds an exclusive lock on its file, so closing it is the only way to release
    /// that lock without dropping every handle to the backend - and handles outlive the cache,
    /// because `read_through_cache` clones one into each `FirestoreDb` it is attached to.
    redb: std::sync::RwLock<Option<Database>>,
    /// Collections whose listener target Firestore has reset or removed, and which are therefore
    /// mid-replay. They must not answer `list`/`query` until the replay completes, because what
    /// they hold in the meantime is a partial view that would look like a complete one.
    suspended_collections: std::sync::RwLock<HashSet<String>>,
}

/// Tuning options for [`FirestorePersistentCacheBackend`].
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct FirestorePersistentCacheOptions {
    /// Where to store the database file.
    ///
    /// When `None`, a file inside the system temporary directory is used, which is **not
    /// durable** - see [`FirestorePersistentCacheBackend::new`].
    pub data_file_path: Option<PathBuf>,
}

impl FirestorePersistentCacheOptions {
    /// Creates options using the default temporary-directory location.
    #[inline]
    pub fn new() -> Self {
        Self {
            data_file_path: None,
        }
    }

    /// Stores the database at the given file path.
    #[inline]
    pub fn with_data_file_path(self, data_file_path: PathBuf) -> Self {
        Self {
            data_file_path: Some(data_file_path),
        }
    }
}

impl FirestorePersistentCacheBackend {
    /// Creates a backend storing its database in the system temporary directory.
    ///
    /// **The temporary directory is not durable.** Operating systems remove its contents, so a
    /// cache stored there can vanish between runs, defeating the point of a persistent cache.
    /// For anything beyond experimentation give it an explicit location with
    /// [`with_options`](Self::with_options), or use the builder's
    /// [`data_dir`](crate::FirestoreCacheBuilder::data_dir), which also keeps the listener resume
    /// tokens alongside the data.
    pub fn new(config: FirestoreCacheConfiguration) -> FirestoreResult<Self> {
        let temp_dir = std::env::temp_dir();
        let firestore_cache_dir = temp_dir.join("firestore_cache");
        let db_dir = firestore_cache_dir.join("persistent");

        if !db_dir.exists() {
            debug!(
                directory = %db_dir.display(),
                "Creating a temp directory to store persistent cache.",
            );
            std::fs::create_dir_all(&db_dir)?;
        } else {
            debug!(
                directory = %db_dir.display(),
                "Using a temp directory to store persistent cache.",
            );
        }
        Self::with_options(config, db_dir.join("redb"))
    }

    /// Creates a backend storing its database at the given file path.
    pub fn with_options(
        config: FirestoreCacheConfiguration,
        data_file_path: PathBuf,
    ) -> FirestoreResult<Self> {
        if data_file_path.exists() {
            debug!(?data_file_path, "Opening database for persistent cache...",);
        } else {
            debug!(?data_file_path, "Creating database for persistent cache...",);
        }

        let mut db = Database::create(data_file_path)?;

        db.compact()?;
        info!("Successfully opened database for persistent cache.");

        Ok(Self {
            config: std::sync::RwLock::new(Arc::new(config)),
            redb: std::sync::RwLock::new(Some(db)),
            suspended_collections: std::sync::RwLock::new(HashSet::new()),
        })
    }

    /// A snapshot of the collections this backend currently caches.
    pub fn config(&self) -> Arc<FirestoreCacheConfiguration> {
        self.config
            .read()
            .expect("cache configuration lock poisoned")
            .clone()
    }

    /// Borrows the open database, or reports that the cache has been shut down.
    ///
    /// The guard must not be held across an await - every caller uses it inside a synchronous
    /// block, which is what keeps `shutdown` able to take the database away promptly.
    fn open_db(&self) -> FirestoreResult<std::sync::RwLockReadGuard<'_, Option<Database>>> {
        let guard = self.redb.read().expect("cache database lock poisoned");
        if guard.is_none() {
            return Err(FirestoreError::CacheError(FirestoreCacheError::new(
                FirestoreErrorPublicGenericDetails::new("CacheShutdown".into()),
                "This persistent cache has been shut down and no longer holds its database."
                    .to_string(),
            )));
        }
        Ok(guard)
    }

    /// Whether the cache is still usable. Reads fall back to Firestore once it is not.
    fn is_open(&self) -> bool {
        self.redb
            .read()
            .expect("cache database lock poisoned")
            .is_some()
    }

    /// Whether a collection is mid-replay after Firestore reset or removed its listener target.
    fn is_suspended(&self, collection_path: &str) -> bool {
        self.suspended_collections
            .read()
            .expect("cache suspended collections lock poisoned")
            .contains(collection_path)
    }

    /// Drops everything cached for one collection, leaving its table in place.
    fn invalidate_collection(&self, collection_path: &str) -> FirestoreResult<()> {
        if !self.config().collections.contains_key(collection_path) {
            return Ok(());
        }

        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let write_txn = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
        {
            debug!(
                collection_path,
                "Invalidating collection and draining the corresponding table.",
            );
            let mut table = write_txn.open_table(td)?;
            table.retain(|_, _| false)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Reads exactly the documents a collection is limited to, rather than downloading the whole
    /// collection and throwing most of it away.
    async fn preload_watched_documents(
        &self,
        db: &FirestoreDb,
        collection_path: &str,
        config: &FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<()> {
        let Some(document_ids) = config.watched_document_ids() else {
            return Ok(());
        };

        let selector = db
            .fluent()
            .select()
            .by_id_in(config.collection_name.as_str());
        let selector = match &config.parent {
            Some(parent) => selector.parent(parent),
            None => selector,
        };

        let mut stream = selector.batch(document_ids.to_vec()).await?;
        let mut found: Vec<Document> = Vec::new();
        while let Some((_, doc)) = stream.next().await {
            if let Some(doc) = doc {
                found.push(doc);
            }
        }

        if !found.is_empty() {
            self.write_batch_docs(collection_path, found)?;
        }
        Ok(())
    }

    /// Removes several documents of one collection in a single transaction.
    fn evict_documents(
        &self,
        collection_path: &str,
        document_ids: &[String],
    ) -> FirestoreResult<()> {
        if !self.config().collections.contains_key(collection_path) {
            return Ok(());
        }

        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let write_txn = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;
            for document_id in document_ids {
                table.remove(document_id.as_str())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Removes a document from the cache, wherever the listener said it went.
    fn evict_doc_by_path(&self, document_path: &str) -> FirestoreResult<()> {
        let (collection_path, document_id) = split_document_path(document_path);

        if !self.config().collections.contains_key(collection_path) {
            return Ok(());
        }

        trace!(
            document_path,
            "Removing document from cache due to listener event.",
        );

        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let write_txn = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;
            table.remove(document_id)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection_path, config) in &self.config().collections {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path.as_str());

            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllDocs
                | FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty => {
                    let existing_records = {
                        let read_tx = self
                            .open_db()?
                            .as_ref()
                            .expect("database checked open")
                            .begin_read()?;
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
                            collection_path = collection_path.as_str(),
                            entries_loaded = existing_records,
                            "Preloading collection has been skipped.",
                        );
                        continue;
                    }

                    debug!(
                        collection_path = collection_path.as_str(),
                        "Preloading collection."
                    );

                    if config.watched_document_ids().is_some() {
                        self.preload_watched_documents(db, collection_path, config)
                            .await?;
                        info!(
                            collection_path = collection_path.as_str(),
                            "Preloading watched documents has been finished.",
                        );
                        continue;
                    }

                    let params = if let Some(parent) = &config.parent {
                        db.fluent()
                            .select()
                            .from(config.collection_name.as_str())
                            .parent(parent)
                    } else {
                        db.fluent().select().from(config.collection_name.as_str())
                    };

                    let stream = params.stream_query().await?;

                    stream
                        .enumerate()
                        .map(|(index, docs)| {
                            if index > 0 && index % 5000 == 0 {
                                debug!(
                                    collection_path = collection_path.as_str(),
                                    entries_loaded = index,
                                    "Collection preload in progress...",
                                );
                            }
                            docs
                        })
                        .ready_chunks(100)
                        .for_each(|docs| async move {
                            if let Err(err) = self.write_batch_docs(collection_path, docs) {
                                error!(?err, "Error while preloading collection.");
                            }
                        })
                        .await;

                    let updated_records = if matches!(
                        config.collection_load_mode,
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs
                    ) || existing_records == 0
                    {
                        let read_tx = self
                            .open_db()?
                            .as_ref()
                            .expect("database checked open")
                            .begin_read()?;
                        let table = read_tx.open_table(td)?;
                        table.len()?
                    } else {
                        existing_records
                    };

                    info!(
                        collection_path = collection_path.as_str(),
                        updated_records, "Preloading collection has been finished.",
                    );
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {
                    let tx = self
                        .open_db()?
                        .as_ref()
                        .expect("database checked open")
                        .begin_write()?;
                    debug!(collection_path, "Creating corresponding collection table.",);
                    tx.open_table(td)?;
                    tx.commit()?;
                }
            }
        }
        Ok(())
    }

    fn write_batch_docs(&self, collection_path: &str, docs: Vec<Document>) -> FirestoreResult<()> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let write_txn = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
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

        if self
            .config()
            .is_document_cached(collection_path, document_id)
        {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

            let write_txn = self
                .open_db()?
                .as_ref()
                .expect("database checked open")
                .begin_write()?;
            {
                let mut table = write_txn.open_table(td)?;
                let doc_bytes = Self::document_to_buf(doc)?;
                table.insert(document_id, doc_bytes.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Creates a collection's table if it does not exist yet.
    fn create_collection_table(&self, collection_path: &str) -> FirestoreResult<()> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let tx = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
        tx.open_table(td)?;
        tx.commit()?;
        Ok(())
    }

    /// Deletes a collection's table entirely. Returns whether there was one.
    fn drop_collection_table(&self, collection_path: &str) -> FirestoreResult<bool> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let tx = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?;
        let existed = tx.delete_table(td)?;
        tx.commit()?;
        Ok(existed)
    }

    fn table_len(&self, collection_id: &str) -> FirestoreResult<u64> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_id);
        let read_tx = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_read()?;
        let len = read_tx.open_table(td)?.len()?;
        Ok(len)
    }

    async fn query_cached_docs<'b>(
        &self,
        collection_path: &str,
        query_engine: FirestoreCacheQueryEngine,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let read_tx = self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_read()?;
        let table = read_tx.open_table(td)?;
        let iter = table.iter()?;

        // It seems there is no way to work with streaming for redb, so this is not efficient
        let mut docs: Vec<FirestoreResult<FirestoreDocument>> = Vec::new();
        for record in iter {
            let (_, v) = record?;
            let doc = Self::buf_to_document(v.value())?;
            if query_engine.matches_doc(&doc) {
                docs.push(Ok(doc));
            }
        }

        let filtered_stream = Box::pin(futures::stream::iter(docs));
        let output_stream = query_engine.process_query_stream(filtered_stream).await?;

        Ok(output_stream)
    }
}

#[async_trait]
impl FirestoreCacheBackend for FirestorePersistentCacheBackend {
    async fn load(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> Result<Vec<FirestoreListenerTargetParams>, FirestoreError> {
        let read_from_time = FirestoreInstant::now();

        self.preload_collections(db).await?;

        Ok(self
            .config()
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
        for collection_path in self.config().collections.keys() {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path.as_str());

            let write_txn = self
                .open_db()?
                .as_ref()
                .expect("database checked open")
                .begin_write()?;
            {
                debug!(
                    collection_path,
                    "Invalidating collection and draining the corresponding table.",
                );
                let mut table = write_txn.open_table(td)?;
                table.retain(|_, _| false)?;
            }
            write_txn.commit()?;
        }

        Ok(())
    }

    fn cache_configuration(&self) -> Arc<FirestoreCacheConfiguration> {
        self.config()
    }

    async fn add_collection(
        &self,
        _options: &FirestoreCacheOptions,
        db: &FirestoreDb,
        collection_config: FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<FirestoreListenerTargetParams> {
        // Captured before reading anything, so that writes landing during the preload are still
        // delivered once the listener target attaches from this point in time.
        let read_from_time = FirestoreInstant::now();
        let collection_path = collection_config.resolve_collection_path(db.get_documents_path());

        // A collection added at runtime starts from nothing - `remove_collection` deletes the
        // table - so both preloading modes mean the same thing here.
        self.create_collection_table(&collection_path)?;

        if collection_config.collection_load_mode.is_preloading() {
            debug!(
                collection_path = collection_path.as_str(),
                "Preloading collection."
            );

            if let Some(document_ids) = collection_config.watched_document_ids() {
                self.preload_watched_documents(db, &collection_path, &collection_config)
                    .await?;
                info!(
                    collection_path = collection_path.as_str(),
                    watched_documents = document_ids.len(),
                    "Preloading watched documents has been finished.",
                );

                let target_params = crate::cache::target_params_for_collection(
                    &collection_config,
                    Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time)),
                );
                let mut config = self
                    .config
                    .write()
                    .expect("cache configuration lock poisoned");
                *config = Arc::new(
                    (**config)
                        .clone()
                        .add_collection_config_at(db.get_documents_path(), collection_config),
                );
                return Ok(target_params);
            }

            let params = if let Some(parent) = &collection_config.parent {
                db.fluent()
                    .select()
                    .from(collection_config.collection_name.as_str())
                    .parent(parent)
            } else {
                db.fluent()
                    .select()
                    .from(collection_config.collection_name.as_str())
            };

            params
                .stream_query()
                .await?
                .ready_chunks(100)
                .for_each(|docs| async {
                    if let Err(err) = self.write_batch_docs(&collection_path, docs) {
                        error!(?err, "Error while preloading collection.");
                    }
                })
                .await;

            info!(
                collection_path = collection_path.as_str(),
                updated_records = self.table_len(&collection_path).unwrap_or(0),
                "Preloading collection has been finished.",
            );
        }

        let target_params = crate::cache::target_params_for_collection(
            &collection_config,
            Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time)),
        );

        // Published only now: until this point the collection does not exist as far as reads are
        // concerned, so no listing can see it half filled.
        {
            let mut config = self
                .config
                .write()
                .expect("cache configuration lock poisoned");
            *config = Arc::new(
                (**config)
                    .clone()
                    .add_collection_config_at(db.get_documents_path(), collection_config),
            );
        }

        Ok(target_params)
    }

    async fn remove_collection(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<Option<FirestoreListenerTarget>> {
        // The configuration goes first, so reads stop using the collection immediately and any
        // listener event arriving before the target is dropped is ignored.
        let removed = {
            let mut config = self
                .config
                .write()
                .expect("cache configuration lock poisoned");
            let mut updated = (**config).clone();
            let removed = updated.collections.remove(collection_path);
            *config = Arc::new(updated);
            removed
        };

        let Some(removed) = removed else {
            return Ok(None);
        };

        debug!(
            collection_path,
            "Dropping the table of a removed collection."
        );
        self.drop_collection_table(collection_path)?;

        self.suspended_collections
            .write()
            .expect("cache suspended collections lock poisoned")
            .remove(collection_path);

        Ok(Some(removed.listener_target))
    }

    async fn shutdown(&self) -> Result<(), FirestoreError> {
        // Closing the database is what releases its exclusive file lock, so that another cache can
        // be opened over the same directory in this process. Dropping it here rather than with the
        // backend matters because handles to the backend outlive the cache: `read_through_cache`
        // clones one into every `FirestoreDb` it is attached to.
        let closed = self
            .redb
            .write()
            .expect("cache database lock poisoned")
            .take();

        if closed.is_some() {
            debug!("Closing the database of the persistent cache.");
        }
        drop(closed);
        Ok(())
    }

    async fn on_listen_event(&self, event: FirestoreListenEvent) -> FirestoreResult<()> {
        if !self.is_open() {
            return Ok(());
        }

        match event {
            FirestoreListenEvent::DocumentChange(doc_change) => {
                if let Some(doc) = doc_change.document {
                    trace!(
                        doc_name = ?doc.name,
                        "Writing document to cache due to listener event.",
                    );

                    self.write_document(&doc)?;
                }
                Ok(())
            }
            FirestoreListenEvent::DocumentDelete(doc_deleted) => {
                self.evict_doc_by_path(&doc_deleted.document)?;
                Ok(())
            }
            // The document went out of view of the target. Firestore sends this instead of a
            // delete when it cannot send the new value, so keeping the document would leave the
            // cache serving something the caller can no longer read.
            FirestoreListenEvent::DocumentRemove(doc_removed) => {
                self.evict_doc_by_path(&doc_removed.document)?;
                Ok(())
            }
            FirestoreListenEvent::TargetChange(ref target_change) => {
                match crate::cache::cache_target_change_action(&self.config(), target_change) {
                    crate::cache::FirestoreCacheTargetChangeAction::SuspendAndInvalidate(
                        invalidations,
                    ) => {
                        {
                            let mut suspended = self
                                .suspended_collections
                                .write()
                                .expect("cache suspended collections lock poisoned");
                            suspended.extend(
                                invalidations
                                    .iter()
                                    .map(|invalidation| invalidation.collection_path().to_string()),
                            );
                        }
                        for invalidation in &invalidations {
                            match invalidation {
                                crate::cache::FirestoreCacheInvalidation::Collection(
                                    collection_path,
                                ) => self.invalidate_collection(collection_path)?,
                                crate::cache::FirestoreCacheInvalidation::Documents {
                                    collection_path,
                                    document_ids,
                                } => self.evict_documents(collection_path, document_ids)?,
                            }
                        }
                    }
                    crate::cache::FirestoreCacheTargetChangeAction::Resume(paths) => {
                        let mut suspended = self
                            .suspended_collections
                            .write()
                            .expect("cache suspended collections lock poisoned");
                        for collection_path in &paths {
                            suspended.remove(collection_path);
                        }
                    }
                    crate::cache::FirestoreCacheTargetChangeAction::Ignore => {}
                }
                Ok(())
            }
            _ => {
                trace!(?event, "Ignoring a listen event the cache does not act on.");
                Ok(())
            }
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
        if self.is_open() && self.config().collections.contains_key(collection_path) {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
            let read_tx = self
                .open_db()?
                .as_ref()
                .expect("database checked open")
                .begin_read()?;
            let table = read_tx.open_table(td)?;
            let value = table.get(document_id)?;
            value.map(|v| Self::buf_to_document(v.value())).transpose()
        } else {
            Ok(None)
        }
    }

    async fn update_doc_by_path(&self, document: &FirestoreDocument) -> FirestoreResult<()> {
        // A shut down cache stops accepting writes rather than failing the caller's read.
        if !self.is_open() {
            return Ok(());
        }
        self.write_document(document)?;
        Ok(())
    }

    async fn list_all_docs<'b>(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>
    {
        if self.is_open()
            && self.config().is_collection_listable(collection_path)
            && !self.is_suspended(collection_path)
        {
            let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

            let read_tx = self
                .open_db()?
                .as_ref()
                .expect("database checked open")
                .begin_read()?;
            let table = read_tx.open_table(td)?;
            let iter = table.iter()?;

            // It seems there is no way to work with streaming for redb, so this is not efficient
            let mut docs: Vec<FirestoreResult<FirestoreDocument>> = Vec::new();
            for record in iter {
                let (_, v) = record?;
                let doc = Self::buf_to_document(v.value())?;
                docs.push(Ok(doc));
            }

            Ok(FirestoreCachedValue::UseCached(Box::pin(
                futures::stream::iter(docs),
            )))
        } else {
            Ok(FirestoreCachedValue::SkipCache)
        }
    }

    async fn query_docs<'b>(
        &self,
        collection_path: &str,
        query: &FirestoreQueryParams,
    ) -> FirestoreResult<FirestoreCachedValue<BoxStream<'b, FirestoreResult<FirestoreDocument>>>>
    {
        if self.is_open()
            && self.config().is_collection_listable(collection_path)
            && !self.is_suspended(collection_path)
        {
            // For now only basic/simple query all supported
            let simple_query_engine = FirestoreCacheQueryEngine::new(query);
            if simple_query_engine.params_supported() {
                Ok(FirestoreCachedValue::UseCached(
                    self.query_cached_docs(collection_path, simple_query_engine)
                        .await?,
                ))
            } else {
                Ok(FirestoreCachedValue::SkipCache)
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::{DocumentDelete, DocumentRemove, TargetChange};

    const DOCS: &str = "projects/test/databases/(default)/documents";

    struct TestBackend {
        backend: FirestorePersistentCacheBackend,
        // Kept alive so the database file outlives the test.
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for TestBackend {
        type Target = FirestorePersistentCacheBackend;
        fn deref(&self) -> &Self::Target {
            &self.backend
        }
    }

    fn preloaded(collections: &[(&str, u32)]) -> TestBackend {
        let config = collections.iter().fold(
            FirestoreCacheConfiguration::new(),
            |config, (name, target)| {
                config.add_collection_config_at(
                    DOCS,
                    FirestoreCacheCollectionConfiguration::new(
                        name,
                        FirestoreListenerTarget::new(*target),
                        FirestoreCacheCollectionLoadMode::PreloadAllDocs,
                    ),
                )
            },
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let backend =
            FirestorePersistentCacheBackend::with_options(config, dir.path().join("redb"))
                .expect("backend");

        TestBackend { backend, _dir: dir }
    }

    fn doc_path(collection: &str, id: &str) -> String {
        format!("{DOCS}/{collection}/{id}")
    }

    async fn seed(backend: &FirestorePersistentCacheBackend, collection: &str, id: &str) {
        backend
            .update_doc_by_path(&FirestoreDocument {
                name: doc_path(collection, id),
                ..Default::default()
            })
            .await
            .expect("seeded document");
    }

    async fn cached(backend: &FirestorePersistentCacheBackend, collection: &str, id: &str) -> bool {
        backend
            .get_doc_by_path(&doc_path(collection, id))
            .await
            .expect("cache lookup")
            .is_some()
    }

    fn target_change(
        change_type: FirestoreListenerTargetChangeType,
        target_ids: Vec<i32>,
    ) -> FirestoreListenEvent {
        FirestoreListenEvent::TargetChange(TargetChange {
            target_change_type: change_type as i32,
            target_ids,
            ..Default::default()
        })
    }

    async fn is_listable(backend: &FirestorePersistentCacheBackend, collection: &str) -> bool {
        matches!(
            backend
                .list_all_docs(&format!("{DOCS}/{collection}"))
                .await
                .expect("listing"),
            FirestoreCachedValue::UseCached(_)
        )
    }

    #[tokio::test]
    async fn document_remove_evicts_the_document() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;

        backend
            .on_listen_event(FirestoreListenEvent::DocumentRemove(DocumentRemove {
                document: doc_path("a", "one"),
                ..Default::default()
            }))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
    }

    #[tokio::test]
    async fn document_delete_still_evicts_the_document() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;

        backend
            .on_listen_event(FirestoreListenEvent::DocumentDelete(DocumentDelete {
                document: doc_path("a", "one"),
                ..Default::default()
            }))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
    }

    #[tokio::test]
    async fn a_reset_empties_only_the_collection_of_that_target() {
        let backend = preloaded(&[("a", 1000), ("b", 1001)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "b", "one").await;

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(!cached(&backend, "a", "one").await);
        assert!(cached(&backend, "b", "one").await);
    }

    #[tokio::test]
    async fn a_reset_stops_the_collection_answering_listings_until_it_is_current() {
        let backend = preloaded(&[("a", 1000), ("b", 1001)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "b", "one").await;

        assert!(is_listable(&backend, "a").await);

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(!is_listable(&backend, "a").await);
        assert!(is_listable(&backend, "b").await);

        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Current,
                vec![1000],
            ))
            .await
            .expect("event handled");

        assert!(is_listable(&backend, "a").await);
    }
}
