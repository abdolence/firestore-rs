use crate::errors::*;
use crate::*;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::cache::cache_query_engine::FirestoreCacheQueryEngine;
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::Document;
use gcloud_sdk::prost::Message;
use gcloud_sdk::prost_types::Timestamp;
use redb::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::*;

/// Collections whose preload has completed. Collection tables are named by full Firestore
/// paths, which start with `projects/`, so this name cannot clash with one.
const PRELOADED_COLLECTIONS: TableDefinition<&str, ()> =
    TableDefinition::new("firestore-rs:preloaded");

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
    /// Held while a collection is emptied, until the emptying commits, and while a poll takes its
    /// generation and its read transaction, so that a poll sees both from before the emptying or
    /// both from after.
    generations: std::sync::Mutex<FirestorePersistentCacheGenerations>,
}

/// A number for each cached collection, given out anew whenever the collection is added or
/// emptied, dropped when it is removed, and never given out twice, so that a poll which read
/// before such a change finds out inside the transaction it writes with, and writes nothing.
#[derive(Default)]
struct FirestorePersistentCacheGenerations {
    last: u64,
    by_collection: HashMap<String, u64>,
}

/// The generation of a collection, and the `update_time` of every document it holds, taken
/// together.
struct FirestorePersistentCachePollSnapshot {
    generation: u64,
    stored_versions: HashMap<String, Option<Timestamp>>,
}

impl FirestorePersistentCacheGenerations {
    fn renew(&mut self, collection_path: &str) {
        self.last += 1;
        self.by_collection
            .insert(collection_path.to_string(), self.last);
    }
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

        db.compact(CompactionMode::Balanced)?;

        let write_txn = db.begin_write()?;
        write_txn.open_table(PRELOADED_COLLECTIONS)?;
        write_txn.commit()?;
        info!("Successfully opened database for persistent cache.");

        let mut generations = FirestorePersistentCacheGenerations::default();
        for collection_path in config.collections.keys() {
            generations.renew(collection_path);
        }

        Ok(Self {
            config: std::sync::RwLock::new(Arc::new(config)),
            redb: std::sync::RwLock::new(Some(db)),
            suspended_collections: std::sync::RwLock::new(HashSet::new()),
            generations: std::sync::Mutex::new(generations),
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
            return Err(FirestoreError::cache_shut_down());
        }
        Ok(guard)
    }

    fn begin_write(&self) -> FirestoreResult<WriteTransaction> {
        Ok(self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_write()?)
    }

    fn begin_read(&self) -> FirestoreResult<ReadTransaction> {
        Ok(self
            .open_db()?
            .as_ref()
            .expect("database checked open")
            .begin_read()?)
    }

    /// Whether the cache is still usable. Reads fall back to Firestore once it is not.
    fn is_open(&self) -> bool {
        self.redb
            .read()
            .expect("cache database lock poisoned")
            .is_some()
    }

    /// Stops a collection answering `list`/`query` until Firestore reports it consistent again.
    fn suspend_collection(&self, collection_path: &str) {
        self.suspended_collections
            .write()
            .expect("cache suspended collections lock poisoned")
            .insert(collection_path.to_string());
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
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let write_txn = self.begin_write()?;
        // Held until the commit: a poll takes its generation and its read transaction under this
        // lock, so it gets both from before the clear or both from after. A collection removed
        // meanwhile has no generation any more, and is left alone.
        let mut generations = self
            .generations
            .lock()
            .expect("cache generations lock poisoned");
        if !generations.by_collection.contains_key(collection_path) {
            write_txn.abort()?;
            return Ok(());
        }
        generations.renew(collection_path);
        {
            debug!(
                collection_path,
                "Invalidating collection and draining the corresponding table.",
            );
            let mut table = write_txn.open_table(td)?;
            table.retain(|_, _| false)?;
            Self::forget_preload(&write_txn, collection_path)?;
        }
        write_txn.commit()?;
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
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;
            for document_id in document_ids {
                table.remove(document_id.as_str())?;
            }
            Self::forget_preload(&write_txn, collection_path)?;
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
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(td)?;
            table.remove(document_id)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn preload_collections(&self, db: &FirestoreDb) -> Result<(), FirestoreError> {
        for (collection_path, config) in &self.config().collections {
            // Reads of a collection with no table fail, and a preload of a collection that is
            // empty in Firestore writes nothing to create one.
            self.create_collection_table(collection_path)?;

            match config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty
                    if self.is_preloaded(collection_path)? =>
                {
                    info!(
                        collection_path = collection_path.as_str(),
                        entries_loaded = self.table_len(collection_path)?,
                        "Preloading collection has been skipped.",
                    );
                }
                FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty => {
                    // Rows without a marker are not known to be complete: a download cut short,
                    // an invalidation and a resync replay all leave them.
                    self.drop_collection_table(collection_path)?;
                    self.create_collection_table(collection_path)?;
                    self.download(db, collection_path, config).await?;
                }
                FirestoreCacheCollectionLoadMode::PreloadAllDocs => {
                    self.download(db, collection_path, config).await?;
                }
                FirestoreCacheCollectionLoadMode::PreloadNone => {}
            }
        }
        Ok(())
    }

    /// Reads a preloaded collection into its table as a poll does: the documents read are
    /// stored, the rows of those Firestore no longer has are removed, and the marker is set by the
    /// last write. A download that fails drops the table, so that no partial collection is left.
    async fn download(
        &self,
        db: &FirestoreDb,
        collection_path: &str,
        collection_config: &FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<()> {
        debug!(collection_path, "Preloading collection.");
        let Some(snapshot) = self.poll_snapshot(collection_path)? else {
            return Ok(());
        };
        let downloaded = self
            .read_into_table(db, collection_path, collection_config, snapshot)
            .await;
        if downloaded.is_err() {
            if let Err(drop_error) = self.drop_collection_table(collection_path) {
                error!(
                    ?drop_error,
                    collection_path, "Could not drop the table of a failed preload."
                );
            }
        }
        downloaded
    }

    /// Reads a collection from Firestore and brings its table in line with what was read, from a
    /// snapshot taken before the read.
    async fn read_into_table(
        &self,
        db: &FirestoreDb,
        collection_path: &str,
        collection_config: &FirestoreCacheCollectionConfiguration,
        snapshot: FirestorePersistentCachePollSnapshot,
    ) -> FirestoreResult<()> {
        let FirestorePersistentCachePollSnapshot {
            generation,
            stored_versions,
        } = snapshot;
        let document_ids = match collection_config.poll_read() {
            FirestoreCachePollRead::Collection => {
                let documents = collection_config.read_all_documents(db).await?;
                return self
                    .reconcile_collection(collection_path, generation, &stored_versions, documents)
                    .await;
            }
            FirestoreCachePollRead::WatchedDocuments(document_ids) => document_ids.to_vec(),
            FirestoreCachePollRead::CachedDocuments => stored_versions.keys().cloned().collect(),
        };
        self.reconcile_documents(
            db,
            collection_config,
            collection_path,
            generation,
            &stored_versions,
            document_ids,
        )
        .await
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

            let write_txn = self.begin_write()?;
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
        let tx = self.begin_write()?;
        tx.open_table(td)?;
        tx.commit()?;
        Ok(())
    }

    /// Deletes a collection's table entirely. Returns whether there was one.
    fn drop_collection_table(&self, collection_path: &str) -> FirestoreResult<bool> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let tx = self.begin_write()?;
        let existed = tx.delete_table(td)?;
        Self::forget_preload(&tx, collection_path)?;
        tx.commit()?;
        Ok(existed)
    }

    fn generation(&self, collection_path: &str) -> Option<u64> {
        self.generations
            .lock()
            .expect("cache generations lock poisoned")
            .by_collection
            .get(collection_path)
            .copied()
    }

    /// Publishes a collection added at runtime. Fails, publishing nothing, once the cache has been
    /// shut down: the database stays open while the collection is published.
    fn publish_collection(
        &self,
        db: &FirestoreDb,
        collection_config: FirestoreCacheCollectionConfiguration,
    ) -> FirestoreResult<()> {
        let _open = self.open_db()?;
        let mut config = self
            .config
            .write()
            .expect("cache configuration lock poisoned");
        *config = Arc::new(
            (**config)
                .clone()
                .add_collection_config_at(db.get_documents_path(), collection_config),
        );
        Ok(())
    }

    /// What a poll of a collection begins from, or `None` for a collection the backend does not
    /// cache.
    fn poll_snapshot(
        &self,
        collection_path: &str,
    ) -> FirestoreResult<Option<FirestorePersistentCachePollSnapshot>> {
        // The database lock before the generations lock, the one order the backend takes them in.
        let open_db = self.open_db()?;
        let (generation, read_tx) = {
            let generations = self
                .generations
                .lock()
                .expect("cache generations lock poisoned");
            let Some(generation) = generations.by_collection.get(collection_path).copied() else {
                return Ok(None);
            };
            let read_tx = open_db
                .as_ref()
                .expect("database checked open")
                .begin_read()?;
            (generation, read_tx)
        };
        drop(open_db);
        let table = match read_tx.open_table(TableDefinition::<&str, &[u8]>::new(collection_path)) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(Some(FirestorePersistentCachePollSnapshot {
                    generation,
                    stored_versions: HashMap::new(),
                }))
            }
            Err(err) => return Err(err.into()),
        };
        let stored_versions = table
            .iter()?
            .map(|record| {
                let (document_id, document) = record?;
                Ok((
                    document_id.value().to_string(),
                    Self::buf_to_document(document.value())?.update_time,
                ))
            })
            .collect::<FirestoreResult<_>>()?;
        Ok(Some(FirestorePersistentCachePollSnapshot {
            generation,
            stored_versions,
        }))
    }

    /// One transaction of a poll that began at `generation`. Writes nothing, and returns `false`,
    /// once the collection has been removed, added again or emptied since.
    ///
    /// Stores each of `stored` unless the table holds the same or a later version, and removes
    /// each of `gone` unless the table no longer holds the version the poll saw before it read.
    /// A write that changes rows withdraws the collection's marker in its transaction, and the
    /// write that `completes` a poll of a preloaded collection sets it in the transaction that
    /// removes the documents found gone, so that after a crash the marker vouches only for a poll
    /// that finished.
    fn write_polled(
        &self,
        collection_path: &str,
        generation: u64,
        stored: Vec<Document>,
        gone: &[(String, Option<Timestamp>)],
        completes: bool,
    ) -> FirestoreResult<bool> {
        let write_txn = self.begin_write()?;
        if self.generation(collection_path) != Some(generation) {
            write_txn.abort()?;
            return Ok(false);
        }

        let mut changed = false;
        {
            let mut table =
                write_txn.open_table(TableDefinition::<&str, &[u8]>::new(collection_path))?;
            for document in stored {
                let (_, document_id) = split_document_path(&document.name);
                let stored_version = table
                    .get(document_id)?
                    .map(|row| Self::buf_to_document(row.value()))
                    .transpose()?
                    .and_then(|row| row.update_time);
                if is_later_version(document.update_time.as_ref(), stored_version.as_ref()) {
                    table.insert(document_id, Self::document_to_buf(&document)?.as_slice())?;
                    changed = true;
                }
            }
            for (document_id, polled_version) in gone {
                let unchanged = table
                    .get(document_id.as_str())?
                    .map(|row| Self::buf_to_document(row.value()))
                    .transpose()?
                    .is_some_and(|row| row.update_time == *polled_version);
                if unchanged {
                    table.remove(document_id.as_str())?;
                    changed = true;
                }
            }

            let mut markers = write_txn.open_table(PRELOADED_COLLECTIONS)?;
            if completes {
                if markers.get(collection_path)?.is_none() {
                    markers.insert(collection_path, ())?;
                    changed = true;
                }
            } else if changed {
                markers.remove(collection_path)?;
            }
        }

        if changed {
            write_txn.commit()?;
        } else {
            write_txn.abort()?;
        }
        Ok(true)
    }

    /// Makes the table hold exactly `documents`, a complete read of the collection that began
    /// after `stored_versions` was taken.
    async fn reconcile_collection(
        &self,
        collection_path: &str,
        generation: u64,
        stored_versions: &HashMap<String, Option<Timestamp>>,
        documents: BoxStream<'_, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<()> {
        let mut chunks = documents
            .ready_chunks(FIRESTORE_CACHE_DOCUMENTS_PER_CHUNK)
            .map(|docs| docs.into_iter().collect::<FirestoreResult<Vec<_>>>());

        let mut read_ids = HashSet::new();
        while let Some(docs) = chunks.try_next().await? {
            let mut changed = Vec::new();
            for doc in docs {
                let document_id = split_document_path(&doc.name).1.to_string();
                let stored_update_time = stored_versions.get(&document_id).and_then(Option::as_ref);
                if is_later_version(doc.update_time.as_ref(), stored_update_time) {
                    changed.push(doc);
                }
                read_ids.insert(document_id);
            }
            if !changed.is_empty()
                && !self.write_polled(collection_path, generation, changed, &[], false)?
            {
                return Ok(());
            }
        }

        let gone: Vec<(String, Option<Timestamp>)> = stored_versions
            .iter()
            .filter(|(document_id, _)| !read_ids.contains(*document_id))
            .map(|(document_id, stored_version)| (document_id.clone(), *stored_version))
            .collect();
        self.write_polled(collection_path, generation, Vec::new(), &gone, true)?;
        Ok(())
    }

    /// Reads these documents of the collection, a chunk at a time, and stores the ones found and
    /// removes the ones reported missing. The write of the last chunk completes the poll of a
    /// preloaded collection.
    async fn reconcile_documents(
        &self,
        db: &FirestoreDb,
        collection_config: &FirestoreCacheCollectionConfiguration,
        collection_path: &str,
        generation: u64,
        stored_versions: &HashMap<String, Option<Timestamp>>,
        document_ids: Vec<String>,
    ) -> FirestoreResult<()> {
        let chunks: Vec<&[String]> = document_ids
            .chunks(FIRESTORE_CACHE_DOCUMENTS_PER_CHUNK)
            .collect();
        for (index, chunk) in chunks.iter().enumerate() {
            let read: Vec<(String, Option<Document>)> = collection_config
                .read_documents(db, chunk.to_vec())
                .await?
                .try_collect()
                .await?;

            let mut found = Vec::new();
            let mut gone = Vec::new();
            for (document_id, document) in read {
                let stored_version = stored_versions.get(&document_id);
                match document {
                    Some(document) => {
                        let stored_update_time = stored_version.and_then(Option::as_ref);
                        if is_later_version(document.update_time.as_ref(), stored_update_time) {
                            found.push(document);
                        }
                    }
                    None => {
                        if let Some(stored_version) = stored_version {
                            gone.push((document_id, *stored_version));
                        }
                    }
                }
            }
            let completes =
                index + 1 == chunks.len() && collection_config.collection_load_mode.is_preloading();
            if (completes || !found.is_empty() || !gone.is_empty())
                && !self.write_polled(collection_path, generation, found, &gone, completes)?
            {
                return Ok(());
            }
        }
        if chunks.is_empty() && collection_config.collection_load_mode.is_preloading() {
            self.write_polled(collection_path, generation, Vec::new(), &[], true)?;
        }
        Ok(())
    }

    /// Removes a collection's marker inside the transaction that changes or drops its table, so
    /// the marker never outlives the rows it vouches for.
    fn forget_preload(write_txn: &WriteTransaction, collection_path: &str) -> FirestoreResult<()> {
        write_txn
            .open_table(PRELOADED_COLLECTIONS)?
            .remove(collection_path)?;
        Ok(())
    }

    /// The collections whose documents a load keeps from an earlier run without reading them
    /// again: a `PreloadAllIfEmpty` one with a marker, whatever its row count, and a lazily filled
    /// one with rows. A `PreloadAllIfEmpty` one without a marker is downloaded afresh, and a
    /// `PreloadAllDocs` one is brought in line with Firestore.
    ///
    /// A cache that keeps any must not resume its listener from the time the load starts, because
    /// it would skip what changed while the cache was closed. A polling cache reads them at once.
    fn collections_kept_from_an_earlier_run(&self) -> FirestoreResult<Vec<String>> {
        let mut kept_collections = Vec::new();
        for (collection_path, collection_config) in &self.config().collections {
            let kept = match collection_config.collection_load_mode {
                FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty => {
                    self.is_preloaded(collection_path)?
                }
                FirestoreCacheCollectionLoadMode::PreloadAllDocs => false,
                FirestoreCacheCollectionLoadMode::PreloadNone => {
                    self.table_len(collection_path)? > 0
                }
            };
            if kept {
                kept_collections.push(collection_path.clone());
            }
        }
        Ok(kept_collections)
    }

    /// Whether a collection has a marker: a download of it ran to its end and nothing has since
    /// cleared its table.
    fn is_preloaded(&self, collection_path: &str) -> FirestoreResult<bool> {
        let read_tx = self.begin_read()?;
        Ok(read_tx
            .open_table(PRELOADED_COLLECTIONS)?
            .get(collection_path)?
            .is_some())
    }

    /// The number of documents in a collection's table; a collection that has no table yet holds
    /// none.
    fn table_len(&self, collection_path: &str) -> FirestoreResult<u64> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);
        let read_tx = self.begin_read()?;
        match read_tx.open_table(td) {
            Ok(table) => Ok(table.len()?),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    async fn query_cached_docs<'b>(
        &self,
        collection_path: &str,
        query_engine: FirestoreCacheQueryEngine,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let td: TableDefinition<&str, &[u8]> = TableDefinition::new(collection_path);

        let read_tx = self.begin_read()?;
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
        let read_from_time = FirestoreCacheCollectionConfiguration::listener_read_time();

        let keeps_earlier_documents = !self.collections_kept_from_an_earlier_run()?.is_empty();
        self.preload_collections(db).await?;

        // A new cache is complete as of `read_from_time`, which was captured before the preload
        // read anything, so resuming from it loses nothing. A cache that holds documents from an
        // earlier run must not resume from it: it would skip what changed while the cache was
        // closed. Its targets resume from their stored tokens, or replay without one.
        let resume_type = if keeps_earlier_documents {
            None
        } else {
            Some(FirestoreListenerTargetResumeType::ReadTime(read_from_time))
        };

        Ok(self
            .config()
            .collections
            .values()
            .map(|collection_config| collection_config.listener_target_params(resume_type.clone()))
            .collect())
    }

    async fn invalidate_all(&self) -> FirestoreResult<()> {
        for collection_path in self.config().collections.keys() {
            self.invalidate_collection(collection_path)?;
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
        let read_from_time = FirestoreCacheCollectionConfiguration::listener_read_time();
        let collection_path = collection_config.resolve_collection_path(db.get_documents_path());

        if self.config().collections.contains_key(&collection_path) {
            return Err(FirestoreError::collection_already_cached(&collection_path));
        }

        // A collection added at runtime starts from nothing, whatever an earlier run left in its
        // table: the target attaches from `read_from_time`, so it never reports what was deleted
        // while the cache was closed. Both preloading modes therefore mean the same thing here.
        self.drop_collection_table(&collection_path)?;
        self.create_collection_table(&collection_path)?;
        // Given before the download, which writes under it; a poll leaves the collection alone
        // until it is published.
        self.generations
            .lock()
            .expect("cache generations lock poisoned")
            .renew(&collection_path);

        if collection_config.collection_load_mode.is_preloading() {
            self.download(db, &collection_path, &collection_config)
                .await?;
        }

        let target_params = collection_config.listener_target_params(Some(
            FirestoreListenerTargetResumeType::ReadTime(read_from_time),
        ));

        // Published only now: until this point the collection does not exist as far as reads are
        // concerned, so no listing can see it half filled.
        self.publish_collection(db, collection_config)?;

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
            self.generations
                .lock()
                .expect("cache generations lock poisoned")
                .by_collection
                .remove(collection_path);
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

    async fn begin_collection_resync(&self, collection_path: &str) -> FirestoreResult<()> {
        if !self.config().collections.contains_key(collection_path) {
            return Ok(());
        }
        self.suspend_collection(collection_path);
        self.invalidate_collection(collection_path)?;
        Ok(())
    }

    async fn authoritative_doc_count(&self, collection_path: &str) -> FirestoreResult<Option<u64>> {
        // redb holds exactly what was written, with no eviction of its own, so a preloaded
        // collection's row count is the server's count.
        let config = self.config();
        let Some(collection_config) = config.collections.get(collection_path) else {
            return Ok(None);
        };
        if !collection_config.collection_load_mode.is_preloading() || !self.is_open() {
            return Ok(None);
        }

        Ok(self.table_len(collection_path).ok())
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
                match self.config().target_change_action(target_change) {
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
impl FirestoreCachePolledBackend for FirestorePersistentCacheBackend {
    async fn load_for_polling(
        &self,
        options: &FirestoreCacheOptions,
        db: &FirestoreDb,
    ) -> FirestoreResult<Vec<String>> {
        let kept_collections = self.collections_kept_from_an_earlier_run()?;
        self.load(options, db).await?;
        Ok(kept_collections)
    }

    async fn poll_collection(
        &self,
        db: &FirestoreDb,
        collection_path: &str,
    ) -> FirestoreResult<()> {
        // Taken before the configuration: a collection removed and added again in between has
        // another generation, so every write of the poll is refused.
        let Some(snapshot) = self.poll_snapshot(collection_path)? else {
            return Ok(());
        };
        let config = self.config();
        let Some(collection_config) = config.collections.get(collection_path) else {
            return Ok(());
        };
        self.read_into_table(db, collection_path, collection_config, snapshot)
            .await
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
            let read_tx = self.begin_read()?;
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

            let read_tx = self.begin_read()?;
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
    use crate::db::fake_firestore::{
        batch_get_found_message, batch_get_missing_message, run_query_message, FakeFirestore,
        FakeResponse,
    };
    use futures::TryStreamExt;
    use gcloud_sdk::google::firestore::v1::{
        run_query_request, BatchGetDocumentsRequest, DocumentDelete, DocumentRemove,
        RunQueryRequest, TargetChange,
    };
    use gcloud_sdk::tonic::Code;
    use std::time::Duration;

    const DOCS: &str = "projects/test/databases/(default)/documents";

    /// Writes rows straight into a collection's table, without its marker, as a download cut
    /// short leaves them.
    fn write_rows(
        backend: &FirestorePersistentCacheBackend,
        collection_path: &str,
        documents: Vec<FirestoreDocument>,
    ) {
        let write_txn = backend.begin_write().expect("write transaction");
        {
            let mut table = write_txn
                .open_table(TableDefinition::<&str, &[u8]>::new(collection_path))
                .expect("collection table");
            for document in documents {
                let encoded =
                    FirestorePersistentCacheBackend::document_to_buf(&document).expect("encoded");
                table
                    .insert(split_document_path(&document.name).1, encoded.as_slice())
                    .expect("row");
            }
        }
        write_txn.commit().expect("commit");
    }

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

        backend_over(config)
    }

    fn backend_over(config: FirestoreCacheConfiguration) -> TestBackend {
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

    /// The documents path of a database served by [`FakeFirestore`].
    const FAKE_DOCS: &str = "projects/fake-firestore/databases/(default)/documents";

    fn fake_document(id: &str) -> FirestoreDocument {
        FirestoreDocument {
            name: format!("{FAKE_DOCS}/a/{id}"),
            ..Default::default()
        }
    }

    fn collection_a() -> FirestoreCacheCollectionConfiguration {
        collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllDocs)
    }

    fn collection_a_loaded(
        load_mode: FirestoreCacheCollectionLoadMode,
    ) -> FirestoreCacheCollectionConfiguration {
        FirestoreCacheCollectionConfiguration::new(
            "a",
            FirestoreListenerTarget::new(1000),
            load_mode,
        )
    }

    fn backend_for(collection: FirestoreCacheCollectionConfiguration) -> TestBackend {
        backend_over(
            FirestoreCacheConfiguration::new().add_collection_config_at(FAKE_DOCS, collection),
        )
    }

    async fn load_once(
        backend: &FirestorePersistentCacheBackend,
        server: &FakeFirestore,
    ) -> FirestoreResult<Vec<FirestoreListenerTargetParams>> {
        backend
            .load(&FirestoreCacheOptions::new("preload".into()), &server.db)
            .await
    }

    fn run_query_messages(ids: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<Vec<u8>> {
        ids.into_iter()
            .map(|id| run_query_message(fake_document(id.as_ref())))
            .collect()
    }

    /// Answers every query with `response`, built afresh for each call.
    async fn query_server(
        response: impl Fn() -> FakeResponse + Send + Sync + 'static,
    ) -> FakeFirestore {
        FakeFirestore::start(move |_, _| ("RunQuery".to_string(), response())).await
    }

    async fn serving(ids: &[&str]) -> FakeFirestore {
        let messages = run_query_messages(ids.iter().copied());
        query_server(move || FakeResponse::Paced(messages.clone(), Duration::ZERO)).await
    }

    async fn serving_documents(ids: &[&str]) -> FakeFirestore {
        let messages: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| batch_get_found_message(fake_document(id)))
            .collect();
        FakeFirestore::start(move |_, _| {
            (
                "BatchGetDocuments".to_string(),
                FakeResponse::Paced(messages.clone(), Duration::ZERO),
            )
        })
        .await
    }

    async fn failing_server() -> FakeFirestore {
        query_server(|| FakeResponse::MessagesThenStatus(Vec::new(), Code::Internal)).await
    }

    /// More documents than one write chunk holds, so that part of them is committed before the
    /// stream fails.
    async fn query_server_failing_after_several_chunks() -> FakeFirestore {
        query_server(|| {
            FakeResponse::MessagesThenStatus(
                run_query_messages((0..250).map(|id| id.to_string())),
                Code::Internal,
            )
        })
        .await
    }

    #[tokio::test]
    async fn a_preload_that_fails_partway_fails_load_and_keeps_no_rows() {
        let backend = preloaded(&[("a", 1000)]);
        let server = query_server_failing_after_several_chunks().await;

        let FirestoreError::DatabaseError(error) = backend
            .load(&FirestoreCacheOptions::new("partial".into()), &server.db)
            .await
            .unwrap_err()
        else {
            panic!("load did not fail with the stream's database error");
        };
        assert_eq!(error.public.code, "Internal");
        assert_eq!(backend.table_len(&format!("{DOCS}/a")).unwrap(), 0);
    }

    #[tokio::test]
    async fn a_watched_documents_preload_that_fails_partway_fails_load() {
        let backend = backend_over(
            FirestoreCacheConfiguration::new()
                .add_collection_config_at(DOCS, collection_a().with_documents(["one", "two"])),
        );
        let server = FakeFirestore::start(|_, _| {
            (
                "BatchGetDocuments".to_string(),
                FakeResponse::MessagesThenStatus(
                    vec![batch_get_found_message(fake_document("one"))],
                    Code::Internal,
                ),
            )
        })
        .await;

        let FirestoreError::DatabaseError(error) = backend
            .load(&FirestoreCacheOptions::new("partial".into()), &server.db)
            .await
            .unwrap_err()
        else {
            panic!("load did not fail with the batch get's database error");
        };
        assert_eq!(error.public.code, "Internal");
    }

    #[tokio::test]
    async fn a_preload_that_fails_partway_fails_add_collection_and_keeps_nothing() {
        let backend = preloaded(&[]);
        let server = query_server_failing_after_several_chunks().await;

        let FirestoreError::DatabaseError(error) = backend
            .add_collection(
                &FirestoreCacheOptions::new("partial".into()),
                &server.db,
                collection_a(),
            )
            .await
            .unwrap_err()
        else {
            panic!("add_collection did not fail with the stream's database error");
        };
        assert_eq!(error.public.code, "Internal");
        assert!(backend.config().collections.is_empty());
        assert_eq!(backend.table_len(&format!("{FAKE_DOCS}/a")).unwrap(), 0);
    }

    #[tokio::test]
    async fn add_collection_discards_rows_left_by_an_earlier_run() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data_file = dir.path().join("redb");

        let earlier_run = FirestorePersistentCacheBackend::with_options(
            FirestoreCacheConfiguration::new(),
            data_file.clone(),
        )
        .expect("backend");
        write_rows(
            &earlier_run,
            &format!("{FAKE_DOCS}/a"),
            vec![fake_document("deleted_while_closed"), fake_document("kept")],
        );
        earlier_run.shutdown().await.expect("shutdown");

        let backend = FirestorePersistentCacheBackend::with_options(
            FirestoreCacheConfiguration::new(),
            data_file,
        )
        .expect("backend");
        let server =
            query_server(|| FakeResponse::Paced(run_query_messages(["kept"]), Duration::ZERO))
                .await;

        backend
            .add_collection(
                &FirestoreCacheOptions::new("reopened".into()),
                &server.db,
                collection_a(),
            )
            .await
            .expect("added");

        let collection_path = format!("{FAKE_DOCS}/a");
        assert_eq!(backend.table_len(&collection_path).unwrap(), 1);
        assert!(backend
            .get_doc_by_path(&fake_document("kept").name)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn add_collection_refuses_a_collection_that_is_already_cached() {
        let backend = preloaded(&[]);
        let server =
            query_server(|| FakeResponse::Paced(run_query_messages(["kept"]), Duration::ZERO))
                .await;
        let options = FirestoreCacheOptions::new("twice".into());
        backend
            .add_collection(&options, &server.db, collection_a())
            .await
            .expect("added");

        let FirestoreError::CacheError(error) = backend
            .add_collection(&options, &server.db, collection_a())
            .await
            .unwrap_err()
        else {
            panic!("the second add_collection did not fail with a cache error");
        };
        assert_eq!(error.public.code, "CacheCollectionAlreadyCached");
        assert_eq!(backend.table_len(&format!("{FAKE_DOCS}/a")).unwrap(), 1);
    }

    #[tokio::test]
    async fn a_table_without_a_marker_is_downloaded_again() {
        let backend = backend_for(collection_a_loaded(
            FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty,
        ));
        let collection_path = format!("{FAKE_DOCS}/a");
        write_rows(&backend, &collection_path, vec![fake_document("cut_short")]);

        let targets = load_once(&backend, &serving(&["one", "two"]).await)
            .await
            .expect("loaded");

        assert!(backend
            .get_doc_by_path(&fake_document("cut_short").name)
            .await
            .unwrap()
            .is_none());
        assert_eq!(backend.table_len(&collection_path).unwrap(), 2);
        assert!(matches!(
            targets[0].resume_type,
            Some(FirestoreListenerTargetResumeType::ReadTime(_))
        ));
    }

    #[tokio::test]
    async fn a_completed_preload_is_not_downloaded_again() {
        for ids in [vec!["one"], Vec::new()] {
            let backend = backend_for(collection_a_loaded(
                FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty,
            ));
            load_once(&backend, &serving(&ids).await)
                .await
                .expect("loaded");

            // A download would fail on this server.
            let targets = load_once(&backend, &failing_server().await)
                .await
                .expect("loaded again without downloading");

            assert_eq!(
                backend.table_len(&format!("{FAKE_DOCS}/a")).unwrap(),
                ids.len() as u64
            );
            assert!(targets[0].resume_type.is_none());
        }
    }

    #[tokio::test]
    async fn a_watched_documents_preload_that_finds_nothing_is_not_downloaded_again() {
        let backend = backend_for(
            collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty)
                .with_documents(["one"]),
        );
        let server = serving_documents(&[]).await;
        load_once(&backend, &server).await.expect("loaded");

        let targets = load_once(&backend, &server).await.expect("loaded again");

        assert!(targets[0].resume_type.is_none());
    }

    #[tokio::test]
    async fn a_watched_documents_reset_makes_the_next_load_download_again() {
        let backend = backend_for(
            collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty)
                .with_documents(["one"]),
        );
        let server = serving_documents(&["one"]).await;
        load_once(&backend, &server).await.expect("loaded");
        backend
            .on_listen_event(target_change(
                FirestoreListenerTargetChangeType::Reset,
                vec![1000],
            ))
            .await
            .expect("event handled");
        assert!(backend
            .get_doc_by_path(&fake_document("one").name)
            .await
            .unwrap()
            .is_none());

        load_once(&backend, &server).await.expect("loaded again");

        assert!(backend
            .get_doc_by_path(&fake_document("one").name)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn a_removed_collection_is_downloaded_again_when_it_is_cached_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data_file = dir.path().join("redb");
        let config = FirestoreCacheConfiguration::new().add_collection_config_at(
            FAKE_DOCS,
            collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty),
        );
        let first_run =
            FirestorePersistentCacheBackend::with_options(config.clone(), data_file.clone())
                .expect("backend");
        load_once(&first_run, &serving(&["one"]).await)
            .await
            .expect("loaded");
        first_run
            .remove_collection(&format!("{FAKE_DOCS}/a"))
            .await
            .expect("removed");
        first_run.shutdown().await.expect("shutdown");

        let second_run =
            FirestorePersistentCacheBackend::with_options(config, data_file).expect("backend");
        load_once(&second_run, &serving(&["one"]).await)
            .await
            .expect("loaded again");

        assert_eq!(second_run.table_len(&format!("{FAKE_DOCS}/a")).unwrap(), 1);
    }

    #[tokio::test]
    async fn an_invalidated_collection_is_downloaded_again() {
        let backend = backend_for(collection_a_loaded(
            FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty,
        ));
        let collection_path = format!("{FAKE_DOCS}/a");
        load_once(&backend, &serving(&["one"]).await)
            .await
            .expect("loaded");
        backend.invalidate_all().await.expect("invalidated");

        load_once(&backend, &serving(&["one"]).await)
            .await
            .expect("loaded again");

        assert_eq!(backend.table_len(&collection_path).unwrap(), 1);
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
    async fn a_documents_watch_builds_a_documents_target_and_is_not_listable() {
        let config = FirestoreCacheConfiguration::new().add_collection_config_at(
            DOCS,
            FirestoreCacheCollectionConfiguration::new(
                "a",
                FirestoreListenerTarget::new(1000),
                FirestoreCacheCollectionLoadMode::PreloadAllDocs,
            )
            .with_documents(["one", "two"]),
        );

        // The target has to watch the named documents, not the whole collection: getting this
        // wrong subscribes to everything while looking correct from the outside.
        let params = config.collections[&format!("{DOCS}/a")].listener_target_params(None);
        match params.target_type {
            FirestoreTargetType::Documents(documents) => {
                assert_eq!(documents.collection, "a");
                assert_eq!(documents.documents, vec!["one", "two"]);
            }
            other => panic!("expected a documents target, got {other:?}"),
        }

        assert!(!config.is_collection_listable(&format!("{DOCS}/a")));
    }

    #[tokio::test]
    async fn a_preloaded_collection_reports_a_comparable_document_count() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "a", "two").await;

        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn a_shut_down_cache_reports_no_comparable_count() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;
        backend.shutdown().await.unwrap();

        assert_eq!(
            backend
                .authoritative_doc_count(&format!("{DOCS}/a"))
                .await
                .unwrap(),
            None
        );
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

    #[tokio::test]
    async fn a_batch_read_fails_when_a_stored_document_cannot_be_read() {
        let backend = preloaded(&[("a", 1000)]);
        seed(&backend, "a", "one").await;
        seed(&backend, "a", "two").await;
        let collection_path = format!("{DOCS}/a");
        let write_txn = backend.begin_write().expect("write transaction");
        {
            let mut table = write_txn
                .open_table(TableDefinition::<&str, &[u8]>::new(&collection_path))
                .expect("collection table");
            table
                .insert("one", [0xFF].as_slice())
                .expect("corrupted document");
        }
        write_txn.commit().expect("commit");

        let read = backend
            .get_docs_by_paths(&[doc_path("a", "one"), doc_path("a", "two")])
            .await
            .expect("stream")
            .try_collect::<Vec<_>>()
            .await;

        assert!(read.is_err());
    }

    fn versioned(id: &str, version: i64) -> FirestoreDocument {
        FirestoreDocument {
            update_time: Some(gcloud_sdk::prost_types::Timestamp {
                seconds: version,
                nanos: 0,
            }),
            ..fake_document(id)
        }
    }

    /// Answers every query with these versions of documents of `a`, `interval` apart.
    async fn serving_versions(versions: &[(&str, i64)], interval: Duration) -> FakeFirestore {
        let messages: Vec<Vec<u8>> = versions
            .iter()
            .map(|(id, version)| run_query_message(versioned(id, *version)))
            .collect();
        query_server(move || FakeResponse::Paced(messages.clone(), interval)).await
    }

    fn collection_path_a() -> String {
        format!("{FAKE_DOCS}/a")
    }

    async fn poll_a(
        backend: &FirestorePersistentCacheBackend,
        server: &FakeFirestore,
    ) -> FirestoreResult<()> {
        backend
            .poll_collection(&server.db, &collection_path_a())
            .await
    }

    async fn stored_ids(backend: &FirestorePersistentCacheBackend) -> Vec<String> {
        let read_tx = backend.begin_read().expect("read transaction");
        let table = read_tx
            .open_table(TableDefinition::<&str, &[u8]>::new(&collection_path_a()))
            .expect("collection table");
        let mut ids: Vec<String> = table
            .iter()
            .expect("rows")
            .map(|record| record.expect("row").0.value().to_string())
            .collect();
        ids.sort();
        ids
    }

    async fn loaded_preloading_a(versions: &[(&str, i64)]) -> TestBackend {
        let backend = backend_for(collection_a_loaded(
            FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty,
        ));
        load_once(&backend, &serving_versions(versions, Duration::ZERO).await)
            .await
            .expect("loaded");
        backend
    }

    #[tokio::test]
    async fn a_completed_poll_leaves_exactly_what_was_read_and_the_marker() {
        let backend = loaded_preloading_a(&[("kept", 1), ("deleted", 1)]).await;

        poll_a(
            &backend,
            &serving_versions(&[("kept", 2), ("created", 1)], Duration::ZERO).await,
        )
        .await
        .expect("polled");

        assert_eq!(stored_ids(&backend).await, vec!["created", "kept"]);
        assert_eq!(
            backend
                .get_doc_by_path(&fake_document("kept").name)
                .await
                .unwrap(),
            Some(versioned("kept", 2))
        );
        assert!(backend.is_preloaded(&collection_path_a()).unwrap());
    }

    #[tokio::test]
    async fn a_poll_that_fails_partway_keeps_every_document_but_not_the_marker() {
        let backend = loaded_preloading_a(&[("kept", 1), ("deleted", 1)]).await;

        let FirestoreError::DatabaseError(error) =
            poll_a(&backend, &query_server_failing_after_several_chunks().await)
                .await
                .unwrap_err()
        else {
            panic!("the poll did not fail with the stream's database error");
        };

        assert_eq!(error.public.code, "Internal");
        let ids = stored_ids(&backend).await;
        assert!(ids.contains(&"kept".to_string()) && ids.contains(&"deleted".to_string()));
        assert!(!backend.is_preloaded(&collection_path_a()).unwrap());
    }

    #[tokio::test]
    async fn a_poll_that_fails_after_reading_only_unchanged_documents_keeps_the_marker() {
        // More unchanged documents than one write chunk holds, so that the read fails only after
        // a whole chunk of them.
        let ids: Vec<String> = (0..150).map(|id| id.to_string()).collect();
        let versions: Vec<(&str, i64)> = ids.iter().map(|id| (id.as_str(), 1)).collect();
        let backend = loaded_preloading_a(&versions).await;
        let messages: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| run_query_message(versioned(id, 1)))
            .collect();
        let server = query_server(move || {
            FakeResponse::MessagesThenStatus(messages.clone(), Code::Internal)
        })
        .await;

        poll_a(&backend, &server).await.unwrap_err();

        assert!(backend.is_preloaded(&collection_path_a()).unwrap());
    }

    #[tokio::test]
    async fn a_poll_neither_writes_into_nor_marks_a_collection_emptied_while_it_read() {
        let backend = loaded_preloading_a(&[]).await;
        let server = serving_versions(
            &[("one", 1), ("two", 1), ("three", 1)],
            Duration::from_millis(100),
        )
        .await;

        let (polled, invalidated) = tokio::join!(poll_a(&backend, &server), async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            backend.invalidate_all().await
        });

        polled.expect("polled");
        invalidated.expect("invalidated");
        assert!(stored_ids(&backend).await.is_empty());
        assert!(!backend.is_preloaded(&collection_path_a()).unwrap());
    }

    #[tokio::test]
    async fn a_preload_after_a_restart_removes_what_firestore_no_longer_has() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data_file = dir.path().join("redb");
        let config = FirestoreCacheConfiguration::new().add_collection_config_at(
            FAKE_DOCS,
            collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllDocs),
        );
        let earlier_run =
            FirestorePersistentCacheBackend::with_options(config.clone(), data_file.clone())
                .expect("backend");
        load_once(&earlier_run, &serving(&["one", "two"]).await)
            .await
            .expect("loaded");
        earlier_run.shutdown().await.expect("shutdown");

        let backend =
            FirestorePersistentCacheBackend::with_options(config, data_file).expect("backend");
        load_once(&backend, &serving(&["one"]).await)
            .await
            .expect("loaded again");

        assert_eq!(stored_ids(&backend).await, vec!["one"]);
    }

    #[tokio::test]
    async fn a_poll_marks_a_watched_collection_without_documents_complete_again() {
        let backend = backend_for(
            collection_a_loaded(FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty)
                .with_documents(Vec::<String>::new()),
        );
        load_once(&backend, &serving_documents(&[]).await)
            .await
            .expect("loaded");
        backend.invalidate_all().await.expect("invalidated");

        poll_a(&backend, &serving_documents(&[]).await)
            .await
            .expect("polled");

        assert!(backend.is_preloaded(&collection_path_a()).unwrap());
    }

    #[tokio::test]
    async fn a_lazy_poll_stores_the_found_and_evicts_the_missing() {
        let backend = backend_for(collection_a_loaded(
            FirestoreCacheCollectionLoadMode::PreloadNone,
        ));
        for id in ["kept", "deleted", "untouched"] {
            backend
                .update_doc_by_path(&versioned(id, 1))
                .await
                .expect("a document read through the cache");
        }
        let server = FakeFirestore::start(|_, request| {
            let request = BatchGetDocumentsRequest::decode(request).expect("batch request");
            let messages = request
                .documents
                .into_iter()
                .map(|name| match split_document_path(&name).1 {
                    "deleted" => batch_get_missing_message(name),
                    "kept" => batch_get_found_message(versioned("kept", 2)),
                    id => batch_get_found_message(versioned(id, 1)),
                })
                .collect();
            (
                "BatchGetDocuments".to_string(),
                FakeResponse::Paced(messages, Duration::ZERO),
            )
        })
        .await;

        poll_a(&backend, &server).await.expect("polled");

        assert_eq!(stored_ids(&backend).await, vec!["kept", "untouched"]);
        assert_eq!(
            backend
                .get_doc_by_path(&fake_document("kept").name)
                .await
                .unwrap(),
            Some(versioned("kept", 2))
        );
    }

    #[tokio::test]
    async fn a_zero_poll_interval_creates_no_database_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let server = serving_versions(&[], Duration::ZERO).await;

        let refused = FirestoreCache::persistent(&server.db)
            .data_dir(dir.path())
            .poll_changes(Duration::ZERO)
            .build()
            .await;

        assert!(matches!(
            refused.err(),
            Some(FirestoreError::InvalidParametersError(_))
        ));
        assert!(!dir.path().join("redb").exists());
    }

    #[tokio::test]
    async fn a_polling_cache_reads_at_once_only_the_collections_it_kept_without_downloading() {
        let dir = tempfile::tempdir().expect("temp dir");
        let hour = Duration::from_secs(3600);
        let server = FakeFirestore::start(|_, request| {
            let request = RunQueryRequest::decode(request).expect("query request");
            let Some(run_query_request::QueryType::StructuredQuery(query)) = request.query_type
            else {
                panic!("a structured query");
            };
            let collection = &query.from[0].collection_id;
            let document = FirestoreDocument {
                name: format!("{}/{collection}/one", request.parent),
                ..Default::default()
            };
            (
                format!("RunQuery {collection}"),
                FakeResponse::Paced(vec![run_query_message(document)], Duration::ZERO),
            )
        })
        .await;
        let earlier_run = FirestoreCache::persistent(&server.db)
            .data_dir(dir.path())
            .preloaded_collection("kept")
            .collection_with("downloaded_again", |c| c.preload_all())
            .poll_changes(hour)
            .build()
            .await
            .expect("earlier run");
        earlier_run.shutdown().await.expect("shutdown");

        let cache = FirestoreCache::persistent(&server.db)
            .data_dir(dir.path())
            .preloaded_collection("kept")
            .collection_with("downloaded_again", |c| c.preload_all())
            .preloaded_collection("downloaded")
            .poll_changes(hour)
            .build()
            .await
            .expect("cache");
        // Two loads of `downloaded_again`, one of `downloaded`, `kept` once in each run.
        tokio::time::timeout(Duration::from_secs(5), server.wait_for_calls(5))
            .await
            .expect("the kept collection was polled at once");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let calls = server.calls();
        let reads_of = |collection: &str| {
            calls
                .iter()
                .filter(|call| **call == format!("RunQuery {collection}"))
                .count()
        };
        assert_eq!(
            (
                reads_of("kept"),
                reads_of("downloaded_again"),
                reads_of("downloaded")
            ),
            (2, 2, 1)
        );
        cache.shutdown().await.expect("shutdown");
    }
}
