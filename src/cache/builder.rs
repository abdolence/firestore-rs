//! A builder for [`FirestoreCache`].
//!
//! This is the recommended way to create a cache. Compared with the raw
//! [`FirestoreCache::new`] constructor it assigns Firestore listener target IDs automatically,
//! picks a listener state storage that matches the backend, and loads the cache for you.

use crate::errors::{FirestoreInvalidParametersError, FirestoreInvalidParametersPublicDetails};
use crate::*;
use std::collections::HashSet;

/// The first Firestore listener target ID assigned to cached collections.
///
/// Targets are handed out sequentially from here. If your application runs its own listeners,
/// either keep their target IDs away from this range or move the cache's range with
/// [`FirestoreCacheBuilder::listener_target_base`].
pub const FIRESTORE_CACHE_DEFAULT_LISTENER_TARGET_BASE: u32 = 1000;

/// The cache name used when none is given.
pub const FIRESTORE_CACHE_DEFAULT_NAME: &str = "firestore-cache";

mod sealed {
    pub trait Sealed {}
}

/// Describes a cache backend flavour - in-memory or persistent - for [`FirestoreCacheBuilder`].
///
/// This trait is sealed: it exists so that one builder can serve both backends, and is not
/// intended to be implemented outside this crate. To plug in your own storage, implement
/// [`FirestoreCacheBackend`] and construct [`FirestoreCache`] directly.
pub trait FirestoreCacheBackendKind: sealed::Sealed + Sized {
    /// The backend this kind builds.
    type Backend: FirestoreCacheBackend + Send + Sync + 'static;
    /// Backend-specific tuning options.
    type Options: Default;
    /// The listener state storage used unless overridden.
    type DefaultStorage: FirestoreResumeStateStorage + Clone + Send + Sync + 'static;

    /// The load mode used by [`FirestoreCacheBuilder::preloaded_collection`].
    fn default_preload_mode() -> FirestoreCacheCollectionLoadMode;

    /// Creates the default listener state storage for this backend.
    fn create_default_storage() -> Self::DefaultStorage;

    /// Creates the backend itself.
    fn create_backend(
        options: Self::Options,
        config: FirestoreCacheConfiguration,
    ) -> FirestoreResult<Self::Backend>;
}

/// The in-memory backend flavour. See [`FirestoreCache::memory`].
#[cfg(feature = "caching-memory")]
pub struct FirestoreMemoryCacheKind;

#[cfg(feature = "caching-memory")]
impl sealed::Sealed for FirestoreMemoryCacheKind {}

#[cfg(feature = "caching-memory")]
impl FirestoreCacheBackendKind for FirestoreMemoryCacheKind {
    type Backend = FirestoreMemoryCacheBackend;
    type Options = FirestoreMemoryCacheOptions;
    type DefaultStorage = FirestoreMemListenStateStorage;

    #[inline]
    fn default_preload_mode() -> FirestoreCacheCollectionLoadMode {
        // An in-memory cache always starts empty, so PreloadAllIfEmpty would be equivalent.
        FirestoreCacheCollectionLoadMode::PreloadAllDocs
    }

    #[inline]
    fn create_default_storage() -> Self::DefaultStorage {
        FirestoreMemListenStateStorage::new()
    }

    #[inline]
    fn create_backend(
        options: Self::Options,
        config: FirestoreCacheConfiguration,
    ) -> FirestoreResult<Self::Backend> {
        FirestoreMemoryCacheBackend::with_options(config, options)
    }
}

/// The persistent backend flavour. See [`FirestoreCache::persistent`].
#[cfg(feature = "caching-persistent")]
pub struct FirestorePersistentCacheKind;

#[cfg(feature = "caching-persistent")]
impl sealed::Sealed for FirestorePersistentCacheKind {}

#[cfg(feature = "caching-persistent")]
impl FirestoreCacheBackendKind for FirestorePersistentCacheKind {
    type Backend = FirestorePersistentCacheBackend;
    type Options = FirestorePersistentCacheOptions;
    type DefaultStorage = FirestoreTempFilesListenStateStorage;

    #[inline]
    fn default_preload_mode() -> FirestoreCacheCollectionLoadMode {
        // Data survives restarts, so avoid re-downloading a collection a previous run stored.
        FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty
    }

    #[inline]
    fn create_default_storage() -> Self::DefaultStorage {
        FirestoreTempFilesListenStateStorage::new()
    }

    #[inline]
    fn create_backend(
        options: Self::Options,
        config: FirestoreCacheConfiguration,
    ) -> FirestoreResult<Self::Backend> {
        match options.data_file_path {
            Some(path) => FirestorePersistentCacheBackend::with_options(config, path),
            None => FirestorePersistentCacheBackend::new(config),
        }
    }
}

/// One collection to cache, as described to [`FirestoreCacheBuilder`].
#[derive(Debug, Clone)]
pub struct FirestoreCacheCollection {
    collection_name: String,
    parent: Option<String>,
    load_mode: FirestoreCacheCollectionLoadMode,
    listener_target: Option<u32>,
}

impl FirestoreCacheCollection {
    /// Describes a collection to cache, filled lazily unless a preloading mode is chosen.
    ///
    /// This is what [`FirestoreCache::add_collection`](crate::FirestoreCache::add_collection)
    /// takes; the builder has its own `collection` / `preloaded_collection` / `collection_with`
    /// methods for describing collections up front.
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// let collection = FirestoreCacheCollection::new("orders").preload_all();
    /// ```
    #[inline]
    pub fn new<S: AsRef<str>>(collection_name: S) -> Self {
        Self::with_load_mode(
            collection_name,
            FirestoreCacheCollectionLoadMode::PreloadNone,
        )
    }

    #[inline]
    pub(crate) fn with_load_mode<S: AsRef<str>>(
        collection_name: S,
        load_mode: FirestoreCacheCollectionLoadMode,
    ) -> Self {
        Self {
            collection_name: collection_name.as_ref().to_string(),
            parent: None,
            load_mode,
            listener_target: None,
        }
    }

    /// The listener target this collection was pinned to, if any.
    #[inline]
    pub(crate) fn requested_listener_target(&self) -> Option<u32> {
        self.listener_target
    }

    /// Turns this description into a configuration entry using the given listener target.
    #[inline]
    pub(crate) fn into_configuration(
        self,
        listener_target: FirestoreListenerTarget,
    ) -> FirestoreCacheCollectionConfiguration {
        let config = FirestoreCacheCollectionConfiguration::new(
            &self.collection_name,
            listener_target,
            self.load_mode,
        );
        match self.parent {
            Some(parent) => config.with_parent(parent),
            None => config,
        }
    }

    /// Caches a sub-collection under the given parent document path.
    ///
    /// Build the parent with [`FirestoreDb::parent_path`].
    #[inline]
    pub fn parent<S: AsRef<str>>(self, parent: S) -> Self {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    /// Downloads every document at startup. Enables cached `list` and `query`.
    #[inline]
    pub fn preload_all(self) -> Self {
        Self {
            load_mode: FirestoreCacheCollectionLoadMode::PreloadAllDocs,
            ..self
        }
    }

    /// Downloads every document only when the cache is empty. Enables cached `list` and `query`.
    #[inline]
    pub fn preload_all_if_empty(self) -> Self {
        Self {
            load_mode: FirestoreCacheCollectionLoadMode::PreloadAllIfEmpty,
            ..self
        }
    }

    /// Fills the cache lazily. Reads by ID are cached; `list` and `query` are not.
    #[inline]
    pub fn preload_none(self) -> Self {
        Self {
            load_mode: FirestoreCacheCollectionLoadMode::PreloadNone,
            ..self
        }
    }

    /// Pins this collection to a specific Firestore listener target ID instead of an
    /// automatically assigned one.
    #[inline]
    pub fn listener_target(self, target: u32) -> Self {
        Self {
            listener_target: Some(target),
            ..self
        }
    }
}

impl From<FirestoreCacheCollectionConfiguration> for FirestoreCacheCollection {
    fn from(config: FirestoreCacheCollectionConfiguration) -> Self {
        Self {
            collection_name: config.collection_name,
            parent: config.parent,
            load_mode: config.collection_load_mode,
            listener_target: Some(*config.listener_target.value()),
        }
    }
}

/// Builds a [`FirestoreCache`].
///
/// Created with [`FirestoreCache::memory`] or [`FirestoreCache::persistent`].
///
/// ```rust,no_run
/// # use firestore::*;
/// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let cache = FirestoreCache::memory(db)
///     .preloaded_collection("countries")
///     .collection("users")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Note that [`listen_state_storage`](Self::listen_state_storage) changes the builder's type, so
/// call it last if you use it.
pub struct FirestoreCacheBuilder<K, LS = <K as FirestoreCacheBackendKind>::DefaultStorage>
where
    K: FirestoreCacheBackendKind,
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    db: FirestoreDb,
    name: Option<FirestoreCacheName>,
    listener_params: Option<FirestoreListenerParams>,
    listener_target_base: u32,
    collections: Vec<FirestoreCacheCollection>,
    backend_options: K::Options,
    storage: LS,
    incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy,
}

impl<K> FirestoreCacheBuilder<K, K::DefaultStorage>
where
    K: FirestoreCacheBackendKind,
{
    #[inline]
    pub(crate) fn new(db: &FirestoreDb) -> Self {
        Self {
            db: db.clone(),
            name: None,
            listener_params: None,
            listener_target_base: FIRESTORE_CACHE_DEFAULT_LISTENER_TARGET_BASE,
            collections: Vec::new(),
            backend_options: K::Options::default(),
            storage: K::create_default_storage(),
            incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy::default(),
        }
    }
}

impl<K, LS> FirestoreCacheBuilder<K, LS>
where
    K: FirestoreCacheBackendKind,
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    /// Names the cache. Used in logs and tracing spans. Defaults to `firestore-cache`.
    #[inline]
    pub fn name<S: Into<String>>(self, name: S) -> Self {
        Self {
            name: Some(FirestoreCacheName::new(name.into())),
            ..self
        }
    }

    /// Caches a collection lazily: reads by ID populate the cache, but `list` and `query` are not
    /// served from it because only part of the collection is ever present.
    ///
    /// Use [`preloaded_collection`](Self::preloaded_collection) if you need cached listings.
    #[inline]
    pub fn collection<S: AsRef<str>>(mut self, collection_name: S) -> Self {
        self.collections
            .push(FirestoreCacheCollection::with_load_mode(
                collection_name,
                FirestoreCacheCollectionLoadMode::PreloadNone,
            ));
        self
    }

    /// Caches a complete copy of a collection, so that `list` and `query` can be served from it.
    #[inline]
    pub fn preloaded_collection<S: AsRef<str>>(mut self, collection_name: S) -> Self {
        self.collections
            .push(FirestoreCacheCollection::with_load_mode(
                collection_name,
                K::default_preload_mode(),
            ));
        self
    }

    /// Caches a collection, configured through a closure.
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// # async fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let cache = FirestoreCache::memory(db)
    ///     .collection_with("orders", |c| c.preload_all().listener_target(9000))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn collection_with<S, F>(mut self, collection_name: S, f: F) -> Self
    where
        S: AsRef<str>,
        F: FnOnce(FirestoreCacheCollection) -> FirestoreCacheCollection,
    {
        self.collections
            .push(f(FirestoreCacheCollection::with_load_mode(
                collection_name,
                FirestoreCacheCollectionLoadMode::PreloadNone,
            )));
        self
    }

    /// Adds a collection from an existing [`FirestoreCacheCollectionConfiguration`].
    #[inline]
    pub fn collection_config(mut self, config: FirestoreCacheCollectionConfiguration) -> Self {
        self.collections.push(config.into());
        self
    }

    /// Sets the first Firestore listener target ID to assign. Defaults to
    /// [`FIRESTORE_CACHE_DEFAULT_LISTENER_TARGET_BASE`].
    ///
    /// Target IDs must be unique across every listener in your application, so move this range if
    /// you run listeners of your own.
    #[inline]
    pub fn listener_target_base(self, listener_target_base: u32) -> Self {
        Self {
            listener_target_base,
            ..self
        }
    }

    /// Configures the cache's internal Firestore listener.
    #[inline]
    pub fn listener_params(self, listener_params: FirestoreListenerParams) -> Self {
        Self {
            listener_params: Some(listener_params),
            ..self
        }
    }

    /// Controls whether collections that are not preloaded may serve `list`/`query` from the
    /// cache. The default refuses to, because the results would be silently partial.
    #[inline]
    pub fn incomplete_collection_policy(
        self,
        incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy,
    ) -> Self {
        Self {
            incomplete_collection_policy,
            ..self
        }
    }

    /// Overrides where the internal listener stores its resume tokens.
    ///
    /// This changes the builder's type, so call it last.
    #[inline]
    pub fn listen_state_storage<LS2>(self, storage: LS2) -> FirestoreCacheBuilder<K, LS2>
    where
        LS2: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
    {
        FirestoreCacheBuilder {
            db: self.db,
            name: self.name,
            listener_params: self.listener_params,
            listener_target_base: self.listener_target_base,
            collections: self.collections,
            backend_options: self.backend_options,
            storage,
            incomplete_collection_policy: self.incomplete_collection_policy,
        }
    }

    /// Creates the cache, loads it and starts listening for changes.
    pub async fn build(self) -> FirestoreResult<FirestoreCache<K::Backend, LS>> {
        let cache = self.build_without_load().await?;
        cache.load().await?;
        Ok(cache)
    }

    /// Creates the cache without loading it.
    ///
    /// You must call [`FirestoreCache::load`] yourself before the cache serves anything or
    /// receives updates. Prefer [`build`](Self::build) unless you need to control when loading
    /// happens.
    pub async fn build_without_load(self) -> FirestoreResult<FirestoreCache<K::Backend, LS>> {
        let config = build_configuration(
            self.db.get_documents_path(),
            self.listener_target_base,
            &self.collections,
            self.incomplete_collection_policy,
        )?;

        let backend = K::create_backend(self.backend_options, config)?;

        let name = self
            .name
            .unwrap_or_else(|| FirestoreCacheName::new(FIRESTORE_CACHE_DEFAULT_NAME.to_string()));

        let options = FirestoreCacheOptions::new(name).opt_listener_params(self.listener_params);

        FirestoreCache::create(options, &self.db, backend, self.storage).await
    }
}

#[cfg(feature = "caching-memory")]
impl<LS> FirestoreCacheBuilder<FirestoreMemoryCacheKind, LS>
where
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    /// Sets the maximum number of documents kept per collection. Defaults to 50 000.
    #[inline]
    pub fn max_capacity(mut self, max_capacity: u64) -> Self {
        self.backend_options = self.backend_options.with_max_capacity(max_capacity);
        self
    }

    /// Evicts a document this long after it was written.
    #[inline]
    pub fn time_to_live(mut self, time_to_live: std::time::Duration) -> Self {
        self.backend_options = self.backend_options.with_time_to_live(time_to_live);
        self
    }

    /// Evicts a document this long after it was last read.
    #[inline]
    pub fn time_to_idle(mut self, time_to_idle: std::time::Duration) -> Self {
        self.backend_options = self.backend_options.with_time_to_idle(time_to_idle);
        self
    }
}

#[cfg(feature = "caching-persistent")]
impl<LS> FirestoreCacheBuilder<FirestorePersistentCacheKind, LS>
where
    LS: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    /// Stores the cache database at an explicit file path.
    ///
    /// Prefer [`data_dir`](Self::data_dir), which also keeps the listener resume tokens beside
    /// the data.
    #[inline]
    pub fn data_file_path<P: Into<std::path::PathBuf>>(mut self, path: P) -> Self {
        self.backend_options = self.backend_options.with_data_file_path(path.into());
        self
    }
}

#[cfg(feature = "caching-persistent")]
impl FirestoreCacheBuilder<FirestorePersistentCacheKind, FirestoreTempFilesListenStateStorage> {
    /// Stores both the cache database and the listener resume tokens in the given directory.
    ///
    /// This is the recommended way to configure a persistent cache: keeping the two together
    /// means they are lost or kept as a unit, so the cache can never resume from a token that
    /// does not match its data.
    #[inline]
    pub fn data_dir<P: AsRef<std::path::Path>>(mut self, dir: P) -> Self {
        let dir = dir.as_ref();
        self.backend_options = self.backend_options.with_data_file_path(dir.join("redb"));
        self.storage = FirestoreTempFilesListenStateStorage::with_temp_dir(dir);
        self
    }
}

/// Turns the collections described to the builder into a [`FirestoreCacheConfiguration`],
/// assigning listener target IDs.
///
/// Kept free of [`FirestoreDb`] so that it can be unit-tested without credentials.
fn build_configuration(
    documents_path: &str,
    listener_target_base: u32,
    collections: &[FirestoreCacheCollection],
    incomplete_collection_policy: FirestoreCacheIncompleteCollectionPolicy,
) -> FirestoreResult<FirestoreCacheConfiguration> {
    // An empty cache is allowed: collections can be added at runtime with
    // `FirestoreCache::add_collection`.

    // Explicitly requested targets are reserved first, so that automatic assignment can route
    // around them regardless of the order collections were added in.
    let mut used_targets: HashSet<u32> = HashSet::new();
    for collection in collections {
        if let Some(target) = collection.listener_target {
            if !used_targets.insert(target) {
                return Err(FirestoreError::InvalidParametersError(
                    FirestoreInvalidParametersError::new(
                        FirestoreInvalidParametersPublicDetails::new(
                            "listener_target".into(),
                            format!(
                                "Listener target {target} was requested by more than one cached \
                                 collection. Target IDs must be unique."
                            ),
                        ),
                    ),
                ));
            }
        }
    }

    let mut config = FirestoreCacheConfiguration::new()
        .with_incomplete_collection_policy(incomplete_collection_policy);
    let mut next_target = listener_target_base;
    let mut seen_paths: HashSet<String> = HashSet::new();

    for collection in collections {
        let target = match collection.listener_target {
            Some(target) => target,
            None => {
                while used_targets.contains(&next_target) {
                    next_target = next_target.checked_add(1).ok_or_else(|| {
                        FirestoreError::InvalidParametersError(
                            FirestoreInvalidParametersError::new(
                                FirestoreInvalidParametersPublicDetails::new(
                                    "listener_target_base".into(),
                                    "Ran out of listener target IDs while assigning them \
                                     automatically."
                                        .into(),
                                ),
                            ),
                        )
                    })?;
                }
                used_targets.insert(next_target);
                next_target
            }
        };

        let listener_target = FirestoreListenerTarget::new(target);
        listener_target.validate()?;

        let mut collection_config = FirestoreCacheCollectionConfiguration::new(
            &collection.collection_name,
            listener_target,
            collection.load_mode,
        );
        if let Some(ref parent) = collection.parent {
            collection_config = collection_config.with_parent(parent);
        }

        let collection_path = collection_config.resolve_collection_path(documents_path);
        if !seen_paths.insert(collection_path.clone()) {
            return Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "collection_name".into(),
                    format!(
                        "The collection `{collection_path}` was configured more than once for \
                         this cache."
                    ),
                )),
            ));
        }

        config = config.add_collection_config_at(documents_path, collection_config);
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOCS: &str = "projects/test/databases/(default)/documents";

    fn lazy(name: &str) -> FirestoreCacheCollection {
        FirestoreCacheCollection::with_load_mode(
            name,
            FirestoreCacheCollectionLoadMode::PreloadNone,
        )
    }

    fn target_of(config: &FirestoreCacheConfiguration, path: &str) -> u32 {
        *config.collections[path].listener_target.value()
    }

    #[test]
    fn assigns_listener_targets_sequentially_from_the_base() {
        let config = build_configuration(
            DOCS,
            1000,
            &[lazy("a"), lazy("b"), lazy("c")],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert_eq!(target_of(&config, &format!("{DOCS}/a")), 1000);
        assert_eq!(target_of(&config, &format!("{DOCS}/b")), 1001);
        assert_eq!(target_of(&config, &format!("{DOCS}/c")), 1002);
    }

    #[test]
    fn honours_a_custom_listener_target_base() {
        let config = build_configuration(
            DOCS,
            5000,
            &[lazy("a")],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert_eq!(target_of(&config, &format!("{DOCS}/a")), 5000);
    }

    #[test]
    fn automatic_targets_do_not_collide_with_explicit_ones() {
        let config = build_configuration(
            DOCS,
            1000,
            &[lazy("a"), lazy("b").listener_target(1000), lazy("c")],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert_eq!(target_of(&config, &format!("{DOCS}/b")), 1000);
        assert_eq!(target_of(&config, &format!("{DOCS}/a")), 1001);
        assert_eq!(target_of(&config, &format!("{DOCS}/c")), 1002);
    }

    #[test]
    fn rejects_duplicate_explicit_targets() {
        let err = build_configuration(
            DOCS,
            1000,
            &[lazy("a").listener_target(7), lazy("b").listener_target(7)],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap_err();

        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn rejects_the_same_collection_twice() {
        let err = build_configuration(
            DOCS,
            1000,
            &[lazy("a"), lazy("a")],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap_err();

        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn allows_an_empty_collection_list_for_a_cache_filled_at_runtime() {
        let config = build_configuration(
            DOCS,
            1000,
            &[],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert!(config.collections.is_empty());
    }

    #[test]
    fn rejects_the_reserved_zero_target() {
        let err = build_configuration(
            DOCS,
            0,
            &[lazy("a")],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap_err();

        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn resolves_sub_collections_under_their_parent() {
        let parent = format!("{DOCS}/users/user-1");
        let config = build_configuration(
            DOCS,
            1000,
            &[lazy("orders").parent(&parent)],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert!(config.collections.contains_key(&format!("{parent}/orders")));
    }

    #[test]
    fn only_preloaded_collections_are_listable() {
        let config = build_configuration(
            DOCS,
            1000,
            &[lazy("lazy_one"), lazy("preloaded").preload_all()],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert!(!config.is_collection_listable(&format!("{DOCS}/lazy_one")));
        assert!(config.is_collection_listable(&format!("{DOCS}/preloaded")));
        assert!(!config.is_collection_listable(&format!("{DOCS}/not_configured")));
    }

    #[test]
    fn supports_many_collections_with_mixed_modes_in_one_cache() {
        let parent = format!("{DOCS}/tenants/tenant-1");
        let config = build_configuration(
            DOCS,
            1000,
            &[
                lazy("users"),
                lazy("countries").preload_all(),
                lazy("currencies").preload_all_if_empty(),
                lazy("sessions").listener_target(2500),
                lazy("orders").parent(&parent).preload_all(),
            ],
            FirestoreCacheIncompleteCollectionPolicy::default(),
        )
        .unwrap();

        assert_eq!(config.collections.len(), 5);

        // Every collection gets its own listener target, and they are all distinct.
        let targets: HashSet<u32> = config
            .collections
            .values()
            .map(|c| *c.listener_target.value())
            .collect();
        assert_eq!(targets.len(), 5, "listener targets must be unique");
        assert!(targets.contains(&2500), "explicit target must be honoured");

        // Load modes are tracked per collection, not per cache.
        assert!(!config.is_collection_listable(&format!("{DOCS}/users")));
        assert!(!config.is_collection_listable(&format!("{DOCS}/sessions")));
        assert!(config.is_collection_listable(&format!("{DOCS}/countries")));
        assert!(config.is_collection_listable(&format!("{DOCS}/currencies")));

        // Sub-collections live under their parent and work alongside root collections.
        assert!(config.is_collection_listable(&format!("{parent}/orders")));
    }

    #[test]
    fn partial_results_policy_allows_listing_lazy_collections() {
        let config = build_configuration(
            DOCS,
            1000,
            &[lazy("lazy_one")],
            FirestoreCacheIncompleteCollectionPolicy::PartialResults,
        )
        .unwrap();

        assert!(config.is_collection_listable(&format!("{DOCS}/lazy_one")));
        // Still false for a collection the cache does not know about at all.
        assert!(!config.is_collection_listable(&format!("{DOCS}/not_configured")));
    }
}
