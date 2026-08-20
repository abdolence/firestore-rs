# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.52.0]

### Changed

- **BREAKING**: the low level "support" traits are now crate private, making the Fluent API
  (`db.fluent()`) the only public API of this library. The affected traits are
  `FirestoreCreateSupport`, `FirestoreUpdateSupport`, `FirestoreDeleteSupport`,
  `FirestoreGetByIdSupport`, `FirestoreQuerySupport`, `FirestoreAggregatedQuerySupport`,
  `FirestoreListingSupport` and `FirestoreListenSupport`, along with the `PeekableBoxStream` type
  alias. See
  [Migrating to 0.52](README.md#migrating-to-052) for the fluent replacement of every removed
  method.

  Batch writers, transactions, listeners, the cache backend traits and the dynamic document
  helpers are unaffected and remain public. In particular `FirestoreTransactionOps` stays public:
  it exists so that transaction operations are available on both `FirestoreTransaction` and
  `FirestoreTransactionData`, which lets callers write transaction-agnostic abstractions over it
  (see [#206](https://github.com/abdolence/firestore-rs/issues/206)).

  If you implemented the `*Support` traits yourself, for example to stub the database in unit
  tests, that is no longer possible. Please open an issue describing your use case.

- **BREAKING**: `list` and `query` are served from the cache only for collections configured to
  be preloaded. A lazily filled collection holds an arbitrary subset of documents, so answering a
  listing from it returned partial results that looked complete. Now `read_through_cache` falls
  back to Firestore for such collections, and `read_cached_only` returns a
  `FirestoreError::CacheError`. The previous behaviour is available with
  `FirestoreCacheIncompleteCollectionPolicy::PartialResults`.

- **BREAKING**: `FirestoreCache::load()` and `FirestoreCache::shutdown()` take `&self` instead of
  `&mut self`, so a built cache can be shared as `Arc<FirestoreMemoryCache>`. Existing
  `let mut cache` bindings will report an unused `mut`.

- `read_through_cache` now uses the cache for `list` and `query` on preloaded collections. It
  previously ignored the cache for both operations entirely.

### Added

- A builder for the cache: `FirestoreCache::memory(&db)` and `FirestoreCache::persistent(&db)`.
  It assigns Firestore listener target IDs automatically, defaults the listener state storage to
  match the backend, and loads the cache as part of `build()`.
- `FirestoreMemoryCache` and `FirestorePersistentCache` type aliases, so the cache can be stored
  in application types without spelling out its generic parameters.
- `FirestoreMemoryCacheOptions` and `FirestorePersistentCacheOptions`, which expose the backend
  tuning knobs without leaking `moka` types.
- `FirestoreCacheIncompleteCollectionPolicy`, to opt back into partial cached listings.
- `FirestoreCacheConfiguration::is_collection_listable()`.

### Fixed

- The persistent cache never committed its delete transaction, so documents deleted in Firestore
  stayed in the cache indefinitely and were still returned by `list` and `query`.
- The in-memory cache returned an empty result set, presented as a cache hit, for queries against
  collections it was not configured to cache.
- Numerous broken rustdoc intra-doc links.

### Deprecated

- `FirestoreCache::new()` and `FirestoreCache::with_options()`, in favour of the builder. They
  keep working and will not be removed in 0.x.
- `FirestoreCacheCollectionConfiguration::with_index()` and `FirestoreCacheIndexConfiguration`.
  Cache indices were never implemented and have no effect.

### Documentation

- `[package.metadata.docs.rs] all-features = true`, without which the entire caching API was
  invisible on docs.rs.
- The cache module documentation described a usage flow that did not exist. It has been rewritten,
  and the previously undocumented cache configuration, options and backend types are now
  documented with examples.
