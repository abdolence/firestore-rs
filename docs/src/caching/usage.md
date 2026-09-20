## Usage

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String }
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Create an instance
let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

// Build the cache. This creates an internal Firestore listener, preloads the configured
// collections and starts listening for changes.
let cache = FirestoreCache::memory(&db)
    .preloaded_collection("test-caching")
    .build()
    .await?;

// Read through the cache: served from the cache when possible, from Firestore otherwise.
let my_struct: Option<MyTestStructure> = db
    .read_through_cache(&cache)
    .fluent()
    .select()
    .by_id_in("test-caching")
    .obj()
    .one("test-1")
    .await?;

// Read only from the cache, never contacting Firestore.
let my_struct: Option<MyTestStructure> = db
    .read_cached_only(&cache)
    .fluent()
    .select()
    .by_id_in("test-caching")
    .obj()
    .one("test-1")
    .await?;

cache.shutdown().await?;
# let _ = my_struct;
# Ok(())
# }
```

For a persistent cache, use `FirestoreCache::persistent(&db)` and give it a directory with
`.data_dir("/var/cache/my-app")`, which keeps the cache database and the listener resume tokens
together.

Listener target IDs are assigned automatically starting at 1000. If your application runs its own
listeners, move the cache's range with `.listener_target_base(...)` or pin individual collections
with `.collection_with(name, |c| c.listener_target(...))`.

Because `load` and `shutdown` take `&self`, a built cache can be shared directly as
`Arc<FirestoreMemoryCache>` in your application state. `FirestoreMemoryCache` and
`FirestorePersistentCache` are aliases that save you from spelling out the generic parameters.

`shutdown` stops the listener and releases the backend's resources: the in-memory cache drops its
documents, and the persistent cache closes its database file, so another cache can be opened over
the same directory.
