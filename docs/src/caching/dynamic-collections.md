## Changing the cached collections at runtime

The set of cached collections does not have to be fixed when the cache is built:

```rust,no_run
# use firestore::*;
# async fn example(cache: FirestoreMemoryCache) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
cache.add_collection(FirestoreCacheCollection::new("currencies").preload_all()).await?;

cache.remove_collection("currencies").await?;
# Ok(())
# }
```

`add_collection` downloads the collection first if it is preloaded, publishes it only once it is
complete, and then extends the listener - so a listing never observes it half filled, and nothing
written during the download is missed. `remove_collection` does the reverse, and also forgets the
collection's resume token so its listener target ID cannot be reused against a different query.
Use `remove_collection_at` for a sub-collection, whose absolute path a bare name cannot address.

`FirestoreDb` handles created earlier with `read_through_cache` or `read_cached_only` pick both up
immediately: they share the cache's backend rather than a copy of it. A cache can also be built
with no collections at all and populated entirely at runtime.
