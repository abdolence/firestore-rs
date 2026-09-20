## Caching named documents instead of a whole collection

`.collection(name)` subscribes the listener to the **entire** collection, even though it does not
preload it - "lazy" only means the initial download is skipped. When you know which documents you
care about, say so:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let cache = FirestoreCache::memory(&db)
    .collection_with("configs", |c| {
        c.documents(["site", "billing"]).preload_all()
    })
    .build()
    .await?;
# let _ = cache;
# Ok(())
# }
```

The listener then watches exactly those documents, so unrelated changes in the collection are never
streamed to your process or written into the cache, and preloading reads just those IDs. This is
the shape to reach for with configuration, feature flags and reference data.

Such a collection is never listable, whatever its load mode: it holds a chosen subset, so `list`
and `query` would return a partial answer that looks complete.
