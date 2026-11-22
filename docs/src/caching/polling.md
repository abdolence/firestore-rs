# Polling instead of listening

A cache built with `poll_changes`, on `FirestoreCache::memory` or `FirestoreCache::persistent`,
opens no listener. It reads its collections again on an interval, for an application that does not
use the listen API, or an Enterprise edition database created without real-time updates:

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let cache = FirestoreCache::memory(&db)
    .preloaded_collection("countries")
    .collection("users")
    .poll_changes(std::time::Duration::from_secs(300))
    .build()
    .await?;
# Ok(())
# }
```

One background task polls every collection of the cache, one after another, in rounds. The
interval is the pause between the end of one round and the start of the next. The first round
starts one interval after the cache loads, and a collection whose documents the load kept from an
earlier run, rather than downloading them, is read at once. In each round the cache reads:

- a collection watched by document ID: those documents, whatever its load mode;
- any other preloaded collection: all its documents. The ones Firestore no longer has are removed,
  while every document present before and after stays readable;
- any other collection: only the documents it already holds, so polling never fills a lazily
  filled collection further.

A document is written to the cache only when Firestore holds a later version of it than the cache.
An unchanged document is left as it is: a poll resets neither its `time_to_live` nor its
`time_to_idle`, and a persistent cache rewrites nothing. A read through the cache while a round
runs can store a newer version of a document, and the round keeps it.

A preloaded collection is read in full every round, so the reads a cache makes grow with its
collections and with how often it polls. A collection that fails to be read keeps what it holds
and is read again in the next round, and the error is logged. Collections added or removed at
runtime are polled from the next round on, and a collection emptied or removed while a round reads
it is left as that change made it.

The cache reads Firestore at its latest state, even when it was built from a `FirestoreDb` that
reads through another cache or at a read time. `listener_params` together with `poll_changes`
fails the build with an `InvalidParametersError`, and `listen_state_storage` has no effect on a
cache that polls.
