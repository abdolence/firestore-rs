# Migration guide

## 0.52

v0.52.0 makes the low level API crate private, so the Fluent API is the only public entry point.
If you called methods on `FirestoreDb` that came from the `*Support` traits, the compiler will
report `no method named ... found`. Here is the mapping.

### Create

| Before | After |
|---|---|
| `db.create_obj::<_, (), _>(C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).object(&obj).execute::<()>()` |
| `db.create_obj(C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).object(&obj).execute()` |
| `db.create_obj(C, None::<&str>, &obj, None)` | `db.fluent().insert().into(C).generate_document_id().object(&obj).execute()` |
| `db.create_obj_at(parent, C, Some(id), &obj, None)` | `db.fluent().insert().into(C).document_id(id).parent(parent).object(&obj).execute()` |
| `db.create_doc(C, Some(id), doc, None)` | `db.fluent().insert().into(C).document_id(id).document(doc).execute()` |

### Read

| Before | After |
|---|---|
| `db.get_obj::<T, _>(C, id)` returning `T` | `db.fluent().select().by_id_in(C).obj::<T>().one(id)` returning `Option<T>` |
| `db.get_doc(C, id, None)` | `db.fluent().select().by_id_in(C).one(id)` |
| `db.batch_stream_get_objects::<T, _, _>(C, ids, None)` | `db.fluent().select().by_id_in(C).obj::<T>().batch(ids)` |
| `db.batch_stream_get_docs(C, ids, None)` | `db.fluent().select().by_id_in(C).batch(ids)` |

Note the return type change on single reads: the fluent `.one()` returns `Option<T>` instead of
erroring when the document does not exist. Add `.expect(...)` or handle the `None` case if you
relied on the old behaviour.

### Update

| Before | After |
|---|---|
| `db.update_obj(C, id, &obj, None, None, None)` | `db.fluent().update().in_col(C).document_id(id).object(&obj).execute()` |
| `db.update_obj(C, id, &obj, Some(paths!(T::{a, b})), None, None)` | `db.fluent().update().fields(paths!(T::{a, b})).in_col(C).document_id(id).object(&obj).execute()` |
| `db.update_obj(C, id, &obj, None, None, Some(precondition))` | `db.fluent().update().in_col(C).document_id(id).object(&obj).precondition(precondition).execute()` |
| `db.update_doc(C, doc, None, None, None)` | `db.fluent().update().in_col(C).document(doc).execute()` |

### Delete

| Before | After |
|---|---|
| `db.delete_by_id(C, id, None)` | `db.fluent().delete().from(C).document_id(id).execute()` |
| `db.delete_by_id_at(parent, C, id, None)` | `db.fluent().delete().from(C).parent(parent).document_id(id).execute()` |

### Query, list, aggregate, listen

| Before | After |
|---|---|
| `db.query_obj::<T>(params)` | `db.fluent().select().from(C).filter(...).obj::<T>().query()` |
| `db.stream_query_obj::<T>(params)` | `db.fluent().select().from(C)...obj::<T>().stream_query()` |
| `db.stream_list_obj::<T>(params)` | `db.fluent().list().from(C).obj::<T>().stream_all()` |
| `db.list_collection_ids(params)` | `db.fluent().list().collections().get_page()` |
| `db.aggregated_query_obj::<T>(params)` | `db.fluent().select().from(C).aggregate(...).obj::<T>().query()` |
| `db.listen_doc_changes(...)` | `db.fluent().select().from(C).listen().add_target(target_id, &mut listener)?` |

Unchanged in 0.52: batch writers, transactions, `db.create_listener()` and the
`FirestoreDb::serialize_*_to_doc` / `deserialize_doc_to` helpers. `FirestoreResumeStateStorage`
gains one method with a default implementation - see [Listener changes](#listener-changes).

`FirestoreTransactionOps` also stays public. It is implemented by both `FirestoreTransaction` and
`FirestoreTransactionData`, so you can keep writing transaction-agnostic abstractions over it.

If you implemented the `*Support` traits yourself, for example to stub the database in unit
tests, that is no longer possible.

### Caching changes

`FirestoreCache::new()` and `FirestoreCache::with_options()` still work but are deprecated in
favour of the builder:

```rust
// Before
let mut cache = FirestoreCache::new(
    "example-mem-cache".into(),
    &db,
    FirestoreMemoryCacheBackend::new(
        FirestoreCacheConfiguration::new().add_collection_config(
            &db,
            FirestoreCacheCollectionConfiguration::new(
                "test-caching",
                FirestoreListenerTarget::new(1000),
                FirestoreCacheCollectionLoadMode::PreloadAllDocs,
            ),
        ),
    )?,
    FirestoreMemListenStateStorage::new(),
).await?;
cache.load().await?;

// After
let cache = FirestoreCache::memory(&db)
    .name("example-mem-cache")
    .preloaded_collection("test-caching")
    .build()
    .await?;
```

Two behaviour changes to be aware of:

- `list` and `query` are now served from the cache only for preloaded collections.
  `read_through_cache` falls back to Firestore for the rest, and `read_cached_only` returns a
  `FirestoreError::CacheError` instead of silently returning partial results. Opt back into the
  old behaviour with
  `.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.
- `FirestoreCache::load()` and `shutdown()` now take `&self`, so `let mut cache` bindings will
  report an unused `mut`. Drop the `mut`.

The persistent cache also had a bug where document deletions were never committed, so deleted
documents stayed cached indefinitely. If you use `caching-persistent`, upgrading is recommended.

### Listener changes

The listener now acts on the target-lifecycle messages it previously ignored. Three things change
for callers.

`FirestoreResumeStateStorage` gains `forget_resume_state`, with a default implementation that does
nothing, so existing implementations keep compiling. Implement it if your storage is durable: it is
called when Firestore reports that it reset or removed a target, and a token kept past that point
is read back on the next process start and rejected. Both shipped storages implement it.

Listener callbacks now receive `TargetChange` events that carried a resume token. Previously the
listener consumed those itself and never passed them on, so a `RESET` or a `REMOVE` was invisible.
Callbacks that only match `DocumentChange` are unaffected; a callback with a catch-all arm will see
more events than before. Match on them with the new `FirestoreListenerTargetChangeType` alias:

```rust
if let FirestoreListenEvent::TargetChange(target_change) = event {
    if FirestoreListenerTargetChangeType::try_from(target_change.target_change_type)
        == Ok(FirestoreListenerTargetChangeType::Current)
    {
        // The target now reflects a consistent snapshot.
    }
}
```

A listen request Firestore rejects as invalid no longer shuts the whole listener down on the first
attempt. Because a stale resume token is a plausible cause, and one bad token used to take down
every target, the listener now discards all stored resume tokens and retries once before treating
the error as permanent.

### Caching changes from the listener work

The cache now removes a document when Firestore reports it as removed rather than deleted
(`DocumentRemove`), which it previously dropped on the floor - such a document stayed cached
indefinitely. It also acts on target resets: it drops what it holds for the affected collection and
lets Firestore replay it. While that replay is in progress the collection does not answer
`list`/`query` from the cache, so `read_through_cache` falls back to Firestore and
`read_cached_only` returns a `FirestoreError::CacheError` for the duration.

### Changing cached collections at runtime

Collections can now be added to and removed from a running cache, so the set of cached collections
no longer has to be decided when the cache is built:

```rust
cache.add_collection(FirestoreCacheCollection::new("currencies").preload_all()).await?;
cache.remove_collection("currencies").await?;
```

Three things changed to make this possible.

`FirestoreListener::add_target` now takes `&self` instead of `&mut self`, and works after `start()`
as well as before it. Calling it through a `&mut` binding still compiles, so the fluent
`.listen().add_target(target_id, &mut listener)` form is unaffected. It now rejects a target ID that
is already registered, where it previously accepted the duplicate and silently collapsed it. There
is also a new `remove_target`, which is `async` because it forgets the target's stored resume state
before returning.

`FirestoreCacheBackend` gains `cache_configuration`, `add_collection` and `remove_collection`. All
three have default implementations - the last two report that the backend does not support the
operation - so existing backends keep compiling. The `config` field on both shipped backends is now
private; use the `config()` accessor instead.

A cache may now be built with no collections at all, where the builder previously rejected that.

### Shutdown releases resources

`FirestoreCache::shutdown` now releases the backend's resources rather than only stopping the
listener. The in-memory backend drops its cached documents; the persistent backend closes its
database, releasing the exclusive lock on the file so another cache can be opened over the same
directory without waiting for the first to be dropped.

Reads through a cache that has been shut down do not fail: they behave as a cache miss, so
`read_through_cache` falls back to Firestore and `read_cached_only` reports the miss as an error.

### Caching named documents

A cached collection can now be limited to a set of document IDs, which makes the listener watch
exactly those documents instead of the whole collection:

```rust
FirestoreCache::memory(&db)
    .collection_with("configs", |c| c.documents(["site", "billing"]).preload_all())
```

`FirestoreCacheCollectionConfiguration` gains a `collection_watch` field for this, so any code
constructing that struct with a literal needs the extra field - use
`FirestoreCacheCollectionConfiguration::new(..)` and the `with_documents` builder instead. Such a
collection is never listable, whatever its load mode.
