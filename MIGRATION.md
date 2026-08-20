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

Unchanged in 0.52: batch writers, transactions, `db.create_listener()`,
`FirestoreResumeStateStorage`, the cache backend traits and the
`FirestoreDb::serialize_*_to_doc` / `deserialize_doc_to` helpers.

`FirestoreTransactionOps` also stays public. It is implemented by both `FirestoreTransaction` and
`FirestoreTransactionData`, so you can keep writing transaction-agnostic abstractions over it.

If you implemented the `*Support` traits yourself, for example to stub the database in unit
tests, that is no longer possible. Please open an issue describing your use case: a supported way
to do it is something we would rather design than leave to a trait that was never meant to be
part of the public API.

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
