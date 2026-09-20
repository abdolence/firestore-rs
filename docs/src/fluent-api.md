# Fluent API

The Fluent API is the only public API of this library. Everything starts from `db.fluent()`:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
#     one_more_string: String,
#     some_num: u64,
# }
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use firestore::*;

const TEST_COLLECTION_NAME: FirestoreCollectionId =
    FirestoreCollectionId::from_static("test");

let my_struct = MyTestStructure {
  some_id: "test-1".to_string(),
  some_string: "Test".to_string(),
  one_more_string: "Test2".to_string(),
  some_num: 42,
};

// Create
let object_returned: MyTestStructure = db.fluent()
  .insert()
  .into(TEST_COLLECTION_NAME)
  .document_id( & my_struct.some_id)
  .object( & my_struct)
  .execute()
  .await?;

// Update or Create 
// (Firestore supports creating documents with update if you provide the document ID).
let object_updated: MyTestStructure = db.fluent()
  .update()
  .fields(paths!(MyTestStructure::{some_num, one_more_string}))
  .in_col(TEST_COLLECTION_NAME)
  .document_id( & my_struct.some_id)
  .object( & MyTestStructure {
      some_num: my_struct.some_num + 1,
      one_more_string: "updated-value".to_string(),
        ..my_struct.clone()
   })
  .execute()
  .await?;

// Get object by id
let find_it_again: Option<MyTestStructure> = db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .obj()
  .one( & my_struct.some_id)
  .await?;

// Delete data
db.fluent()
  .delete()
  .from(TEST_COLLECTION_NAME)
  .document_id( & my_struct.some_id)
  .execute()
  .await?;

# Ok(())
# }
```

The low level "support" traits were made crate private in v0.52.0. If you used them, see the
[migration guide](https://github.com/abdolence/firestore-rs/blob/master/MIGRATION.md) for the fluent replacement of every removed method.

Alongside it, the library provides core functionality that is public API in its own right:

- Batch writes: `db.create_simple_batch_writer()`, `db.create_streaming_batch_writer()`
- Transactions: `db.begin_transaction()`, `db.run_transaction()`, and the
  `FirestoreTransactionOps` trait implemented by both `FirestoreTransaction` and
  `FirestoreTransactionData`
- Listeners: `db.create_listener()`, and the `FirestoreResumeStateStorage` trait for custom
  resume token storage
- Caching: the `FirestoreCacheBackend` / `FirestoreCacheDocsByPathSupport` traits for custom cache
  backends
- Dynamic documents: `FirestoreDb::serialize_map_to_doc()`, `FirestoreDb::serialize_to_doc()` and
  `FirestoreDb::deserialize_doc_to()`, used together with the fluent `.document(...)` builders
