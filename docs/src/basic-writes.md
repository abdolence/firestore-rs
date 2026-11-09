# Create, update and delete documents

The library provides `insert()`, `update()` and `delete()` in the Fluent API for writing a single
document. Each of them sends one request when you call `.execute()`. To write several documents
together, see [Transactions](./transactions.md) and [Batch writes](./batch-writes.md).

## Creating a document

`insert()` creates a document in a collection, and fails when a document with the same ID already
exists:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
#     some_num: u64,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let my_struct = MyTestStructure {
    some_id: "test-1".to_string(),
    some_string: "Test".to_string(),
    some_num: 42,
};

let object_returned: MyTestStructure = db
    .fluent()
    .insert()
    .into(TEST_COLLECTION_NAME)
    .document_id(&my_struct.some_id)
    .object(&my_struct)
    .execute()
    .await?;
# Ok(())
# }
```

The conflict is reported as `FirestoreError::DataConflictError`.

`.execute()` returns the stored document deserialised into the type you ask for. Use
`.execute::<()>()` when you do not need it.

To have an ID chosen for you, call `.generate_document_id()` instead of `.document_id(..)`. The library generates it as
Firestore's own clients do: 20 random letters and digits. A write that is sent again after a failure carries the same
ID, so it cannot create a second document. The generated ID comes back in a field marked with the `_firestore_id` alias, see
[Document metadata as struct fields](./document-metadata.md):

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyGeneratedStructure {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    some_string: String,
}
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let created: MyGeneratedStructure = db
    .fluent()
    .insert()
    .into(TEST_COLLECTION_NAME)
    .generate_document_id()
    .object(&MyGeneratedStructure {
        id: None,
        some_string: "Test".to_string(),
    })
    .execute()
    .await?;

let generated_id: Option<String> = created.id;
# Ok(())
# }
```

## Updating a document

`update()` writes the object to the document with the given ID. Without `.fields(..)` the object
replaces the whole document, and the fields it does not have are removed. With `.fields(..)` only
the listed fields are written, and the rest of the stored document is left as it is:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
#     some_num: u64,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb, my_struct: MyTestStructure) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Replaces the document
db.fluent()
    .update()
    .in_col(TEST_COLLECTION_NAME)
    .document_id(&my_struct.some_id)
    .object(&my_struct)
    .execute::<()>()
    .await?;

// Writes `some_num` only
let object_updated: MyTestStructure = db
    .fluent()
    .update()
    .fields(paths!(MyTestStructure::{some_num}))
    .in_col(TEST_COLLECTION_NAME)
    .document_id(&my_struct.some_id)
    .object(&MyTestStructure {
        some_num: my_struct.some_num + 1,
        ..my_struct.clone()
    })
    .execute()
    .await?;
# Ok(())
# }
```

`update()` also creates the document when it does not exist yet, and then stores only the listed
fields when `.fields(..)` is given. Add a precondition to make it fail on a missing document, or
to make it create only, see [Update/delete preconditions](./preconditions.md). Server side
changes such as incrementing a number are in [Document transformations](./document-transformations.md).

## Deleting a document

`delete()` removes the document with the given ID. Deleting a document that does not exist is not
an error:

```rust,no_run
# use firestore::*;
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .delete()
    .from(TEST_COLLECTION_NAME)
    .document_id("test-1")
    .execute()
    .await?;
# Ok(())
# }
```

Documents in the subcollections of a deleted document stay in place. To remove a whole collection
see [Bulk delete](./bulk-delete.md).

## When a write fails

The library sends `insert()`, `update()` and `delete()` again after `RESOURCE_EXHAUSTED` and `UNAVAILABLE`, up to
`max_retries` times with a growing delay. These are the codes that Google's Firestore server libraries retry a write on,
see [Single writes](./request-timeouts.md#single-writes). When the error still comes back after the last retry, the
write may or may not be applied.

Other failures to expect from a single write:
- `ABORTED`: Firestore received too many updates for one document per second, and answers with "Too much contention on
  these datastore entities. Please try again." Google's Java, Go and Python server libraries do not retry it on a
  single write, and neither does the library. Lower the rate of writes to that document;
- `ALREADY_EXISTS`, as `FirestoreError::DataConflictError`: an `insert()` of an ID that exists, or an `update()` with
  `Exists(false)` on a document that exists. After a retried write it can mean that the first attempt created the
  document;
- `NOT_FOUND`, as `FirestoreError::DataNotFoundError`: a `delete()` with `Exists(true)` on a document that does not
  exist. After a retried `delete()` it can mean that the first attempt deleted the document;
- `FAILED_PRECONDITION`: the precondition of the write did not hold, see
  [Update/delete preconditions](./preconditions.md). After a retried write with an `UpdateTime` precondition it can mean
  that the first attempt was applied.

All three builders accept `.parent(..)` for documents in [nested collections](./nested-collections.md).
`insert()` and `update()` accept `.document(..)` instead of `.object(..)` for
[working on dynamic/document level](./dynamic-documents.md).

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/crud.rs).
