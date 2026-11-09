# Update/delete preconditions

The library supports the preconditions:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_num: u64,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let object_updated: MyTestStructure = db
    .fluent()
    .update()
    .fields(paths!(MyTestStructure::{some_num}))
    .in_col(TEST_COLLECTION_NAME)
    .precondition(FirestoreWritePrecondition::Exists(true))
    .document_id("test-1")
    .object(&MyTestStructure { some_num: 1 })
    .execute()
    .await?;
# let _ = object_updated;
# Ok(())
# }
```

## Concurrent writes to one document

Single writes to the same document are applied one after another, and the last one wins. There is no conflict error
between them: an `update()` overwrites the fields an earlier write stored, whether or not its sender saw that write.
A document that gets more writes per second than it can take fails them with `ABORTED`, see
[When a write fails](./basic-writes.md#when-a-write-fails).

`FirestoreWritePrecondition::UpdateTime` makes a write conditional on the document not having changed since it was
read. A write that finds a different update time fails with `FAILED_PRECONDITION`, as a `DatabaseError` with the code
`FailedPrecondition`. The library does not retry it, and sending the same write again fails the same way. The error also
comes back when the library sent the write again after a first attempt that was applied, see
[Single writes](./request-timeouts.md#single-writes).

To read a document and write a value computed from it, use `run_transaction`, which runs the whole function again when
the document changed, see [Transactions](./transactions.md).
