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

Single writes to the same document are applied one after another, and the last one wins. No write fails because of
another: an `update()` overwrites the fields an earlier write stored, whether or not its sender saw that write.

`FirestoreWritePrecondition::UpdateTime` makes a write conditional on the document not having changed since you read
it. This is an optimistic lock: instead of blocking other writers, the write is rejected when one got there first. The
rejected write fails with `FAILED_PRECONDITION`, as a `DatabaseError` with the code `FailedPrecondition`, which the
library does not retry. Sending the same write again fails the same way. Read the document again, compute the new value
from what you read, and write that:

```rust,no_run
# use firestore::errors::FirestoreError;
# use firestore::*;
# use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Counter {
    #[serde(alias = "_firestore_updated", default, skip_serializing)]
    updated_at: Option<FirestoreTimestamp>,
    hits: u64,
}
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
for _ in 0..5 {
    let counter: Counter = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one("counter-1")
        .await?
        .expect("the counter exists");
    let read_at = counter.updated_at.clone().expect("a stored document has an update time");

    let result: FirestoreResult<()> = db
        .fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .precondition(FirestoreWritePrecondition::UpdateTime(read_at.into()))
        .document_id("counter-1")
        .object(&Counter {
            hits: counter.hits + 1,
            ..counter
        })
        .execute()
        .await;

    match result {
        Err(FirestoreError::DatabaseError(error)) if error.public.code == "FailedPrecondition" => {
            continue
        }
        other => return Ok(other?),
    }
}
Err("the counter changed under each of the 5 attempts".into())
# }
```

To read and write several documents consistently, or to have the retry done for you, use `run_transaction`, see
[Transactions](./transactions.md).
