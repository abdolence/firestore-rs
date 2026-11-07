# Transactions

To manage transactions manually you can use `db.begin_transaction()`, and
then the Fluent API to add the operations needed in the transaction.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String, some_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut transaction = db.begin_transaction().await?;

db.fluent()
    .update()
    .fields(paths!(MyTestStructure::{
      some_string
    }))
    .in_col(TEST_COLLECTION_NAME)
    .document_id("test-0")
    .object(&MyTestStructure {
        some_id: format!("test-0"),
        some_string: "UpdatedTest".to_string(),
    })
    .add_to_transaction(&mut transaction)?;

db.fluent()
    .delete()
    .from(TEST_COLLECTION_NAME)
    .document_id("test-5")
    .add_to_transaction(&mut transaction)?;

transaction.commit().await?;
# Ok(())
# }
```

You may also execute transactions that automatically retry with exponential backoff using `run_transaction`.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { test_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# const TEST_DOCUMENT_ID: &str = "test_doc_id";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.run_transaction(|db, transaction| {
    Box::pin(async move {
        let mut test_structure: MyTestStructure = db
            .fluent()
            .select()
            .by_id_in(TEST_COLLECTION_NAME)
            .obj()
            .one(TEST_DOCUMENT_ID)
            .await?
            .expect("Missing document");

        // Perform some kind of operation that depends on the state of the document
        test_structure.test_string += "a";

        db.fluent()
            .update()
            .fields(paths!(MyTestStructure::{
             test_string
            }))
            .in_col(TEST_COLLECTION_NAME)
            .document_id(TEST_DOCUMENT_ID)
            .object(&test_structure)
            .add_to_transaction(transaction)?;

        Ok(())
    })
})
.await?;
# Ok(())
# }
```

The transaction retries automatically on a transient failure: the callback returning
`BackoffError::Transient` (a plain `?` on a `FirestoreResult` inside the callback already converts
to one), or `commit()` answering `ABORTED`. Either way the whole transaction reruns from the start
as a new attempt, so the callback must be safe to run more than once; a failed attempt is rolled
back first, releasing its locks, before the retry begins. A read inside the transaction that comes
back `ABORTED` is not retried on its own; it fails the attempt, which then retries as usual.

`Commit` is sent only once per attempt. Any failure other than `ABORTED`, such as `UNAVAILABLE` or a
dropped connection, is returned to the caller without retrying, since the writes may
already be applied, and the callback is not run again in that case.

Retries are capped at `max_retries` attempts after the first, 4 by default (5 attempts in total),
the same default as Google's official clients; `retry_max_elapsed_time` adds an optional time cap
on top, off by default. Each retry waits a backoff delay first, or the delay the callback named through
`BackoffError::Transient`'s `retry_after`. Return `Err(BackoffError::Permanent(err))` instead for an
error that must not be retried; it stops immediately and comes back wrapped in
`FirestoreError::ErrorInTransaction`.

Set `max_retries` and `retry_max_elapsed_time` through `run_transaction_with_options`:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { test_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# const TEST_DOCUMENT_ID: &str = "test_doc_id";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use std::time::Duration;

let options = FirestoreTransactionOptions::new()
    .with_max_retries(2)
    .with_retry_max_elapsed_time(Duration::from_secs(30));

db.run_transaction_with_options(
    |db, _transaction| {
        Box::pin(async move {
            let _existing: Option<MyTestStructure> = db
                .fluent()
                .select()
                .by_id_in(TEST_COLLECTION_NAME)
                .obj()
                .one(TEST_DOCUMENT_ID)
                .await?;
            Ok(())
        })
    },
    options,
)
.await?;
# Ok(())
# }
```

See the complete example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/read-write-transactions.rs).

A Core write in a transaction cannot create a document with an ID that Firestore generates. To
create a document in a transaction, use `update()` with an ID of your own, which creates the
document when it does not exist. A pipeline `insert` without `.document_id(..)` is the exception on
the Enterprise edition: it gets an ID from Firestore and runs in a transaction, see
[Pipeline writes](./pipeline-writes.md#writes-in-a-transaction).

## Transactions and the cache

A read in a transaction never uses the [cache](./caching.md). The same holds for a handle from
`clone_with_consistency_selector`, which reads at a read time:

- under `read_through_cache`, `get`, batch get, `query` and `list` are answered by Firestore, so the
  transaction reads and locks the documents. A document read this way is not written to the cache;
- under `read_cached_only`, the same reads return `FirestoreError::CacheError` with the code
  `CacheConsistencySelectorUnsupported`, because that mode never contacts Firestore. A transaction
  needs a `read_through_cache` handle instead.

Inside `run_transaction`, a `?` on that error makes it transient, so the transaction is retried 4
times, with a few seconds of backoff, before the error is returned wrapped in
`FirestoreError::ErrorInTransaction`. Return `BackoffError::permanent` from the closure to fail on
the first attempt.

## On the Enterprise edition

Core aggregations and transactions behave the same on both editions; under contention an
Enterprise database, which is optimistic by default, aborts and retries transactions more often
than a Standard one. The concurrency mode is a setting of the database, shown as `concurrencyMode`
by `gcloud firestore databases describe`, see
[Telling which edition a database is](./editions.md#telling-which-edition-a-database-is).

A transaction on Enterprise can also hold [pipeline and SQL writes](./pipeline-writes.md#writes-in-a-transaction).
They are sent when they run, where Core writes are buffered and sent with the commit. A transaction
takes pipeline writes or buffered writes: Firestore rejects a commit that mixes them with
`INVALID_ARGUMENT`.
