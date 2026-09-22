# Batch writes

The library supports batch writes as an alternative to [transactions](./transactions.md) for bulk
loads and migrations. A batch write does not commit atomically: each queued write succeeds or
fails on its own. Use a transaction instead when the writes need to commit together.

There are two batch writers:

- `db.create_simple_batch_writer()`: one Firestore `BatchWrite` request per batch, the right
  default for occasional or moderate-sized batches;
- `db.create_streaming_batch_writer()`: one long-lived `Write` stream for many batches sent
  back-to-back, throttled to stay under Firestore's per-stream write rate limit.

## The simple writer

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let batch_writer = db.create_simple_batch_writer().await?;

let mut current_batch = batch_writer.new_batch();

for idx in 0..500 {
    let my_struct = MyTestStructure {
        some_id: format!("test-{idx}"),
        some_string: "Test".to_string(),
    };

    db.fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .add_to_batch(&mut current_batch)?;

    if idx % 100 == 0 {
        let response = current_batch.write().await?;
        current_batch = batch_writer.new_batch();
        println!("{response:?}");
    }
}
# Ok(())
# }
```

`write()` returns a `FirestoreBatchWriteResponse` with `write_results` and `statuses` for each
queued write, in order, plus a `commit_time`. One failing write does not stop the rest from being
reported. The request retries transient failures with exponential backoff;
`FirestoreSimpleBatchWriteOptions::retry_max_elapsed_time` bounds how long, unbounded by default,
set through `db.create_simple_batch_writer_with_options(...)`.

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/batch-write-simple.rs).

## The streaming writer

```rust,no_run
# use firestore::*;
# use futures::TryStreamExt;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let (batch_writer, mut batch_results_reader) = db.create_streaming_batch_writer().await?;

let response_thread = tokio::spawn(async move {
    while let Ok(Some(response)) = batch_results_reader.try_next().await {
        println!("{response:?}");
    }
});

let mut current_batch = batch_writer.new_batch();
for idx in 0..10000 {
    let my_struct = MyTestStructure {
        some_id: format!("test-{idx}"),
        some_string: "Test".to_string(),
    };

    db.fluent()
        .update()
        .in_col(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .add_to_batch(&mut current_batch)?;

    if idx % 100 == 0 {
        current_batch.write().await?;
        current_batch = batch_writer.new_batch();
    }
}

batch_writer.finish().await;
let _ = tokio::join!(response_thread);
# Ok(())
# }
```

The response stream yields one `FirestoreBatchWriteResponse` per batch, in send order, with
`position` as the batch's sequence number in the stream. `statuses` is always empty on this
writer; Firestore's `Write` RPC does not report per-write status the way `BatchWrite` does. A
write failure fails the whole stream instead, surfacing as an `Err` on the response stream and
ending it.

Consume the response stream, for example from a spawned task as above, before calling `finish()`,
or it blocks forever waiting for responses nobody reads. Be aware not to just drop the writer
instead of calling `finish()`; that only logs a warning and leaves its background task running.

`options.throttle_batch_duration` paces how often batches go out over the stream, 500ms by
default. See [request tags](./request-tags.md) for attaching request tags through
`options.request_options` on either writer.

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/batch-write-streaming.rs).

## Adding writes to a batch

`update()` and `delete()` both have `add_to_batch(&mut batch)`, the same as
`add_to_transaction(&mut transaction)` on a [transaction](./transactions.md). See
[document transformations](./document-transformations.md) for `.transforms(...)`.

`insert()` has no `add_to_batch`: a batch write cannot create a document with a server-generated
ID. Use `update()` with an explicit `document_id` to create one instead.

For writes the fluent builders do not cover, queue them directly on `FirestoreBatch`:
`update_object`, `delete_by_id` and `transform`, plus `_at` variants for an explicit parent path.
Each takes an optional [write precondition](./preconditions.md). On the simple writer a failed
precondition fails just that one write; on the streaming writer it fails the whole stream.
