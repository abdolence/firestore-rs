# Batch writes

A batch groups many writes into as few requests as possible without the atomicity of a
[transaction](./transactions.md): each queued write in a batch succeeds or fails independently of
the others, so a batch is the right tool for bulk loads and migrations, where throughput matters
more than an all-or-nothing guarantee. Reach for a transaction instead when the writes must commit
together or depend on a value read moments before.

The library offers two batch writers with the same `FirestoreBatch` API for queuing writes, and
different wire behavior for sending them.

## The simple writer

`db.create_simple_batch_writer()` sends each batch as one Firestore `BatchWrite` request and is
the right default for occasional or moderate-sized batches.

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

`write()` sends the queued writes and returns a `FirestoreBatchWriteResponse` with, per write and
in the same order they were queued, an `update_time`/`transform_results` entry in `write_results`
and a `google.rpc.Status` in `statuses` - so one failing write in a batch does not stop the rest
from being reported. The request itself retries on transient failures with exponential backoff;
`FirestoreSimpleBatchWriteOptions::retry_max_elapsed_time` bounds how long that keeps retrying
before giving up (unbounded by default), and is set through
`db.create_simple_batch_writer_with_options(...)`.

## The streaming writer

`db.create_streaming_batch_writer()` sends batches over one long-lived Firestore `Write` stream
instead of one request each, and paces them with a throttle (500ms by default) to stay under
Firestore's per-stream write-rate limit. Reach for it once enough batches are sent back-to-back
that one `BatchWrite` request per batch becomes the bottleneck; the simple writer above needs no
such pacing and is the simpler choice otherwise.

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
`position` as that batch's sequence number in the stream; unlike the simple writer, streaming
responses always carry an empty `statuses` (Firestore's `Write` RPC does not report per-write
status) and only fill in `write_results`. A write failure fails the whole stream instead: it
surfaces as an `Err` on the response stream and ends it, so nothing after it is retried
automatically. The stream must be consumed - for example from the spawned task above - before
calling `finish()`, or `finish()` blocks forever waiting for responses nobody is reading; dropping
the writer instead of calling `finish()` only logs a warning and leaves its background task
running.

See [request tags](./request-tags.md) for attaching request tags to either writer through its
`options.request_options`.

## Adding writes to a batch

The fluent builders queue onto a batch the same way they queue onto a
[transaction](./transactions.md): `update()` (with or without `.transforms(...)`, see
[document transformations](./document-transformations.md)) and `delete()` both have an
`add_to_batch(&mut batch)`. `insert()` does not - Firestore's batch write cannot create a document
with a server-generated ID, so creating one through a batch means calling `update()` with an
explicit `document_id` instead.

For writes the fluent builders don't cover, `FirestoreBatch` itself exposes `update_object`,
`delete_by_id` and `transform` (plus `_at` variants that take an explicit parent path instead of
the batch's own documents path), each taking an optional
[write precondition](./preconditions.md) that fails just that one write if the document's current
state doesn't match it.
