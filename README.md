[![Cargo](https://img.shields.io/crates/v/firestore.svg)](https://crates.io/crates/firestore)
![tests and formatting](https://github.com/abdolence/firestore-rs/workflows/tests%20&%20formatting/badge.svg)
![security audit](https://github.com/abdolence/firestore-rs/workflows/security%20audit/badge.svg)

# Firestore for Rust

Library provides a simple API for Google Firestore based on the official gRPC API:

- Fluent high-level and strongly typed API, the only public API of this library;
- Create or update documents using Rust structures and Serde;
- Support for:
    - Querying/streaming docs/objects;
    - Listing documents/objects (`stream_all` follows the page tokens for you);
    - Listening changes from Firestore;
    - Transactions;
    - Aggregated queries: count, sum, avg;
    - Streaming batch writes with automatic throttling to stay under the Firestore write rate limits;
    - K-nearest neighbor (KNN) vector search with Euclidean, Cosine and Dot Product measures;
    - Query cursors and partition queries;
    - Collection group queries and listing collection IDs;
    - Server side document transforms: increment, array append/remove, server timestamp;
    - Update/delete preconditions;
    - Consistency selectors to read at a given time or inside a transaction;
    - Explaining queries;
    - Request tags to attribute Firestore usage;
- Full async based on Tokio runtime;
- Macros that help you use your structure fields as Firestore field paths;
- Implements own Serde serializer to Firestore protobuf values;
- Support for multiple database IDs;
- Support for extended datatypes:
    - Firestore timestamp as a `FirestoreTimestamp` type or with `#[serde(with)]` attributes (based on [jiff](https://github.com/BurntSushi/jiff));
    - Lat/Lng;
    - References;
    - Explicit nulls;
- Caching support for collections and documents:
    - In-memory cache;
    - Persistent cache;
- Works with the Firestore emulator through `FIRESTORE_EMULATOR_HOST`, no credentials needed;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;

## Documentation

Please follow to the official website: <https://firestore-rust.abdolence.dev>.

Upgrading from an older version? See the [migration guide](MIGRATION.md).

## Quick start

Cargo.toml:

```toml
[dependencies]
firestore = "0.54"
```

```rust,no_run
use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: FirestoreDocumentId,
    some_string: String,
    some_num: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = FirestoreDb::new("my-project-id").await?;

    const TEST_COLLECTION_NAME: FirestoreCollectionId =
        FirestoreCollectionId::from_static("test");

    let my_struct = MyTestStructure {
        some_id: FirestoreDocumentId::from_static("test-1"),
        some_string: "Test".to_string(),
        some_num: 42,
    };

    // Create
    let object_returned: MyTestStructure = db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .execute()
        .await?;

    // Update or create
    // (Firestore supports creating documents with update if you provide the document ID).
    let object_updated: MyTestStructure = db.fluent()
        .update()
        .fields(paths!(MyTestStructure::{some_num, some_string}))
        .in_col(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&MyTestStructure {
            some_num: my_struct.some_num + 1,
            some_string: "updated-value".to_string(),
            ..my_struct.clone()
        })
        .execute()
        .await?;

    // Get object by id
    let find_it_again: Option<MyTestStructure> = db.fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(&my_struct.some_id)
        .await?;

    // Query as a stream
    let object_stream: BoxStream<FirestoreResult<MyTestStructure>> = db.fluent()
        .select()
        .fields(paths!(MyTestStructure::{some_id, some_num, some_string}))
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_all([
                q.field(path!(MyTestStructure::some_num)).is_not_null(),
                q.field(path!(MyTestStructure::some_string)).eq("Test"),
            ])
        })
        .order_by([(
            path!(MyTestStructure::some_num),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_query_with_errors()
        .await?;

    let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;

    // Delete data
    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    println!("{object_returned:?} {object_updated:?} {find_it_again:?} {as_vec:?}");

    Ok(())
}
```

If you see `no process-level CryptoProvider available`, see
[Crypto provider error](https://firestore-rust.abdolence.dev/getting-started.html#crypto-provider-error).

## Examples

All examples available in the [examples](examples) directory.

To run an example with environment variables:

```bash
PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

For local development don't confuse `gcloud auth login` with `gcloud auth application-default login`,
since the first authorize only `gcloud` tool to access the Cloud Platform.
See [Google authentication](https://firestore-rust.abdolence.dev/auth.html) for the details.

## How this library is tested

There are integration tests in the tests directory that runs for every commit against the real
Firestore instance allocated for testing purposes. Be aware not to introduce huge document reads/updates
and collection isolation from other tests.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
