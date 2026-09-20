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
- Validated `FirestoreDocumentId` and `FirestoreCollectionId` types that reject a malformed ID before it reaches Firestore;
- Full async based on Tokio runtime;
- Macros that help you use your structure fields as Firestore field paths;
- Implements own Serde serializer to Firestore protobuf values;
- Support for multiple database IDs;
- Support for extended datatypes:
    - Firestore timestamp as a `FirestoreTimestamp` type or with `#[serde(with)]` attributes (based on [jiff](https://github.com/BurntSushi/jiff));
    - Lat/Lng;
    - References;
    - Explicit nulls with `#[serde(with = "firestore::serialize_as_null")]`;
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

```rust
use firestore::*;

let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

let object_returned: MyTestStructure = db.fluent()
  .insert()
  .into("test")
  .document_id(&my_struct.some_id)
  .object(&my_struct)
  .execute()
  .await?;
```

If you see `no process-level CryptoProvider available`, see
[Crypto provider error](https://firestore-rust.abdolence.dev/getting-started.html#crypto-provider-error).

## Examples

All examples available in the [examples](examples) directory.

To run an example with environment variables:

```
PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
