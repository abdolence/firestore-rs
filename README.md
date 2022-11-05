[![Cargo](https://img.shields.io/crates/v/firestore.svg)](https://crates.io/crates/firestore)
![tests and formatting](https://github.com/abdolence/firestore-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/firestore-rs/workflows/security%20audit/badge.svg)

# Firestore for Rust

Library provides a simple API for Google Firestore based on the official gRPC API:
- Create or update documents using Rust structures and Serde; 
- Support for:
  - Querying/streaming docs/objects;
  - Listing documents/objects (and auto pages scrolling support);
  - Listening changes from Firestore;
  - Transactions;
  - Aggregated Queries;
- Fluent high-level and strongly typed API;
- Full async based on Tokio runtime;
- Macro that helps you use JSON paths as references to your structure fields;
- Implements own Serde serializer to Firestore protobuf values;
- Supports for Firestore timestamp with `#[serde(with)]`
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs) 
  that automatically detects GKE environment or application default accounts for local development;

## Quick start

Cargo.toml:
```toml
[dependencies]
firestore = "0.12"
```

## Examples
All examples available at [examples](examples) directory.

To run example use it with environment variables:
```
# PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Fluent API

The library provides two APIs:
- Fluent API: To simplify development and developer experience the library provides more high level API starting with v0.12.x.
- Classic API: the API existing before 0.12 is still available and not deprecated, so it is fine to continue to use when needed. Furthermore the Fluent API is based on the same classic API and generally speaking are something like smart and convenient constructors.

```rust
use firestore::*;

// Create an instance
let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

const TEST_COLLECTION_NAME: &'static str = "test";

let my_struct = MyTestStructure {
  some_id: "test-1".to_string(),
  some_string: "Test".to_string(),
  one_more_string: "Test2".to_string(),
  some_num: 42,
};

// Create data
let object_returned: MyTestStructure = db.fluent()
  .insert()
  .into(TEST_COLLECTION_NAME)
  .document_id(&my_struct.some_id)
  .object(&my_struct)
  .execute()
  .await?;

// Update data
let object_updated: MyTestStructure = db.fluent()
  .update()
  .fields(paths!(MyTestStructure::{some_num, one_more_string}))
  .in_col(TEST_COLLECTION_NAME)
  .document_id(&my_struct.some_id)
  .object(&MyTestStructure {
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
  .one(&my_struct.some_id)
  .await?;

// Query as a stream our data
let object_stream: BoxStream<MyTestStructure> = db.fluent()
    .select()
    .fields(paths!(MyTestStructure::{some_id, some_num, some_string, one_more_string, created_at})) // Optionally select the fields needed
    .from(TEST_COLLECTION_NAME)
    .filter(|q| { // Fluent filter API example
        q.for_all([
            q.field(path!(MyTestStructure::some_num)).is_not_null(),
            q.field(path!(MyTestStructure::some_string)).eq("Test"),
            // Sometimes you have optional filters
            Some("Test2")
                .and_then(|value| q.field(path!(MyTestStructure::one_more_string)).eq(value)),
        ])
    })
    .order_by([(
        path!(MyTestStructure::some_num),
        FirestoreQueryDirection::Descending,
    )])
    .obj() // Reading documents as structures using Serde gRPC deserializer
    .stream_query()
    .await?;

let as_vec: Vec<MyTestStructure> = object_stream.collect().await;
println!("{:?}", as_vec);

// Delete data
db.fluent()
  .delete()
  .from(TEST_COLLECTION_NAME)
  .document_id(&my_struct.some_id)
  .execute()
  .await?;

```

## Get and batch get support

```rust

let find_it_again: Option<MyTestStructure> = db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .obj()
  .one(&my_struct.some_id)
  .await?;

let object_stream: BoxStream<(String, Option<MyTestStructure>)> = db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .obj()
  .batch(vec!["test-0", "test-5"])
  .await?;
```

## Timestamps support
By default, the types such as DateTime<Utc> serializes as a string
to Firestore (while deserialization works from Timestamps and Strings).
To change it to support Timestamp natively use `#[serde(with)]`:

```
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,
}
```
This will change it only for firestore serialization and it still serializes as string
to JSON (so you can reuse the same model for JSON and Firestore).

In queries you need to use a special wrapping class `firestore::FirestoreTimestamp`, for example:
```
   q.field(path!(MyTestStructure::created_at))
     .eq(firestore::FirestoreTimestamp(Utc::now()))
```

## Nested collections
You can work with nested collection specifying path/location to a parent for documents:

```rust

// Creating a parent doc
db.fluent()
  .insert()
  .into(TEST_PARENT_COLLECTION_NAME)
  .document_id(&parent_struct.some_id)
  .object(&parent_struct)
  .execute()
  .await?;

// The doc path where we store our children
let parent_path = db.parent_path(TEST_PARENT_COLLECTION_NAME, parent_struct.some_id);

// Create a child doc
db.fluent()
  .insert()
  .into(TEST_CHILD_COLLECTION_NAME)
  .document_id(&child_struct.some_id)
  .parent(&parent_path)
  .object(&child_struct)
  .execute()
  .await?;

// Listing children
println!("Listing all children");

let objs_stream: BoxStream<MyChildStructure> = db.fluent()
  .list()
  .from(TEST_CHILD_COLLECTION_NAME)
  .parent(&parent_path)
  .obj()
  .stream_all()
  .await?;

```
Complete example available [here](examples/nested_collections.rs).

## Google authentication

Looks for credentials in the following places, preferring the first location found:
- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

### Local development
Don't confuse `gcloud auth login` with `gcloud auth application-default login` for local development,
since the first authorize only `gcloud` tool to access the Cloud Platform.

The latter obtains user access credentials via a web flow and puts them in the well-known location for Application Default Credentials (ADC).
This command is useful when you are developing code that would normally use a service account but need to run the code in a local development environment where it's easier to provide user credentials.
So to work for local development you need to use `gcloud auth application-default login`.

## Firestore emulator
To work with the Google Firestore emulator you can use environment variable:
```
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```
or specify it as an option using `FirestoreDb::with_options()`

## Licence
Apache Software License (ASL)

## Author
Abdulla Abdurakhmanov
