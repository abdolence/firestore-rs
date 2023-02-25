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
  - Streaming batch writes with automatic throttling to avoid time limits from Firestore;
- Fluent high-level and strongly typed API;
- Full async based on Tokio runtime;
- Macro that helps you use JSON paths as references to your structure fields;
- Implements own Serde serializer to Firestore protobuf values;
- Supports for Firestore timestamp with `#[serde(with)]` and a specialized structure
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GKE environment or application default accounts for local development;

## Quick start

Cargo.toml:
```toml
[dependencies]
firestore = "0.29"
```

## Examples
All examples available at [examples](examples) directory.

To run example use it with environment variables:
```
PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Firestore database client instance lifecycle

To create a new instance of Firestore client you need to provide at least a GCP project ID.
The client is created using the `Firestore::new` method:
```rust
use firestore::*;

// Create an instance
let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;
```

It is not recommended creating a new client for each request, so it is recommended to create a client once and reuse it whenever possible. 
Cloning instances is much cheaper than creating a new one.

In case if it is needed you can also create a new client instance using preconfigured token source.

For example:
```rust
FirestoreDb::with_options_token_source(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?.to_string()),
        gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
        gcloud_sdk::TokenSourceType::File("/tmp/key.json".into())
).await?;
```

## Fluent API

The library provides two APIs:
- Fluent API: To simplify development and developer experience the library provides more high level API starting with v0.12.x. This is the recommended API for all applications to use.
- Classic and low level API: the API existing before 0.12 is still available and not deprecated, so it is fine to continue to use when needed. Furthermore the Fluent API is based on the same classic API and generally speaking are something like smart and convenient constructors. The API can be changed with introducing incompatible changes so it is not recommended to use in long term. 

```rust
use firestore::*;

const TEST_COLLECTION_NAME: &'static str = "test";

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
  .document_id(&my_struct.some_id)
  .object(&my_struct)
  .execute()
  .await?;

// Update or Create 
// (Firestore supports creating documents with update if you provide the document ID).
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

To change this behaviour and support Firestore timestamps on database level there are two options:
- `#[serde(with)]` and attributes:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,

    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at: Option<DateTime<Utc>>,
}
```
- using a type `FirestoreTimestamp`:
```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    created_at: firestore::FirestoreTimestamp,
    updated_at: Option<firestore::FirestoreTimestamp>
}
```

This will change it only for firestore serialization, but it still serializes as string
to JSON (so you can reuse the same model for JSON and Firestore).

In your queries you need to use the wrapping class `firestore::FirestoreTimestamp`, for example:
```rust
   q.field(path!(MyTestStructure::created_at))
     .less_than_or_equal(firestore::FirestoreTimestamp(Utc::now()))
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
let parent_path = db.parent_path(TEST_PARENT_COLLECTION_NAME, parent_struct.some_id)?;

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

## Transactions

To manage transactions manually you can use `db.begin_transaction()`, and
then the Fluent API to add the operations needed in the transaction.

```rust
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
```

You may also execute transactions that automatically retry with exponential backoff using `run_transaction`.
```rust
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
```
See the complete example available [here](examples/read-write-transactions.rs).

Please note that Firestore doesn't support creating documents in the transactions (generating
document IDs automatically), so you need to use `update()` to implicitly create documents and specifying your own IDs.

## Reading Firestore document metadata as struct fields

Firestore provides additional generated fields for each of document you create:
- `_firestore_id`: Generated document ID (when it is not specified from the client);
- `_firestore_created`: The time at which the document was created;
- `_firestore_updated`: The time at which the document was last changed;

To be able to read them the library makes them available
as system fields for the Serde deserializer with reserved names,
so you can specify them in your structures as:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    #[serde(alias = "_firestore_created")]
    created_at: Option<DateTime<Utc>>,
    #[serde(alias = "_firestore_updated")]
    updated_at: Option<DateTime<Utc>>,
    some_string: String,
    one_more_string: String,
    some_num: u64,
}
```

Complete example available [here](examples/generated-document-id.rs).

## Document transformations
The library supports server side document transformations in transactions and batch writes:

```rust

// Only transformation
db.fluent()
  .update()
  .in_col(TEST_COLLECTION_NAME)
  .document_id("test-4")
  .transforms(|t| { // Transformations
    t.fields([
      t.field(path!(MyTestStructure::some_num)).increment(10),
      t.field(path!(MyTestStructure::some_array)).append_missing_elements([4, 5]),
      t.field(path!(MyTestStructure::some_array)).remove_all_from_array([3]),
    ])
  })
  .only_transform()
  .add_to_transaction(&mut transaction)?; // or add_to_batch

// Update and transform (in this order and atomically):
db.fluent()
  .update()
  .in_col(TEST_COLLECTION_NAME)
  .document_id("test-5")
  .object(&my_obj) // Updating the objects with the fields here
  .transforms(|t| { // Transformations after the update
    t.fields([
      t.field(path!(MyTestStructure::some_num)).increment(10),
    ])
  })
  .add_to_transaction(&mut transaction)?; // or add_to_batch
```

## Listening the document changes on Firestore
To help to work with asynchronous event listener the library supports high level API for
listening the events from Firestore on a separate thread:

```rust

let mut listener = db.create_listener(TempFileTokenStorage).await?;

// Adding query listener
db.fluent()
  .select()
  .from(TEST_COLLECTION_NAME)
  .listen()
  .add_target(TEST_TARGET_ID_BY_QUERY, &mut listener)?;

// Adding docs listener by IDs
db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .batch_listen([doc_id1, doc_id2])
  .add_target(TEST_TARGET_ID_BY_DOC_IDS, &mut listener)?;

listener
    .start(|event| async move {
        match event {
            FirestoreListenEvent::DocumentChange(ref doc_change) => {
                println!("Doc changed: {:?}", doc_change);

                if let Some(doc) = &doc_change.document {
                    let obj: MyTestStructure =
                        FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                            .expect("Deserialized object");
                    println!("As object: {:?}", obj);
                }
            }
            _ => {
                println!("Received a listen response event to handle: {:?}", event);
            }
        }

        Ok(())
    })
    .await?;

// Wait some events like Ctrl-C, signals, etc
// <put-your-implementation-for-wait-here>

// and then shutdown
listener.shutdown().await?;

```

See complete example in examples directory.

## Explicit null value serialization

By default, all Option<> serialized as absent fields, which is convenient for many cases. 
However sometimes you need to have explicit nulls.

To help with that there are additional attributes implemented for `serde(with)`:

* For any type:
```rust
#[serde(default)]
#[serde(with = "firestore::serialize_as_null")]
test_null: Option<String>,
```
* For Firestore timestamps attribute:
```rust
#[serde(default)]
#[serde(with = "firestore::serialize_as_null_timestamp")]
test_null: Option<DateTime<Utc>>,
```

## Select aggregate functions

The library supports the aggregation functions for the queries:

```rust
db.fluent()
  .select()
  .from(TEST_COLLECTION_NAME)
  .aggregate(|a| a.fields([a.field(path!(MyAggTestStructure::counter)).count()]))
  .obj()
  .query()
  .await?;
```

## Update/delete preconditions

The library supports the preconditions:

```rust
  .precondition(FirestoreWritePrecondition::Exists(true))
```

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

## How this library is tested

There are integration tests in tests directory that runs for every commit against the real
Firestore instance allocated for testing purposes. Be aware not to introduce huge document reads/updates
and collection isolation from other tests.

## Licence
Apache Software License (ASL)

## Author
Abdulla Abdurakhmanov
