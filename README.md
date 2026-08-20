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
    - K-nearest neighbor (KNN) vector search;
    - Explaining queries;
    - Request tags to attribute Firestore usage;
- Fluent high-level and strongly typed API;
- Full async based on Tokio runtime;
- Macro that helps you use JSON paths as references to your structure fields;
- Implements own Serde serializer to Firestore protobuf values;
- Support for multiple database IDs
- Supports for extended datatypes:
    - Firestore timestamp as a `FirestoreTimestamp` type or with `#[serde(with)]` attributes (based on [jiff](https://github.com/BurntSushi/jiff))
    - Lat/Lng
    - References
- Caching support for collections and documents:
    - In-memory cache;
    - Persistent cache;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;

## Quick start

Cargo.toml:

```toml
[dependencies]
firestore = "0.52"
```

### Crypto provider error

Depends on your other dependencies you may see the error like:

```
no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point 
```

This is because the TLS providers are not installed by default and you can choose different.
The easiest way to fix is just to include one of the provider, for example:

```toml
[dependencies]
rustls = "0.23"
```

If you have multiple you may need to call `CryptoProvider::install_default()` before using the Firestore client, e.g.:

```rust
rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
```


## Examples

All examples available in the [examples](examples) directory.

To run an example with environment variables:

```
PROJECT_ID=<your-google-project-id> cargo run --example crud
```

## Firestore database client instance and lifecycle

To create a new instance of Firestore client you need to provide at least a GCP project ID.
It is not recommended creating a new client for each request, so it is recommended to create a client once and reuse it
whenever possible.
Cloning instances is much cheaper than creating a new one.

The client is created using the `Firestore::new` method:

```rust
use firestore::*;

// Create an instance
let db = FirestoreDb::new( & config_env_var("PROJECT_ID") ? ).await?;
```

This is the recommended way to create a new instance of the client, since it
automatically detects the environment and uses credentials, service accounts, Workload Identity on GCP, etc.
Look at the section below [Google authentication](#google-authentication) for more details.

In cases if you need to create a new instance explicitly specifying a key file, you can use:

```rust
FirestoreDb::with_options_service_account_key_file(
  FirestoreDbOptions::new(config_env_var("PROJECT_ID") ?.to_string()),
  "/tmp/key.json".into()
).await?
```

or if you need even more flexibility you can use a preconfigured token source and scopes with:

```rust
FirestoreDb::with_options_token_source(
  FirestoreDbOptions::new(config_env_var("PROJECT_ID") ?.to_string()),
  gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
  gcloud_sdk::TokenSourceType::File("/tmp/key.json".into())
).await?
```

Firebase supports [multiple databases per project now](https://cloud.google.com/firestore/docs/manage-databases),
so you can specify the database ID in the options:

```rust
FirestoreDb::with_options(
  FirestoreDbOptions::new("your-project-id".to_string())
    .with_database_id("your-database-id".to_string())
  )
.await?
```

## Fluent API

The Fluent API is the only public API of this library. Everything starts from `db.fluent()`:

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
  .document_id( & my_struct.some_id)
  .object( & my_struct)
  .execute()
  .await?;

// Update or Create 
// (Firestore supports creating documents with update if you provide the document ID).
let object_updated: MyTestStructure = db.fluent()
  .update()
  .fields(paths!(MyTestStructure::{some_num, one_more_string}))
  .in_col(TEST_COLLECTION_NAME)
  .document_id( & my_struct.some_id)
  .object( & MyTestStructure {
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
  .one( & my_struct.some_id)
  .await?;

// Delete data
db.fluent()
  .delete()
  .from(TEST_COLLECTION_NAME)
  .document_id( & my_struct.some_id)
  .execute()
  .await?;

```

The low level "support" traits were made crate private in v0.52.0. If you used them, see the
[migration guide](MIGRATION.md) for the fluent replacement of every removed method.

Some operations have no fluent equivalent yet and remain public in their own right. They are
supported API, not leftovers:

- Batch writes: `db.create_simple_batch_writer()`, `db.create_streaming_batch_writer()`
- Transactions: `db.begin_transaction()`, `db.run_transaction()`, and the
  `FirestoreTransactionOps` trait implemented by both `FirestoreTransaction` and
  `FirestoreTransactionData`
- Listeners: `db.create_listener()`, and the `FirestoreResumeStateStorage` trait for custom
  resume token storage
- Caching: the `FirestoreCacheBackend` / `FirestoreCacheDocsByPathSupport` traits for custom cache
  backends
- Dynamic documents: `FirestoreDb::serialize_map_to_doc()`, `FirestoreDb::serialize_to_doc()` and
  `FirestoreDb::deserialize_doc_to()`, used together with the fluent `.document(...)` builders

If you hit something with no fluent equivalent, please open an issue: that is a gap we would
rather close than reopen the low level API for.

## Querying

The library supports rich querying API with filters, ordering, pagination, etc.

```rust
// Query as a stream our data
let object_stream: BoxStream<FirestoreResult<MyTestStructure> > = db.fluent()
  .select()
  .fields(paths!(MyTestStructure::{some_id, some_num, some_string, one_more_string, created_at})) // Optionally select the fields needed
  .from(TEST_COLLECTION_NAME)
  .filter( | q| { // Fluent filter API example
      q.for_all([
        q.field(path!(MyTestStructure::some_num)).is_not_null(),
        q.field(path!(MyTestStructure::some_string)).eq("Test"),
        // Sometimes you have optional filters
        Some("Test2")
          .and_then( | value | q.field(path ! (MyTestStructure::one_more_string)).eq(value)),        
      ])
  })
  .order_by([(
    path!(MyTestStructure::some_num),
    FirestoreQueryDirection::Descending,
  )])
  .obj() // Reading documents as structures using Serde gRPC deserializer
  .stream_query_with_errors()
  .await?;

let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;
println!("{:?}", as_vec);
```

Use:

- `q.for_all` for AND conditions
- `q.for_any` for OR conditions (Firestore has just recently added support for OR conditions)

You can nest `q.for_all`/`q.for_any`.

## Get and batch get support

```rust

let find_it_again: Option<MyTestStructure> = db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .obj()
  .one( & my_struct.some_id)
  .await?;

let object_stream: BoxStream<(String, Option<MyTestStructure>) > = db.fluent()
  .select()
  .by_id_in(TEST_COLLECTION_NAME)
  .obj()
  .batch(vec!["test-0", "test-5"])
  .await?;
```

## Timestamps support

By default, date/time values serialize as a string to Firestore (while
deserialization works from Timestamps and Strings). To store them as native
Firestore timestamps there are three options.

- Using `std::time::SystemTime` directly, with no attribute and no wrapping type,
  since it is recognised automatically:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    created_at: SystemTime,
    updated_at: Option<SystemTime>
}
```

  It works in the queries as well:

```rust
   q.field(path!(MyTestStructure::created_at)).less_than_or_equal(SystemTime::now())
```

  Note that `SystemTime` cannot carry the instants before the Unix epoch, since
  serde itself refuses them.

- Using the type `FirestoreTimestamp`, which needs no attributes:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    created_at: FirestoreTimestamp,
    updated_at: Option<FirestoreTimestamp>
}
```

It can be created with `FirestoreTimestamp::now()`, parsed from a string, and
converted from/to `std::time::SystemTime`:

```rust
let now = FirestoreTimestamp::now();
let from_system_time: FirestoreTimestamp = SystemTime::now().try_into()?;
let back_to_system_time: SystemTime = now.into();
```

Use it in your queries as well, for example:

```rust
   q.field(path!(MyTestStructure::created_at)).less_than_or_equal(FirestoreTimestamp::now())
```

- Or, if you prefer to keep a plain instant in your model, use
  `FirestoreInstant` (an alias for `jiff::Timestamp`) with `#[serde(with)]`
  attributes:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: FirestoreInstant,

    #[serde(default)]
    #[serde(with = "firestore::serialize_as_optional_timestamp")]
    updated_at: Option<FirestoreInstant>,
}
```

Firestore stores the timestamps with microsecond precision and discards the
nanoseconds on write. `FirestoreTimestamp` truncates to that precision in all of
its constructors and conversions, so its values survive a round trip unchanged.
A `FirestoreInstant` or a `SystemTime` carrying nanoseconds does not, since those
are the standard types this library cannot change.

All of them change the representation only for Firestore serialization.
`FirestoreTimestamp` and `FirestoreInstant` still serialize as a string to JSON,
so the same model can be reused for JSON and Firestore, while a plain `SystemTime`
keeps the default serde representation.

## Nested collections

You can work with nested collections specifying path/location to a parent for documents:

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
  .parent( & parent_path)
  .obj()
  .stream_all()
  .await?;

```

Complete example available [here](examples/nested_collections.rs).

You can nest multiple levels of collections using `at()`:

```rust
let parent_path =
db.parent_path(TEST_PARENT_COLLECTION_NAME, "parent-id")?
  .at(TEST_CHILD_COLLECTION_NAME, "child-id")?
  .at(TEST_GRANDCHILD_COLLECTION_NAME, "grand-child-id")?;
```

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
  .object( & MyTestStructure {
    some_id: format!("test-0"),
    some_string: "UpdatedTest".to_string(),
  })
  .add_to_transaction( & mut transaction) ?;

db.fluent()
  .delete()
  .from(TEST_COLLECTION_NAME)
  .document_id("test-5")
  .add_to_transaction( & mut transaction) ?;

transaction.commit().await?;
```

You may also execute transactions that automatically retry with exponential backoff using `run_transaction`.

```rust
    db.run_transaction( | db, transaction| {
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
        .add_to_transaction(transaction) ?;

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
    created_at: Option<FirestoreTimestamp>,
    #[serde(alias = "_firestore_updated")]
    updated_at: Option<FirestoreTimestamp>,
    some_string: String,
    one_more_string: String,
    some_num: u64,
}
```

Complete example available [here](examples/generated-document-id.rs).

## Working on dynamic/document level

Sometimes having static structure may restrict you from working with dynamic data,
so there is a way to use Fluent API to work with documents without introducing structures at all.

```rust
let object_returned = db
.fluent()
.insert()
.into(TEST_COLLECTION_NAME)
.document_id("test-1")
.document(FirestoreDb::serialize_map_to_doc("",
    [
      ("some_id", "test-id".into()),
      ("some_string", "test-value".into()),
      ("some_num", 42.into()),
      (
      "embedded_obj",
        FirestoreValue::from_map([
          ("inner_some_id", "inner-id-value".into()),
          ("inner_some_string", "inner-some-value".into()),
        ]),
      ),
      ("created_at", FirestoreTimestamp::now().into()),
    ])?
)
.execute()
.await?;

```

Full example available [here](examples/dynamic_doc_level_crud.rs).

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
.add_to_transaction( & mut transaction) ?; // or add_to_batch

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
.add_to_transaction(&mut transaction) ?; // or add_to_batch
```

## Listening the document changes on Firestore

To help to work with asynchronous event listener the library supports high level API for
listening the events from Firestore on a separate thread:

The listener implementation needs to be provided with a storage for the last received token for specified targets to be
able to resume listening the changes from the last handled token and to avoid receiving all previous changes.

The library provides basic implementations for storing the tokens but you can implement your own more sophisticated
storage if needed:

- `FirestoreTempFilesListenStateStorage` - resume tokens stored as temporary files on local FS;
- `FirestoreMemListenStateStorage` - in memory storage backed by HashMap (with this implementation if you restart your
  app, you will receive all notifications again);

```rust

let mut listener = db.create_listener(
    FirestoreTempFilesListenStateStorage::new() // or FirestoreMemListenStateStorage or your own implementation 
).await?;

// Adding query listener
db.fluent()
.select()
.from(TEST_COLLECTION_NAME)
.listen()
.add_target(TEST_TARGET_ID_BY_QUERY, &mut listener) ?;

// Adding docs listener by IDs
db.fluent()
.select()
.by_id_in(TEST_COLLECTION_NAME)
.batch_listen([doc_id1, doc_id2])
.add_target(TEST_TARGET_ID_BY_DOC_IDS, &mut listener) ?;

listener
.start( | event| async move {
    match event {
        FirestoreListenEvent::DocumentChange( ref doc_change) => {
            println ! ("Doc changed: {:?}", doc_change);
            
            if let Some(doc) = & doc_change.document {
              let obj: MyTestStructure =
              FirestoreDb::deserialize_doc_to::<MyTestStructure > (doc)
              .expect("Deserialized object");
              println ! ("As object: {:?}", obj);
            }
        }
        _ => {
          println ! ("Received a listen response event to handle: {:?}", event);
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
test_null: Option<FirestoreInstant>,
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

## Explaining the query

The library supports the query explanation:

```rust
db.fluent()
  .select()
  .from(TEST_COLLECTION_NAME)
  .explain()
  // or use explain_with_options if you want to provide additional options like analyze which run query to gather additional statistics 
  // .explain_with_options(FirestoreExplainOptions::new().with_analyze(true))
  .stream_query_with_metadata()
  .await?;
```

## Request tags

Firestore supports attaching request tags to requests. They are reported by Firestore
in its monitoring and billing breakdowns, which makes them useful to attribute reads
and writes to a specific feature, tenant or background job.

Tags can be set per operation for queries, aggregations, listings and listeners:

```rust
db.fluent()
  .select()
  .from(TEST_COLLECTION_NAME)
  .request_tags(["nightly-report"])
  // or use request_options if you want to provide the options structure directly
  // .request_options(FirestoreRequestOptions::from_tags(["nightly-report"]))
  .obj::<MyTestStructure>()
  .query()
  .await?;
```

Or session wide, for every request issued through a client instance. This is also how
you attach tags to the CRUD operations (insert/update/delete/get):

```rust
let tagged_db = db.clone_with_request_tags(["nightly-report"]);

tagged_db.fluent()
  .insert()
  .into(TEST_COLLECTION_NAME)
  .document_id(&my_struct.some_id)
  .object(&my_struct)
  .execute::<MyTestStructure>()
  .await?;
```

Transactions and batch writers accept them through their options:

```rust
db.run_transaction_with_options(
    |db, tx| { /* ... */ },
    FirestoreTransactionOptions::new()
        .with_request_options(FirestoreRequestOptions::from_tags(["checkout"])),
).await?;
```

A per operation value replaces the session wide default rather than merging with it.

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

### Local development

Don't confuse `gcloud auth login` with `gcloud auth application-default login` for local development,
since the first authorize only `gcloud` tool to access the Cloud Platform.

The latter obtains user access credentials via a web flow and puts them in the well-known location for Application
Default Credentials (ADC).
This command is useful when you are developing code that would normally use a service account but need to run the code
in a local development environment where it's easier to provide user credentials.
So to work for local development you need to use `gcloud auth application-default login`.

## Working with docker images

When you design your Dockerfile make sure you either installed Root CA certificates or use base images that already
include them.
If you don't have certs installed you usually observe the errors such as:

```
SystemError(FirestoreSystemError { public: FirestoreErrorPublicGenericDetails { code: "GrpcStatus(tonic::transport::Error(Transport, hyper::Error(Connect, Custom { kind: InvalidData, error: InvalidCertificateData(\"invalid peer certificate: UnknownIssuer\") })))" }, message: "GCloud system error: Tonic/gRPC error: transport error" })
```

For example for Debian based images, this usually can be fixed using this package:

```
RUN apt-get install -y ca-certificates
```

Also, I recommend considering using [Google Distroless images](https://github.com/GoogleContainerTools/distroless) since
they are secure, already include Root CA certs, and are optimised for size.

## Firestore emulator

To work with the Google Firestore emulator you can use the environment variable:

```
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```

or specify it as an option using `FirestoreDb::with_options()`.

When `FIRESTORE_EMULATOR_HOST` is set, the library does not look up the Google
credentials and uses a stub token instead, since the emulator does not
authenticate the requests. This means you do not need any credentials
configured to develop against it. Specifying a token source explicitly, with
`FirestoreDb::with_options_token_source()` for example, still takes precedence.

## Caching

The library supports caching for collections and documents. A Firestore listener keeps the cache
up to date when documents change, so updates are propagated across distributed instances
automatically.

This avoids reading, and paying for, the same documents repeatedly. It is particularly useful for
dictionaries, configuration and other data that changes rarely, and can reduce both cost and
latency noticeably.

Caching is opt-in through cargo features:

- `caching-memory` for an in-memory cache, implemented with the
  [moka cache library](https://github.com/moka-rs/moka);
- `caching-persistent` for a persistent, disk backed cache, implemented with
  [redb](https://github.com/cberner/redb) and protobuf.

### Usage

```rust
// Create an instance
let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

// Build the cache. This creates an internal Firestore listener, preloads the configured
// collections and starts listening for changes.
let cache = FirestoreCache::memory(&db)
    .preloaded_collection("test-caching")
    .build()
    .await?;

// Read through the cache: served from the cache when possible, from Firestore otherwise.
let my_struct: Option<MyTestStructure> = db.read_through_cache(&cache)
    .fluent()
    .select()
    .by_id_in("test-caching")
    .obj()
    .one("test-1")
    .await?;

// Read only from the cache, never contacting Firestore.
let my_struct: Option<MyTestStructure> = db.read_cached_only(&cache)
    .fluent()
    .select()
    .by_id_in("test-caching")
    .obj()
    .one("test-1")
    .await?;

cache.shutdown().await?;
```

For a persistent cache, use `FirestoreCache::persistent(&db)` and give it a directory with
`.data_dir("/var/cache/my-app")`, which keeps the cache database and the listener resume tokens
together.

Listener target IDs are assigned automatically starting at 1000. If your application runs its own
listeners, move the cache's range with `.listener_target_base(...)` or pin individual collections
with `.collection_with(name, |c| c.listener_target(...))`.

Because `load` and `shutdown` take `&self`, a built cache can be shared directly as
`Arc<FirestoreMemoryCache>` in your application state. `FirestoreMemoryCache` and
`FirestorePersistentCache` are aliases that save you from spelling out the generic parameters.

### Choosing a cache mode

- `db.read_through_cache(&cache)` serves what it can from the cache and goes to Firestore for the
  rest. This is the mode to reach for by default.
- `db.read_cached_only(&cache)` never contacts Firestore. Reads by ID return `None` on a miss, and
  requests the cache cannot answer completely return an error.

Which operations use the cache:

| Operation | Cached |
|---|---|
| Read by ID, batch read by IDs | yes, for any cached collection |
| Listing all documents in a collection | only for **preloaded** collections |
| Querying a collection (filtering, ordering, cursors) | only for **preloaded** collections, and only for supported queries |
| Paged listing, queries with metadata, aggregations, transactions, writes | never |

### Load modes, and why listings need preloading

- `PreloadNone` (`.collection(name)`): don't preload anything, just fill the cache while working;
- `PreloadAllDocs` (`.collection_with(name, |c| c.preload_all())`): preload all documents in the
  collection;
- `PreloadAllIfEmpty` (`.collection_with(name, |c| c.preload_all_if_empty())`): preload all
  documents only if the cache is empty. This is useful for the persistent cache; for the memory
  cache it is the same as `PreloadAllDocs`, since an in-memory cache always starts empty.

`.preloaded_collection(name)` picks the appropriate preloading mode for the backend.

A lazily filled collection holds only the documents that happened to be read through it.
Answering a `list` or `query` from it would return a subset that looks like a complete answer, so
the library refuses to do so: `read_through_cache` quietly falls back to Firestore, and
`read_cached_only` returns an error naming the collection. If partial results are genuinely
acceptable, opt in with
`.incomplete_collection_policy(FirestoreCacheIncompleteCollectionPolicy::PartialResults)`.

### How the cache is updated

- When you read a document by ID through the cache and it is not there, it is fetched from
  Firestore and cached;
- The Firestore listener updates the cache when a document changes, whether the change came from
  your application or from elsewhere;
- Preloading at startup.

Cached results are eventually consistent: they reflect the last state the listener delivered. A
write may take a moment to show up, and a stalled or reset listener can leave the cache stale
without saying so. Do not cache data that must be read at strong consistency.

Full examples are available [here](examples/caching_memory_collections.rs)
and [here](examples/caching_persistent_collections.rs).

## TLS related features
Cargo provides support for different TLS features for dependencies:
- `tls-roots`: default feature to support native TLS roots
- `tls-webpki-roots`: feature to switch to webpki crate roots

## Testing your own code

The Fluent API is built on `FirestoreDb` directly, so there is no built-in way to substitute a
fake database into `db.fluent()`. There are two approaches that work well.

### Abstract at your own boundary

Define a trait for what your code needs and implement it over a type holding a `FirestoreDb`.
Your business logic then depends on your trait, and tests provide their own implementation. This
keeps Firestore concerns in one place and needs nothing special from this library:

```rust
#[async_trait::async_trait]
trait UserRepository {
    async fn find_user(&self, id: &str) -> Result<Option<User>, MyError>;
}

struct FirestoreUserRepository {
    db: FirestoreDb,
}

#[async_trait::async_trait]
impl UserRepository for FirestoreUserRepository {
    async fn find_user(&self, id: &str) -> Result<Option<User>, MyError> {
        Ok(self.db
            .fluent()
            .select()
            .by_id_in("users")
            .obj()
            .one(id)
            .await?)
    }
}

// In tests, implement `UserRepository` with an in-memory HashMap.
```

### Run against the Firestore emulator

For tests that should exercise real query, listener and transaction behaviour, point the library
at the [Firestore emulator](https://firebase.google.com/docs/emulator-suite):

```bash
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```

No credentials are needed in that mode. This is how the caching and transaction behaviour of this
library itself is verified, and it catches things a hand-written fake cannot.

> **Changed in 0.52**: the low level `*Support` traits are no longer public, so code written to be
> generic over them no longer compiles. Note that they could never be used with the Fluent API
> from outside the crate anyway, since the builders' constructors are crate private. See the
> [migration guide](MIGRATION.md). If neither approach above covers your case, please open an
> issue.

## How this library is tested

There are integration tests in the tests directory that runs for every commit against the real
Firestore instance allocated for testing purposes. Be aware not to introduce huge document reads/updates
and collection isolation from other tests.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
