# Querying

The library supports rich querying API with filters, ordering, pagination, etc.

```rust,no_run
# use firestore::*;
# use futures::stream::BoxStream;
# use futures::TryStreamExt;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_string: String,
#     one_more_string: String,
#     some_num: u64,
#     created_at: FirestoreTimestamp,
# }
# const TEST_COLLECTION_NAME: &str = "test-query";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Query as a stream our data
let object_stream: BoxStream<FirestoreResult<MyTestStructure>> = db
    .fluent()
    .select()
    .fields(
        paths!(MyTestStructure::{some_id, some_num, some_string, one_more_string, created_at}),
    ) // Optionally select the fields needed
    .from(TEST_COLLECTION_NAME)
    .filter(|q| {
        // Fluent filter API example
        q.for_all([
            q.field(path!(MyTestStructure::some_num)).is_not_null(),
            q.field(path!(MyTestStructure::some_string)).eq("Test"),
            // Sometimes you have optional filters
            Some("Test2")
                .and_then(|value| q.field(path!(MyTestStructure::one_more_string)).eq(value)),
        ])
    })
    .order(|o| o.fields([o.field(path!(MyTestStructure::some_num)).desc()]))
    .obj() // Reading documents as structures using Serde gRPC deserializer
    .stream_query_with_errors()
    .await?;

let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;
println!("{:?}", as_vec);
# Ok(())
# }
```

Use:

- `q.for_all` for AND conditions
- `q.for_any` for OR conditions (Firestore has just recently added support for OR conditions)

You can nest `q.for_all`/`q.for_any`.

## Ordering

`.order()` takes a closure that receives an order builder and returns a `Vec`, the same shape as
`.filter()` and `.transforms()`. List multiple fields to sort by more than one, in priority order:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_num: u64,
# }
# const TEST_COLLECTION_NAME: &str = "test-query";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let ordered: Vec<MyTestStructure> = db
    .fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .order(|o| {
        o.fields([
            o.field(path!(MyTestStructure::some_num)).desc(),
            o.field(path!(MyTestStructure::some_id)).asc(),
        ])
    })
    .obj()
    .query()
    .await?;
# let _ = ordered;
# Ok(())
# }
```

An entry can be conditional, since `field(..).asc()`/`.desc()` return `Option<FirestoreQueryOrder>`
and `None` is dropped:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
#     some_num: u64,
# }
# const TEST_COLLECTION_NAME: &str = "test-query";
# async fn example(db: FirestoreDb, sort_by_num: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let ordered: Vec<MyTestStructure> = db
    .fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .order(|o| {
        o.fields([sort_by_num.then(|| o.field(path!(MyTestStructure::some_num)).desc())])
    })
    .obj()
    .query()
    .await?;
# let _ = ordered;
# Ok(())
# }
```

Each call to `.order()` replaces any ordering set by a previous call; it does not add to it. An
empty result (every entry conditional and every condition false) clears the ordering entirely.

If the query also uses `start_at`/`end_at`, the cursor's value count must match the number of
ordered fields - this is not enforced by the library, so a mismatch is a runtime error from
Firestore rather than a compile-time one.

`.order_by([(path!(..), FirestoreQueryDirection::Descending)])` still works but is deprecated in
favor of `.order()`.
