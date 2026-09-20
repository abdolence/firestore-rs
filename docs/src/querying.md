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
# Ok(())
# }
```

Use:

- `q.for_all` for AND conditions
- `q.for_any` for OR conditions (Firestore has just recently added support for OR conditions)

You can nest `q.for_all`/`q.for_any`.
