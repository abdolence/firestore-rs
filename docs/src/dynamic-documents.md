# Working on dynamic/document level

Sometimes having static structure may restrict you from working with dynamic data,
so there is a way to use Fluent API to work with documents without introducing structures at all.

```rust,no_run
# use firestore::*;
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

# let _ = object_returned;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/dynamic_doc_level_crud.rs).
