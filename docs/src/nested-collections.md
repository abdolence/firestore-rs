# Nested collections

You can work with nested collections specifying path/location to a parent for documents.
`parent_path` and the builder's `at` also validate the collection name they are given, so a
`FirestoreCollectionId` or a bad raw `&str` is caught in the same call rather than at the server.

```rust,no_run
# use firestore::*;
# use futures::stream::BoxStream;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyParentStructure { some_id: String }
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyChildStructure { some_id: String }
# const TEST_PARENT_COLLECTION_NAME: &str = "nested-test";
# const TEST_CHILD_COLLECTION_NAME: &str = "test-childs";
# async fn example(
#     db: FirestoreDb,
#     parent_struct: MyParentStructure,
#     child_struct: MyChildStructure,
# ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
// Creating a parent doc
db.fluent()
    .insert()
    .into(TEST_PARENT_COLLECTION_NAME)
    .document_id(&parent_struct.some_id)
    .object(&parent_struct)
    .execute::<()>()
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
    .execute::<()>()
    .await?;

// Listing children
println!("Listing all children");

let objs_stream: BoxStream<MyChildStructure> = db
    .fluent()
    .list()
    .from(TEST_CHILD_COLLECTION_NAME)
    .parent(&parent_path)
    .obj()
    .stream_all()
    .await?;
# let _ = objs_stream;
# Ok(())
# }
```

Complete example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/nested_collections.rs).

You can nest multiple levels of collections using `at()`:

```rust,no_run
# use firestore::*;
# const TEST_PARENT_COLLECTION_NAME: &str = "nested-test";
# const TEST_CHILD_COLLECTION_NAME: &str = "test-childs";
# const TEST_GRANDCHILD_COLLECTION_NAME: &str = "test-grandchilds";
# fn example(db: &FirestoreDb) -> Result<(), Box<dyn std::error::Error>> {
let parent_path = db
    .parent_path(TEST_PARENT_COLLECTION_NAME, "parent-id")?
    .at(TEST_CHILD_COLLECTION_NAME, "child-id")?
    .at(TEST_GRANDCHILD_COLLECTION_NAME, "grand-child-id")?;
# let _ = parent_path;
# Ok(())
# }
```
