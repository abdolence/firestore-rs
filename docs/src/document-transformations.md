# Document transformations

The library supports server side document transformations in [transactions](./transactions.md) and
[batch writes](./batch-writes.md):

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_num: i32, some_array: Vec<i32> }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb, my_obj: MyTestStructure) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# let mut transaction = db.begin_transaction().await?;
// Only transformation
db.fluent()
    .update()
    .in_col(TEST_COLLECTION_NAME)
    .document_id("test-4")
    .transforms(|t| {
        // Transformations
        t.fields([
            t.field(path!(MyTestStructure::some_num)).increment(10),
            t.field(path!(MyTestStructure::some_array))
                .append_missing_elements([4, 5]),
            t.field(path!(MyTestStructure::some_array))
                .remove_all_from_array([3]),
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
    .transforms(|t| {
        // Transformations after the update
        t.fields([t.field(path!(MyTestStructure::some_num)).increment(10)])
    })
    .add_to_transaction(&mut transaction)?; // or add_to_batch
# Ok(())
# }
```
