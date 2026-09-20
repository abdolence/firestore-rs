# Update/delete preconditions

The library supports the preconditions:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_num: u64 }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# let object_updated: MyTestStructure = db.fluent()
#   .update()
#   .fields(paths!(MyTestStructure::{some_num}))
#   .in_col(TEST_COLLECTION_NAME)
  .precondition(FirestoreWritePrecondition::Exists(true))
#   .document_id("test-1")
#   .object(&MyTestStructure { some_num: 1 })
#   .execute()
#   .await?;
# let _ = object_updated;
# Ok(())
# }
```
