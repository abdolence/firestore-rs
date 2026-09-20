# Select aggregate functions

The library supports the aggregation functions for the queries:

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyAggTestStructure { counter: usize }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let objs: Vec<MyAggTestStructure> = db.fluent()
  .select()
  .from(TEST_COLLECTION_NAME)
  .aggregate(|a| a.fields([a.field(path!(MyAggTestStructure::counter)).count()]))
  .obj()
  .query()
  .await?;
# let _ = objs;
# Ok(())
# }
```
