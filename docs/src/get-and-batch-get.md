# Get and batch get support

```rust,no_run
# use firestore::*;
# use futures::stream::BoxStream;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure {
#     some_id: String,
# }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb, my_struct: MyTestStructure) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

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
# Ok(())
# }
```
