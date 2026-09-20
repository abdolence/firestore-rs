# Transactions

To manage transactions manually you can use `db.begin_transaction()`, and
then the Fluent API to add the operations needed in the transaction.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { some_id: String, some_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
# Ok(())
# }
```

You may also execute transactions that automatically retry with exponential backoff using `run_transaction`.

```rust,no_run
# use firestore::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Deserialize, Serialize)]
# struct MyTestStructure { test_string: String }
# const TEST_COLLECTION_NAME: &str = "test";
# const TEST_DOCUMENT_ID: &str = "test_doc_id";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
# Ok(())
# }
```

See the complete example available [here](https://github.com/abdolence/firestore-rs/blob/master/examples/read-write-transactions.rs).

Please note that Firestore doesn't support creating documents in the transactions (generating
document IDs automatically), so you need to use `update()` to implicitly create documents and specifying your own IDs.
