# Explaining the query

The library supports the query explanation:

```rust,no_run
# use firestore::*;
# const TEST_COLLECTION_NAME: &str = "test";
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
db.fluent()
    .select()
    .from(TEST_COLLECTION_NAME)
    .explain()
    // or use explain_with_options if you want to provide additional options like analyze which run query to gather additional statistics
    // .explain_with_options(FirestoreExplainOptions::new().with_analyze(true))
    .stream_query_with_metadata()
    .await?;
# Ok(())
# }
```
