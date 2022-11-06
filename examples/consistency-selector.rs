use firestore::*;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    const TEST_COLLECTION_NAME: &'static str = "test";

    println!("Populating a test collection");
    for i in 0..10 {
        let my_struct = MyTestStructure {
            some_id: format!("test-{}", i),
            some_string: "Test".to_string(),
        };

        // Remove if it already exist
        db.fluent()
            .delete()
            .from(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .execute()
            .await?;

        // Let's insert some data
        db.fluent()
            .insert()
            .into(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .object(&my_struct)
            .execute()
            .await?;
    }

    println!("Read only transaction to read the state before changes");

    let transaction = db
        .begin_transaction_with_options(
            FirestoreTransactionOptions::new().with_mode(FirestoreTransactionMode::ReadOnly),
        )
        .await?;

    // Working with consistency selector for reading when necessary
    let cdb = db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(
        transaction.transaction_id.clone(),
    ));

    let consistency_read_test: Option<MyTestStructure> = cdb
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one("test-0")
        .await?;

    println!("Should be the original one: {:?}", consistency_read_test);

    transaction.commit().await?;

    println!("Listing objects as a stream with updated test-0 and removed test-5");
    // Query as a stream our data
    let mut objs_stream: BoxStream<MyTestStructure> = db
        .stream_list_obj(
            FirestoreListDocParams::new(TEST_COLLECTION_NAME.into()).with_order_by(vec![
                FirestoreQueryOrder::new(
                    path!(MyTestStructure::some_id),
                    FirestoreQueryDirection::Descending,
                ),
            ]),
        )
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
