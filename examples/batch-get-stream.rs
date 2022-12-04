use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::BoxStream;
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
    one_more_string: String,
    some_num: u64,
    created_at: DateTime<Utc>,
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
            one_more_string: "Test2".to_string(),
            some_num: 42,
            created_at: Utc::now(),
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

    println!("Getting objects by IDs as a stream");
    // Query as a stream our data
    let mut object_stream: BoxStream<(String, Option<MyTestStructure>)> = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .batch(vec!["test-0", "test-5"])
        .await?;

    while let Some(object) = object_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    // Getting as a stream with errors when needed
    let mut object_stream_with_errors: BoxStream<
        FirestoreResult<(String, Option<MyTestStructure>)>,
    > = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .batch_with_errors(vec!["test-0", "test-5"])
        .await?;

    while let Some(object) = object_stream_with_errors.try_next().await? {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
