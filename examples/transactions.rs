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

    println!("Transaction update/delete on collection");

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

    println!("Listing objects as a stream with updated test-0 and removed test-5");
    // Query as a stream our data
    let mut objs_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .order_by([(
            path!(MyTestStructure::some_id),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_query()
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
