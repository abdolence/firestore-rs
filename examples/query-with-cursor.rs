use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

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

    const TEST_COLLECTION_NAME: &str = "test";

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
            .execute::<()>()
            .await?;
    }

    println!("Querying a test collection in defined order");

    // Querying as a stream with errors when needed
    let object_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .start_at(FirestoreQueryCursor::BeforeValue(vec!["test-5".into()]))
        .order_by([(
            path!(MyTestStructure::some_id),
            FirestoreQueryDirection::Ascending,
        )])
        .obj()
        .stream_query()
        .await?;

    let as_vec: Vec<MyTestStructure> = object_stream.collect().await;
    println!("{:?}", as_vec);

    Ok(())
}
