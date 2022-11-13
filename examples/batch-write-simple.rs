use chrono::{DateTime, Utc};
use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
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

    const TEST_COLLECTION_NAME: &'static str = "test-batch-write";

    println!("Populating a test collection");
    let batch_writer = db.create_simple_batch_writer().await?;

    let mut current_batch = batch_writer.new_batch();

    for idx in 0..500 {
        let my_struct = MyTestStructure {
            some_id: format!("test-{}", idx),
            some_string: "Test".to_string(),
            created_at: Utc::now(),
        };

        db.fluent()
            .update()
            .in_col(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .object(&my_struct)
            .add_to_batch(&mut current_batch)?;

        if idx % 100 == 0 {
            let response = current_batch.write().await?;
            current_batch = batch_writer.new_batch();
            println!("{:?}", response);
        }
    }

    Ok(())
}
