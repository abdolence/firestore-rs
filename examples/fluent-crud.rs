use chrono::{DateTime, Utc};
use firestore::*;
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

    const TEST_COLLECTION_NAME: &'static str = "test";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        one_more_string: "Test2".to_string(),
        some_num: 41,
        created_at: Utc::now(),
    };

    // A fluent version of create document/object
    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .execute()
        .await?;

    println!("Created {:?}", object_returned);

    Ok(())
}
