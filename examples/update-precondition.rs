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

    const TEST_COLLECTION_NAME: &str = "test";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        one_more_string: "Test2".to_string(),
        some_num: 41,
        created_at: Utc::now(),
    };

    let object_updated: MyTestStructure = db
        .fluent()
        .update()
        .fields(paths!(MyTestStructure::{some_num, one_more_string}))
        .in_col(TEST_COLLECTION_NAME)
        .precondition(FirestoreWritePrecondition::Exists(true))
        .document_id(&my_struct.some_id)
        .object(&MyTestStructure {
            some_num: my_struct.some_num + 1,
            one_more_string: "updated-value".to_string(),
            ..my_struct.clone()
        })
        .execute()
        .await?;

    println!("Updated {:?}", object_updated);

    Ok(())
}
