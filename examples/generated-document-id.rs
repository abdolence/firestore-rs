use chrono::{DateTime, Utc};
use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(alias = "_firestore_id")]
    id: Option<String>,
    #[serde(alias = "_firestore_created")]
    created_at: Option<DateTime<Utc>>,
    #[serde(alias = "_firestore_updated")]
    updated_at: Option<DateTime<Utc>>,
    test: Option<String>,
    some_string: String,
    one_more_string: String,
    some_num: u64,
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
        id: None,
        created_at: None,
        updated_at: None,
        test: Some("tsst".to_string()),
        some_string: "Test".to_string(),
        one_more_string: "Test2".to_string(),
        some_num: 41,
    };

    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .generate_document_id()
        .object(&my_struct)
        .execute()
        .await?;

    println!("Created {object_returned:?}");

    let generated_id = object_returned.id.unwrap();

    let object_read: Option<MyTestStructure> = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(generated_id)
        .await?;

    println!("Read {object_read:?}");

    Ok(())
}
