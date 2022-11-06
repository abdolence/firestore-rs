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
    // Using a special attribute to indicate timestamp serialization for Firestore
    // (for serde_json it will be still the same, usually String serialization, so you can reuse the models)
    #[serde(with = "firestore::serialize_as_timestamp")]
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

    const TEST_COLLECTION_NAME: &'static str = "test-ts1";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        created_at: Utc::now(),
    };

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    // A fluent version of create document/object
    let object_returned: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .execute()
        .await?;

    println!("Created: {:?}", object_returned);

    // Query our data
    let objects: Vec<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_all([q
                .field(path!(MyTestStructure::created_at))
                .less_than_or_equal(
                    firestore::FirestoreTimestamp(Utc::now()), // Using the wrapping type to indicate serialization without attribute
                )])
        })
        .obj()
        .query()
        .await?;

    println!("Now in the list: {:?}", objects);

    Ok(())
}
