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

    // Remove if it already exist
    db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id)
        .await?;

    // Let's insert some data
    db.create_obj(TEST_COLLECTION_NAME, &my_struct.some_id, &my_struct)
        .await?;

    // Get object by id
    let find_it_again: MyTestStructure =
        db.get_obj(TEST_COLLECTION_NAME, &my_struct.some_id).await?;

    println!("Should be the same: {:?}", find_it_again);

    // Query our data
    let objects: Vec<MyTestStructure> = db
        .query_obj(
            FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_filter(
                FirestoreQueryFilter::Compare(Some(FirestoreQueryFilterCompare::LessThanOrEqual(
                    path!(MyTestStructure::created_at),
                    firestore::FirestoreTimestamp(Utc::now()).into(), // Using the wrapping type to indicate serialization without attribute
                ))),
            ),
        )
        .await?;

    println!("Now in the list: {:?}", objects);

    Ok(())
}
