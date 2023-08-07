use chrono::{DateTime, Utc};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    let fields: HashMap<String, FirestoreValue> = [
        ("some_id".to_string(), my_struct.some_id.clone().into()),
        (
            "some_string".to_string(),
            my_struct.some_string.clone().into(),
        ),
        (
            "one_more_string".to_string(),
            my_struct.one_more_string.clone().into(),
        ),
        ("some_num".to_string(), my_struct.some_num.into()),
        ("created_at".to_string(), my_struct.created_at.into()),
    ]
    .into_iter()
    .collect();

    let object_returned = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .document(FirestoreDb::serialize_map_to_doc("", fields)?)
        .execute()
        .await?;

    println!("Created {:?}", object_returned);

    let updated_fields: HashMap<String, FirestoreValue> = [
        ("one_more_string".to_string(), "update-string".into()),
        ("some_num".to_string(), 42.into()),
    ]
    .into_iter()
    .collect();

    let object_updated = db
        .fluent()
        .update()
        .fields(paths!(MyTestStructure::{some_num, one_more_string}))
        .in_col(TEST_COLLECTION_NAME)
        .document(FirestoreDb::serialize_map_to_doc(
            db.parent_path(TEST_COLLECTION_NAME, &my_struct.some_id)?,
            updated_fields,
        )?)
        .execute()
        .await?;

    println!("Updated {:?}", object_updated);

    Ok(())
}
