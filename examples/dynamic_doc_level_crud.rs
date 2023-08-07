use chrono::prelude::*;
use firestore::*;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
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

    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id("test-1")
        .execute()
        .await?;

    let object_returned = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id("test-1")
        .document(FirestoreDb::serialize_map_to_doc(
            "",
            [
                ("some_id", "some-id-value".into()),
                ("some_string", "some-string-value".into()),
                ("one_more_string", "another-string-value".into()),
                ("some_num", 41.into()),
                (
                    "embedded_obj",
                    FirestoreValue::from_map([
                        ("inner_some_id", "inner-value".into()),
                        ("inner_some_string", "inner-value".into()),
                    ]),
                ),
                ("created_at", FirestoreTimestamp(Utc::now()).into()),
            ],
        )?)
        .execute()
        .await?;

    println!("Created {:?}", object_returned);

    let object_updated = db
        .fluent()
        .update()
        .fields(["some_num", "one_more_string"])
        .in_col(TEST_COLLECTION_NAME)
        .document(FirestoreDb::serialize_map_to_doc(
            db.parent_path(TEST_COLLECTION_NAME, "test-1")?,
            [
                ("one_more_string", "update-string".into()),
                ("some_num", 42.into()),
            ],
        )?)
        .execute()
        .await?;

    println!("Updated {:?}", object_updated);

    Ok(())
}
