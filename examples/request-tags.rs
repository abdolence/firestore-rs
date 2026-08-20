use chrono::{DateTime, Utc};
use firestore::*;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
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

    const TEST_COLLECTION_NAME: &str = "test-request-tags";

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "Test".to_string(),
        some_num: 42,
        created_at: Utc::now(),
    };

    // Request tags are attached to every request issued through this instance.
    // This is also the way to tag the CRUD operations, which have no per operation
    // options of their own.
    let tagged_db = db.clone_with_request_tags(["nightly-report"]);

    tagged_db
        .fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .execute()
        .await?;

    tagged_db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(&my_struct.some_id)
        .object(&my_struct)
        .execute::<MyTestStructure>()
        .await?;

    // Or per operation, overriding any session wide default
    let objects: Vec<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .request_tags(["hot-path", "tenant-42"])
        // or use request_options to provide the options structure directly
        // .request_options(FirestoreRequestOptions::from_tags(["hot-path"]))
        .obj()
        .query()
        .await?;

    println!("Found {} objects", objects.len());

    // Transactions carry them through their options
    db.run_transaction_with_options(
        |db, _transaction| {
            async move {
                db.fluent()
                    .select()
                    .by_id_in(TEST_COLLECTION_NAME)
                    .obj::<MyTestStructure>()
                    .one("test-1")
                    .await?;

                Ok(())
            }
            .boxed()
        },
        FirestoreTransactionOptions::new()
            .with_request_options(FirestoreRequestOptions::from_tags(["checkout"])),
    )
    .await?;

    Ok(())
}
