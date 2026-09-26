use firestore::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Order {
    some_id: String,
    some_string: String,
}

// A dedicated group, so this example only ever bulk-deletes documents it wrote itself. A bulk
// delete named on this group removes every document in every `firestore-rs-example-bulk-delete`
// collection anywhere in the database, not only the ones this example creates below.
const BULK_DELETE_EXAMPLE_GROUP: &str = "firestore-rs-example-bulk-delete";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    println!("Writing a few documents to delete...");
    for i in 0..3 {
        db.fluent()
            .insert()
            .into(BULK_DELETE_EXAMPLE_GROUP)
            .document_id(format!("doc-{i}"))
            .object(&Order {
                some_id: format!("doc-{i}"),
                some_string: format!("bulk delete example {i}"),
            })
            .execute::<Order>()
            .await?;
    }

    println!("Bulk-deleting every document in the collection group, waiting for it to finish...");
    let result = db
        .fluent()
        .delete()
        .bulk()
        .collection_groups([BULK_DELETE_EXAMPLE_GROUP])
        .wait_until_done(Duration::from_secs(300))
        .execute()
        .await?;
    println!("{result}");

    Ok(())
}
