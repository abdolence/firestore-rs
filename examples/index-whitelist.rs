use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with; used only for its field names via `path!()`.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Order {
    customer_id: String,
    status: String,
    notes: String,
}

// A dedicated group, so this example never plans or syncs a collection real data lives in.
const WHITELIST_INDEX_GROUP: &str = "firestore-rs-example-orders-whitelist";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    // Exempt every field in the group from automatic single-field indexing, then name the two
    // fields that should still be indexed. `notes` (never named) stays exempt.
    let plan = db
        .fluent()
        .indexes()
        .collection_group(WHITELIST_INDEX_GROUP)
        .field_overrides(|f| {
            f.fields([
                f.all_fields().exempt(),
                f.field(path!(Order::customer_id)).indexes([f.ascending()]),
                f.field(path!(Order::status))
                    .indexes([f.ascending(), f.descending()]),
            ])
        })
        .plan()
        .await?;
    println!("{plan}");

    Ok(())
}
