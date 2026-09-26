use firestore::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with; used only for its field names via `path!()`.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Order {
    customer_id: String,
    placed_at: FirestoreTimestamp,
    tags: Vec<String>,
    embedding: Vec<f64>,
    bio: String,
    expires_at: FirestoreTimestamp,
}

// A dedicated group, so this example never plans or syncs a collection real data lives in.
const ORDERS_INDEX_GROUP: &str = "firestore-rs-example-orders";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    let declared = db
        .fluent()
        .indexes()
        .collection_group(ORDERS_INDEX_GROUP)
        .composite(|i| {
            i.indexes([
                i.index([
                    i.field(path!(Order::customer_id)).asc(),
                    i.field(path!(Order::placed_at)).desc(),
                ]),
                i.index([
                    i.field(path!(Order::tags)).array_contains(),
                    i.field(path!(Order::placed_at)).desc(),
                ])
                .all_descendants(),
                i.index([
                    i.field(path!(Order::customer_id)).asc(),
                    i.field(path!(Order::embedding)).vector(768),
                ]),
            ])
        })
        .field_overrides(|f| f.fields([f.field(path!(Order::bio)).exempt()]))
        .ttl([path!(Order::expires_at)]);

    println!("Planning...");
    let plan = declared.clone().plan().await?;
    println!("{plan}");

    println!("Syncing...");
    let report = declared
        .clone()
        .wait_until_ready(Duration::from_secs(900))
        .sync()
        .await?;
    println!("{report}");

    // Syncing the same declaration again should report no changes: the listed state now
    // matches what was declared above.
    println!("Syncing again...");
    let unchanged_report = declared.sync().await?;
    println!("{unchanged_report}");

    Ok(())
}
