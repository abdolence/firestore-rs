use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyAggTestStructure {
    counter: usize,
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

    println!("Aggregated query a test collection as a stream");

    let objs: Vec<MyAggTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .aggregate(|a| a.fields([a.field(path!(MyAggTestStructure::counter)).count()]))
        .obj()
        .query()
        .await?;

    println!("{:?}", objs);

    Ok(())
}
