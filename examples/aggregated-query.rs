use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

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
    // Query as a stream our data
    let mut object_stream: BoxStream<MyAggTestStructure> = db
        .stream_aggregated_query_obj(FirestoreAggregatedQueryParams::new(
            FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()),
            vec![
                FirestoreAggregation::new(path!(MyAggTestStructure::counter)).with_operator(
                    FirestoreAggregationOperator::Count(FirestoreAggregationOperatorCount::new()),
                ),
            ],
        ))
        .await?;

    while let Some(object) = object_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
