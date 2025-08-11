use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
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

    const TEST_COLLECTION_NAME: &str = "test-partitions";

    //println!("Populating a test collection");
    // for i in 0..40000 {
    //     let my_struct = MyTestStructure {
    //         some_id: format!("test-{}", i),
    //         some_string: "Test".to_string(),
    //         one_more_string: "Test2".to_string(),
    //         some_num: i,
    //         created_at: Utc::now(),
    //     };
    //
    //     if db
    //         .fluent()
    //         .select()
    //         .by_id_in(TEST_COLLECTION_NAME)
    //         .one(&my_struct.some_id)
    //         .await?
    //         .is_none()
    //     {
    //         // Let's insert some data
    //         db.fluent()
    //             .insert()
    //             .into(TEST_COLLECTION_NAME)
    //             .document_id(&my_struct.some_id)
    //             .object(&my_struct)
    //             .execute()
    //             .await?;
    //     }
    // }

    let partition_stream: BoxStream<FirestoreResult<(FirestorePartition, MyTestStructure)>> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .obj()
        .partition_query()
        .parallelism(2)
        .page_size(10)
        .stream_partitions_with_errors()
        .await?;

    let as_vec: Vec<(FirestorePartition, MyTestStructure)> = partition_stream.try_collect().await?;
    println!("{}", as_vec.len());

    Ok(())
}
