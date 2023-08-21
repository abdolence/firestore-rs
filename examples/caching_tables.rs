use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

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

    const TEST_COLLECTION_NAME: &'static str = "test-caching";

    const TEST_CACHE: &'static str = "example-cache";

    db.register_cache(
        TEST_CACHE,
        FirestoreCacheConfiguration::new()
            .collection(TEST_COLLECTION_NAME)
            .collection("test-caching2"),
        FirestoreMemOnlyOnDemandCacheBackend::new(),
    )
    .await?;

    db.load_caches().await?;

    if db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .one("test-0")
        .await?
        .is_none()
    {
        println!("Populating a test collection");
        let batch_writer = db.create_simple_batch_writer().await?;
        let mut current_batch = batch_writer.new_batch();

        for i in 0..500 {
            let my_struct = MyTestStructure {
                some_id: format!("test-{}", i),
                some_string: "Test".to_string(),
                one_more_string: "Test2".to_string(),
                some_num: i,
                created_at: Utc::now(),
            };

            // Let's insert some data
            db.fluent()
                .update()
                .in_col(TEST_COLLECTION_NAME)
                .document_id(&my_struct.some_id)
                .object(&my_struct)
                .add_to_batch(&mut current_batch)?;
        }
        current_batch.write().await?;
    }

    println!("Getting by id");
    let my_struct: Option<MyTestStructure> = db
        .read_through_cache(TEST_CACHE)
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one("test-1")
        .await?;

    println!("{:?}", my_struct);

    println!("Querying a test collection as a stream using Fluent API");

    // Query as a stream our data
    let object_stream: BoxStream<FirestoreResult<MyTestStructure>> = db
        .fluent()
        .select()
        .fields(
            paths!(MyTestStructure::{some_id, some_num, some_string, one_more_string, created_at}),
        )
        .from(TEST_COLLECTION_NAME)
        .filter(|q| q.for_all([q.field(path!(MyTestStructure::some_id)).eq("test-1")]))
        .obj()
        .stream_query_with_errors()
        .await?;

    let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;
    println!("{:?}", as_vec);

    db.shutdown().await?;

    Ok(())
}
