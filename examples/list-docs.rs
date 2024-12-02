use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?)
        .await?
        .clone();

    const TEST_COLLECTION_NAME: &'static str = "test";

    println!("Populating a test collection");
    for i in 0..10 {
        let my_struct = MyTestStructure {
            some_id: format!("test-{}", i),
            some_string: "Test".to_string(),
            one_more_string: "Test2".to_string(),
            some_num: 42,
        };

        // Remove if it already exist
        db.fluent()
            .delete()
            .from(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .execute()
            .await?;

        // Let's insert some data
        db.fluent()
            .insert()
            .into(TEST_COLLECTION_NAME)
            .document_id(&my_struct.some_id)
            .object(&my_struct)
            .execute::<()>()
            .await?;
    }

    println!("Listing objects as a stream");
    // Query as a stream our data
    let objs_stream: BoxStream<MyTestStructure> = db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME)
        .page_size(3) // This is decreased just to show an example of automatic pagination, in the real usage please use bigger figure or don't specify it (default is 100)
        .order_by([(
            path!(MyTestStructure::some_id),
            FirestoreQueryDirection::Descending,
        )])
        .obj()
        .stream_all()
        .await?;

    let as_vec: Vec<MyTestStructure> = objs_stream.collect().await;
    println!("{:?}", as_vec);

    Ok(())
}
