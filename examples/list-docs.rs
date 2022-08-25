use firestore::*;
use futures_util::stream::BoxStream;
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
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

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
        db.delete_by_id(TEST_COLLECTION_NAME, &my_struct.some_id)
            .await?;

        // Let's insert some data
        db.create_obj(TEST_COLLECTION_NAME, &my_struct.some_id, &my_struct)
            .await?;
    }

    println!("Listing objects as a stream");
    // Query as a stream our data
    let mut objs_stream: BoxStream<MyTestStructure> = db
        .stream_list_obj(
            FirestoreListDocParams::new(TEST_COLLECTION_NAME.into())
                .with_page_size(3) // This is decreased just to show an example, in real life please use bigger figure (default is 100)
                .with_order_by(vec![FirestoreQueryOrder::new(
                    path!(MyTestStructure::some_id),
                    FirestoreQueryDirection::Descending,
                )]),
        )
        .await?;

    while let Some(object) = objs_stream.next().await {
        println!("Object in stream: {:?}", object);
    }

    Ok(())
}
