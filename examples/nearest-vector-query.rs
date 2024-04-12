use firestore::*;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    some_vec: FirestoreVector,
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

    const TEST_COLLECTION_NAME: &'static str = "test-query-vec";

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
                some_vec: FirestoreVector::new(vec![i as f64, (i * 1000) as f64]),
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

    println!("Search for a test collection with a vector closest to [100.0, 100000.0]");

    let as_vec: Vec<MyTestStructure> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .obj()
        .query()
        .await?;

    println!("Found: {:?}", as_vec);

    Ok(())
}
