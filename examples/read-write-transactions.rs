use firestore::{errors::FirestoreError, paths, FirestoreDb};
use futures::stream::FuturesOrdered;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    counter: i64,
}

const TEST_COLLECTION_NAME: &'static str = "test";
const TEST_DOCUMENT_ID: &str = "test_doc_id";

/// Creates a document with a counter set to 0 and then concurrently executes futures for `COUNT_ITERATIONS` iterations.
/// Finally, it reads the document again and verifies that the counter matches the expected number of iterations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    const COUNT_ITERATIONS: i64 = 50;

    println!("Creating initial document...");

    // Remove if it already exists
    db.fluent()
        .delete()
        .from(TEST_COLLECTION_NAME)
        .document_id(TEST_DOCUMENT_ID)
        .execute()
        .await?;

    // Let's insert some data
    let my_struct = MyTestStructure { counter: 0 };

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .document_id(TEST_DOCUMENT_ID)
        .object(&my_struct)
        .execute()
        .await?;

    println!("Running transactions...");

    let mut futures = FuturesOrdered::new();

    for _ in 0..COUNT_ITERATIONS {
        futures.push_back(increment_counter(&db));
    }

    let results = futures.collect::<Vec<_>>().await;
    dbg!(results);

    println!("Testing results...");

    let test_structure: MyTestStructure = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one(TEST_DOCUMENT_ID)
        .await?
        .expect("Missing document");

    assert_eq!(test_structure.counter, COUNT_ITERATIONS);

    Ok(())
}

async fn increment_counter(db: &FirestoreDb) -> Result<(), FirestoreError> {
    db.run_transaction(|db, transaction| {
        Box::pin(async move {
            let mut test_structure: MyTestStructure = db
                .fluent()
                .select()
                .by_id_in(TEST_COLLECTION_NAME)
                .obj()
                .one(TEST_DOCUMENT_ID)
                .await?
                .expect("Missing document");

            test_structure.counter += 1;

            db.fluent()
                .update()
                .fields(paths!(MyTestStructure::{
                    counter
                }))
                .in_col(TEST_COLLECTION_NAME)
                .document_id(TEST_DOCUMENT_ID)
                .object(&test_structure)
                .add_to_transaction(transaction)?;

            Ok(())
        })
    })
    .await?;

    Ok(())
}
