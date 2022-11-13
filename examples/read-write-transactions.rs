use std::pin::Pin;

use backoff::{future::retry, ExponentialBackoff};
use firestore::{
    errors::{FirestoreDatabaseError, FirestoreError},
    *,
};
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
    run_transaction(&db, |db, transaction| {
        Box::pin(async move {
            let mut test_structure: MyTestStructure = db
                .fluent()
                .select()
                .by_id_in(TEST_COLLECTION_NAME)
                .obj()
                // .one_with_transaction(TEST_DOCUMENT_ID, &mut transaction)
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

async fn run_transaction<T, F>(db: &FirestoreDb, func: F) -> Result<T, FirestoreError>
where
    F: for<'a> Fn(
        FirestoreDb,
        &'a mut FirestoreTransaction,
    ) -> Pin<Box<dyn futures::Future<Output = Result<T, FirestoreError>> + 'a>>,
{
    // Perform our initial attempt. If this fails and the backend tells us we can retry,
    // we'll try again with exponential backoff using the first attempt's transaction ID.
    let transaction_id = {
        let mut transaction = db.begin_transaction().await?;

        let cdb = db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(
            transaction.transaction_id.clone(),
        ));

        let ret_val = func(cdb, &mut transaction).await?;

        let transaction_id = transaction.transaction_id.clone();

        match transaction.commit().await {
            Ok(_) => return Ok(ret_val),
            Err(e) => match e {
                FirestoreError::DatabaseError(FirestoreDatabaseError {
                    retry_possible: true,
                    ..
                }) => {
                    // Ignore; we'll try again
                }
                FirestoreError::DatabaseError(FirestoreDatabaseError {
                    retry_possible: false,
                    ..
                }) => {
                    return Err(e);
                }
                e => return Err(e),
            },
        }

        transaction_id
    };

    // We failed the first time. Now we must change the transaction mode to signal that we're retrying with the original transaction ID.
    let result = retry(ExponentialBackoff::default(), || async {
        let options = FirestoreTransactionOptions {
            mode: FirestoreTransactionMode::ReadWriteRetry(transaction_id.clone()),
        };
        let mut transaction = db.begin_transaction_with_options(options).await?;

        let cdb = db.clone_with_consistency_selector(FirestoreConsistencySelector::Transaction(
            transaction.transaction_id.clone(),
        ));

        let ret_val = func(cdb, &mut transaction).await?;

        match transaction.commit().await {
            Ok(_) => return Ok(ret_val),
            Err(e) => match e {
                FirestoreError::DatabaseError(FirestoreDatabaseError {
                    retry_possible: true,
                    ..
                }) => {
                    eprintln!("Got back retryable error: {e}");
                    return Err(backoff::Error::transient(e));
                }
                FirestoreError::DatabaseError(FirestoreDatabaseError {
                    retry_possible: false,
                    ..
                }) => {
                    return Err(backoff::Error::permanent(e));
                }
                e => return Err(backoff::Error::permanent(e)),
            },
        }
    })
    .await;

    // TODO If the final result was Err, do we still need to call `rollback`?

    result
}
