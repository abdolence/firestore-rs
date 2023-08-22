use firestore::*;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio::time::{sleep, Duration};
use tracing::*;

#[allow(dead_code)]
pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

#[allow(dead_code)]
pub async fn setup() -> Result<FirestoreDb, Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let filter = tracing_subscriber::EnvFilter::builder().parse("info,firestore=debug")?;

    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("GCP_PROJECT")?).await?;

    Ok(db)
}

#[allow(dead_code)]
pub async fn populate_collection<'a, T, DF>(
    db: &FirestoreDb,
    collection_name: &'a str,
    max_items: usize,
    sf: fn(usize) -> T,
    df: DF,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Serialize + Send + Sync + 'static,
    for<'de> T: Deserialize<'de>,
    DF: Fn(&T) -> String,
{
    info!("Populating {} collection", collection_name);
    let batch_writer = db.create_simple_batch_writer().await?;
    let mut current_batch = batch_writer.new_batch();

    for i in 0..max_items {
        let my_struct = sf(i);

        // Let's insert some data
        db.fluent()
            .update()
            .in_col(collection_name)
            .document_id(df(&my_struct).as_str())
            .object(&my_struct)
            .add_to_batch(&mut current_batch)?;
    }
    current_batch.write().await?;
    Ok(())
}

#[allow(dead_code)]
pub fn eventually_async<'a, F, FN>(
    max_retries: usize,
    sleep_duration: std::time::Duration,
    f: FN,
) -> BoxFuture<'a, Result<bool, Box<dyn std::error::Error + Send + Sync>>>
where
    FN: Fn() -> F + Send + Sync + 'a,
    F: Future<Output = Result<bool, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a,
{
    async move {
        let mut retries = 0;
        loop {
            if f().await? {
                return Ok(true);
            }
            retries += 1;
            if retries > max_retries {
                return Ok(false);
            }
            sleep(Duration::from(sleep_duration)).await;
        }
    }
    .boxed()
}
