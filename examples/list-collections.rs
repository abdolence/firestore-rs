use firestore::*;
use futures::TryStreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
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

    println!("Listing collections as a stream");

    let collections_stream = db
        .fluent()
        .list()
        .collections()
        .stream_all_with_errors()
        .await?;

    let collections: Vec<String> = collections_stream.try_collect().await?;
    println!("Collections: {collections:?}");

    Ok(())
}
