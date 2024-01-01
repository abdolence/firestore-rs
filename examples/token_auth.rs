use chrono::{DateTime, Utc};
use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::ops::Add;

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

async fn my_token() -> gcloud_sdk::error::Result<gcloud_sdk::Token> {
    Ok(gcloud_sdk::Token::new(
        "Bearer".to_string(),
        config_env_var("TOKEN_VALUE")
            .expect("TOKEN_VALUE must be specified")
            .into(),
        chrono::Utc::now().add(std::time::Duration::from_secs(3600)),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::with_options_token_source(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?),
        gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
        gcloud_sdk::TokenSourceType::ExternalSource(Box::new(
            gcloud_sdk::ExternalJwtFunctionSource::new(my_token),
        )),
    )
    .await?;

    const TEST_COLLECTION_NAME: &'static str = "test-query";

    // Query as a stream our data
    let object_stream: BoxStream<FirestoreResult<MyTestStructure>> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .obj()
        .stream_query_with_errors()
        .await?;

    let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;
    println!("{:?}", as_vec);

    Ok(())
}
