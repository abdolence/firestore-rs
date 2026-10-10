use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use gcloud_sdk::google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use gcloud_sdk::google_cloud_auth::errors::CredentialsError;
use gcloud_sdk::HeaderMap;
use hyper::header::{HeaderValue, InvalidHeaderValue, AUTHORIZATION};
use hyper::http::Extensions;
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
    created_at: FirestoreTimestamp,
}

/// Serves a token obtained outside of this application, here from the `TOKEN_VALUE` environment
/// variable, as the `authorization` header of every request.
#[derive(Debug)]
struct ExternalTokenCredentials {
    authorization: HeaderValue,
    entity_tag: EntityTag,
}

impl ExternalTokenCredentials {
    fn new(token: &str) -> Result<Self, InvalidHeaderValue> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))?;
        authorization.set_sensitive(true);
        Ok(Self {
            authorization,
            entity_tag: EntityTag::new(),
        })
    }
}

impl CredentialsProvider for ExternalTokenCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        if extensions.get::<EntityTag>() == Some(&self.entity_tag) {
            return Ok(CacheableResource::NotModified);
        }
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, self.authorization.clone());
        Ok(CacheableResource::New {
            entity_tag: self.entity_tag.clone(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let credentials = ExternalTokenCredentials::new(&config_env_var("TOKEN_VALUE")?)?;
    let db = FirestoreDb::with_options_auth(
        FirestoreDbOptions::new(config_env_var("PROJECT_ID")?),
        Credentials::from(credentials),
    )
    .await?;

    const TEST_COLLECTION_NAME: &str = "test-query";

    // Query as a stream our data
    let object_stream: BoxStream<FirestoreResult<MyTestStructure>> = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .obj()
        .stream_query_with_errors()
        .await?;

    let as_vec: Vec<MyTestStructure> = object_stream.try_collect().await?;
    println!("{as_vec:?}");

    Ok(())
}
