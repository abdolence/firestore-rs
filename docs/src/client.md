# Firestore database client instance and lifecycle

To create a new instance of Firestore client you need to provide at least a GCP project ID.
It is not recommended creating a new client for each request, so it is recommended to create a client once and reuse it
whenever possible.
Cloning instances is much cheaper than creating a new one.

The client is created using the `Firestore::new` method:

```rust,no_run
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use firestore::*;

// Create an instance
let db = FirestoreDb::new( & config_env_var("PROJECT_ID") ? ).await?;
# Ok(())
# }
```

This is the recommended way to create a new instance of the client, since it
automatically detects the environment and uses credentials, service accounts, Workload Identity on GCP, etc.
Look at the section below [Google authentication](./auth.md) for more details.

In cases if you need to create a new instance explicitly specifying a key file, you can use:

```rust,no_run
# use firestore::*;
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
FirestoreDb::with_options_service_account_key_file(
  FirestoreDbOptions::new(config_env_var("PROJECT_ID") ?.to_string()),
  "/tmp/key.json".into()
).await?
# ;
# Ok(())
# }
```

or if you need even more flexibility you can use a preconfigured token source and scopes with:

```rust,no_run
# use firestore::*;
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
FirestoreDb::with_options_token_source(
  FirestoreDbOptions::new(config_env_var("PROJECT_ID") ?.to_string()),
  gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
  gcloud_sdk::TokenSourceType::File("/tmp/key.json".into())
).await?
# ;
# Ok(())
# }
```

Firebase supports [multiple databases per project now](https://cloud.google.com/firestore/docs/manage-databases),
so you can specify the database ID in the options:

```rust,no_run
# use firestore::*;
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
FirestoreDb::with_options(
  FirestoreDbOptions::new("your-project-id".to_string())
    .with_database_id("your-database-id".to_string())
  )
.await?
# ;
# Ok(())
# }
```
