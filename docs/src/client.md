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
let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;
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
    FirestoreDbOptions::new(config_env_var("PROJECT_ID")?.to_string()),
    "/tmp/key.json".into(),
)
.await?
# ;
# Ok(())
# }
```

The key file is read as a service account key.

For any other credentials use `FirestoreDb::with_options_auth`, which takes everything that converts into gcloud-sdk's
`GoogleAuthHeaders`:
- `Credentials` from the builders of `gcloud_sdk::google_cloud_auth::credentials`: service account keys, user credentials,
  impersonation, workload identity federation, the metadata server, etc.;
- a `CredentialsProvider` of your own, passed to `Credentials::from`;
- `GoogleAuthHeaders::from_adc_with_scopes` for the Application Default Credentials with other scopes.

```rust,no_run
# use firestore::*;
# fn config_env_var(name: &str) -> Result<String, String> {
#     std::env::var(name).map_err(|e| format!("{name}: {e}"))
# }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
FirestoreDb::with_options_auth(
    FirestoreDbOptions::new(config_env_var("PROJECT_ID")?.to_string()),
    gcloud_sdk::GoogleAuthHeaders::from_adc_with_scopes(vec![
        "https://www.googleapis.com/auth/datastore".to_string(),
    ])
    .await?,
)
.await?
# ;
# Ok(())
# }
```

These types come from gcloud-sdk, so your project needs it as a dependency at the same version as the library:

```toml
[dependencies]
gcloud-sdk = { version = "0.33", default-features = false }
```

Full example with a custom `CredentialsProvider` available [here](https://github.com/abdolence/firestore-rs/tree/master/examples/token_auth.rs).

Firebase supports [multiple databases per project now](https://cloud.google.com/firestore/docs/manage-databases),
so you can specify the database ID in the options:

```rust,no_run
# use firestore::*;
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
FirestoreDb::with_options(
    FirestoreDbOptions::new("your-project-id".to_string())
        .with_database_id("your-database-id".to_string()),
)
.await?
# ;
# Ok(())
# }
```
