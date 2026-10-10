//! A service account key file without a rustls crypto provider.
//!
//! The provider is process-global, so this file is its own test binary, and nothing in it
//! installs one. Run it with `--no-default-features --features tls-webpki-roots`.
#![cfg(not(feature = "auth-default-crypto"))]

use firestore::errors::FirestoreError;
use firestore::{FirestoreDb, FirestoreDbOptions};

/// A service account key. The check fails before the private key is parsed, so it does not
/// need to be a valid key.
fn service_account_key() -> serde_json::Value {
    serde_json::json!({
        "type": "service_account",
        "project_id": "orders",
        "private_key_id": "orders-key",
        "private_key": "-----BEGIN PRIVATE KEY-----\nunused\n-----END PRIVATE KEY-----\n",
        "client_email": "invoker@orders.iam.gserviceaccount.com",
    })
}

#[tokio::test]
async fn service_account_key_file_without_a_crypto_provider_is_a_typed_error() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let key_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key_file.path(), service_account_key().to_string()).unwrap();
    // A plain HTTP endpoint the channel can connect to, so that only the credentials can fail.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let options = FirestoreDbOptions::new("orders".to_string())
        .with_firebase_api_url(format!("http://{}", listener.local_addr().unwrap()));

    let db =
        FirestoreDb::with_options_service_account_key_file(options, key_file.path().to_path_buf())
            .await;

    match db {
        Err(FirestoreError::SystemError(error)) => {
            assert_eq!(error.public.code, "CryptoProviderMissing")
        }
        other => panic!("expected the crypto provider error, got {other:?}"),
    }
}
