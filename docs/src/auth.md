# Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

## Local development

Don't confuse `gcloud auth login` with `gcloud auth application-default login` for local development,
since the first authorize only `gcloud` tool to access the Cloud Platform.

The latter obtains user access credentials via a web flow and puts them in the well-known location for Application
Default Credentials (ADC).
This command is useful when you are developing code that would normally use a service account but need to run the code
in a local development environment where it's easier to provide user credentials.
So to work for local development you need to use `gcloud auth application-default login`.

## Crypto provider

Service account keys sign their tokens with a rustls crypto provider.
The `auth-default-crypto` feature, on by default, brings the aws-lc-rs provider of `google-cloud-auth`.
Be aware that aws-lc-rs is built from C sources.

With `default-features = false`, either enable the feature next to your TLS feature:

```toml
[dependencies]
firestore = { version = "0.58", default-features = false, features = ["tls-webpki-roots", "auth-default-crypto"] }
```

or install a rustls `CryptoProvider` before creating a client.
Without a provider `FirestoreDb::new`, `FirestoreDb::with_options` and `FirestoreDb::with_options_service_account_key_file`
return an error with the code `CryptoProviderMissing` for a service account key.
Credentials you build yourself with the `google-cloud-auth` builders panic on their first token request instead,
so build them with `GoogleAuthHeaders::from_service_account_key` to get the same error.
