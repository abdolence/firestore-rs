use crate::{FirestoreDbOptions, FirestoreResult};
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use gcloud_sdk::google_cloud_auth::errors::CredentialsError;
use gcloud_sdk::{
    GoogleApi, GoogleAuthHeaders, GoogleAuthMiddleware, GoogleAuthMiddlewareLayer, HeaderMap,
    GCP_DEFAULT_SCOPES,
};
use hyper::header::{HeaderValue, AUTHORIZATION};
use hyper::http::Extensions;

const GOOGLE_FIREBASE_API_URL: &str = "https://firestore.googleapis.com";
const GOOGLE_FIRESTORE_EMULATOR_HOST_ENV: &str = "FIRESTORE_EMULATOR_HOST";

/// The token the Firebase tools conventionally send to the local emulators, which do not
/// authenticate the requests.
const GOOGLE_FIRESTORE_EMULATOR_AUTHORIZATION: &str = "Bearer owner";

/// The database a client addresses and the URL it sends its requests to.
pub(super) struct FirestoreDbEndpoint {
    pub(super) database_path: String,
    pub(super) api_url: String,
    /// The address `FIRESTORE_EMULATOR_HOST` names, when it is set.
    pub(super) emulator_host: Option<String>,
}

impl FirestoreDbEndpoint {
    /// The endpoint of `options`, with `FIRESTORE_EMULATOR_HOST` read from the environment.
    pub(super) fn from_env(options: &FirestoreDbOptions) -> Self {
        Self::new(
            options,
            std::env::var(GOOGLE_FIRESTORE_EMULATOR_HOST_ENV).ok(),
        )
    }

    /// The endpoint of `options`: their `firebase_api_url`, else the emulator at
    /// `emulator_host`, else the production service.
    pub(super) fn new(options: &FirestoreDbOptions, emulator_host: Option<String>) -> Self {
        let api_url = options
            .firebase_api_url
            .clone()
            .or_else(|| emulator_host.clone().map(ensure_url_scheme))
            .unwrap_or_else(|| GOOGLE_FIREBASE_API_URL.to_string());
        Self {
            database_path: format!(
                "projects/{}/databases/{}",
                options.google_project_id, options.database_id
            ),
            api_url,
            emulator_host,
        }
    }

    /// Whether requests reach the `FIRESTORE_EMULATOR_HOST` emulator: only when that host is
    /// the URL actually used. An explicit `firebase_api_url` pointing elsewhere, production
    /// included, is not the emulator just because the variable is set.
    #[cfg(feature = "admin")]
    pub(super) fn is_emulator(&self) -> bool {
        self.emulator_host
            .as_ref()
            .is_some_and(|host| ensure_url_scheme(host.clone()) == self.api_url)
    }

    /// A client of this endpoint that authenticates every request with `auth`.
    pub(super) async fn connect(
        &self,
        auth: GoogleAuthHeaders,
    ) -> FirestoreResult<GoogleApi<FirestoreClient<GoogleAuthMiddleware>>> {
        let middleware = GoogleAuthMiddlewareLayer::new(auth, Some(self.database_path.clone()))?;
        Ok(GoogleApi::from_function_with_middleware(
            FirestoreClient::new,
            &self.api_url,
            middleware,
        )
        .await?)
    }

    /// A client of this endpoint that authenticates with the Application Default Credentials,
    /// for the `cloud-platform` scope.
    ///
    /// Built by gcloud-sdk's `from_function_with_scopes` rather than by [`connect`](Self::connect)
    /// with `GoogleAuthHeaders::from_adc_with_scopes`: gcloud-sdk opens the channel before it
    /// builds the credentials, so that with a single rustls provider compiled in, the TLS setup
    /// installs the provider a service account key signs with. With no provider at all, the
    /// credentials fail with an error instead of panicking on the first request.
    pub(super) async fn connect_with_adc(
        &self,
    ) -> FirestoreResult<GoogleApi<FirestoreClient<GoogleAuthMiddleware>>> {
        Ok(GoogleApi::from_function_with_scopes(
            FirestoreClient::new,
            &self.api_url,
            Some(self.database_path.clone()),
            GCP_DEFAULT_SCOPES.clone(),
        )
        .await?)
    }
}

fn ensure_url_scheme(url: String) -> String {
    if !url.contains("://") {
        format!("http://{url}")
    } else {
        url
    }
}

/// Credentials for the Firestore emulator: they serve its conventional stub token, so that a
/// client of the emulator looks up no Google credentials, which a development machine often
/// does not have.
#[derive(Debug)]
pub(super) struct FirestoreEmulatorCredentials {
    /// The tag of the one header these credentials ever serve, so that
    /// [`GoogleAuthHeaders`] keeps it cached.
    entity_tag: EntityTag,
}

impl FirestoreEmulatorCredentials {
    pub(super) fn new() -> Self {
        Self {
            entity_tag: EntityTag::new(),
        }
    }
}

impl CredentialsProvider for FirestoreEmulatorCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        if extensions.get::<EntityTag>() == Some(&self.entity_tag) {
            return Ok(CacheableResource::NotModified);
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static(GOOGLE_FIRESTORE_EMULATOR_AUTHORIZATION),
        );
        Ok(CacheableResource::New {
            entity_tag: self.entity_tag.clone(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

impl From<FirestoreEmulatorCredentials> for GoogleAuthHeaders {
    fn from(credentials: FirestoreEmulatorCredentials) -> Self {
        Self::from(Credentials::from(credentials))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
    use crate::FirestoreDb;
    use gcloud_sdk::tonic::Code;

    #[tokio::test]
    async fn the_emulator_is_sent_the_owner_bearer_token() {
        let server = FakeFirestore::start(|_, _| {
            (
                "GetDocument".to_string(),
                FakeResponse::Status(Code::NotFound),
            )
        })
        .await;
        let options = FirestoreDbOptions::new("emulated-project".to_string());
        let endpoint = FirestoreDbEndpoint::new(&options, Some(server.address().to_string()));
        let db = FirestoreDb::with_default_auth(options, endpoint)
            .await
            .unwrap();

        db.ping().await.unwrap();

        assert_eq!(
            server.authorizations(),
            [Some(HeaderValue::from_static("Bearer owner"))]
        );
    }

    #[test]
    fn test_ensure_url_scheme() {
        assert_eq!(
            ensure_url_scheme("localhost:8080".into()),
            "http://localhost:8080"
        );
        assert_eq!(
            ensure_url_scheme("any://localhost:8080".into()),
            "any://localhost:8080"
        );
        assert_eq!(
            ensure_url_scheme("invalid:localhost:8080".into()),
            "http://invalid:localhost:8080"
        );
    }

    #[cfg(feature = "admin")]
    fn endpoint(
        firebase_api_url: Option<&str>,
        emulator_host: Option<&str>,
    ) -> FirestoreDbEndpoint {
        let mut options = FirestoreDbOptions::new("emulated-project".to_string());
        options.firebase_api_url = firebase_api_url.map(str::to_string);
        FirestoreDbEndpoint::new(&options, emulator_host.map(str::to_string))
    }

    #[cfg(feature = "admin")]
    #[test]
    fn the_emulator_host_is_the_emulator_only_when_it_is_the_url_used() {
        assert!(endpoint(None, Some("localhost:8080")).is_emulator());
        assert!(endpoint(None, Some("http://localhost:8080")).is_emulator());
        assert!(endpoint(Some("http://localhost:8080"), Some("localhost:8080")).is_emulator());
        assert!(!endpoint(None, None).is_emulator());
        assert!(
            !endpoint(Some(GOOGLE_FIREBASE_API_URL), Some("localhost:8080")).is_emulator(),
            "an explicit production URL is not the emulator"
        );
    }
}
