use crate::*;
use std::sync::Arc;

mod options;
pub use options::*;

mod configuration;
pub use configuration::*;

mod backends;
pub use backends::*;

use async_trait::async_trait;

pub struct FirestoreCache {
    inner: Arc<FirestoreCacheInner>,
}

struct FirestoreCacheInner {
    pub options: FirestoreCacheOptions,
    pub config: FirestoreCacheConfiguration,
    pub backend: Box<dyn FirestoreCacheBackend + Send + Sync + 'static>,
}

impl FirestoreCache {
    pub fn new<B>(name: FirestoreCacheName, config: FirestoreCacheConfiguration, backend: B) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        let temp_dir = std::env::temp_dir();
        let firestore_cache_dir = temp_dir.join("firestore_cache").join(name.value().clone());

        let options = FirestoreCacheOptions::new(name, firestore_cache_dir);
        Self::with_options(options, config, backend)
    }

    pub fn with_options<B>(
        options: FirestoreCacheOptions,
        config: FirestoreCacheConfiguration,
        backend: B,
    ) -> Self
    where
        B: FirestoreCacheBackend + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(FirestoreCacheInner {
                options,
                config,
                backend: Box::new(backend),
            }),
        }
    }

    pub async fn load(&self) -> Result<(), FirestoreError> {
        self.inner
            .backend
            .load(&self.inner.options, &self.inner.config)
            .await?;
        Ok(())
    }
}

#[async_trait]
pub trait FirestoreCacheBackend {
    async fn load(
        &self,
        options: &FirestoreCacheOptions,
        config: &FirestoreCacheConfiguration,
    ) -> Result<(), FirestoreError>;
}
