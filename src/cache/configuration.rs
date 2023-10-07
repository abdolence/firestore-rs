use crate::{FirestoreDb, FirestoreListenerTarget};
use std::collections::HashMap;

#[derive(Clone)]
pub struct FirestoreCacheConfiguration {
    pub collections: HashMap<String, FirestoreCacheCollectionConfiguration>,
}

impl FirestoreCacheConfiguration {
    #[inline]
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    #[inline]
    pub fn add_collection_config(
        mut self,
        db: &FirestoreDb,
        config: FirestoreCacheCollectionConfiguration,
    ) -> Self {
        let collection_path = {
            if let Some(ref parent) = config.parent {
                format!("{}/{}", parent, config.collection_name)
            } else {
                format!("{}/{}", db.get_documents_path(), config.collection_name)
            }
        };

        self.collections.extend(
            [(collection_path, config)]
                .into_iter()
                .collect::<HashMap<String, FirestoreCacheCollectionConfiguration>>(),
        );
        self
    }
}

#[derive(Debug, Clone)]
pub struct FirestoreCacheCollectionConfiguration {
    pub collection_name: String,
    pub parent: Option<String>,
    pub listener_target: FirestoreListenerTarget,
    pub collection_load_mode: FirestoreCacheCollectionLoadMode,
}

impl FirestoreCacheCollectionConfiguration {
    #[inline]
    pub fn new<S>(
        collection_name: S,
        listener_target: FirestoreListenerTarget,
        collection_load_mode: FirestoreCacheCollectionLoadMode,
    ) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            collection_name: collection_name.as_ref().to_string(),
            parent: None,
            listener_target,
            collection_load_mode,
        }
    }

    #[inline]
    pub fn with_parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub enum FirestoreCacheCollectionLoadMode {
    PreloadAllDocs,
    PreloadAllIfEmpty,
    PreloadNone,
}
