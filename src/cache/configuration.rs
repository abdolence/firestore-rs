use crate::FirestoreListenerTarget;
use rsb_derive::Builder;
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
    pub fn add_collection_config<S>(
        mut self,
        collection_path: S,
        listener_target: FirestoreListenerTarget,
        collection_load_mode: FirestoreCacheCollectionLoadMode,
    ) -> Self
    where
        S: AsRef<str>,
    {
        let collection_name_str = collection_path.as_ref().to_string();

        self.collections.extend(
            [(
                collection_name_str.clone(),
                FirestoreCacheCollectionConfiguration::new(
                    collection_name_str.to_string(),
                    listener_target,
                    collection_load_mode,
                ),
            )]
            .into_iter()
            .collect::<HashMap<String, FirestoreCacheCollectionConfiguration>>(),
        );
        self
    }
}

#[derive(Debug, Builder, Clone)]
pub struct FirestoreCacheCollectionConfiguration {
    pub collection: String,
    pub listener_target: FirestoreListenerTarget,
    pub collection_load_mode: FirestoreCacheCollectionLoadMode,
}

#[derive(Debug, Clone)]
pub enum FirestoreCacheCollectionLoadMode {
    PreloadAllDocs,
    PreloadAllIfEmpty,
    PreloadNone,
}
