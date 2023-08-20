use std::collections::HashMap;

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
    pub fn collection(mut self, collection: &str) -> Self {
        let collection_name = collection.to_string();
        self.collections.extend(
            [(
                collection_name,
                FirestoreCacheCollectionConfiguration::new(collection.to_string()),
            )]
            .into_iter()
            .collect::<HashMap<String, FirestoreCacheCollectionConfiguration>>(),
        );
        self
    }
}

pub struct FirestoreCacheCollectionConfiguration {
    pub collection: String,
}

impl FirestoreCacheCollectionConfiguration {
    #[inline]
    pub fn new(collection: String) -> Self {
        Self { collection }
    }
}
