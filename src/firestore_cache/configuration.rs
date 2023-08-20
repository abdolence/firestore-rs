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
    pub fn collection<S>(mut self, collection_path: S) -> Self
    where
        S: AsRef<str>,
    {
        let collection_name_str = collection_path.as_ref().to_string();

        self.collections.extend(
            [(
                collection_name_str.clone(),
                FirestoreCacheCollectionConfiguration::new(collection_name_str.to_string()),
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
