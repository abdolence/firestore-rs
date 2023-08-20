pub struct FirestoreCacheConfiguration {
    collections: Vec<FirestoreCacheCollectionConfiguration>,
}

impl FirestoreCacheConfiguration {
    #[inline]
    pub fn new() -> Self {
        Self {
            collections: Vec::new(),
        }
    }

    #[inline]
    pub fn collection(self, collection: &str) -> Self {
        Self {
            collections: self
                .collections
                .into_iter()
                .chain(vec![FirestoreCacheCollectionConfiguration::new(
                    collection.to_string(),
                )])
                .collect(),
        }
    }
}

pub struct FirestoreCacheCollectionConfiguration {
    collection: String,
}

impl FirestoreCacheCollectionConfiguration {
    #[inline]
    pub fn new(collection: String) -> Self {
        Self { collection }
    }
}
