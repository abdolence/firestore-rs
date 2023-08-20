use crate::FirestoreConsistencySelector;
use rsb_derive::*;

#[derive(Debug, Clone, Builder)]
pub struct FirestoreDbSessionParams {
    pub consistency_selector: Option<FirestoreConsistencySelector>,

    #[default = "FirestoreDbSessionCacheMode::None"]
    pub cache_mode: FirestoreDbSessionCacheMode,
}

#[derive(Debug, Clone)]
pub enum FirestoreDbSessionCacheMode {
    None,
    #[cfg(feature = "caching")]
    ReadThrough(crate::FirestoreCacheName),
}
