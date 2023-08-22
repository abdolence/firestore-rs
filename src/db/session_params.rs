use crate::FirestoreConsistencySelector;
use rsb_derive::*;

#[derive(Clone, Builder)]
pub struct FirestoreDbSessionParams {
    pub consistency_selector: Option<FirestoreConsistencySelector>,

    #[default = "FirestoreDbSessionCacheMode::None"]
    pub cache_mode: FirestoreDbSessionCacheMode,
}

#[derive(Clone)]
pub enum FirestoreDbSessionCacheMode {
    None,
    #[cfg(feature = "caching")]
    ReadThrough(crate::FirestoreSharedCacheBackend),
    #[cfg(feature = "caching")]
    ReadOnlyCached(crate::FirestoreSharedCacheBackend),
}
