use crate::FirestoreConsistencySelector;
use rsb_derive::*;

#[derive(Debug, Clone, Builder)]
pub struct FirestoreDbSessionParams {
    pub consistency_selector: Option<FirestoreConsistencySelector>,
    #[cfg(feature = "caching")]
    pub read_through_caches: Option<std::collections::HashSet<crate::FirestoreCacheName>>,
}
