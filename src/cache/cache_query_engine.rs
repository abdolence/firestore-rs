use crate::cache::cache_filter_engine::FirestoreCacheFilterEngine;
use crate::*;

pub struct FirestoreCacheQueryEngine {
    query: FirestoreQueryParams,
}

impl FirestoreCacheQueryEngine {
    pub fn new(query: &FirestoreQueryParams) -> Self {
        Self {
            query: query.clone(),
        }
    }

    pub fn params_supported(&self) -> bool {
        self.query.all_descendants.iter().all(|x| !*x)
            && self.query.order_by.is_none()
            && self.query.start_at.is_none()
            && self.query.end_at.is_none()
            && self.query.offset.is_none()
            && self.query.limit.is_none()
            && self.query.return_only_fields.is_none()
    }

    pub fn matches_doc(&self, doc: &FirestoreDocument) -> bool {
        if let Some(filter) = &self.query.filter {
            let filter_engine = FirestoreCacheFilterEngine::new(filter);
            filter_engine.matches_doc(doc)
        } else {
            true
        }
    }
}
