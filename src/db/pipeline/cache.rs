use crate::cache::{
    FirestoreCachePipeline, FirestoreCachePipelineMiss, FirestoreCachePipelineRows,
};
use crate::*;
use gcloud_sdk::google::firestore::v1::Pipeline;
use tracing::*;

impl FirestoreDb {
    /// Under `read_cached_only`, a pipeline the cache does not answer fails, and a read by ID
    /// leaves out the documents the cache does not hold, as a Core batch read does.
    pub(super) async fn pipeline_docs_from_cache(
        &self,
        params: &FirestorePipelineParams,
    ) -> FirestoreResult<FirestoreCachedValue<Vec<FirestoreDocument>>> {
        let (cache, cached_only) = match &self.session_params.cache_mode {
            FirestoreDbSessionCacheMode::None => return Ok(FirestoreCachedValue::SkipCache),
            FirestoreDbSessionCacheMode::ReadThroughCache(cache) => (cache, false),
            FirestoreDbSessionCacheMode::ReadCachedOnly(cache) => (cache, true),
        };
        // A pipeline Firestore would refuse fails the same way from the cache.
        Pipeline::try_from(params.pipeline.clone())?;

        let span = span!(
            Level::DEBUG,
            "Firestore Pipeline Cached",
            "/firestore/cache_result" = field::Empty,
        );
        let missed = |miss: FirestoreCachePipelineMiss| {
            span.record("/firestore/cache_result", "miss");
            if cached_only {
                Err(miss.into())
            } else {
                span.in_scope(|| debug!(%miss, "The pipeline runs on Firestore."));
                Ok(FirestoreCachedValue::SkipCache)
            }
        };

        let pipeline = match self.cacheable_pipeline(params) {
            Ok(pipeline) => pipeline,
            Err(miss) => return missed(miss),
        };
        let documents = match pipeline.read(cache.as_ref()).await? {
            Ok(FirestoreCachePipelineRows::All(documents)) => documents,
            Ok(FirestoreCachePipelineRows::Held(documents)) if cached_only => documents,
            Ok(FirestoreCachePipelineRows::Held(_)) => {
                return missed(FirestoreCachePipelineMiss::unsupported(
                    "the cache does not hold every named document",
                ))
            }
            Err(miss) => return missed(miss),
        };
        match pipeline.evaluate(documents) {
            Ok(documents) => {
                span.record("/firestore/cache_result", "hit");
                Ok(FirestoreCachedValue::UseCached(documents))
            }
            Err(miss) => missed(miss),
        }
    }

    fn cacheable_pipeline<'a>(
        &self,
        params: &'a FirestorePipelineParams,
    ) -> Result<FirestoreCachePipeline<'a>, FirestoreCachePipelineMiss> {
        let FirestorePipelineParams {
            pipeline,
            explain_options,
            // Request tags only label the requests sent to Firestore.
            request_options: _,
        } = params;
        if explain_options.is_some() {
            return Err(FirestoreCachePipelineMiss::unsupported(
                "Query Explain runs on Firestore",
            ));
        }
        // What a transaction or a read time sees is not what the cache holds.
        if self.session_params.consistency_selector.is_some() {
            return Err(FirestoreCachePipelineMiss::unsupported(
                "the session reads in a transaction or at a read time",
            ));
        }
        FirestoreCachePipeline::new(pipeline, self.get_documents_path())
    }
}
