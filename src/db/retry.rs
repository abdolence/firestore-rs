use crate::errors::FirestoreDatabaseError;
use crate::{FirestoreConsistencySelector, FirestoreDb};
use rand::RngExt;
use std::time::Duration;

impl FirestoreDb {
    /// Whether a read that failed with `error` after `retries` retries may be sent again.
    ///
    /// An `ABORTED` read inside a transaction is never sent again: the transaction ID is no longer
    /// valid, so only running the whole transaction again can succeed.
    pub(crate) fn read_retry_possible(
        &self,
        error: &FirestoreDatabaseError,
        retries: usize,
    ) -> bool {
        let in_transaction = matches!(
            self.session_params.consistency_selector,
            Some(FirestoreConsistencySelector::Transaction(_))
        );
        error.retry_possible
            && retries < self.inner.options.max_retries
            && !(in_transaction && error.public.code == "Aborted")
    }
}

/// How long to wait before retry number `retries + 1`: a random delay of up to `2^retries`
/// seconds.
pub(crate) fn retry_delay(retries: usize) -> Duration {
    Duration::from_millis(rand::rng().random_range(0..2u64.pow(retries as u32) * 1000 + 1))
}
