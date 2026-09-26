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
/// seconds, saturating rather than overflowing for large `retries`.
pub(crate) fn retry_delay(retries: usize) -> Duration {
    let max_millis = 2u64
        .saturating_pow(u32::try_from(retries).unwrap_or(u32::MAX))
        .saturating_mul(1000);
    Duration::from_millis(rand::rng().random_range(0..=max_millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_saturates_for_any_retry_count() {
        assert!(retry_delay(0) <= Duration::from_secs(1));
        for retries in [63, 64, 1000, usize::MAX] {
            retry_delay(retries);
        }
    }
}
