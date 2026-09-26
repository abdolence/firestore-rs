use crate::errors::FirestoreDatabaseError;
use crate::{FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreResult};
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::tonic::{Request, Response, Status};
use gcloud_sdk::GoogleAuthMiddleware;
use rand::RngExt;
use std::future::Future;
use std::time::Duration;
use tracing::{warn, Span};

impl FirestoreDb {
    /// Sends a read built from `request` through `send`, and sends it again after a random
    /// [`retry_delay`] while it fails with a `retry_possible` error, up to
    /// [`max_retries`](crate::FirestoreDbOptions::max_retries) times. Each retry is logged in
    /// `span` as "Failed to `action`".
    ///
    /// An `ABORTED` read inside a transaction is never sent again: the transaction ID is no longer
    /// valid, so only running the whole transaction again can succeed.
    pub(crate) async fn retry_read<R, T, F, Fut>(
        &self,
        span: &Span,
        action: &str,
        request: &R,
        send: F,
    ) -> FirestoreResult<T>
    where
        R: Clone,
        F: Fn(FirestoreClient<GoogleAuthMiddleware>, Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let max_retries = self.inner.options.max_retries;
        let mut retries = 0;
        loop {
            match send(self.client().get(), Request::new(request.clone())).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => match FirestoreError::from(status) {
                    FirestoreError::DatabaseError(err)
                        if retries < max_retries && self.read_retry_possible(&err) =>
                    {
                        let delay = retry_delay(retries);
                        span.in_scope(|| {
                            warn!(
                                %err,
                                current_retry = retries + 1,
                                max_retries,
                                delay = delay.as_millis(),
                                "Failed to {action}. Retrying up to the specified number of times.",
                            );
                        });
                        tokio::time::sleep(delay).await;
                        retries += 1;
                    }
                    err => return Err(err),
                },
            }
        }
    }

    fn read_retry_possible(&self, error: &FirestoreDatabaseError) -> bool {
        let in_transaction = matches!(
            self.session_params.consistency_selector,
            Some(FirestoreConsistencySelector::Transaction(_))
        );
        error.retry_possible && !(in_transaction && error.public.code == "Aborted")
    }
}

/// How long to wait before retry number `retries + 1`: a random delay of up to `2^retries`
/// seconds, saturating rather than overflowing for large `retries`.
fn retry_delay(retries: usize) -> Duration {
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
