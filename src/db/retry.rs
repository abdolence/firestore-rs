use crate::errors::FirestoreDatabaseError;
use crate::{FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreResult};
use backon::{BackoffBuilder, ExponentialBuilder};
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::tonic::{Request, Response, Status};
use gcloud_sdk::{GoogleAuthMiddleware, RequestTimeouts};
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{warn, Span};

/// The delays between retries of reads, simple batch writes and transactions: 500 ms, growing by
/// 1.5 times for each next retry up to 60 s, each randomised by up to a third either way. The
/// jitter lengthens a delay by up to 100%, so the delays set here are two thirds of those. They
/// never run out: each path stops retrying at its own limit.
pub(crate) const RETRY_BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_nanos(500_000_000 * 2 / 3))
    .with_factor(1.5)
    .with_max_delay(Duration::from_secs(60 * 2 / 3))
    .with_jitter()
    .without_max_times();

/// The latest time a retry may start: `max_elapsed_time` after [`start`](Self::start).
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryDeadline(Option<Instant>);

impl RetryDeadline {
    /// Starts counting `max_elapsed_time` from now. `None`, or a time too far ahead for an
    /// `Instant`, sets no deadline.
    pub(crate) fn start(max_elapsed_time: Option<Duration>) -> Self {
        Self(
            max_elapsed_time
                .and_then(|max_elapsed_time| Instant::now().checked_add(max_elapsed_time)),
        )
    }

    /// Whether a retry that first waits `delay` still starts by the deadline.
    pub(crate) fn allows(self, delay: Duration) -> bool {
        self.0.is_none_or(|deadline| {
            Instant::now()
                .checked_add(delay)
                .is_some_and(|retry_start| retry_start <= deadline)
        })
    }
}

impl FirestoreDb {
    /// Sends a read built from `request` through `send`, limited by the request timeout as
    /// `limit` applies it. While it fails with a `retry_possible` error, or with any
    /// `DEADLINE_EXCEEDED`, which a read, unlike a write, can safely send again, it is sent again
    /// after a backoff delay, up to [`max_retries`](crate::FirestoreDbOptions::max_retries) times.
    /// Each retry is logged in `span` as "Failed to `action`".
    ///
    /// An `ABORTED` read inside a transaction is never sent again: the transaction ID is no longer
    /// valid, so only running the whole transaction again can succeed.
    pub(crate) async fn retry_read<R, T, F, Fut>(
        &self,
        span: &Span,
        action: &str,
        request: &R,
        limit: fn(RequestTimeouts, Duration) -> RequestTimeouts,
        send: F,
    ) -> FirestoreResult<T>
    where
        R: Clone,
        F: Fn(FirestoreClient<GoogleAuthMiddleware>, Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let max_retries = self.inner.options.max_retries;
        let mut delays = RETRY_BACKOFF.build();
        let mut retries = 0;
        loop {
            let request = self.request_with_timeout(request.clone(), limit);
            let error = match send(self.client().get(), request).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => FirestoreError::from_read_status(status),
            };
            match error {
                FirestoreError::DatabaseError(err)
                    if retries < max_retries && self.read_retry_possible(&err) =>
                {
                    let Some(delay) = delays.next() else {
                        return Err(FirestoreError::DatabaseError(err));
                    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delays_centre_on_500_ms_growing_by_1_5_times() {
        // backon computes the delays in f32 seconds.
        let rounding = Duration::from_millis(1);
        let mut delays = RETRY_BACKOFF.build();
        let mut centre = Duration::from_millis(500);
        for retry in 1..=8 {
            let delay = delays.next().expect("retry delays never run out");
            let shortest = centre.mul_f64(2.0 / 3.0);
            let longest = centre.mul_f64(4.0 / 3.0);
            assert!(
                delay + rounding >= shortest && delay <= longest + rounding,
                "retry {retry}: {delay:?} outside {shortest:?}..={longest:?}"
            );
            centre = centre.mul_f64(1.5);
        }
    }
}
