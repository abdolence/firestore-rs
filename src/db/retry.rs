use crate::errors::FirestoreDatabaseError;
use crate::{FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreResult};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::tonic::{Request, Response, Status};
use gcloud_sdk::{GoogleAuthMiddleware, RequestTimeouts};
use std::future::Future;
use std::time::Duration;
use tracing::{warn, Span};

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
        // Reads stop at `max_retries`, so the backoff only sets the delays.
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(None)
            .build();
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
                    let Some(delay) = backoff.next_backoff() else {
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
