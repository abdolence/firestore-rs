use crate::errors::FirestoreDatabaseError;
use crate::{FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreResult};
use backon::{BackoffBuilder, ExponentialBuilder};
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::tonic::{Code, Request, Response, Status};
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

/// The codes a create or an update is sent again after, as in the retry policy of Google's
/// Firestore clients (`google/firestore/v1/firestore_grpc_service_config.json`).
pub(crate) const CREATE_AND_UPDATE_RETRIED_CODES: [Code; 2] =
    [Code::ResourceExhausted, Code::Unavailable];

/// The codes a delete is sent again after, as in the retry policy of Google's Firestore clients
/// (`google/firestore/v1/firestore_grpc_service_config.json`).
pub(crate) const DELETE_RETRIED_CODES: [Code; 4] = [
    Code::ResourceExhausted,
    Code::Unavailable,
    Code::Internal,
    Code::DeadlineExceeded,
];

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
        self.retry_request(span, action, request, limit, send, |status| {
            let error = FirestoreError::from_read_status(status);
            let retry = matches!(&error, FirestoreError::DatabaseError(err)
                if self.read_retry_possible(err));
            (error, retry)
        })
        .await
    }

    /// Sends a single write built from `request` through `send`, limited by the request timeout
    /// as a whole call. While it fails with one of `retried_codes` it is sent again after a
    /// backoff delay, up to [`max_retries`](crate::FirestoreDbOptions::max_retries) times. Each
    /// retry is logged in `span` as "Failed to `action`".
    ///
    /// Only the codes in `retried_codes` are sent again, whatever the error's `retry_possible`
    /// says: it is also set for `ABORTED` and `CANCELLED`, which Google's clients do not retry a
    /// create or an update on. A connection lost in the middle of a call counts as `UNAVAILABLE`,
    /// as it does in Google's clients.
    pub(crate) async fn retry_write<R, T, F, Fut>(
        &self,
        span: &Span,
        action: &str,
        request: &R,
        retried_codes: &[Code],
        send: F,
    ) -> FirestoreResult<T>
    where
        R: Clone,
        F: Fn(FirestoreClient<GoogleAuthMiddleware>, Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.retry_request(
            span,
            action,
            request,
            RequestTimeouts::with_total,
            send,
            |status| {
                let code = status.code();
                let error = FirestoreError::from(status);
                let code = if code == Code::Unknown && error.retry_possible() {
                    Code::Unavailable
                } else {
                    code
                };
                (error, retried_codes.contains(&code))
            },
        )
        .await
    }

    /// Sends `request` through `send` and sends it again after a backoff delay, up to
    /// `max_retries` times, while `classify` calls the failed status retryable. `classify` returns
    /// the error to report for the status, and whether to retry it.
    async fn retry_request<R, T, F, Fut, C>(
        &self,
        span: &Span,
        action: &str,
        request: &R,
        limit: fn(RequestTimeouts, Duration) -> RequestTimeouts,
        send: F,
        classify: C,
    ) -> FirestoreResult<T>
    where
        R: Clone,
        F: Fn(FirestoreClient<GoogleAuthMiddleware>, Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
        C: Fn(Status) -> (FirestoreError, bool),
    {
        let max_retries = self.inner.options.max_retries;
        let mut delays = RETRY_BACKOFF.build();
        let mut retries = 0;
        loop {
            let request = self.request_with_timeout(request.clone(), limit);
            let (error, retry) = match send(self.client().get(), request).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => classify(status),
            };
            if !retry || retries >= max_retries {
                return Err(error);
            }
            let Some(delay) = delays.next() else {
                return Err(error);
            };
            span.in_scope(|| {
                warn!(
                    err = %error,
                    current_retry = retries + 1,
                    max_retries,
                    delay = delay.as_millis(),
                    "Failed to {action}. Retrying up to the specified number of times.",
                );
            });
            tokio::time::sleep(delay).await;
            retries += 1;
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
    use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
    use crate::db::support::{
        FirestoreCreateSupport, FirestoreDeleteSupport, FirestoreUpdateSupport,
    };
    use gcloud_sdk::google::firestore::v1::{CreateDocumentRequest, Document};
    use gcloud_sdk::prost::Message as _;

    #[derive(Clone, Copy, Debug)]
    enum SingleWrite {
        Insert,
        Update,
        Delete,
    }

    impl SingleWrite {
        fn rpc(self) -> &'static str {
            match self {
                Self::Insert => "CreateDocument",
                Self::Update => "UpdateDocument",
                Self::Delete => "DeleteDocument",
            }
        }

        async fn run(self, db: &FirestoreDb) -> FirestoreResult<()> {
            match self {
                Self::Insert => db
                    .create_doc("items", Some("a"), Document::default(), None)
                    .await
                    .map(drop),
                Self::Update => {
                    let document = Document {
                        name: format!("{}/items/a", db.get_documents_path()),
                        ..Document::default()
                    };
                    db.update_doc("items", document, None, None, None)
                        .await
                        .map(drop)
                }
                Self::Delete => db.delete_by_id("items", "a", None).await,
            }
        }
    }

    /// A server that answers the first attempt with `failure` and every next one with success.
    async fn failing_once(write: SingleWrite, failure: FakeResponse) -> FakeFirestore {
        let failure = std::sync::Mutex::new(Some(failure));
        FakeFirestore::start(move |_, _| {
            let response = failure
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(FakeResponse::empty);
            (write.rpc().to_string(), response)
        })
        .await
    }

    #[tokio::test]
    async fn every_single_write_retries_unavailable() {
        for write in [
            SingleWrite::Insert,
            SingleWrite::Update,
            SingleWrite::Delete,
        ] {
            let server = failing_once(write, FakeResponse::Status(Code::Unavailable)).await;

            write.run(&server.db).await.unwrap();

            assert_eq!(server.calls(), [write.rpc(); 2], "{write:?}");
        }
    }

    #[tokio::test]
    async fn a_connection_lost_mid_call_is_retried_like_unavailable() {
        let server = failing_once(SingleWrite::Delete, FakeResponse::Drop).await;

        SingleWrite::Delete.run(&server.db).await.unwrap();

        assert_eq!(server.calls(), ["DeleteDocument"; 2]);
    }

    #[tokio::test]
    async fn a_write_does_not_retry_aborted() {
        let server = failing_once(SingleWrite::Insert, FakeResponse::Status(Code::Aborted)).await;

        let error = SingleWrite::Insert.run(&server.db).await.unwrap_err();

        assert!(matches!(error, FirestoreError::DatabaseError(ref error)
            if error.public.code == "Aborted"));
        assert_eq!(server.calls(), ["CreateDocument"; 1]);
    }

    #[tokio::test]
    async fn delete_retries_a_request_timeout_and_insert_does_not() {
        let timeout = Duration::from_millis(200);
        for (write, expected_calls) in [(SingleWrite::Delete, 2), (SingleWrite::Insert, 1)] {
            let server = failing_once(write, FakeResponse::Hang).await;
            let db = server.db.clone_with_request_timeout(Some(timeout));

            let result = write.run(&db).await;

            assert_eq!(server.calls().len(), expected_calls, "{write:?}");
            assert_eq!(result.is_ok(), expected_calls == 2, "{write:?}: {result:?}");
            if let Err(error) = result {
                assert!(
                    matches!(error, FirestoreError::DatabaseError(ref error)
                        if error.public.code == "DeadlineExceeded"),
                    "got {error:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn generated_id_insert_sends_the_same_client_made_id_on_each_attempt() {
        let attempts = std::sync::Mutex::new(0);
        let server = FakeFirestore::start(move |_, request| {
            let id = CreateDocumentRequest::decode(request).unwrap().document_id;
            let mut attempts = attempts.lock().unwrap();
            *attempts += 1;
            let response = if *attempts == 1 {
                FakeResponse::Status(Code::Unavailable)
            } else {
                FakeResponse::empty()
            };
            (id, response)
        })
        .await;

        server
            .db
            .fluent()
            .insert()
            .into("items")
            .generate_document_id()
            .document(Document::default())
            .execute()
            .await
            .unwrap();

        let ids = server.calls();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[0].len(), 20);
        assert!(ids[0].chars().all(|c| c.is_ascii_alphanumeric()), "{ids:?}");
    }

    #[test]
    fn first_retry_delays_fall_on_both_sides_of_500_ms() {
        let first_delays: Vec<Duration> = (0..300)
            .map(|_| {
                RETRY_BACKOFF
                    .build()
                    .next()
                    .expect("retry delays never run out")
            })
            .collect();
        let centre = Duration::from_millis(500);

        assert!(first_delays.iter().any(|&delay| delay < centre));
        assert!(first_delays.iter().any(|&delay| delay > centre));
    }

    #[test]
    fn retry_delays_grow_by_1_5_times_within_the_jitter_range() {
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
