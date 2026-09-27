//! Waiting on long-running Firestore admin operations, shared by index sync and bulk delete.
//!
//! One wait serves both: it polls every still-pending operation once per round against one
//! [`OperationDeadline`], polls again after a retryable poll error or a poll that did not answer
//! in time, judges every outcome of a round before it stops at the first operation that failed,
//! and gives up once the deadline has passed. A caller that needs more than the done/error state,
//! such as bulk delete's progress metadata, observes every polled [`Operation`] through the
//! `on_poll` hook.

use crate::errors::{
    FirestoreDatabaseError, FirestoreError, FirestoreErrorPublicGenericDetails,
    FirestoreSystemError,
};
use crate::{FirestoreDb, FirestoreOperationWaitOptions, FirestoreResult};
use futures::StreamExt;
use gcloud_sdk::google::longrunning::operation::Result as LroResult;
use gcloud_sdk::google::longrunning::{GetOperationRequest, Operation};
use gcloud_sdk::tonic::Code;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::time::Instant;
use tracing::*;

/// The longest one `GetOperation` poll may take before it is abandoned and sent again in the
/// next round, unless the wait's whole timeout is shorter. A poll is one small read that a
/// healthy service answers in well under a second, so ten seconds only ever cuts off a call that
/// is stuck, and a stuck call then costs one round instead of the rest of the wait.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// How many `GetOperation` polls one round keeps in flight at once. The admin channel is the
/// same HTTP/2 connection the data API uses, where Google's front end allows 100 concurrent
/// streams; 16 leaves most of those to the application's own requests, while a sync that started
/// a few hundred operations still polls all of them in a few dozen round trips.
const MAX_CONCURRENT_POLLS: usize = 16;

/// Longer than any wait a caller can mean. A timeout above it, such as `Duration::MAX`, is
/// treated as this, since a deadline that far out does not fit in an [`Instant`].
const NO_PRACTICAL_DEADLINE: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

/// What a started operation carries out. `Display` is the human-readable form a log line or a
/// timeout error shows, such as `create index (a DESC, tags CONTAINS)`.
pub(crate) trait OperationAction: Display {
    /// A short, stable identifier for the action's kind, independent of its target, logged as a
    /// field a caller can filter or group on without parsing the `Display` text.
    fn kind(&self) -> &'static str;
}

/// A long-running operation this crate started: its server-assigned resource name, and the
/// action it carries out.
pub(crate) struct StartedOperation<A> {
    pub name: String,
    pub action: A,
}

impl<A: Display> Display for StartedOperation<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.action, self.name)
    }
}

/// The one deadline every wait of a call works to, and how those waits poll meanwhile.
///
/// Fixed once, when the call starts, so several waits in a row, such as an index sync's waits
/// between dependent writes and its final wait, all end by the same instant instead of each
/// getting the full timeout again. The last round of polls goes out no later than the deadline,
/// and each poll may take the poll timeout to answer, so a wait returns at most one poll timeout
/// after the deadline.
#[derive(Debug, Clone)]
pub(crate) struct OperationDeadline {
    at: Instant,
    timeout: Duration,
    poll_interval: Duration,
    poll_timeout: Duration,
}

impl OperationDeadline {
    /// A deadline `options.timeout` from now, polled every `options.poll_interval`. Each poll
    /// may take [`POLL_TIMEOUT`], or the whole timeout when that is shorter.
    pub(crate) fn from_now(options: &FirestoreOperationWaitOptions) -> Self {
        Self {
            at: Instant::now() + options.timeout.min(NO_PRACTICAL_DEADLINE),
            timeout: options.timeout,
            poll_interval: options.poll_interval,
            poll_timeout: options.timeout.min(POLL_TIMEOUT),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_poll_timeout(self, poll_timeout: Duration) -> Self {
        Self {
            poll_timeout,
            ..self
        }
    }

    /// When a poll sent now is abandoned. A poll sent after the deadline gets only what is left
    /// of the poll timeout past it, so a late wait cannot extend the call further. Shared with
    /// index sync's `GetField` read-back after a refused write, the only other per-poll read this
    /// crate bounds against the same deadline.
    pub(super) fn poll_cutoff(&self) -> Instant {
        Instant::now().min(self.at) + self.poll_timeout
    }

    pub(super) fn has_passed(&self) -> bool {
        Instant::now() >= self.at
    }

    /// The error a wait that reached this deadline fails with, still `waiting_for` something.
    pub(super) fn timed_out(&self, waiting_for: &str) -> FirestoreError {
        FirestoreError::SystemError(FirestoreSystemError::new(
            FirestoreErrorPublicGenericDetails::new("OPERATION_WAIT_TIMEOUT".to_string()),
            format!(
                "timed out after {:?} waiting for: {waiting_for}",
                self.timeout
            ),
        ))
    }

    /// Sleeps one poll interval, or only until the deadline when that comes first.
    pub(super) async fn sleep_until_next_round(&self) {
        let remaining = self.at.saturating_duration_since(Instant::now());
        tokio::time::sleep(self.poll_interval.min(remaining)).await;
    }
}

/// What one poll decided about its operation.
enum PollVerdict {
    Pending,
    Done,
    Failed(FirestoreError),
}

/// One operation's progress through a wait, and the span its polls are logged in.
struct TrackedOperation<'a, A> {
    operation: &'a StartedOperation<A>,
    span: Span,
    polls: u32,
}

impl<A: OperationAction> TrackedOperation<'_, A> {
    fn finish(&self, state: &'static str, began: Instant) {
        self.span.record("/firestore/polls", self.polls);
        self.span.record("/firestore/state", state);
        self.span
            .record("/firestore/response_time", began.elapsed().as_millis());
    }

    /// Judges one poll's `result`, logging it and, once the operation is settled, recording its
    /// final state on its span.
    fn judge<F>(
        &mut self,
        result: Result<FirestoreResult<Operation>, Elapsed>,
        deadline: &OperationDeadline,
        began: Instant,
        on_poll: &mut F,
    ) -> PollVerdict
    where
        F: FnMut(&StartedOperation<A>, &Operation),
    {
        self.polls += 1;
        let span = self.span.clone();
        let _entered = span.enter();
        let operation = self.operation;
        let polled = match result {
            Err(_) => {
                warn!(
                    poll = self.polls,
                    operation = operation.name.as_str(),
                    action = operation.action.kind(),
                    poll_timeout_ms = deadline.poll_timeout.as_millis(),
                    "Polling an operation did not answer in time; polling it again.",
                );
                return PollVerdict::Pending;
            }
            Ok(Err(FirestoreError::DatabaseError(err))) if err.retry_possible => {
                warn!(
                    %err,
                    poll = self.polls,
                    operation = operation.name.as_str(),
                    action = operation.action.kind(),
                    "Polling an operation failed with a retryable error; polling it again.",
                );
                return PollVerdict::Pending;
            }
            Ok(Err(err)) => {
                error!(
                    %err,
                    operation = operation.name.as_str(),
                    action = operation.action.kind(),
                    label = %operation.action,
                    "Polling an operation failed.",
                );
                self.finish("poll_failed", began);
                return PollVerdict::Failed(err);
            }
            Ok(Ok(polled)) => polled,
        };

        on_poll(operation, &polled);
        debug!(
            poll = self.polls,
            operation = operation.name.as_str(),
            action = operation.action.kind(),
            label = %operation.action,
            done = polled.done,
            "Polled a pending operation.",
        );
        if !polled.done {
            return PollVerdict::Pending;
        }
        if let Some(LroResult::Error(status)) = polled.result {
            error!(
                operation = operation.name.as_str(),
                action = operation.action.kind(),
                label = %operation.action,
                code = status.code,
                message = status.message.as_str(),
                "Operation failed.",
            );
            self.finish("failed", began);
            // The server's message never says which operation it is about, and a sync can have
            // several failing at once.
            let status = gcloud_sdk::google::rpc::Status {
                message: format!("{operation} failed: {}", status.message),
                ..status
            };
            return PollVerdict::Failed(FirestoreError::from(status));
        }
        info!(
            operation = operation.name.as_str(),
            action = operation.action.kind(),
            label = %operation.action,
            polls = self.polls,
            elapsed_ms = began.elapsed().as_millis(),
            "Operation reached a terminal state.",
        );
        self.finish("done", began);
        PollVerdict::Done
    }
}

impl FirestoreDb {
    /// Polls one operation. `GetOperation` only reads, so a poll answered `DEADLINE_EXCEEDED`,
    /// or `NOT_FOUND` for an operation too new to be visible yet, is marked retryable here even
    /// though the crate-wide classification, which also covers writes, marks neither so.
    async fn get_operation(&self, name: &str) -> FirestoreResult<Operation> {
        self.operations_client()
            .get_operation(GetOperationRequest {
                name: name.to_string(),
            })
            .await
            .map(|response| response.into_inner())
            .map_err(|status| match status.code() {
                Code::DeadlineExceeded | Code::NotFound => {
                    FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                        FirestoreErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                        status.to_string(),
                        true,
                    ))
                }
                _ => FirestoreError::from(status),
            })
    }

    /// Polls `operations` until every one reaches a terminal state or `deadline` passes.
    ///
    /// Each round polls every still-pending operation, at most [`MAX_CONCURRENT_POLLS`] at once,
    /// and then sleeps the poll interval, or only until the deadline when that comes first. So
    /// one slow operation never uses up the deadline before the others are checked, and the last
    /// round goes out at the deadline itself. A poll that does not answer within the poll
    /// timeout, or fails with an error marked `retry_possible`, is logged and sent again next
    /// round. Each operation gets a `Wait For Operation` span, a child of the caller's current
    /// span, that records its poll count, final state (`done`, `failed`, `poll_failed`,
    /// `pending` at the deadline, or `abandoned` when another operation's failure ended the
    /// wait) and elapsed time.
    ///
    /// `on_poll` sees every successfully polled [`Operation`], done or not, before it is judged,
    /// and every poll of a round is judged before the wait returns.
    ///
    /// # Errors
    /// - The first operation, in the order given, that finished with an error in a round, as that
    ///   error, naming the operation. The others keep running on the server.
    /// - A poll that fails with an error not marked `retry_possible`.
    /// - Once the deadline passes, an `OPERATION_WAIT_TIMEOUT` system error naming the action and
    ///   operation name of each operation still pending at its last poll, and only those.
    pub(crate) async fn wait_for_operations<A, F>(
        &self,
        operations: &[&StartedOperation<A>],
        deadline: &OperationDeadline,
        mut on_poll: F,
    ) -> FirestoreResult<()>
    where
        A: OperationAction,
        F: FnMut(&StartedOperation<A>, &Operation),
    {
        let began = Instant::now();
        let mut pending: Vec<TrackedOperation<A>> = operations
            .iter()
            .map(|operation| TrackedOperation {
                operation,
                span: span!(
                    Level::INFO,
                    "Wait For Operation",
                    "/firestore/operation" = operation.name.as_str(),
                    "/firestore/action" = operation.action.kind(),
                    "/firestore/polls" = field::Empty,
                    "/firestore/state" = field::Empty,
                    "/firestore/response_time" = field::Empty,
                ),
                polls: 0,
            })
            .collect();

        loop {
            // Collected before streaming: a stream over a lazy `map` makes the future
            // higher-ranked over the borrow, which the `Send` bound of an `async_trait` caller
            // cannot prove.
            let polls: Vec<_> = pending
                .iter()
                .map(|tracked| {
                    // The cutoff is taken when the poll is actually sent, which can be later
                    // than the start of the round when more operations are pending than may be
                    // polled at once.
                    async move {
                        tokio::time::timeout_at(
                            deadline.poll_cutoff(),
                            self.get_operation(&tracked.operation.name),
                        )
                        .await
                    }
                    .instrument(tracked.span.clone())
                })
                .collect();
            let polled: Vec<_> = futures::stream::iter(polls)
                .buffered(MAX_CONCURRENT_POLLS)
                .collect()
                .await;

            let mut first_failure = None;
            let mut still_pending = Vec::with_capacity(pending.len());
            for (mut tracked, result) in pending.into_iter().zip(polled) {
                match tracked.judge(result, deadline, began, &mut on_poll) {
                    PollVerdict::Pending => still_pending.push(tracked),
                    PollVerdict::Done => {}
                    PollVerdict::Failed(err) => {
                        first_failure.get_or_insert(err);
                    }
                }
            }
            pending = still_pending;

            if let Some(err) = first_failure {
                for tracked in &pending {
                    tracked.finish("abandoned", began);
                }
                return Err(err);
            }
            if pending.is_empty() {
                return Ok(());
            }
            if deadline.has_passed() {
                let names: Vec<String> = pending
                    .iter()
                    .map(|tracked| tracked.operation.to_string())
                    .collect();
                for tracked in &pending {
                    tracked.finish("pending", began);
                }
                warn!(
                    pending = names.join(", "),
                    timeout_ms = deadline.timeout.as_millis(),
                    "Timed out waiting for operations to finish.",
                );
                return Err(deadline.timed_out(&names.join(", ")));
            }
            deadline.sleep_until_next_round().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake_firestore::{
        done_operation_response, failed_operation_response, pending_operation_response,
        FakeFirestore, FakeResponse,
    };
    use gcloud_sdk::prost::Message as _;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    const GET_OPERATION: &str = "/google.longrunning.Operations/GetOperation";

    struct TestAction;

    impl Display for TestAction {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "test action")
        }
    }

    impl OperationAction for TestAction {
        fn kind(&self) -> &'static str {
            "test"
        }
    }

    fn started(id: &str) -> StartedOperation<TestAction> {
        StartedOperation {
            name: format!("projects/fake-firestore/databases/(default)/operations/{id}"),
            action: TestAction,
        }
    }

    /// Starts a fake whose `GetOperation` answers through `answer`, given the polled operation's
    /// ID and how many times that operation has been polled, this poll included.
    async fn fake_polls(
        answer: impl Fn(&str, &str, u32) -> FakeResponse + Send + Sync + 'static,
    ) -> FakeFirestore {
        let polls: Mutex<HashMap<String, u32>> = Mutex::default();
        FakeFirestore::start(move |method, bytes| {
            assert_eq!(method, GET_OPERATION, "unexpected RPC");
            let name = GetOperationRequest::decode(bytes).unwrap().name;
            let id = name.rsplit('/').next().unwrap().to_string();
            let count = {
                let mut polls = polls.lock().unwrap();
                let count = polls.entry(id.clone()).or_default();
                *count += 1;
                *count
            };
            (
                format!("GetOperation({id})#{count}"),
                answer(&name, &id, count),
            )
        })
        .await
    }

    #[tokio::test]
    async fn a_hung_poll_is_abandoned_while_the_other_operations_are_polled() {
        let fake = fake_polls(|name, id, count| match (id, count) {
            ("hung", 1) => FakeResponse::Hang,
            (_, 1) => pending_operation_response(name),
            _ => done_operation_response(name),
        })
        .await;
        let (hung, other) = (started("hung"), started("other"));
        let mut observed_done = Vec::new();

        let began = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            fake.db.wait_for_operations(
                &[&hung, &other],
                &OperationDeadline::from_now(
                    &FirestoreOperationWaitOptions::new(Duration::from_secs(3))
                        .with_poll_interval(Duration::from_millis(10)),
                )
                .with_poll_timeout(Duration::from_millis(100)),
                |operation, polled| {
                    if polled.done {
                        observed_done.push(operation.name.clone());
                    }
                },
            ),
        )
        .await
        .expect("a hung poll must not hold the wait");

        result.unwrap();
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "a hung poll held the wait for {:?}",
            began.elapsed()
        );
        assert!(observed_done.contains(&other.name), "{observed_done:?}");
        assert!(observed_done.contains(&hung.name), "{observed_done:?}");
    }

    #[tokio::test]
    async fn every_outcome_of_a_round_is_observed_before_the_first_failure_returns() {
        let fake = fake_polls(|name, id, _| match id {
            "failing" => failed_operation_response(name, 9, "build failed"),
            _ => done_operation_response(name),
        })
        .await;
        let (failing, finishing) = (started("failing"), started("finishing"));
        let mut observed = Vec::new();

        let err = fake
            .db
            .wait_for_operations(
                &[&failing, &finishing],
                &OperationDeadline::from_now(
                    &FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                        .with_poll_interval(Duration::from_millis(10)),
                ),
                |operation, polled| observed.push((operation.name.clone(), polled.done)),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("build failed"), "{err}");
        assert_eq!(
            observed,
            vec![(failing.name.clone(), true), (finishing.name.clone(), true)]
        );
    }
}
