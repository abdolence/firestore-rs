//! Waiting on long-running Firestore admin operations, shared by index sync and bulk delete.
//!
//! One wait serves both: it polls every still-pending operation once per round under one shared
//! deadline, polls again after a retryable poll error, and stops at the first operation that
//! fails. A caller that needs more than the done/error state, such as bulk delete's progress
//! metadata, observes every polled [`Operation`] through the `on_poll` hook.

use crate::errors::{FirestoreError, FirestoreErrorPublicGenericDetails, FirestoreSystemError};
use crate::{FirestoreDb, FirestoreOperationWaitOptions, FirestoreResult};
use futures::future::join_all;
use gcloud_sdk::google::longrunning::operation::Result as LroResult;
use gcloud_sdk::google::longrunning::{GetOperationRequest, Operation};
use std::fmt::{Display, Formatter};
use tokio::time::Instant;
use tracing::*;

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

/// One operation's progress through a wait, and the span its polls are logged in.
struct TrackedOperation<'a, A> {
    operation: &'a StartedOperation<A>,
    span: Span,
    polls: u32,
}

impl<A> TrackedOperation<'_, A> {
    fn finish(&self, state: &'static str, began: Instant) {
        self.span.record("/firestore/polls", self.polls);
        self.span.record("/firestore/state", state);
        self.span
            .record("/firestore/response_time", began.elapsed().as_millis());
    }
}

impl FirestoreDb {
    async fn get_operation(&self, name: &str) -> FirestoreResult<Operation> {
        self.operations_client()
            .get_operation(GetOperationRequest {
                name: name.to_string(),
            })
            .await
            .map_err(FirestoreError::from)
            .map(|response| response.into_inner())
    }

    /// Polls `operations` until every one reaches a terminal state, under one deadline
    /// `options.timeout` from the call.
    ///
    /// Each round polls every still-pending operation concurrently and then sleeps
    /// `options.poll_interval`, so one slow operation can never use up the deadline before the
    /// others are checked, and a round costs the slowest poll rather than the sum of them. A poll
    /// still unanswered at the deadline is abandoned and its operation counts as pending. Each
    /// operation gets a `Wait For Operation` span, a child of the caller's current span, that
    /// records its poll count, final state (`done`, `failed`, `poll_failed` or `pending`) and
    /// elapsed time.
    ///
    /// `on_poll` sees every successfully polled [`Operation`], done or not, before it is judged.
    ///
    /// # Errors
    /// - The first operation that finishes with an error, as that error. The others keep running
    ///   on the server.
    /// - A poll that fails with an error the crate does not mark `retry_possible`. A retryable
    ///   one, such as `UNAVAILABLE`, is logged and polled again the next round.
    /// - Once the deadline passes, an `OPERATION_WAIT_TIMEOUT` system error naming the action and
    ///   operation name of each operation still pending at its last poll, and only those.
    pub(crate) async fn wait_for_operations<A, F>(
        &self,
        operations: &[&StartedOperation<A>],
        options: &FirestoreOperationWaitOptions,
        mut on_poll: F,
    ) -> FirestoreResult<()>
    where
        A: OperationAction,
        F: FnMut(&StartedOperation<A>, &Operation),
    {
        let began = Instant::now();
        let deadline = began + options.timeout;
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
            let polled = join_all(pending.iter().map(|tracked| {
                tokio::time::timeout_at(deadline, self.get_operation(&tracked.operation.name))
                    .instrument(tracked.span.clone())
            }))
            .await;

            let mut still_pending = Vec::with_capacity(pending.len());
            for (mut tracked, result) in pending.into_iter().zip(polled) {
                tracked.polls += 1;
                let span = tracked.span.clone();
                let _entered = span.enter();
                let operation = tracked.operation;
                let polled = match result {
                    Err(_deadline_passed) => {
                        still_pending.push(tracked);
                        continue;
                    }
                    Ok(Err(FirestoreError::DatabaseError(err))) if err.retry_possible => {
                        warn!(
                            %err,
                            poll = tracked.polls,
                            operation = operation.name.as_str(),
                            action = operation.action.kind(),
                            "Polling an operation failed with a retryable error; polling it again.",
                        );
                        still_pending.push(tracked);
                        continue;
                    }
                    Ok(Err(err)) => {
                        error!(
                            %err,
                            operation = operation.name.as_str(),
                            action = operation.action.kind(),
                            label = %operation.action,
                            "Polling an operation failed.",
                        );
                        tracked.finish("poll_failed", began);
                        return Err(err);
                    }
                    Ok(Ok(polled)) => polled,
                };

                on_poll(operation, &polled);
                debug!(
                    poll = tracked.polls,
                    operation = operation.name.as_str(),
                    action = operation.action.kind(),
                    label = %operation.action,
                    done = polled.done,
                    "Polled a pending operation.",
                );
                if !polled.done {
                    still_pending.push(tracked);
                    continue;
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
                    tracked.finish("failed", began);
                    return Err(FirestoreError::from(status));
                }
                info!(
                    operation = operation.name.as_str(),
                    action = operation.action.kind(),
                    label = %operation.action,
                    polls = tracked.polls,
                    elapsed_ms = began.elapsed().as_millis(),
                    "Operation reached a terminal state.",
                );
                tracked.finish("done", began);
            }
            pending = still_pending;

            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let names: Vec<String> = pending
                    .iter()
                    .map(|tracked| tracked.operation.to_string())
                    .collect();
                for tracked in &pending {
                    tracked.finish("pending", began);
                }
                warn!(
                    pending = names.join(", "),
                    timeout_ms = options.timeout.as_millis(),
                    "Timed out waiting for operations to finish.",
                );
                return Err(FirestoreError::SystemError(FirestoreSystemError::new(
                    FirestoreErrorPublicGenericDetails::new("OPERATION_WAIT_TIMEOUT".to_string()),
                    format!(
                        "timed out after {:?} waiting for: {}",
                        options.timeout,
                        names.join(", ")
                    ),
                )));
            }
            tokio::time::sleep_until(deadline.min(Instant::now() + options.poll_interval)).await;
        }
    }
}
