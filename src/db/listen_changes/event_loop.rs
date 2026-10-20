use super::listener::FirestoreListenerControl;
use super::registration::{FirestoreListenerRegistration, FirestoreListenerTargetsState};
use super::stream_targets::{FirestoreListenerRemoval, FirestoreListenerStreamTargets};
use crate::db::support::FirestoreListenSupport;
use crate::errors::*;
use crate::{
    FirestoreListenEvent, FirestoreListenerParams, FirestoreListenerTarget,
    FirestoreListenerTargetParams, FirestoreListenerTargetResumeType, FirestoreListenerToken,
    FirestoreResult, FirestoreResumeStateStorage,
};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::target_change::TargetChangeType;
use gcloud_sdk::google::firestore::v1::{listen_response, ListenResponse, TargetChange};
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::*;

/// The loop a started listener runs. It keeps one listen stream open for every registered target
/// and reopens it whenever it ends, until the listener is shut down or an error no retry can fix.
///
/// Each handler returns [`ControlFlow::Break`] to give up the open stream. [`run`](Self::run) then
/// opens a new one once there is a target to listen on, unless the listener has been shut down.
pub(super) struct FirestoreListenerLoop<D, S, FN> {
    db: D,
    storage: Arc<S>,
    shutdown_signal: watch::Sender<bool>,
    targets_state: FirestoreListenerTargetsState,
    control_receiver: UnboundedReceiver<FirestoreListenerControl>,
    callback: FN,
    retry_delay: Duration,
    idle_timeout: Option<Duration>,
    /// Set once we have retried with every resume token discarded, so that a request Firestore
    /// still rejects as invalid is treated as permanent instead of looping forever.
    retried_without_resume_tokens: bool,
}

impl<D, S, FN, F> FirestoreListenerLoop<D, S, FN>
where
    D: FirestoreListenSupport,
    S: FirestoreResumeStateStorage,
    FN: Fn(FirestoreListenEvent) -> F,
    F: Future<Output = AnyBoxedErrResult<()>>,
{
    pub(super) fn new(
        db: D,
        storage: Arc<S>,
        shutdown_signal: watch::Sender<bool>,
        targets_state: FirestoreListenerTargetsState,
        listener_params: &FirestoreListenerParams,
        control_receiver: UnboundedReceiver<FirestoreListenerControl>,
        callback: FN,
    ) -> Self {
        Self {
            db,
            storage,
            shutdown_signal,
            targets_state,
            control_receiver,
            callback,
            retry_delay: listener_params
                .retry_delay
                .unwrap_or_else(|| Duration::from_secs(5)),
            idle_timeout: listener_params.idle_timeout,
            retried_without_resume_tokens: false,
        }
    }

    pub(super) async fn run(mut self) -> FirestoreResult<()> {
        while !*self.shutdown_signal.borrow() {
            let no_targets = self.targets_state.read().is_empty();
            if no_targets {
                self.wait_for_targets().await;
                continue;
            }

            let (requests, request_reader) = tokio::sync::mpsc::unbounded_channel();
            let mut on_stream = FirestoreListenerStreamTargets::new(requests);
            self.add_due_targets(&mut on_stream).await?;
            if on_stream.is_empty() {
                // Every target was removed while its resume state was read. A stream opened with
                // no target could wait for an answer that never comes.
                continue;
            }
            debug!(
                num_targets = on_stream.len(),
                "Start listening on targets..."
            );

            let Some(connection) = self
                .until_shutdown(self.db.listen_doc_changes(request_reader))
                .await
            else {
                break;
            };
            match connection {
                Err(err) => self.on_listen_error(err).await?,
                Ok(listen_stream) => self.listen(listen_stream, &mut on_stream).await?,
            }
        }
        Ok(())
    }

    /// Idles on the control channel while nothing is registered, rather than opening an empty
    /// stream, so that a listener can be started before its targets exist.
    async fn wait_for_targets(&mut self) {
        debug!("Firestore listener has no targets. Waiting for one to be added...");
        match self.control_receiver.recv().await {
            None | Some(FirestoreListenerControl::Shutdown) => {
                self.shutdown_signal.send_replace(true);
            }
            // Nothing is being listened to, so there is nothing to remove or resynchronise.
            Some(FirestoreListenerControl::TargetsAdded)
            | Some(FirestoreListenerControl::TargetRemoved(..))
            | Some(FirestoreListenerControl::ResyncTarget(_)) => {}
        }
    }

    async fn listen(
        &mut self,
        mut listen_stream: BoxStream<'_, FirestoreResult<ListenResponse>>,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<()> {
        // When the stream last had nothing left to handle while Firestore owed it an answer.
        let mut silent_since: Option<Instant> = None;
        loop {
            let next_add_back = on_stream.next_add_back();
            let silence_deadline = match self.idle_timeout {
                Some(idle_timeout) if on_stream.awaits_firestore() => {
                    Some(*silent_since.get_or_insert_with(Instant::now) + idle_timeout)
                }
                _ => {
                    silent_since = None;
                    None
                }
            };
            let flow = tokio::select! {
                // Control messages go first, so that the stream state they record is in place
                // before Firestore's answers to them are read.
                biased;
                control = self.control_receiver.recv() => {
                    self.on_control(control, on_stream).await?
                }
                () = sleep_until_some(next_add_back) => {
                    self.add_due_targets(on_stream).await?;
                    ControlFlow::Continue(())
                }
                tried = listen_stream.try_next() => {
                    silent_since = None;
                    self.on_stream_item(tried, on_stream).await?
                }
                // After the stream, so that a response already waiting wins over an expired timer.
                () = sleep_until_some(silence_deadline) => {
                    self.on_silence(on_stream).await
                }
            };
            if flow.is_break() {
                return Ok(());
            }
        }
    }

    /// Gives up a stream that stayed open but sent nothing for the idle timeout, which a stalled
    /// stream on a live connection does: HTTP/2 keep-alive does not notice it.
    async fn on_silence(&self, on_stream: &FirestoreListenerStreamTargets) -> ControlFlow<()> {
        warn!(
            idle_timeout = ?self.idle_timeout,
            num_targets = on_stream.len(),
            effective_delay = ?self.retry_delay,
            "Listen stream received nothing within the idle timeout. Reconnecting after the specified delay..."
        );
        // The delay keeps a timeout shorter than Firestore's quiet periods from reconnecting in a
        // tight loop.
        self.until_shutdown(tokio::time::sleep(self.retry_delay))
            .await;
        ControlFlow::Break(())
    }

    async fn on_control(
        &mut self,
        control: Option<FirestoreListenerControl>,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<ControlFlow<()>> {
        match control {
            None => {
                debug!("Listener dropped. Exiting...");
                self.shutdown_signal.send_replace(true);
                Ok(ControlFlow::Break(()))
            }
            Some(FirestoreListenerControl::Shutdown) => {
                debug!("Exiting from listener on targets...");
                self.control_receiver.close();
                Ok(ControlFlow::Break(()))
            }
            Some(FirestoreListenerControl::TargetsAdded) => {
                self.add_due_targets(on_stream).await?;
                Ok(ControlFlow::Continue(()))
            }
            Some(FirestoreListenerControl::TargetRemoved(target, registration)) => {
                on_stream.remove(&self.db, &target, registration, None)?;
                Ok(self.close_if_idle(on_stream))
            }
            Some(FirestoreListenerControl::ResyncTarget(target)) => {
                self.resync_target(target, on_stream).await?;
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    async fn resync_target(
        &self,
        target: FirestoreListenerTarget,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<()> {
        if let Some(registration) = self.targets_state.registration_of(&target) {
            warn!(
                ?target,
                effective_delay = ?self.retry_delay,
                "Resynchronising a listener target from scratch at the cache's request.",
            );
            self.forget_resume_states(&[(target.clone(), registration)])
                .await;
            // Without the delay a target that keeps diverging would re-download in a tight loop.
            on_stream.remove(
                &self.db,
                &target,
                registration,
                Some(Instant::now() + self.retry_delay),
            )?;
        }
        Ok(())
    }

    async fn on_stream_item(
        &mut self,
        tried: FirestoreResult<Option<ListenResponse>>,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<ControlFlow<()>> {
        if *self.shutdown_signal.borrow() {
            return Ok(ControlFlow::Break(()));
        }
        match tried {
            Ok(Some(event)) => {
                trace!(?event, "Received a listen response event to handle.");

                // The connection works, so a later rejection deserves its own token-discarding
                // retry.
                self.retried_without_resume_tokens = false;

                match event.response_type {
                    Some(listen_response::ResponseType::TargetChange(target_change)) => {
                        self.on_target_change(target_change, on_stream).await
                    }
                    Some(response_type) => Ok(self.deliver(response_type).await),
                    None => Ok(ControlFlow::Continue(())),
                }
            }
            Ok(None) => {
                // A clean close still needs the retry delay: without it a server that closes
                // immediately would be reconnected to in a tight loop.
                debug!(
                    effective_delay = ?self.retry_delay,
                    "Listen stream closed. Reconnecting after the specified delay..."
                );
                self.until_shutdown(tokio::time::sleep(self.retry_delay))
                    .await;
                Ok(ControlFlow::Break(()))
            }
            Err(FirestoreError::DataNotFoundError(err)) if on_stream.crossed_removal() => {
                // Firestore's answer to the crossed `RemoveTarget`. The other targets are added
                // back on a new stream at once.
                debug!(%err, "Firestore closed the listen stream over a target it had already removed. Reconnecting...");
                Ok(ControlFlow::Break(()))
            }
            Err(err) => {
                self.on_listen_error(err).await?;
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn on_target_change(
        &self,
        target_change: TargetChange,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<ControlFlow<()>> {
        let change_type = TargetChangeType::try_from(target_change.target_change_type).ok();
        let affected = on_stream.affected_targets(&self.targets_state, &target_change.target_ids);
        let cause = target_change.cause.clone();
        let resume_token = target_change.resume_token.clone();
        let target_ids = target_change.target_ids.clone();
        // A token for every target at once is a consistent snapshot of the stream only on a
        // NO_CHANGE with a read time, which is where the Firebase SDKs adopt it.
        let stores_token = !resume_token.is_empty()
            && (!target_ids.is_empty()
                || (change_type == Some(TargetChangeType::NoChange)
                    && target_change.read_time.is_some()));

        // Recorded, and its ended targets unregistered, before the callback sees the removal, so
        // that `has_target` already tells it whether the target will be added back. A callback
        // that fails on the removal does not bring an ended target back.
        let removal = (change_type == Some(TargetChangeType::Remove)).then(|| {
            on_stream.removed(
                &target_ids,
                cause.as_ref(),
                Instant::now() + self.retry_delay,
            )
        });
        if let Some(removal) = &removal {
            self.end_targets(&removal.ended).await;
        }

        // Anything listening on this - the cache above all - has to see the change, and drop what
        // it holds for a reset target, before the target is listened to again.
        if self
            .deliver(listen_response::ResponseType::TargetChange(target_change))
            .await
            .is_break()
        {
            return Ok(ControlFlow::Break(()));
        }

        // Stored only once the change has been applied. A token saved before that would, after a
        // failure here, resume past a change nothing acted on.
        if stores_token
            && !self
                .store_resume_token(on_stream, &resume_token, &target_ids)
                .await
        {
            return Ok(ControlFlow::Break(()));
        }

        if let Some(removal) = removal {
            return self
                .on_targets_removed(removal, cause.as_ref(), on_stream)
                .await;
        }

        match (change_type, affected) {
            (Some(TargetChangeType::Add), _) => on_stream.added(&target_ids),
            (Some(TargetChangeType::Current), _) => on_stream.current(&target_ids),
            (Some(TargetChangeType::Reset), Ok(affected)) => {
                // Firestore resends the initial state on this same stream and only then issues a
                // new token. Dropping the old one now means a disconnect in between resumes from
                // scratch rather than from a point the reset has already invalidated.
                warn!(
                    ?affected,
                    "Firestore reset listener targets. Their initial state will be resent.",
                );
                self.forget_resume_states(&affected).await;
            }
            _ => {}
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn on_targets_removed(
        &self,
        removal: FirestoreListenerRemoval,
        cause: Option<&gcloud_sdk::google::rpc::Status>,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<ControlFlow<()>> {
        if !removal.retried.is_empty() {
            // The delay keeps a target Firestore keeps rejecting from being added back in a tight
            // loop.
            error!(
                retried = ?removal.retried,
                ?cause,
                effective_delay = ?self.retry_delay,
                "Firestore removed listener targets. Discarding their resume state and adding them back after the retry delay.",
            );
            self.forget_resume_states(&removal.retried).await;
        }
        if !removal.ended.is_empty() {
            if cause.is_some() {
                error!(
                    ended = ?removal.ended,
                    ?cause,
                    "Firestore removed listener targets for a reason a retry cannot fix. They are no longer listened to.",
                );
            } else {
                debug!(ended = ?removal.ended, "Listener targets added once have ended.");
            }
        }
        self.add_due_targets(on_stream).await?;
        Ok(self.close_if_idle(on_stream))
    }

    async fn deliver(&self, event: FirestoreListenEvent) -> ControlFlow<()> {
        match (self.callback)(event).await {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => {
                error!(%err, "Listener callback function error occurred.");
                ControlFlow::Break(())
            }
        }
    }

    fn close_if_idle(&self, on_stream: &FirestoreListenerStreamTargets) -> ControlFlow<()> {
        if on_stream.is_idle(&self.targets_state) {
            debug!("Firestore listener has no targets left. Closing the listen stream...");
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    async fn until_shutdown<Fut: Future>(&self, future: Fut) -> Option<Fut::Output> {
        let mut receiver = self.shutdown_signal.subscribe();
        tokio::select! {
            biased;
            _ = receiver.wait_for(|stopped| *stopped) => None,
            result = future => Some(result),
        }
    }

    /// Adds to the stream every registered target it lacks, and every waiting one whose time has
    /// come, each with its resume state resolved first.
    async fn add_due_targets(
        &self,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<()> {
        let due = on_stream.take_due(&self.targets_state.read(), Instant::now());
        for target in due {
            if let Some((target_params, registration)) = self.resolve_target(&target).await {
                on_stream.add(&self.db, target_params, registration)?;
            }
        }
        Ok(())
    }

    /// The target's parameters with its resume state filled in from the storage if it holds none
    /// yet, and the registration they belong to.
    ///
    /// Reading the storage here rather than only in `start` is what lets a target added at runtime
    /// pick up a token stored by an earlier run, and what makes a target whose token was discarded
    /// come back cleanly. Returns `None` once the target is no longer registered, or has been
    /// registered again during the read: the new registration is added through its own
    /// notification, and what the read returned may be the old registration's state.
    async fn resolve_target(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Option<(FirestoreListenerTargetParams, FirestoreListenerRegistration)> {
        let (registration, unresolved) = {
            let targets = self.targets_state.read();
            let registered = targets.get(target)?;
            (
                registered.registration,
                registered.params.resume_type.is_none(),
            )
        };

        let stored = if unresolved {
            self.storage
                .read_resume_state(target)
                .await
                .unwrap_or_else(|err| {
                    warn!(%err, ?target, "Could not read the resume state of a listener target. Listening on it from scratch.");
                    None
                })
        } else {
            None
        };

        self.targets_state
            .update_if_current(target, registration, |params| {
                if let Some(stored) = stored {
                    params.resume_type = Some(stored);
                }
                (params.clone(), registration)
            })
    }

    /// Persists a resume token for every target the change applies to.
    ///
    /// Returns `false` when the token could not be stored, in which case the caller reopens the
    /// stream rather than carrying on from a position the storage does not know about.
    async fn store_resume_token(
        &self,
        on_stream: &FirestoreListenerStreamTargets,
        resume_token: &[u8],
        target_ids: &[i32],
    ) -> bool {
        let Ok(affected) = on_stream.affected_targets(&self.targets_state, target_ids) else {
            return false;
        };

        let new_token: FirestoreListenerToken = resume_token.to_vec().into();

        for (target_id, registration) in affected {
            if let Err(err) = self
                .storage
                .update_resume_token(&target_id, new_token.clone())
                .await
            {
                error!(%err, "Listener token storage error occurred.");
                return false;
            }
            let still_registered = self
                .targets_state
                .update_if_current(&target_id, registration, |params| {
                    params.resume_type =
                        Some(FirestoreListenerTargetResumeType::Token(new_token.clone()));
                })
                .is_some();
            if !still_registered {
                // Removed while the token was written, possibly after the removal forgot the
                // stored state, so the token can still be in the storage for a later
                // registration of the ID to resume from.
                if let Err(err) = self.storage.forget_resume_state(&target_id).await {
                    warn!(%err, ?target_id, "Could not forget the resume token of a removed listener target.");
                }
            }
        }

        true
    }

    /// Unregisters targets that have ended for good and forgets their resume state, unless the ID
    /// has been registered again since.
    async fn end_targets(
        &self,
        ended: &[(FirestoreListenerTarget, FirestoreListenerRegistration)],
    ) {
        for (target_id, registration) in ended {
            if self
                .targets_state
                .remove_if_current(target_id, *registration)
            {
                if let Err(err) = self.storage.forget_resume_state(target_id).await {
                    warn!(%err, ?target_id, "Could not forget the resume state of an ended listener target.");
                }
            }
        }
    }

    /// Discards the stored resume state of the given registrations, so that they are listened to
    /// from scratch the next time they are added to a stream. A target registered again since
    /// keeps its state: it belongs to the new registration.
    async fn forget_resume_states(
        &self,
        targets: &[(FirestoreListenerTarget, FirestoreListenerRegistration)],
    ) {
        for (target_id, registration) in targets {
            if self.targets_state.registration_of(target_id) != Some(*registration) {
                continue;
            }
            if let Err(err) = self.storage.forget_resume_state(target_id).await {
                warn!(%err, ?target_id, "Could not forget the resume state of a listener target.");
            }
            self.targets_state
                .update_if_current(target_id, *registration, |params| {
                    params.resume_type = None;
                });
        }
    }

    async fn on_listen_error(&mut self, err: FirestoreError) -> FirestoreResult<()> {
        let delay = self.retry_delay;
        match FirestoreListenerErrorAction::for_error(&err) {
            FirestoreListenerErrorAction::Retry => {
                debug!(%err, ?delay, "Listen EOF.. Restarting after the specified delay...");
                self.until_shutdown(tokio::time::sleep(delay)).await;
            }
            FirestoreListenerErrorAction::RetryWithoutResumeTokens
                if !self.retried_without_resume_tokens =>
            {
                // A stored resume token belongs to one target's query, so a stale or expired one
                // is rejected as invalid. Dropping every token costs a replay; treating this as
                // permanent would instead take down the listener for all the other targets too.
                self.retried_without_resume_tokens = true;
                warn!(
                    %err, ?delay,
                    "Firestore rejected the listen request as invalid. Discarding all stored resume tokens and retrying once from scratch...",
                );
                let all_targets: Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)> =
                    self.targets_state
                        .read()
                        .iter()
                        .map(|(target, registered)| (target.clone(), registered.registration))
                        .collect();
                self.forget_resume_states(&all_targets).await;
                self.until_shutdown(tokio::time::sleep(delay)).await;
            }
            // Either the retry without resume tokens was rejected too, so the request itself is
            // invalid, or no retry can fix the error at all.
            FirestoreListenerErrorAction::RetryWithoutResumeTokens
            | FirestoreListenerErrorAction::Fatal => return Err(err),
            FirestoreListenerErrorAction::RetryAfterDelay => {
                error!(%err, ?delay, "Listen error. Restarting after the specified delay...");
                self.until_shutdown(tokio::time::sleep(delay)).await;
            }
        }
        Ok(())
    }
}

/// Completes at `deadline`, or never when there is none, so that a `select!` arm without a
/// deadline stays disabled.
async fn sleep_until_some(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// How the listener should react to a failed listen request or stream.
enum FirestoreListenerErrorAction {
    /// Reconnect after the retry delay, keeping the stored resume state.
    Retry,
    /// Same, but logged as an error rather than as an expected end of stream.
    RetryAfterDelay,
    /// Firestore rejected the request as invalid, which a stale resume token can cause. Retry once
    /// with every resume token discarded before giving up.
    RetryWithoutResumeTokens,
    /// Nothing a retry can fix.
    Fatal,
}

impl FirestoreListenerErrorAction {
    fn for_error(err: &FirestoreError) -> Self {
        match err {
            FirestoreError::DatabaseError(db_err)
                if db_err.details.contains("unexpected end of file")
                    || db_err.details.contains("stream error received") =>
            {
                Self::Retry
            }
            FirestoreError::DatabaseError(db_err)
                if db_err.public.code.contains("InvalidArgument") =>
            {
                Self::RetryWithoutResumeTokens
            }
            FirestoreError::InvalidParametersError(_) => Self::Fatal,
            _ => Self::RetryAfterDelay,
        }
    }
}
