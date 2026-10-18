use super::listener::FirestoreListenerControl;
use super::registration::{FirestoreListenerRegistration, FirestoreListenerTargetsState};
use super::stream_targets::FirestoreListenerStreamTargets;
use crate::db::support::FirestoreListenSupport;
use crate::errors::*;
use crate::{
    FirestoreListenEvent, FirestoreListener, FirestoreListenerParams, FirestoreListenerTarget,
    FirestoreListenerTargetParams, FirestoreListenerTargetResumeType, FirestoreListenerToken,
    FirestoreResult, FirestoreResumeStateStorage,
};
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::*;
use std::future::Future;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::*;

impl<D, S> FirestoreListener<D, S>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
    S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    async fn until_shutdown<F: Future>(
        shutdown: &watch::Sender<bool>,
        future: F,
    ) -> Option<F::Output> {
        let mut receiver = shutdown.subscribe();
        tokio::select! {
            biased;
            _ = receiver.wait_for(|stopped| *stopped) => None,
            result = future => Some(result),
        }
    }

    pub(super) async fn listener_loop<FN, F>(
        db: D,
        storage: S,
        shutdown_signal: watch::Sender<bool>,
        targets_state: FirestoreListenerTargetsState,
        listener_params: FirestoreListenerParams,
        mut control_receiver: UnboundedReceiver<FirestoreListenerControl>,
        cb: FN,
    ) -> FirestoreResult<()>
    where
        D: FirestoreListenSupport + Clone + Send + Sync,
        FN: Fn(FirestoreListenEvent) -> F + Send + Sync,
        F: Future<Output = AnyBoxedErrResult<()>> + Send,
    {
        let effective_delay = listener_params
            .retry_delay
            .unwrap_or_else(|| std::time::Duration::from_secs(5));

        // Set once we have retried with every resume token discarded, so that a request Firestore
        // still rejects as invalid is treated as permanent instead of looping forever.
        let mut retried_without_resume_tokens = false;

        while !*shutdown_signal.borrow() {
            let no_targets = targets_state.read().is_empty();
            if no_targets {
                // Nothing to listen on yet. Idle on the control channel rather than opening an
                // empty stream, so that a listener can be started before its targets exist.
                debug!("Firestore listener has no targets. Waiting for one to be added...");
                match control_receiver.recv().await {
                    None | Some(FirestoreListenerControl::Shutdown) => {
                        shutdown_signal.send_replace(true);
                    }
                    // Nothing is being listened to, so there is nothing to remove or resynchronise.
                    Some(FirestoreListenerControl::TargetsAdded)
                    | Some(FirestoreListenerControl::TargetRemoved(..))
                    | Some(FirestoreListenerControl::ResyncTarget(_)) => {}
                }
                continue;
            }

            let (requests, request_reader) = tokio::sync::mpsc::unbounded_channel();
            let mut on_stream = FirestoreListenerStreamTargets::new(requests);
            Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
            if on_stream.targets.is_empty() {
                // Every target was removed while its resume state was read. A stream opened with
                // no target could wait for an answer that never comes.
                continue;
            }
            debug!(
                num_targets = on_stream.targets.len(),
                "Start listening on targets..."
            );

            let Some(connection) =
                Self::until_shutdown(&shutdown_signal, db.listen_doc_changes(request_reader)).await
            else {
                break;
            };
            match connection {
                Err(err) => {
                    Self::handle_listener_error(
                        err,
                        effective_delay,
                        &storage,
                        &targets_state,
                        &shutdown_signal,
                        &mut retried_without_resume_tokens,
                    )
                    .await?;
                }
                Ok(mut listen_stream) => loop {
                    let next_add_back = on_stream.next_add_back();
                    tokio::select! {
                        // Control messages go first, so that the stream state they record is in
                        // place before Firestore's answers to them are read.
                        biased;
                        control = control_receiver.recv() => {
                            match control {
                                None => {
                                    debug!("Listener dropped. Exiting...");
                                    shutdown_signal.send_replace(true);
                                    break;
                                }
                                Some(FirestoreListenerControl::Shutdown) => {
                                    debug!("Exiting from listener on targets...");
                                    control_receiver.close();
                                    break;
                                }
                                Some(FirestoreListenerControl::TargetsAdded) => {
                                    Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
                                }
                                Some(FirestoreListenerControl::TargetRemoved(target, registration)) => {
                                    on_stream.remove(&db, &target, registration, None)?;
                                    if on_stream.is_idle(&targets_state) {
                                        debug!("Firestore listener has no targets left. Closing the listen stream...");
                                        break;
                                    }
                                }
                                Some(FirestoreListenerControl::ResyncTarget(target)) => {
                                    if let Some(registration) = targets_state.registration_of(&target) {
                                        warn!(
                                            ?target,
                                            ?effective_delay,
                                            "Resynchronising a listener target from scratch at the cache's request.",
                                        );
                                        Self::forget_resume_states(&storage, &targets_state, &[(target.clone(), registration)]).await;
                                        // Without the delay a target that keeps diverging would
                                        // re-download in a tight loop.
                                        on_stream.remove(&db, &target, registration, Some(Instant::now() + effective_delay))?;
                                    }
                                }
                            }
                        }
                        () = async {
                            match next_add_back {
                                Some(at) => tokio::time::sleep_until(at).await,
                                None => std::future::pending().await,
                            }
                        } => {
                            Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
                        }
                        tried = listen_stream.try_next() => {
                            if *shutdown_signal.borrow() {
                                break;
                            }
                            else {
                                match tried {
                                    Ok(Some(event)) => {
                                        trace!(?event, "Received a listen response event to handle.");

                                        // The connection works, so a later rejection deserves its
                                        // own token-discarding retry.
                                        retried_without_resume_tokens = false;

                                        match event.response_type {
                                            Some(listen_response::ResponseType::TargetChange(target_change)) => {
                                                let change_type = target_change::TargetChangeType::try_from(
                                                    target_change.target_change_type,
                                                ).ok();
                                                let affected = Self::affected_targets(&targets_state, &on_stream, &target_change.target_ids);
                                                let cause = target_change.cause.clone();
                                                let resume_token = target_change.resume_token.clone();
                                                let target_ids = target_change.target_ids.clone();
                                                // A token for every target at once is a consistent
                                                // snapshot of the stream only on a NO_CHANGE with a
                                                // read time, which is where the Firebase SDKs adopt it.
                                                let stores_token = !resume_token.is_empty()
                                                    && (!target_ids.is_empty()
                                                        || (change_type == Some(target_change::TargetChangeType::NoChange)
                                                            && target_change.read_time.is_some()));

                                                // Anything listening on this - the cache above all -
                                                // has to see the change, and drop what it holds for a
                                                // reset target, before the target is listened to again.
                                                if let Err(err) = cb(listen_response::ResponseType::TargetChange(target_change)).await {
                                                    error!(%err, "Listener callback function error occurred.");
                                                    break;
                                                }

                                                // Stored only once the change has been applied. A
                                                // token saved before that would, after a failure
                                                // here, resume past a change nothing acted on.
                                                if stores_token
                                                    && !Self::store_resume_token(&storage, &targets_state, &on_stream, &resume_token, &target_ids).await
                                                {
                                                    break;
                                                }

                                                match (change_type, affected) {
                                                    (Some(target_change::TargetChangeType::Add), _) => {
                                                        on_stream.added(&target_ids);
                                                    }
                                                    (Some(target_change::TargetChangeType::Current), _) => {
                                                        on_stream.current(&target_ids);
                                                    }
                                                    (Some(target_change::TargetChangeType::Remove), _) => {
                                                        let removal = on_stream.removed(&target_ids, cause.as_ref(), Instant::now() + effective_delay);
                                                        if !removal.retried.is_empty() {
                                                            // The delay keeps a target Firestore keeps
                                                            // rejecting from being added back in a
                                                            // tight loop.
                                                            error!(
                                                                retried = ?removal.retried,
                                                                ?cause,
                                                                ?effective_delay,
                                                                "Firestore removed listener targets. Discarding their resume state and adding them back after the retry delay.",
                                                            );
                                                            Self::forget_resume_states(&storage, &targets_state, &removal.retried).await;
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
                                                            Self::end_targets(&storage, &targets_state, &removal.ended).await;
                                                        }
                                                        Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
                                                        if on_stream.is_idle(&targets_state) {
                                                            debug!("Firestore listener has no targets left. Closing the listen stream...");
                                                            break;
                                                        }
                                                    }
                                                    (Some(target_change::TargetChangeType::Reset), Ok(affected)) => {
                                                        // Firestore resends the initial state on this
                                                        // same stream and only then issues a new
                                                        // token. Dropping the old one now means a
                                                        // disconnect in between resumes from scratch
                                                        // rather than from a point the reset has
                                                        // already invalidated.
                                                        warn!(
                                                            ?affected,
                                                            "Firestore reset listener targets. Their initial state will be resent.",
                                                        );
                                                        Self::forget_resume_states(&storage, &targets_state, &affected).await;
                                                    }
                                                    _ => {}
                                                }
                                            }
                                            Some(response_type) => {
                                                if let Err(err) = cb(response_type).await {
                                                    error!(%err, "Listener callback function error occurred.");
                                                    break;
                                                }
                                            }
                                            None  =>  {}
                                        }
                                    }
                                    Ok(None) => {
                                        // A clean close still needs the retry delay: without it a
                                        // server that closes immediately would be reconnected to in
                                        // a tight loop.
                                        debug!(?effective_delay, "Listen stream closed. Reconnecting after the specified delay...");
                                        Self::until_shutdown(&shutdown_signal, tokio::time::sleep(effective_delay)).await;
                                        break;
                                    }
                                    Err(FirestoreError::DataNotFoundError(err)) if on_stream.crossed_removal => {
                                        // Firestore's answer to the crossed `RemoveTarget`. The
                                        // other targets are added back on a new stream at once.
                                        debug!(%err, "Firestore closed the listen stream over a target it had already removed. Reconnecting...");
                                        break;
                                    }
                                    Err(err) => {
                                        Self::handle_listener_error(
                                            err,
                                            effective_delay,
                                            &storage,
                                            &targets_state,
                                            &shutdown_signal,
                                            &mut retried_without_resume_tokens,
                                        ).await?;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }

    /// Resolves the targets a target change applies to, each with the registration it applies to.
    ///
    /// Firestore uses an empty set of target IDs to mean every target on the stream, which is how
    /// global resume tokens and stream-wide resets arrive. Such a change includes a target only
    /// once Firestore has acknowledged adding it. A change that names a target applies to it only
    /// while the stream carries the target's current registration, so a change still in flight
    /// for a removed target is not taken for a new one under the same ID. Target IDs this listener
    /// does not know about are ignored; an ID that is not a valid target at all is a protocol
    /// error and returns `Err`.
    fn affected_targets(
        targets_state: &FirestoreListenerTargetsState,
        on_stream: &FirestoreListenerStreamTargets,
        target_ids: &[i32],
    ) -> Result<Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)>, ()> {
        let targets = targets_state.read();

        if target_ids.is_empty() {
            return Ok(targets
                .iter()
                .filter(|(target, registered)| on_stream.includes(target, registered.registration))
                .map(|(target, registered)| (target.clone(), registered.registration))
                .collect());
        }

        let mut affected = Vec::with_capacity(target_ids.len());
        for target_id_num in target_ids {
            match FirestoreListenerTarget::try_from(*target_id_num) {
                Ok(target_id) => {
                    if let Some(registered) = targets.get(&target_id) {
                        if on_stream.carries(&target_id, registered.registration) {
                            affected.push((target_id, registered.registration));
                        }
                    }
                }
                Err(err) => {
                    error!(%err, target_id_num, "Listener system error - unexpected target ID.");
                    return Err(());
                }
            }
        }
        Ok(affected)
    }

    /// Adds to the stream every registered target it lacks, and every waiting one whose time has
    /// come, each with its resume state resolved first.
    async fn add_due_targets(
        db: &D,
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        on_stream: &mut FirestoreListenerStreamTargets,
    ) -> FirestoreResult<()> {
        let due = on_stream.take_due(&targets_state.read(), Instant::now());
        for target in due {
            if let Some((target_params, registration)) =
                Self::resolve_target(storage, targets_state, &target).await
            {
                on_stream.add(db, target_params, registration)?;
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
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        target: &FirestoreListenerTarget,
    ) -> Option<(FirestoreListenerTargetParams, FirestoreListenerRegistration)> {
        let (registration, unresolved) = {
            let targets = targets_state.read();
            let registered = targets.get(target)?;
            (
                registered.registration,
                registered.params.resume_type.is_none(),
            )
        };

        let stored = if unresolved {
            storage
                .read_resume_state(target)
                .await
                .unwrap_or_else(|err| {
                    warn!(%err, ?target, "Could not read the resume state of a listener target. Listening on it from scratch.");
                    None
                })
        } else {
            None
        };

        targets_state.update_if_current(target, registration, |params| {
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
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        on_stream: &FirestoreListenerStreamTargets,
        resume_token: &[u8],
        target_ids: &[i32],
    ) -> bool {
        let Ok(affected) = Self::affected_targets(targets_state, on_stream, target_ids) else {
            return false;
        };

        let new_token: FirestoreListenerToken = resume_token.to_vec().into();

        for (target_id, registration) in affected {
            if let Err(err) = storage
                .update_resume_token(&target_id, new_token.clone())
                .await
            {
                error!(%err, "Listener token storage error occurred.");
                return false;
            }
            let still_registered = targets_state
                .update_if_current(&target_id, registration, |params| {
                    params.resume_type =
                        Some(FirestoreListenerTargetResumeType::Token(new_token.clone()));
                })
                .is_some();
            if !still_registered {
                // Removed while the token was written, possibly after the removal forgot the
                // stored state, so the token can still be in the storage for a later
                // registration of the ID to resume from.
                if let Err(err) = storage.forget_resume_state(&target_id).await {
                    warn!(%err, ?target_id, "Could not forget the resume token of a removed listener target.");
                }
            }
        }

        true
    }

    /// Unregisters targets that have ended for good and forgets their resume state, unless the ID
    /// has been registered again since.
    async fn end_targets(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        ended: &[(FirestoreListenerTarget, FirestoreListenerRegistration)],
    ) {
        for (target_id, registration) in ended {
            if targets_state.remove_if_current(target_id, *registration) {
                if let Err(err) = storage.forget_resume_state(target_id).await {
                    warn!(%err, ?target_id, "Could not forget the resume state of an ended listener target.");
                }
            }
        }
    }

    /// Discards the stored resume state of the given registrations, so that they are listened to
    /// from scratch the next time they are added to a stream. A target registered again since
    /// keeps its state: it belongs to the new registration.
    async fn forget_resume_states(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        targets: &[(FirestoreListenerTarget, FirestoreListenerRegistration)],
    ) {
        for (target_id, registration) in targets {
            if targets_state.registration_of(target_id) != Some(*registration) {
                continue;
            }
            if let Err(err) = storage.forget_resume_state(target_id).await {
                warn!(%err, ?target_id, "Could not forget the resume state of a listener target.");
            }
            targets_state.update_if_current(target_id, *registration, |params| {
                params.resume_type = None;
            });
        }
    }

    async fn handle_listener_error(
        err: FirestoreError,
        delay: std::time::Duration,
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        shutdown_signal: &watch::Sender<bool>,
        retried_without_resume_tokens: &mut bool,
    ) -> FirestoreResult<()> {
        match Self::classify_listener_error(&err) {
            FirestoreListenerErrorAction::Retry => {
                debug!(%err, ?delay, "Listen EOF.. Restarting after the specified delay...");
                Self::until_shutdown(shutdown_signal, tokio::time::sleep(delay)).await;
            }
            FirestoreListenerErrorAction::RetryWithoutResumeTokens
                if !*retried_without_resume_tokens =>
            {
                // A stored resume token belongs to one target's query, so a stale or expired one
                // is rejected as invalid. Dropping every token costs a replay; treating this as
                // permanent would instead take down the listener for all the other targets too.
                *retried_without_resume_tokens = true;
                warn!(
                    %err, ?delay,
                    "Firestore rejected the listen request as invalid. Discarding all stored resume tokens and retrying once from scratch...",
                );
                let all_targets: Vec<(FirestoreListenerTarget, FirestoreListenerRegistration)> =
                    targets_state
                        .read()
                        .iter()
                        .map(|(target, registered)| (target.clone(), registered.registration))
                        .collect();
                Self::forget_resume_states(storage, targets_state, &all_targets).await;
                Self::until_shutdown(shutdown_signal, tokio::time::sleep(delay)).await;
            }
            // Either the retry without resume tokens was rejected too, so the request itself is
            // invalid, or no retry can fix the error at all.
            FirestoreListenerErrorAction::RetryWithoutResumeTokens
            | FirestoreListenerErrorAction::Fatal => return Err(err),
            FirestoreListenerErrorAction::RetryAfterDelay => {
                error!(%err, ?delay, "Listen error. Restarting after the specified delay...");
                Self::until_shutdown(shutdown_signal, tokio::time::sleep(delay)).await;
            }
        }
        Ok(())
    }

    fn classify_listener_error(err: &FirestoreError) -> FirestoreListenerErrorAction {
        match err {
            FirestoreError::DatabaseError(db_err)
                if db_err.details.contains("unexpected end of file")
                    || db_err.details.contains("stream error received") =>
            {
                FirestoreListenerErrorAction::Retry
            }
            FirestoreError::DatabaseError(db_err)
                if db_err.public.code.contains("InvalidArgument") =>
            {
                FirestoreListenerErrorAction::RetryWithoutResumeTokens
            }
            FirestoreError::InvalidParametersError(_) => FirestoreListenerErrorAction::Fatal,
            _ => FirestoreListenerErrorAction::RetryAfterDelay,
        }
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
