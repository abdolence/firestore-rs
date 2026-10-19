use super::event_loop::FirestoreListenerLoop;
use super::registration::{FirestoreListenerRegistration, FirestoreListenerTargetsState};
use crate::db::support::FirestoreListenSupport;
use crate::errors::*;
use crate::{
    FirestoreDb, FirestoreListenerTarget, FirestoreListenerTargetParams, FirestoreResult,
    FirestoreResumeStateStorage,
};
use futures::TryFutureExt;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, WeakUnboundedSender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::*;

impl FirestoreDb {
    /// Creates a listener with the default retry delay.
    ///
    /// `storage` decides what a restart replays: [`crate::FirestoreTempFilesListenStateStorage`]
    /// persists each target's resume token to disk, so a restarted process continues from where
    /// it left off, while [`crate::FirestoreMemListenStateStorage`] keeps nothing across restarts
    /// and replays every target's current state from scratch. Register what to listen on with
    /// [`FirestoreListener::add_target`], then call [`FirestoreListener::start`] to begin
    /// receiving events and [`FirestoreListener::shutdown`] to stop.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use firestore::*;
    /// # async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let mut listener = db.create_listener(FirestoreTempFilesListenStateStorage::new()).await?;
    ///
    /// db.fluent()
    ///     .select()
    ///     .from("test")
    ///     .listen()
    ///     .add_target(FirestoreListenerTarget::new(1000), &mut listener)?;
    ///
    /// listener
    ///     .start(|event| async move {
    ///         println!("{event:?}");
    ///         Ok(())
    ///     })
    ///     .await?;
    ///
    /// listener.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_listener<S>(
        &self,
        storage: S,
    ) -> FirestoreResult<FirestoreListener<FirestoreDb, S>>
    where
        S: FirestoreResumeStateStorage + Send + Sync + 'static,
    {
        self.create_listener_with_params(storage, FirestoreListenerParams::new())
            .await
    }

    /// Creates a listener with the given [`FirestoreListenerParams`], for example to override the
    /// retry delay. See [`create_listener`](Self::create_listener) for the storage argument and a
    /// full lifecycle example.
    pub async fn create_listener_with_params<S>(
        &self,
        storage: S,
        params: FirestoreListenerParams,
    ) -> FirestoreResult<FirestoreListener<FirestoreDb, S>>
    where
        S: FirestoreResumeStateStorage + Send + Sync + 'static,
    {
        FirestoreListener::new(self.clone(), storage, params).await
    }
}

pub type FirestoreListenEvent = listen_response::ResponseType;

#[derive(Debug, Clone, Eq, PartialEq, Builder)]
pub struct FirestoreListenerParams {
    pub retry_delay: Option<std::time::Duration>,
}

/// A message to a running listener loop.
///
/// [`TargetsAdded`](Self::TargetsAdded) carries no payload: the shared target map is the single
/// source of truth, and the loop adds whatever registered target its stream lacks. A removal
/// names the registration it removes, because the ID may be registered again before the loop gets
/// to it, and the stream still has to drop the old registration first.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum FirestoreListenerControl {
    Shutdown,
    TargetsAdded,
    TargetRemoved(FirestoreListenerTarget, FirestoreListenerRegistration),
    /// Listen to this target again from scratch, discarding its stored resume state.
    ResyncTarget(FirestoreListenerTarget),
}

/// A cheap, cloneable handle for asking a listener whether it listens on a target, and for
/// removing or resynchronising one while it runs.
///
/// Separate from [`FirestoreListener`] so that code reacting to listen events - the callback given
/// to [`start`](FirestoreListener::start), or the cache - can do so without taking any lock the
/// listener's own shutdown holds. The handle shares the listener's resume-state storage `S`
/// rather than a copy of it.
///
/// A handle does not keep the listener's loop running, so the callback can hold one and the loop
/// still ends once the listener is dropped.
#[derive(Debug)]
pub struct FirestoreListenerControlHandle<S> {
    /// Weak, because the loop ends only once every strong sender is gone, and the callback the
    /// loop owns may hold a handle.
    weak_control_writer: WeakUnboundedSender<FirestoreListenerControl>,
    targets: FirestoreListenerTargetsState,
    storage: Arc<S>,
}

impl<S> FirestoreListenerControlHandle<S>
where
    S: FirestoreResumeStateStorage,
{
    /// Whether the listener listens on the given target, as [`FirestoreListener::has_target`].
    ///
    /// Inside the callback, the answer for a target in a
    /// [`Remove`](crate::FirestoreListenerTargetChangeType::Remove) target change is final: a target
    /// Firestore has ended for good is already unregistered, and one it removed for a cause a retry
    /// can fix is still registered and is added back after the retry delay.
    ///
    /// Once the listener has been shut down or dropped, or its loop has stopped on its own, this
    /// still answers from the targets registered with it, though none of them is listened to
    /// anymore.
    pub fn has_target(&self, target: &FirestoreListenerTarget) -> bool {
        self.targets.read().contains_key(target)
    }

    /// Stops listening on a target and forgets its stored resume state, as
    /// [`FirestoreListener::remove_target`].
    ///
    /// The callback can await this without racing the loop, which waits for the callback and then
    /// takes the target off the stream before it reads the next event. For a target in a
    /// [`Remove`](crate::FirestoreListenerTargetChangeType::Remove) target change, this keeps one
    /// Firestore removed for a cause a retry can fix from being added back, and returns `false` for
    /// one Firestore has ended for good, which is already unregistered.
    ///
    /// Once the listener has been shut down or dropped, or its loop has stopped on its own, there
    /// is no stream to take the target off, but this still unregisters the target, forgets its
    /// stored resume state and returns whether it was registered.
    pub async fn remove_target(&self, target: &FirestoreListenerTarget) -> FirestoreResult<bool> {
        let Some(removed) = self.targets.write().remove(target) else {
            return Ok(false);
        };

        // Sent before the await below, so that a caller cancelled there still takes the target
        // off the stream before the ID can be registered again.
        if let Some(control_writer) = self.weak_control_writer.upgrade() {
            control_writer
                .send(FirestoreListenerControl::TargetRemoved(
                    target.clone(),
                    removed.registration,
                ))
                .ok();
        }

        if let Err(err) = self.storage.forget_resume_state(target).await {
            warn!(%err, ?target, "Could not forget the resume state of a removed listener target.");
        }
        Ok(true)
    }

    /// Discards the target's stored resume state and adds the target to the stream again after
    /// the retry delay, so Firestore sends its initial state again.
    ///
    /// Use this when the cached view of a target is known to have diverged from the server and
    /// resuming would only carry the divergence forward. Does nothing once the listener has been
    /// shut down or dropped, or its loop has stopped on its own.
    pub fn resync_target(&self, target: FirestoreListenerTarget) {
        if let Some(control_writer) = self.weak_control_writer.upgrade() {
            control_writer
                .send(FirestoreListenerControl::ResyncTarget(target))
                .ok();
        }
    }
}

// Derived, `Clone` would require `S: Clone`, which sharing the storage does not need.
impl<S> Clone for FirestoreListenerControlHandle<S> {
    fn clone(&self) -> Self {
        Self {
            weak_control_writer: self.weak_control_writer.clone(),
            targets: self.targets.clone(),
            storage: self.storage.clone(),
        }
    }
}

pub struct FirestoreListener<D, S>
where
    D: FirestoreListenSupport,
    S: FirestoreResumeStateStorage,
{
    db: D,
    listener_params: FirestoreListenerParams,
    shutdown_signal: watch::Sender<bool>,
    shutdown_handle: Option<JoinHandle<FirestoreResult<()>>>,
    /// The only strong sender of the control channel, so that the loop ends once the listener is
    /// dropped, whatever handles are left.
    control_writer: UnboundedSender<FirestoreListenerControl>,
    /// Created up front rather than in `start`, so that targets can be added and removed before,
    /// during and after the listener runs.
    control: FirestoreListenerControlHandle<S>,
    control_reader: Option<UnboundedReceiver<FirestoreListenerControl>>,
}

impl<D, S> FirestoreListener<D, S>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
    S: FirestoreResumeStateStorage + Send + Sync + 'static,
{
    /// Creates a listener with no targets yet, for any type implementing `FirestoreListenSupport`.
    ///
    /// Prefer [`FirestoreDb::create_listener`] or [`FirestoreDb::create_listener_with_params`]
    /// when listening through a [`FirestoreDb`] - this constructor exists so a listener can be
    /// built against another implementation instead. Call [`FirestoreListener::add_target`] to
    /// register what to listen on and [`FirestoreListener::start`] to begin receiving events.
    pub async fn new(
        db: D,
        storage: S,
        listener_params: FirestoreListenerParams,
    ) -> FirestoreResult<FirestoreListener<D, S>> {
        let (control_writer, control_reader) = tokio::sync::mpsc::unbounded_channel();

        Ok(FirestoreListener {
            control: FirestoreListenerControlHandle {
                weak_control_writer: control_writer.downgrade(),
                targets: FirestoreListenerTargetsState::default(),
                storage: Arc::new(storage),
            },
            control_writer,
            db,
            listener_params,
            shutdown_signal: watch::Sender::new(false),
            shutdown_handle: None,
            control_reader: Some(control_reader),
        })
    }

    /// Adds a target to listen on.
    ///
    /// This works before and after [`start`](Self::start): a target added to a running listener
    /// is added to its open listen stream, and the other targets keep listening as they were.
    ///
    /// Returns [`FirestoreError::InvalidParametersError`] if the target cannot be turned into a
    /// listen request - an invalid ID or document ID, for example - if the listener has been shut
    /// down, or if a target with the same ID is already registered - reusing an ID for a different
    /// query would resume it from a token that does not belong to it. Returns
    /// [`FirestoreError::SystemError`] if the listener's loop has ended on its own;
    /// [`shutdown`](Self::shutdown) then returns why.
    pub fn add_target(&self, target_params: FirestoreListenerTargetParams) -> FirestoreResult<()> {
        // Built only to be checked, so that a target no stream could take is rejected here instead
        // of stopping the running loop for every other target.
        self.db.add_target_request(target_params.clone())?;

        if *self.shutdown_signal.borrow() {
            return Err(FirestoreError::invalid_parameters(
                "target",
                "Cannot add a target to a listener that has been shut down",
            ));
        }
        if self
            .shutdown_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Err(FirestoreError::system_error(
                "Cannot add a target to a listener whose loop has ended",
            ));
        }

        self.control.targets.register(target_params)?;

        // A send error only means the loop is gone, which the shutdown check above already covers.
        self.control_writer
            .send(FirestoreListenerControl::TargetsAdded)
            .ok();
        Ok(())
    }

    /// Stops listening on a target and forgets its stored resume state.
    ///
    /// Returns `false` if the target was not registered. Unlike [`add_target`](Self::add_target)
    /// this is asynchronous, because the resume state has to be forgotten before the target ID can
    /// safely be handed out again. If the returned future is dropped before it completes, the
    /// target is still taken off the stream, but its stored resume state may be kept.
    ///
    /// A running listener keeps the target on its stream until Firestore acknowledges the
    /// removal, so events for it can reach the callback until then. The acknowledgement itself
    /// reaches the callback as a [`Remove`](crate::FirestoreListenerTargetChangeType::Remove)
    /// target change with no `cause`; a `Remove` with a `cause` is Firestore removing a target on
    /// its own.
    ///
    /// When Firestore removes a target on its own while the removal is on its way, no
    /// acknowledgement comes: Firestore closes the stream with `NOT_FOUND` instead, and the
    /// listener reconnects at once and adds the other targets back.
    pub async fn remove_target(&self, target: &FirestoreListenerTarget) -> FirestoreResult<bool> {
        self.control.remove_target(target).await
    }

    /// The targets this listener currently listens on.
    pub fn targets(&self) -> Vec<FirestoreListenerTarget> {
        self.control.targets.read().keys().cloned().collect()
    }

    /// A handle for checking, removing and resynchronising this listener's targets.
    ///
    /// The handle keeps working after [`start`](Self::start), so the callback can hold one and
    /// remove the targets it no longer needs without reaching the listener, which `start` and
    /// [`shutdown`](Self::shutdown) borrow mutably.
    pub fn control_handle(&self) -> FirestoreListenerControlHandle<S> {
        self.control.clone()
    }

    /// Whether this listener listens on the given target.
    ///
    /// A target Firestore removes on its own stays registered while the listener means to add it
    /// back after the retry delay. One Firestore has ended for good, by a `cause` no retry can fix
    /// or by finishing a `once` target, is unregistered before the callback receives the
    /// [`Remove`](crate::FirestoreListenerTargetChangeType::Remove) target change, so from inside
    /// that callback this already tells whether the target will be added back.
    pub fn has_target(&self, target: &FirestoreListenerTarget) -> bool {
        self.control.has_target(target)
    }

    /// Starts listening and spawns the loop that keeps the stream open, invoking `cb` for every
    /// event.
    ///
    /// Targets added before this call resolve their stored resume state first, so a restart with
    /// [`crate::FirestoreTempFilesListenStateStorage`] continues from where it left off instead of
    /// replaying every change; a target with no stored state - the first run, or one using
    /// [`crate::FirestoreMemListenStateStorage`] - listens from scratch and Firestore sends its
    /// current state as the initial snapshot.
    ///
    /// The returned future resolves once the loop is spawned, not once it exits: the loop keeps
    /// running, reconnecting on a dropped stream, until [`shutdown`](Self::shutdown) is called or
    /// it stops on its own, after which [`add_target`](Self::add_target) fails and `shutdown`
    /// returns why. Call `shutdown` when the listener is no longer needed: it lets the current
    /// callback and resume-state writes finish. Dropping the listener instead ends the loop without
    /// waiting for it, even while the callback holds a [control handle](Self::control_handle). The
    /// loop then ends the next time it reads its control channel, which it does between stream
    /// events and while it has no targets, but not while it connects or waits out the retry delay.
    ///
    /// A target that has ended for good, including a finished `once` target, stays unregistered
    /// even if `cb` returns an error for its
    /// [`Remove`](crate::FirestoreListenerTargetChangeType::Remove) target change; see
    /// [`has_target`](Self::has_target).
    ///
    /// Returns an error if this listener has already been started, or if reading a target's
    /// stored resume state fails.
    pub async fn start<FN, F>(&mut self, cb: FN) -> FirestoreResult<()>
    where
        FN: Fn(FirestoreListenEvent) -> F + Send + Sync + 'static,
        F: Future<Output = AnyBoxedErrResult<()>> + Send + 'static,
    {
        if self.control_reader.is_none() {
            return Err(FirestoreError::invalid_parameters(
                "listener",
                "This Firestore listener has already been started",
            ));
        }

        let initial_targets: Vec<FirestoreListenerTargetParams> = self
            .control
            .targets
            .read()
            .values()
            .map(|registered| registered.params.clone())
            .collect();

        info!(
            num_targets = initial_targets.len(),
            "Starting a Firestore listener for targets...",
        );

        self.read_stored_resume_states(initial_targets).await?;

        let mut control_reader = self
            .control_reader
            .take()
            .expect("checked above, and start holds the listener exclusively");

        // Targets added or removed before `start` are already reflected in the shared state the
        // loop is about to connect with, so their queued notifications have nothing left to do.
        while let Ok(queued) = control_reader.try_recv() {
            if queued == FirestoreListenerControl::Shutdown {
                self.shutdown_signal.send_replace(true);
            }
        }

        let listener_loop = FirestoreListenerLoop::new(
            self.db.clone(),
            self.control.storage.clone(),
            self.shutdown_signal.clone(),
            self.control.targets.clone(),
            &self.listener_params,
            control_reader,
            cb,
        );
        self.shutdown_handle = Some(tokio::spawn(async move {
            listener_loop
                .run()
                .await
                .inspect_err(|err| error!(%err, "Listen error. Exiting..."))
        }));
        Ok(())
    }

    /// Resolved eagerly rather than left to the loop, so that a broken resume-state storage is
    /// reported to the caller of `start` instead of only appearing in the logs.
    async fn read_stored_resume_states(
        &self,
        initial_targets: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<()> {
        for target_params in initial_targets {
            if target_params.resume_type.is_some() {
                continue;
            }
            let resume_type = self
                .control
                .storage
                .read_resume_state(&target_params.target)
                .map_err(|err| FirestoreError::system_error(format!("Listener init error: {err}")))
                .await?;

            if let Some(resume_type) = resume_type {
                let mut targets = self.control.targets.write();
                if let Some(registered) = targets.get_mut(&target_params.target) {
                    registered.params.resume_type = Some(resume_type);
                }
            }
        }
        Ok(())
    }

    /// Stops the listener's loop and waits for it to exit, releasing its connection.
    ///
    /// Finishes the current callback and resume-state writes, but interrupts connection attempts
    /// and reconnect delays. Use [`shutdown_with_timeout`](Self::shutdown_with_timeout) to limit
    /// how long to wait for application work.
    ///
    /// Returns the error the loop stopped with, if it stopped on its own, or a
    /// [`FirestoreError::SystemError`] if it panicked. Safe to call on a listener that was never
    /// started, or one already shut down: both return `Ok`.
    ///
    /// The loop stops on its own only on an error a retry cannot fix:
    ///
    /// - Firestore rejecting the listen request as invalid again after every resume token was
    ///   discarded;
    /// - a [`FirestoreError::InvalidParametersError`] from the listen request;
    /// - a target whose `AddTarget` or `RemoveTarget` request cannot be built;
    /// - a panic in the callback or the resume-state storage.
    ///
    /// Every other error, a dropped connection or a closed stream among them, is retried after the
    /// retry delay, and a target Firestore removes is handled on its own, without stopping the
    /// loop.
    pub async fn shutdown(&mut self) -> FirestoreResult<()> {
        self.shutdown_with(None).await
    }

    /// Shuts down gracefully, then aborts and joins the task if it outlives `timeout`.
    /// A zero timeout aborts immediately.
    ///
    /// Aborting can interrupt callbacks and resume-state writes, so changes may be replayed.
    /// Returns what [`shutdown`](Self::shutdown) returns; an aborted loop returns `Ok`.
    pub async fn shutdown_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> FirestoreResult<()> {
        self.shutdown_with(Some(timeout)).await
    }

    async fn shutdown_with(&mut self, timeout: Option<std::time::Duration>) -> FirestoreResult<()> {
        debug!("Shutting down Firestore listener...");
        self.shutdown_signal.send_replace(true);
        self.control_writer
            .send(FirestoreListenerControl::Shutdown)
            .ok();
        // Keep ownership until joined, even if this shutdown future is cancelled.
        let stopped_with = match self.shutdown_handle.as_mut() {
            None => Ok(()),
            Some(handle) => {
                let finished = match timeout {
                    None => Some((&mut *handle).await),
                    // `tokio::time::timeout` would still let the task run until the next timer
                    // tick.
                    Some(timeout) if timeout.is_zero() => None,
                    Some(timeout) => tokio::time::timeout(timeout, &mut *handle).await.ok(),
                };
                let joined = match finished {
                    Some(joined) => joined,
                    None => {
                        handle.abort();
                        handle.await
                    }
                };
                match joined {
                    Ok(stopped_with) => stopped_with,
                    // Aborted above, as the caller's timeout asked.
                    Err(err) if err.is_cancelled() => Ok(()),
                    Err(err) => Err(FirestoreError::system_error(format!(
                        "Firestore listener loop panicked: {err}"
                    ))),
                }
            }
        };
        self.shutdown_handle = None;
        debug!("Shutting down Firestore listener has been finished...");
        stopped_with
    }
}

#[cfg(test)]
mod lifecycle_tests;
