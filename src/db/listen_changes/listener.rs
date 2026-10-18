use super::registration::{
    FirestoreListenerRegisteredTarget, FirestoreListenerRegistration, FirestoreListenerTargetsState,
};
use crate::db::support::FirestoreListenSupport;
use crate::errors::*;
use crate::{
    FirestoreDb, FirestoreListenerTarget, FirestoreListenerTargetParams, FirestoreResult,
    FirestoreResumeStateStorage,
};
use futures::TryFutureExt;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use rvstruct::ValueStruct;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
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
        S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
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
        S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
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

/// A cheap, cloneable handle for asking a running listener to resynchronise a target.
///
/// Separate from [`FirestoreListener`] so that code reacting to listen events - the cache above
/// all - can ask for a resync without taking any lock the listener's own shutdown holds.
#[derive(Clone, Debug)]
pub struct FirestoreListenerControlHandle {
    control_writer: Arc<UnboundedSender<FirestoreListenerControl>>,
}

impl FirestoreListenerControlHandle {
    /// Discards the target's stored resume state and adds the target to the stream again after
    /// the retry delay, so Firestore sends its initial state again.
    ///
    /// Use this when the cached view of a target is known to have diverged from the server and
    /// resuming would only carry the divergence forward. Does nothing if the listener has stopped.
    pub fn resync_target(&self, target: FirestoreListenerTarget) {
        self.control_writer
            .send(FirestoreListenerControl::ResyncTarget(target))
            .ok();
    }
}

pub struct FirestoreListener<D, S>
where
    D: FirestoreListenSupport,
    S: FirestoreResumeStateStorage,
{
    db: D,
    storage: S,
    listener_params: FirestoreListenerParams,
    targets: FirestoreListenerTargetsState,
    next_registration: AtomicU64,
    shutdown_signal: watch::Sender<bool>,
    pub(super) shutdown_handle: Option<JoinHandle<FirestoreResult<()>>>,
    /// Created up front rather than in `start`, so that targets can be added and removed before,
    /// during and after the listener runs.
    control_writer: Arc<UnboundedSender<FirestoreListenerControl>>,
    control_reader: Option<UnboundedReceiver<FirestoreListenerControl>>,
}

impl<D, S> FirestoreListener<D, S>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
    S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
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
            db,
            storage,
            listener_params,
            targets: FirestoreListenerTargetsState::default(),
            next_registration: AtomicU64::new(0),
            shutdown_signal: watch::Sender::new(false),
            shutdown_handle: None,
            control_writer: Arc::new(control_writer),
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

        {
            let mut targets = self.targets.write();
            if targets.contains_key(&target_params.target) {
                return Err(FirestoreError::invalid_parameters(
                    "target",
                    format!(
                        "Listener target {} is already registered on this listener",
                        target_params.target.value()
                    ),
                ));
            }
            let registration = FirestoreListenerRegistration(
                self.next_registration.fetch_add(1, Ordering::Relaxed),
            );
            targets.insert(
                target_params.target.clone(),
                FirestoreListenerRegisteredTarget {
                    params: target_params,
                    registration,
                },
            );
        }

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
        let Some(removed) = self.targets.write().remove(target) else {
            return Ok(false);
        };

        // Sent before the await below, so that a caller cancelled there still takes the target
        // off the stream before the ID can be registered again.
        self.control_writer
            .send(FirestoreListenerControl::TargetRemoved(
                target.clone(),
                removed.registration,
            ))
            .ok();

        if let Err(err) = self.storage.forget_resume_state(target).await {
            warn!(%err, ?target, "Could not forget the resume state of a removed listener target.");
        }
        Ok(true)
    }

    /// The targets this listener currently listens on.
    pub fn targets(&self) -> Vec<FirestoreListenerTarget> {
        self.targets.read().keys().cloned().collect()
    }

    /// A handle for asking this listener to resynchronise a target while it runs.
    pub fn control_handle(&self) -> FirestoreListenerControlHandle {
        FirestoreListenerControlHandle {
            control_writer: self.control_writer.clone(),
        }
    }

    /// Whether this listener listens on the given target.
    pub fn has_target(&self, target: &FirestoreListenerTarget) -> bool {
        self.targets.read().contains_key(target)
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
    /// returns why. Always call `shutdown` when the listener is no longer needed, or the spawned
    /// task and its connection are never released.
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
            .targets
            .read()
            .values()
            .map(|registered| registered.params.clone())
            .collect();

        info!(
            num_targets = initial_targets.len(),
            "Starting a Firestore listener for targets...",
        );

        // Resolved eagerly rather than left to the loop, so that a broken resume-state storage is
        // reported to the caller of `start` instead of only appearing in the logs.
        for target_params in initial_targets {
            if target_params.resume_type.is_some() {
                continue;
            }
            let resume_type = self
                .storage
                .read_resume_state(&target_params.target)
                .map_err(|err| FirestoreError::system_error(format!("Listener init error: {err}")))
                .await?;

            if let Some(resume_type) = resume_type {
                let mut targets = self.targets.write();
                if let Some(registered) = targets.get_mut(&target_params.target) {
                    registered.params.resume_type = Some(resume_type);
                }
            }
        }

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

        let listener_loop = Self::listener_loop(
            self.db.clone(),
            self.storage.clone(),
            self.shutdown_signal.clone(),
            self.targets.clone(),
            self.listener_params.clone(),
            control_reader,
            cb,
        );
        self.shutdown_handle = Some(tokio::spawn(async move {
            listener_loop
                .await
                .inspect_err(|err| error!(%err, "Listen error. Exiting..."))
        }));
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

    pub(super) async fn shutdown_with(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> FirestoreResult<()> {
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
