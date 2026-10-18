use crate::db::safe_document_path;
use crate::db::support::FirestoreListenSupport;
use crate::errors::*;
use crate::timestamp_utils::to_timestamp;
use crate::FirestoreInstant;
use crate::{
    FirestoreDb, FirestoreQueryParams, FirestoreRequestOptions, FirestoreResult,
    FirestoreResumeStateStorage,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
use rvstruct::ValueStruct;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::*;

#[cfg(test)]
mod lifecycle_tests;

#[derive(Debug, Clone, Builder)]
pub struct FirestoreListenerTargetParams {
    pub target: FirestoreListenerTarget,
    pub target_type: FirestoreTargetType,
    pub resume_type: Option<FirestoreListenerTargetResumeType>,
    pub add_target_once: Option<bool>,
    pub labels: HashMap<String, String>,

    /// Request options (e.g. request tags) for the listen requests of this target.
    pub request_options: Option<FirestoreRequestOptions>,
}

impl FirestoreListenerTargetParams {
    /// Checks that the target ID is valid before the target is registered on a listener.
    ///
    /// Returns an error if the target ID is zero or greater than `i32::MAX` - Firestore's wire
    /// protocol represents a target ID as a signed 32-bit integer.
    pub fn validate(&self) -> FirestoreResult<()> {
        self.target.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreCollectionDocuments {
    pub parent: Option<String>,
    pub collection: String,
    pub documents: Vec<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum FirestoreTargetType {
    Query(FirestoreQueryParams),
    Documents(FirestoreCollectionDocuments),
}

#[derive(Debug, Clone)]
pub enum FirestoreListenerTargetResumeType {
    Token(FirestoreListenerToken),
    ReadTime(FirestoreInstant),
}

#[async_trait]
impl FirestoreListenSupport for FirestoreDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        mut requests: UnboundedReceiver<ListenRequest>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        // Closing the response must also end the request producer owned by the HTTP/2 transport.
        let (keep_request_open, response_closed) = tokio::sync::oneshot::channel::<()>();
        let request = gcloud_sdk::tonic::Request::new(
            futures::stream::poll_fn(move |cx| requests.poll_recv(cx))
                .chain(futures::stream::pending())
                .take_until(response_closed),
        );

        let response = self.client().get().listen(request).await?;

        Ok(futures::stream::unfold(
            (response.into_inner(), keep_request_open),
            |(mut response, keep_request_open)| async move {
                response
                    .next()
                    .await
                    .map(|item| (item.map_err(Into::into), (response, keep_request_open)))
            },
        )
        .boxed())
    }

    fn add_target_request(
        &self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            database: self.get_database_path().to_string(),
            labels: target_params.labels,
            request_options: self.resolve_request_options(target_params.request_options.as_ref()),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: target_params.target.try_into()?,
                once: target_params.add_target_once.unwrap_or(false),
                target_type: Some(match target_params.target_type {
                    FirestoreTargetType::Query(query_params) => {
                        target::TargetType::Query(target::QueryTarget {
                            parent: query_params
                                .parent
                                .as_ref()
                                .unwrap_or_else(|| self.get_documents_path())
                                .clone(),
                            query_type: Some(target::query_target::QueryType::StructuredQuery(
                                query_params.try_into()?,
                            )),
                        })
                    }
                    FirestoreTargetType::Documents(collection_documents) => {
                        target::TargetType::Documents(target::DocumentsTarget {
                            documents: collection_documents
                                .documents
                                .into_iter()
                                .map(|doc_id| {
                                    safe_document_path(
                                        collection_documents
                                            .parent
                                            .as_deref()
                                            .unwrap_or_else(|| self.get_documents_path()),
                                        collection_documents.collection.as_str(),
                                        doc_id,
                                    )
                                })
                                .collect::<FirestoreResult<Vec<String>>>()?,
                        })
                    }
                }),
                resume_type: target_params
                    .resume_type
                    .map(|resume_type| match resume_type {
                        FirestoreListenerTargetResumeType::Token(token) => {
                            target::ResumeType::ResumeToken(token.into_value())
                        }
                        FirestoreListenerTargetResumeType::ReadTime(dt) => {
                            target::ResumeType::ReadTime(to_timestamp(dt))
                        }
                    }),
                ..Default::default()
            })),
        })
    }

    fn remove_target_request(
        &self,
        target: &FirestoreListenerTarget,
    ) -> FirestoreResult<ListenRequest> {
        Ok(ListenRequest {
            database: self.get_database_path().to_string(),
            labels: HashMap::new(),
            request_options: self.resolve_request_options(None),
            target_change: Some(listen_request::TargetChange::RemoveTarget(
                target.clone().try_into()?,
            )),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreListenerTarget(u32);

impl FirestoreListenerTarget {
    /// Checks the target ID against Firestore's wire representation.
    ///
    /// Returns an error if the ID is zero, or greater than `i32::MAX` - Firestore's `Target`
    /// message carries it as a signed 32-bit integer, so a larger value cannot be sent.
    pub fn validate(&self) -> FirestoreResult<()> {
        if *self.value() == 0 {
            Err(FirestoreError::invalid_parameters(
                "target_id",
                "Listener target ID cannot be zero",
            ))
        } else if *self.value() > i32::MAX as u32 {
            Err(FirestoreError::invalid_parameters(
                "target_id",
                format!(
                    "Listener target ID cannot be more than: {}. {} is specified",
                    i32::MAX,
                    self.value()
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl TryInto<i32> for FirestoreListenerTarget {
    type Error = FirestoreError;

    fn try_into(self) -> FirestoreResult<i32> {
        self.validate()?;
        (*self.value()).try_into().map_err(|e| {
            FirestoreError::invalid_parameters(
                "target_id",
                format!("Invalid target ID: {} {}", self.value(), e),
            )
        })
    }
}

impl TryFrom<i32> for FirestoreListenerTarget {
    type Error = FirestoreError;

    fn try_from(value: i32) -> FirestoreResult<Self> {
        value
            .try_into()
            .map_err(|e| {
                FirestoreError::invalid_parameters(
                    "target_id",
                    format!("Invalid target ID: {value} {e}"),
                )
            })
            .map(FirestoreListenerTarget)
    }
}

#[derive(Clone, Debug, ValueStruct)]
pub struct FirestoreListenerToken(Vec<u8>);

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

/// The set of targets a listener listens on, shared between the handle and its running loop.
///
/// A `std::sync::RwLock` rather than tokio's: it keeps [`FirestoreListener::add_target`]
/// synchronous, and the compiler stops a guard being held across an await. Every use site clones
/// out what it needs and drops the guard before awaiting.
pub(crate) type FirestoreListenerTargetsState =
    Arc<std::sync::RwLock<HashMap<FirestoreListenerTarget, FirestoreListenerTargetParams>>>;

/// A message to a running listener loop.
///
/// [`TargetsAdded`](Self::TargetsAdded) carries no payload: the shared target map is the single
/// source of truth, and the loop adds whatever registered target its stream lacks. A removal
/// names its target, because the ID may be registered again before the loop gets to it, and the
/// stream still has to drop the old target first.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum FirestoreListenerControl {
    Shutdown,
    TargetsAdded,
    TargetRemoved(FirestoreListenerTarget),
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
    shutdown_signal: watch::Sender<bool>,
    /// The error the loop gave up on, set before it signals shutdown.
    stop_error: Arc<OnceLock<FirestoreError>>,
    shutdown_handle: Option<JoinHandle<()>>,
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
            targets: Arc::new(std::sync::RwLock::new(HashMap::new())),
            shutdown_signal: watch::Sender::new(false),
            stop_error: Arc::new(OnceLock::new()),
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
    /// Returns an error if the listener has been shut down, or if a target with the same ID is
    /// already registered - reusing an ID for a different query would resume it from a token that
    /// does not belong to it.
    pub fn add_target(&self, target_params: FirestoreListenerTargetParams) -> FirestoreResult<()> {
        target_params.validate()?;

        if *self.shutdown_signal.borrow() {
            return Err(FirestoreError::invalid_parameters(
                "target",
                "Cannot add a target to a listener that has been shut down",
            ));
        }

        {
            let mut targets = self
                .targets
                .write()
                .expect("listener targets lock poisoned");
            if targets.contains_key(&target_params.target) {
                return Err(FirestoreError::invalid_parameters(
                    "target",
                    format!(
                        "Listener target {} is already registered on this listener",
                        target_params.target.value()
                    ),
                ));
            }
            targets.insert(target_params.target.clone(), target_params);
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
    /// safely be handed out again.
    pub async fn remove_target(&self, target: &FirestoreListenerTarget) -> FirestoreResult<bool> {
        let removed = {
            let mut targets = self
                .targets
                .write()
                .expect("listener targets lock poisoned");
            targets.remove(target).is_some()
        };

        if !removed {
            return Ok(false);
        }

        if let Err(err) = self.storage.forget_resume_state(target).await {
            warn!(%err, ?target, "Could not forget the resume state of a removed listener target.");
        }

        self.control_writer
            .send(FirestoreListenerControl::TargetRemoved(target.clone()))
            .ok();
        Ok(true)
    }

    /// The targets this listener currently listens on.
    pub fn targets(&self) -> Vec<FirestoreListenerTarget> {
        self.targets
            .read()
            .expect("listener targets lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// A handle for asking this listener to resynchronise a target while it runs.
    pub fn control_handle(&self) -> FirestoreListenerControlHandle {
        FirestoreListenerControlHandle {
            control_writer: self.control_writer.clone(),
        }
    }

    /// Whether this listener listens on the given target.
    pub fn has_target(&self, target: &FirestoreListenerTarget) -> bool {
        self.targets
            .read()
            .expect("listener targets lock poisoned")
            .contains_key(target)
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
    /// it gives up on an error that [`stopped`](Self::stopped) reports.
    /// Always call `shutdown` when the listener is no longer needed, or the spawned task and its
    /// connection are never released.
    ///
    /// Returns an error if this listener has already been started, or if reading a target's
    /// stored resume state fails.
    pub async fn start<FN, F>(&mut self, cb: FN) -> FirestoreResult<()>
    where
        FN: Fn(FirestoreListenEvent) -> F + Send + Sync + 'static,
        F: Future<Output = AnyBoxedErrResult<()>> + Send + 'static,
    {
        let initial_targets: Vec<FirestoreListenerTargetParams> = self
            .targets
            .read()
            .expect("listener targets lock poisoned")
            .values()
            .cloned()
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
                .map_err(|err| {
                    FirestoreError::SystemError(FirestoreSystemError::new(
                        FirestoreErrorPublicGenericDetails::new("SystemError".into()),
                        format!("Listener init error: {err}"),
                    ))
                })
                .await?;

            if let Some(resume_type) = resume_type {
                let mut targets = self
                    .targets
                    .write()
                    .expect("listener targets lock poisoned");
                if let Some(target) = targets.get_mut(&target_params.target) {
                    target.resume_type = Some(resume_type);
                }
            }
        }

        let Some(mut control_reader) = self.control_reader.take() else {
            return Err(FirestoreError::invalid_parameters(
                "listener",
                "This Firestore listener has already been started",
            ));
        };

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
        let shutdown_signal = self.shutdown_signal.clone();
        let stop_error = self.stop_error.clone();
        self.shutdown_handle = Some(tokio::spawn(async move {
            if let Err(err) = listener_loop.await {
                error!(%err, "Listen error. Exiting...");
                stop_error.set(err).ok();
                shutdown_signal.send_replace(true);
            }
        }));
        Ok(())
    }

    /// Stops the listener's loop and waits for it to exit, releasing its connection.
    ///
    /// Finishes the current callback and resume-state writes, but interrupts connection attempts
    /// and reconnect delays. Use [`shutdown_with_timeout`](Self::shutdown_with_timeout) to limit
    /// how long to wait for application work.
    ///
    /// Safe to call on a listener that was never started, or one already shut down - both are a
    /// no-op beyond signalling shutdown. Never returns an error itself; if the spawned loop
    /// panicked, that is logged rather than surfaced here.
    pub async fn shutdown(&mut self) -> FirestoreResult<()> {
        self.shutdown_with(None).await
    }

    /// Shuts down gracefully, then aborts and joins the task if it outlives `timeout`.
    /// A zero timeout aborts immediately.
    ///
    /// Aborting can interrupt callbacks and resume-state writes, so changes may be replayed.
    pub async fn shutdown_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> FirestoreResult<()> {
        self.shutdown_with(Some(timeout)).await
    }

    /// Waits until the listener has stopped, and returns the error that stopped it, if any.
    ///
    /// Resolves with `None` once [`shutdown`](Self::shutdown) has been called, and with the error
    /// once the loop has given up on one that a retry cannot fix: a target that cannot be turned
    /// into a listen request, or Firestore rejecting the listen request as invalid again after
    /// every resume token was discarded. Stays pending until then, including before
    /// [`start`](Self::start).
    ///
    /// A listener stopped by an error rejects new targets, and still needs `shutdown` to release
    /// its task.
    pub async fn stopped(&self) -> Option<&FirestoreError> {
        // `self` owns the sender, so the channel cannot close while this waits.
        let _ = self
            .shutdown_signal
            .subscribe()
            .wait_for(|stopped| *stopped)
            .await;
        self.stop_error.get()
    }

    async fn shutdown_with(&mut self, timeout: Option<std::time::Duration>) -> FirestoreResult<()> {
        debug!("Shutting down Firestore listener...");
        self.shutdown_signal.send_replace(true);
        self.control_writer
            .send(FirestoreListenerControl::Shutdown)
            .ok();
        // Keep ownership until joined, even if this shutdown future is cancelled.
        if let Some(handle) = self.shutdown_handle.as_mut() {
            let finished = match timeout {
                None => Some((&mut *handle).await),
                // `tokio::time::timeout` would still let the task run until the next timer tick.
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
            if let Err(err) = joined {
                if !err.is_cancelled() {
                    warn!(%err, "Firestore listener exit error!");
                }
            }
        }
        self.shutdown_handle = None;
        debug!("Shutting down Firestore listener has been finished...");
        Ok(())
    }

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

    async fn listener_loop<FN, F>(
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
            let no_targets = targets_state
                .read()
                .expect("listener targets lock poisoned")
                .is_empty();
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
                    | Some(FirestoreListenerControl::TargetRemoved(_))
                    | Some(FirestoreListenerControl::ResyncTarget(_)) => {}
                }
                continue;
            }

            let (requests, request_reader) = tokio::sync::mpsc::unbounded_channel();
            let mut on_stream = FirestoreListenerStreamTargets::new(requests);
            Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
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
                                Some(FirestoreListenerControl::TargetRemoved(target)) => {
                                    on_stream.remove(&db, &target, None)?;
                                }
                                Some(FirestoreListenerControl::ResyncTarget(target)) => {
                                    warn!(
                                        ?target,
                                        ?effective_delay,
                                        "Resynchronising a listener target from scratch at the cache's request.",
                                    );
                                    Self::forget_resume_states(&storage, &targets_state, std::slice::from_ref(&target)).await;
                                    // Without the delay a target that keeps diverging would
                                    // re-download in a tight loop.
                                    on_stream.remove(&db, &target, Some(Instant::now() + effective_delay))?;
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
                                                    (Some(target_change::TargetChangeType::Remove), _) => {
                                                        let dropped = on_stream.removed(&target_ids, Instant::now() + effective_delay);
                                                        if !dropped.is_empty() {
                                                            // The delay keeps a target Firestore keeps
                                                            // rejecting from being added back in a
                                                            // tight loop.
                                                            error!(
                                                                ?dropped,
                                                                ?cause,
                                                                ?effective_delay,
                                                                "Firestore removed listener targets. Discarding their resume state and adding them back after the retry delay.",
                                                            );
                                                            Self::forget_resume_states(&storage, &targets_state, &dropped).await;
                                                        }
                                                        Self::add_due_targets(&db, &storage, &targets_state, &mut on_stream).await?;
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

    /// Resolves the targets a target change applies to.
    ///
    /// Firestore uses an empty set of target IDs to mean every target on the stream, which is how
    /// global resume tokens and stream-wide resets arrive. Such a change includes a target only
    /// once Firestore has acknowledged adding it. A change that names a target applies to it only
    /// while the stream carries it, so a change still in flight for a removed target is not taken
    /// for a new one under the same ID. Target IDs this listener does not know about are ignored;
    /// an ID that is not a valid target at all is a protocol error and returns `Err`.
    fn affected_targets(
        targets_state: &FirestoreListenerTargetsState,
        on_stream: &FirestoreListenerStreamTargets,
        target_ids: &[i32],
    ) -> Result<Vec<FirestoreListenerTarget>, ()> {
        let targets = targets_state
            .read()
            .expect("listener targets lock poisoned");

        if target_ids.is_empty() {
            return Ok(targets
                .keys()
                .filter(|target| on_stream.includes(target))
                .cloned()
                .collect());
        }

        let mut affected = Vec::with_capacity(target_ids.len());
        for target_id_num in target_ids {
            match FirestoreListenerTarget::try_from(*target_id_num) {
                Ok(target_id) => {
                    if targets.contains_key(&target_id) && on_stream.carries(&target_id) {
                        affected.push(target_id);
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
        let due = on_stream.take_due(
            &targets_state
                .read()
                .expect("listener targets lock poisoned"),
            Instant::now(),
        );
        for target in due {
            if let Some(target_params) = Self::resolve_target(storage, targets_state, &target).await
            {
                on_stream.add(db, target_params)?;
            }
        }
        Ok(())
    }

    /// The target's parameters with its resume state filled in from the storage if it holds none
    /// yet, or `None` once it is no longer registered.
    ///
    /// Reading the storage here rather than only in `start` is what lets a target added at runtime
    /// pick up a token stored by an earlier run, and what makes a target whose token was discarded
    /// come back cleanly.
    async fn resolve_target(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        target: &FirestoreListenerTarget,
    ) -> Option<FirestoreListenerTargetParams> {
        let unresolved = targets_state
            .read()
            .expect("listener targets lock poisoned")
            .get(target)?
            .resume_type
            .is_none();

        if unresolved {
            match storage.read_resume_state(target).await {
                Ok(Some(resume_type)) => {
                    let mut targets = targets_state
                        .write()
                        .expect("listener targets lock poisoned");
                    if let Some(params) = targets.get_mut(target) {
                        params.resume_type = Some(resume_type);
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(%err, ?target, "Could not read the resume state of a listener target. Listening on it from scratch.");
                }
            }
        }

        targets_state
            .read()
            .expect("listener targets lock poisoned")
            .get(target)
            .cloned()
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

        for target_id in affected {
            if let Err(err) = storage
                .update_resume_token(&target_id, new_token.clone())
                .await
            {
                error!(%err, "Listener token storage error occurred.");
                return false;
            }
            let mut targets = targets_state
                .write()
                .expect("listener targets lock poisoned");
            if let Some(target) = targets.get_mut(&target_id) {
                target.resume_type =
                    Some(FirestoreListenerTargetResumeType::Token(new_token.clone()));
            }
        }

        true
    }

    /// Discards the stored resume state of the given targets, so that they are listened to from
    /// scratch the next time the stream is opened.
    async fn forget_resume_states(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        targets: &[FirestoreListenerTarget],
    ) {
        for target_id in targets {
            if let Err(err) = storage.forget_resume_state(target_id).await {
                warn!(%err, ?target_id, "Could not forget the resume state of a listener target.");
            }
            let mut state = targets_state
                .write()
                .expect("listener targets lock poisoned");
            if let Some(target) = state.get_mut(target_id) {
                target.resume_type = None;
            }
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
                let all_targets: Vec<FirestoreListenerTarget> = targets_state
                    .read()
                    .expect("listener targets lock poisoned")
                    .keys()
                    .cloned()
                    .collect();
                Self::forget_resume_states(storage, targets_state, &all_targets).await;
                Self::until_shutdown(shutdown_signal, tokio::time::sleep(delay)).await;
            }
            // The retry without resume tokens was rejected too, so the request itself is invalid.
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

/// Where a target stands on one open listen stream, going by the requests sent on it and the
/// target changes Firestore answered them with.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FirestoreListenerStreamTargetState {
    /// `AddTarget` was sent, and Firestore has not acknowledged it yet.
    Adding,
    /// Firestore acknowledged the `AddTarget`, so a change for every target includes this one.
    Listening,
    /// `RemoveTarget` was sent, and Firestore has not acknowledged it yet. The ID is not added
    /// again before it does, so that changes still in flight for the removed target are never
    /// taken for a new one. With `add_back_at`, the target comes back from scratch at that time.
    Removing { add_back_at: Option<Instant> },
    /// Off the stream, and added back from scratch at that time.
    Waiting(Instant),
}

/// The targets on one open listen stream, and the request side that changes them.
struct FirestoreListenerStreamTargets {
    requests: UnboundedSender<ListenRequest>,
    targets: HashMap<FirestoreListenerTarget, FirestoreListenerStreamTargetState>,
}

impl FirestoreListenerStreamTargets {
    fn new(requests: UnboundedSender<ListenRequest>) -> Self {
        Self {
            requests,
            targets: HashMap::new(),
        }
    }

    /// Whether a change that names `target` is about the target registered under that ID.
    fn carries(&self, target: &FirestoreListenerTarget) -> bool {
        matches!(
            self.targets.get(target),
            Some(
                FirestoreListenerStreamTargetState::Adding
                    | FirestoreListenerStreamTargetState::Listening
            )
        )
    }

    /// Whether a change for every target on the stream includes `target`.
    fn includes(&self, target: &FirestoreListenerTarget) -> bool {
        self.targets.get(target) == Some(&FirestoreListenerStreamTargetState::Listening)
    }

    /// When the next waiting target is added back.
    fn next_add_back(&self) -> Option<Instant> {
        self.targets
            .values()
            .filter_map(|state| match state {
                FirestoreListenerStreamTargetState::Waiting(at) => Some(*at),
                _ => None,
            })
            .min()
    }

    /// The registered targets to add now: those the stream lacks and those whose wait is over.
    /// A waiting target that is no longer registered is dropped.
    fn take_due(
        &mut self,
        registered: &HashMap<FirestoreListenerTarget, FirestoreListenerTargetParams>,
        now: Instant,
    ) -> Vec<FirestoreListenerTarget> {
        self.targets.retain(|target, state| {
            !matches!(state, FirestoreListenerStreamTargetState::Waiting(_))
                || registered.contains_key(target)
        });
        registered
            .keys()
            .filter(|target| match self.targets.get(target) {
                None => true,
                Some(FirestoreListenerStreamTargetState::Waiting(at)) => *at <= now,
                Some(_) => false,
            })
            .cloned()
            .collect()
    }

    fn add<D: FirestoreListenSupport>(
        &mut self,
        db: &D,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<()> {
        let target = target_params.target.clone();
        // A send fails only once the stream is going away, and the reconnect adds every target.
        self.requests
            .send(db.add_target_request(target_params)?)
            .ok();
        self.targets
            .insert(target, FirestoreListenerStreamTargetState::Adding);
        Ok(())
    }

    /// Takes `target` off the stream, to come back from scratch at `add_back_at` if that is set.
    /// Does nothing to a target the stream does not carry.
    fn remove<D: FirestoreListenSupport>(
        &mut self,
        db: &D,
        target: &FirestoreListenerTarget,
        add_back_at: Option<Instant>,
    ) -> FirestoreResult<()> {
        if self.carries(target) {
            self.requests.send(db.remove_target_request(target)?).ok();
            self.targets.insert(
                target.clone(),
                FirestoreListenerStreamTargetState::Removing { add_back_at },
            );
        }
        Ok(())
    }

    /// Records that Firestore acknowledged adding the targets.
    fn added(&mut self, target_ids: &[i32]) {
        for target in target_ids
            .iter()
            .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
        {
            if let Some(state) = self.targets.get_mut(&target) {
                if *state == FirestoreListenerStreamTargetState::Adding {
                    *state = FirestoreListenerStreamTargetState::Listening;
                }
            }
        }
    }

    /// Records that Firestore removed the targets, every target on the stream for no IDs, and
    /// returns those it removed on its own rather than to acknowledge a `RemoveTarget`. They are
    /// added back at `add_back_at`.
    fn removed(
        &mut self,
        target_ids: &[i32],
        add_back_at: Instant,
    ) -> Vec<FirestoreListenerTarget> {
        let named: Vec<FirestoreListenerTarget> = if target_ids.is_empty() {
            self.targets
                .keys()
                .filter(|target| self.includes(target))
                .cloned()
                .collect()
        } else {
            target_ids
                .iter()
                .filter_map(|id| FirestoreListenerTarget::try_from(*id).ok())
                .collect()
        };

        let mut dropped = Vec::new();
        for target in named {
            match self.targets.get(&target).copied() {
                Some(FirestoreListenerStreamTargetState::Removing {
                    add_back_at: Some(at),
                }) => {
                    self.targets
                        .insert(target, FirestoreListenerStreamTargetState::Waiting(at));
                }
                Some(FirestoreListenerStreamTargetState::Removing { add_back_at: None }) => {
                    self.targets.remove(&target);
                }
                Some(
                    FirestoreListenerStreamTargetState::Adding
                    | FirestoreListenerStreamTargetState::Listening,
                ) => {
                    self.targets.insert(
                        target.clone(),
                        FirestoreListenerStreamTargetState::Waiting(add_back_at),
                    );
                    dropped.push(target);
                }
                Some(FirestoreListenerStreamTargetState::Waiting(_)) | None => {}
            }
        }
        dropped
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
