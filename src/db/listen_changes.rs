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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::*;

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
        targets: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        let listen_requests = targets
            .into_iter()
            .map(|target_params| self.create_listen_request(target_params))
            .collect::<FirestoreResult<Vec<ListenRequest>>>()?;

        let request = gcloud_sdk::tonic::Request::new(
            futures::stream::iter(listen_requests).chain(futures::stream::pending()),
        );

        let response = self.client().get().listen(request).await?;

        Ok(response.into_inner().map_err(|e| e.into()).boxed())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreListenerTarget(u32);

impl FirestoreListenerTarget {
    pub fn validate(&self) -> FirestoreResult<()> {
        if *self.value() == 0 {
            Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "target_id".to_string(),
                    "Listener target ID cannot be zero".to_string(),
                )),
            ))
        } else if *self.value() > i32::MAX as u32 {
            Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "target_id".to_string(),
                    format!(
                        "Listener target ID cannot be more than: {}. {} is specified",
                        i32::MAX,
                        self.value()
                    ),
                )),
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
            FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
                FirestoreInvalidParametersPublicDetails::new(
                    "target_id".to_string(),
                    format!("Invalid target ID: {} {}", self.value(), e),
                ),
            ))
        })
    }
}

impl TryFrom<i32> for FirestoreListenerTarget {
    type Error = FirestoreError;

    fn try_from(value: i32) -> FirestoreResult<Self> {
        value
            .try_into()
            .map_err(|e| {
                FirestoreError::InvalidParametersError(FirestoreInvalidParametersError::new(
                    FirestoreInvalidParametersPublicDetails::new(
                        "target_id".to_string(),
                        format!("Invalid target ID: {value} {e}"),
                    ),
                ))
            })
            .map(FirestoreListenerTarget)
    }
}

#[derive(Clone, Debug, ValueStruct)]
pub struct FirestoreListenerToken(Vec<u8>);

impl FirestoreDb {
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

    fn create_listen_request(
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
/// [`TargetsChanged`](Self::TargetsChanged) deliberately carries no payload: the shared target map
/// is the single source of truth, so a change applies on the next reconnect even if the message is
/// lost or arrives late.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum FirestoreListenerControl {
    Shutdown,
    TargetsChanged,
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
    /// Discards the target's stored resume state and reopens the stream, so Firestore sends its
    /// initial state again.
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
    shutdown_flag: Arc<AtomicBool>,
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
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_handle: None,
            control_writer: Arc::new(control_writer),
            control_reader: Some(control_reader),
        })
    }

    /// Adds a target to listen on.
    ///
    /// This works before and after [`start`](Self::start): a target added to a running listener
    /// joins it once the listen stream has been reopened with the new set, which happens straight
    /// away.
    ///
    /// Returns an error if the listener has been shut down, or if a target with the same ID is
    /// already registered - reusing an ID for a different query would resume it from a token that
    /// does not belong to it.
    pub fn add_target(&self, target_params: FirestoreListenerTargetParams) -> FirestoreResult<()> {
        target_params.validate()?;

        if self.shutdown_flag.load(Ordering::Relaxed) {
            return Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "target".to_string(),
                    "Cannot add a target to a listener that has been shut down".to_string(),
                )),
            ));
        }

        {
            let mut targets = self
                .targets
                .write()
                .expect("listener targets lock poisoned");
            if targets.contains_key(&target_params.target) {
                return Err(FirestoreError::InvalidParametersError(
                    FirestoreInvalidParametersError::new(
                        FirestoreInvalidParametersPublicDetails::new(
                            "target".to_string(),
                            format!(
                                "Listener target {} is already registered on this listener",
                                target_params.target.value()
                            ),
                        ),
                    ),
                ));
            }
            targets.insert(target_params.target.clone(), target_params);
        }

        // A send error only means the loop is gone, which the shutdown check above already covers.
        self.control_writer
            .send(FirestoreListenerControl::TargetsChanged)
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
            .send(FirestoreListenerControl::TargetsChanged)
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
            return Err(FirestoreError::InvalidParametersError(
                FirestoreInvalidParametersError::new(FirestoreInvalidParametersPublicDetails::new(
                    "listener".to_string(),
                    "This Firestore listener has already been started".to_string(),
                )),
            ));
        };

        // Targets added before `start` already appear in the shared state the loop is about to
        // connect with, so their queued notifications would only cause a pointless reconnect.
        while let Ok(queued) = control_reader.try_recv() {
            if queued == FirestoreListenerControl::Shutdown {
                self.shutdown_flag.store(true, Ordering::Relaxed);
            }
        }

        self.shutdown_handle = Some(tokio::spawn(Self::listener_loop(
            self.db.clone(),
            self.storage.clone(),
            self.shutdown_flag.clone(),
            self.targets.clone(),
            self.listener_params.clone(),
            control_reader,
            cb,
        )));
        Ok(())
    }

    pub async fn shutdown(&mut self) -> FirestoreResult<()> {
        debug!("Shutting down Firestore listener...");
        self.shutdown_flag.store(true, Ordering::Relaxed);
        self.control_writer
            .send(FirestoreListenerControl::Shutdown)
            .ok();
        if let Some(signaller) = self.shutdown_handle.take() {
            if let Err(err) = signaller.await {
                warn!(%err, "Firestore listener exit error!");
            };
        }
        debug!("Shutting down Firestore listener has been finished...");
        Ok(())
    }

    async fn listener_loop<FN, F>(
        db: D,
        storage: S,
        shutdown_flag: Arc<AtomicBool>,
        targets_state: FirestoreListenerTargetsState,
        listener_params: FirestoreListenerParams,
        mut control_receiver: UnboundedReceiver<FirestoreListenerControl>,
        cb: FN,
    ) where
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

        while !shutdown_flag.load(Ordering::Relaxed) {
            let snapshot = Self::resolve_targets(&storage, &targets_state).await;

            if snapshot.is_empty() {
                // Nothing to listen on yet. Idle on the control channel rather than opening an
                // empty stream, so that a listener can be started before its targets exist.
                debug!("Firestore listener has no targets. Waiting for one to be added...");
                match control_receiver.recv().await {
                    None | Some(FirestoreListenerControl::Shutdown) => {
                        shutdown_flag.store(true, Ordering::Relaxed);
                    }
                    // Nothing is being listened to, so there is nothing to resynchronise either.
                    Some(FirestoreListenerControl::TargetsChanged)
                    | Some(FirestoreListenerControl::ResyncTarget(_)) => {}
                }
                continue;
            }

            debug!(
                num_targets = snapshot.len(),
                "Start listening on targets..."
            );

            match db.listen_doc_changes(snapshot).await {
                Err(err) => {
                    Self::handle_listener_error(
                        err,
                        effective_delay,
                        &storage,
                        &targets_state,
                        &shutdown_flag,
                        &mut retried_without_resume_tokens,
                    )
                    .await;
                }
                Ok(mut listen_stream) => loop {
                    tokio::select! {
                        control = control_receiver.recv() => {
                            match control {
                                None => {
                                    debug!("Listener dropped. Exiting...");
                                    shutdown_flag.store(true, Ordering::Relaxed);
                                    break;
                                }
                                Some(FirestoreListenerControl::Shutdown) => {
                                    debug!("Exiting from listener on targets...");
                                    control_receiver.close();
                                    break;
                                }
                                Some(FirestoreListenerControl::ResyncTarget(target)) => {
                                    warn!(
                                        ?target,
                                        ?effective_delay,
                                        "Resynchronising a listener target from scratch at the cache's request.",
                                    );
                                    Self::forget_resume_states(&storage, &targets_state, &[target]).await;
                                    // Without this a target that keeps diverging would reconnect
                                    // and re-download in a tight loop.
                                    tokio::time::sleep(effective_delay).await;
                                    break;
                                }
                                Some(FirestoreListenerControl::TargetsChanged) => {
                                    // Collapse a burst of changes into a single reconnect.
                                    while let Ok(queued) = control_receiver.try_recv() {
                                        if queued == FirestoreListenerControl::Shutdown {
                                            shutdown_flag.store(true, Ordering::Relaxed);
                                            control_receiver.close();
                                            break;
                                        }
                                    }
                                    debug!("Listener targets changed. Reopening the stream with the new set...");
                                    break;
                                }
                            }
                        }
                        tried = listen_stream.try_next() => {
                            if shutdown_flag.load(Ordering::Relaxed) {
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
                                                if !target_change.resume_token.is_empty()
                                                    && !Self::store_resume_token(&storage, &targets_state, &target_change).await
                                                {
                                                    break;
                                                }

                                                let change_type = target_change::TargetChangeType::try_from(
                                                    target_change.target_change_type,
                                                ).ok();
                                                let affected = Self::affected_targets(&targets_state, &target_change.target_ids);
                                                let cause = target_change.cause.clone();

                                                // Anything listening on this - the cache above all -
                                                // has to see the change, and drop what it holds for a
                                                // reset target, before the stream is reopened.
                                                if let Err(err) = cb(listen_response::ResponseType::TargetChange(target_change)).await {
                                                    error!(%err, "Listener callback function error occurred.");
                                                    break;
                                                }

                                                match (change_type, affected) {
                                                    (Some(target_change::TargetChangeType::Remove), Ok(affected)) => {
                                                        error!(
                                                            ?affected,
                                                            ?cause,
                                                            ?effective_delay,
                                                            "Firestore removed listener targets. Discarding their resume state and reopening the stream after the retry delay to add them again.",
                                                        );
                                                        Self::forget_resume_states(&storage, &targets_state, &affected).await;
                                                        // Without this a target Firestore keeps
                                                        // rejecting would reconnect in a tight loop.
                                                        tokio::time::sleep(effective_delay).await;
                                                        break;
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
                                    Ok(None) => break,
                                    Err(err) => {
                                        Self::handle_listener_error(
                                            err,
                                            effective_delay,
                                            &storage,
                                            &targets_state,
                                            &shutdown_flag,
                                            &mut retried_without_resume_tokens,
                                        ).await;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    /// Resolves the targets a target change applies to.
    ///
    /// Firestore uses an empty set of target IDs to mean *all* targets, which is how global resume
    /// tokens and stream-wide resets arrive. Target IDs this listener does not know about are
    /// ignored; an ID that is not a valid target at all is a protocol error and returns `Err`.
    fn affected_targets(
        targets_state: &FirestoreListenerTargetsState,
        target_ids: &[i32],
    ) -> Result<Vec<FirestoreListenerTarget>, ()> {
        let targets = targets_state
            .read()
            .expect("listener targets lock poisoned");

        if target_ids.is_empty() {
            return Ok(targets.keys().cloned().collect());
        }

        let mut affected = Vec::with_capacity(target_ids.len());
        for target_id_num in target_ids {
            match FirestoreListenerTarget::try_from(*target_id_num) {
                Ok(target_id) => {
                    if targets.contains_key(&target_id) {
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

    /// Snapshots the targets to listen on, filling in any resume state that is not resolved yet.
    ///
    /// Reading the storage here rather than only in `start` is what lets a target added at runtime
    /// pick up a token stored by an earlier run, and what makes a target whose token was discarded
    /// come back cleanly.
    async fn resolve_targets(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
    ) -> Vec<FirestoreListenerTargetParams> {
        let unresolved: Vec<FirestoreListenerTarget> = {
            let targets = targets_state
                .read()
                .expect("listener targets lock poisoned");
            targets
                .values()
                .filter(|params| params.resume_type.is_none())
                .map(|params| params.target.clone())
                .collect()
        };

        for target in unresolved {
            match storage.read_resume_state(&target).await {
                Ok(Some(resume_type)) => {
                    let mut targets = targets_state
                        .write()
                        .expect("listener targets lock poisoned");
                    if let Some(params) = targets.get_mut(&target) {
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
            .values()
            .cloned()
            .collect()
    }

    /// Persists a resume token for every target the change applies to.
    ///
    /// Returns `false` when the token could not be stored, in which case the caller reopens the
    /// stream rather than carrying on from a position the storage does not know about.
    async fn store_resume_token(
        storage: &S,
        targets_state: &FirestoreListenerTargetsState,
        target_change: &TargetChange,
    ) -> bool {
        let Ok(affected) = Self::affected_targets(targets_state, &target_change.target_ids) else {
            return false;
        };

        let new_token: FirestoreListenerToken = target_change.resume_token.clone().into();

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
        shutdown_flag: &Arc<AtomicBool>,
        retried_without_resume_tokens: &mut bool,
    ) {
        match Self::classify_listener_error(&err) {
            FirestoreListenerErrorAction::Retry => {
                debug!(%err, ?delay, "Listen EOF.. Restarting after the specified delay...");
                tokio::time::sleep(delay).await;
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
                tokio::time::sleep(delay).await;
            }
            FirestoreListenerErrorAction::RetryWithoutResumeTokens
            | FirestoreListenerErrorAction::Fatal => {
                error!(%err, "Listen error. Exiting...");
                shutdown_flag.store(true, Ordering::Relaxed);
            }
            FirestoreListenerErrorAction::RetryAfterDelay => {
                error!(%err, ?delay, "Listen error. Restarting after the specified delay...");
                tokio::time::sleep(delay).await;
            }
        }
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
