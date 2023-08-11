use crate::db::safe_document_path;
use crate::errors::*;
use crate::timestamp_utils::to_timestamp;
use crate::{FirestoreDb, FirestoreQueryParams, FirestoreResult, FirestoreResumeStateStorage};
pub use async_trait::async_trait;
use chrono::prelude::*;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::*;
use rsb_derive::*;
pub use rvstruct::ValueStruct;
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

#[derive(Debug, Clone)]
pub enum FirestoreTargetType {
    Query(FirestoreQueryParams),
    Documents(FirestoreCollectionDocuments),
}

#[derive(Debug, Clone)]
pub enum FirestoreListenerTargetResumeType {
    Token(FirestoreListenerToken),
    ReadTime(DateTime<Utc>),
}

#[async_trait]
pub trait FirestoreListenSupport {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        targets: Vec<FirestoreListenerTargetParams>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>>;
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

        let request = tonic::Request::new(
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
                        format!("Invalid target ID: {} {}", value, e),
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
                                query_params.into(),
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

pub trait FirestoreTargetManager {
    fn add_target(&mut self, target: FirestoreListenerTargetParams) -> FirestoreResult<()>;
    fn remove_target(&mut self, target: &FirestoreListenerTarget) -> FirestoreResult<()>;
}

#[derive(Debug, Clone, Builder)]
pub struct FirestoreListenerParams {
    pub retry_delay: Option<std::time::Duration>,
}

pub struct FirestoreListener<D, S>
where
    D: FirestoreListenSupport,
    S: FirestoreResumeStateStorage,
{
    db: D,
    initial_targets_storage: FirestoreTargetManagerStorage,
    resume_state_storage: S,
    listener_params: FirestoreListenerParams,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_handle: Option<JoinHandle<()>>,
    shutdown_writer: Option<Arc<UnboundedSender<i8>>>,
}

impl<D, S> FirestoreListener<D, S>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
    S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    pub async fn new(
        db: D,
        resume_state_storage: S,
        listener_params: FirestoreListenerParams,
    ) -> FirestoreResult<FirestoreListener<D, S>> {
        Ok(FirestoreListener {
            db,
            initial_targets_storage: FirestoreTargetManagerStorage::new(false),
            resume_state_storage,
            listener_params,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_handle: None,
            shutdown_writer: None,
        })
    }

    pub async fn start<FN, F>(&mut self, cb: FN) -> FirestoreResult<()>
    where
        FN: Fn(FirestoreListenEvent, D) -> F + Send + Sync + 'static,
        F: Future<Output = AnyBoxedErrResult<()>> + Send + 'static,
    {
        info!(
            "Starting a Firestore listener for initial targets: {:?}...",
            &self.initial_targets_storage.targets.len()
        );

        let mut targets_storage = self.initial_targets_storage.clone();

        for target_params in &mut targets_storage.targets.values_mut() {
            let maybe_initial_token = self
                .resume_state_storage
                .read_resume_state(&target_params.target)
                .map_err(|err| {
                    FirestoreError::SystemError(FirestoreSystemError::new(
                        FirestoreErrorPublicGenericDetails::new("SystemError".into()),
                        format!("Listener init error: {err}"),
                    ))
                })
                .await?;

            target_params.mopt_resume_type(maybe_initial_token);
        }

        let (tx, rx): (UnboundedSender<i8>, UnboundedReceiver<i8>) =
            tokio::sync::mpsc::unbounded_channel();

        self.shutdown_writer = Some(Arc::new(tx));
        self.shutdown_handle = Some(tokio::spawn(Self::listener_loop(
            self.db.clone(),
            self.resume_state_storage.clone(),
            self.shutdown_flag.clone(),
            targets_storage,
            self.listener_params.clone(),
            rx,
            cb,
        )));
        Ok(())
    }

    pub async fn shutdown(&mut self) -> FirestoreResult<()> {
        debug!("Shutting down Firestore listener...");
        self.shutdown_flag.store(true, Ordering::Relaxed);
        if let Some(shutdown_writer) = self.shutdown_writer.take() {
            shutdown_writer.send(1).ok();
        }
        if let Some(signaller) = self.shutdown_handle.take() {
            if let Err(err) = signaller.await {
                warn!("Firestore listener exit error: {}...", err);
            };
        }
        debug!("Shutting down Firestore listener has been finished...");
        Ok(())
    }

    async fn listener_loop<FN, F>(
        db: D,
        storage: S,
        shutdown_flag: Arc<AtomicBool>,
        mut targets_state: FirestoreTargetManagerStorage,
        listener_params: FirestoreListenerParams,
        mut shutdown_receiver: UnboundedReceiver<i8>,
        cb: FN,
    ) where
        D: FirestoreListenSupport + Clone + Send + Sync,
        FN: Fn(FirestoreListenEvent, D) -> F + Send + Sync,
        F: Future<Output = AnyBoxedErrResult<()>> + Send,
    {
        let effective_delay = listener_params
            .retry_delay
            .unwrap_or_else(|| std::time::Duration::from_secs(5));

        while !shutdown_flag.load(Ordering::Relaxed) {
            debug!(
                "Start listening on {} targets ... ",
                targets_state.targets.len()
            );

            match db
                .listen_doc_changes(targets_state.targets.values().cloned().collect())
                .await
            {
                Err(err) => {
                    if Self::check_listener_if_permanent_error(err, effective_delay).await {
                        shutdown_flag.store(true, Ordering::Relaxed);
                    }
                }
                Ok(mut listen_stream) => loop {
                    tokio::select! {
                        _ = shutdown_receiver.recv() => {
                            debug!("Exiting from listener on {} targets...", targets_state.targets.len());
                            shutdown_receiver.close();
                            break;
                        }
                        tried = listen_stream.try_next() => {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            else {
                                match tried {
                                    Ok(Some(event)) => {
                                        trace!("Received a listen response event to handle: {:?}", event);
                                        if let Some(response_type) = event.response_type {
                                            if let listen_response::ResponseType::TargetChange(ref target_change) = &response_type {
                                                if !target_change.resume_token.is_empty() {
                                                    for target_id_num in &target_change.target_ids {
                                                        match FirestoreListenerTarget::try_from(*target_id_num) {
                                                            Ok(target_id) => {
                                                                if let Some(target) = targets_state.targets.get_mut(&target_id) {
                                                                    let new_token: FirestoreListenerToken = target_change.resume_token.clone().into();

                                                                    if let Err(err) = storage.update_resume_token(&target.target, new_token.clone()).await {
                                                                        error!("Listener token storage error occurred {:?}.", err);
                                                                        break;
                                                                    }
                                                                    else {
                                                                        target.resume_type = Some(FirestoreListenerTargetResumeType::Token(new_token))
                                                                    }
                                                                }
                                                            },
                                                            Err(err) => {
                                                                error!("Listener system error - unexpected target ID: {} {:?}.", target_id_num, err);
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if let Err(err) = cb(response_type, db.clone()).await {
                                                error!("Listener callback function error occurred {:?}.", err);
                                                break;
                                            }
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(err) => {
                                        if Self::check_listener_if_permanent_error(err, effective_delay).await {
                                            shutdown_flag.store(true, Ordering::Relaxed);
                                        }
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

    async fn check_listener_if_permanent_error(
        err: FirestoreError,
        delay: std::time::Duration,
    ) -> bool {
        match err {
            FirestoreError::DatabaseError(ref db_err)
                if db_err.details.contains("unexpected end of file")
                    || db_err.details.contains("stream error received") =>
            {
                debug!("Listen EOF ({:?}). Restarting in {:?}...", err, delay);
                tokio::time::sleep(delay).await;
                false
            }
            FirestoreError::DatabaseError(ref db_err)
                if db_err.public.code.contains("InvalidArgument") =>
            {
                error!("Listen error {:?}. Exiting...", err);
                true
            }
            FirestoreError::InvalidParametersError(_) => {
                error!("Listen error {:?}. Exiting...", err);
                true
            }
            _ => {
                error!("Listen error {:?}. Restarting in {:?}...", err, delay);
                tokio::time::sleep(delay).await;
                false
            }
        }
    }
}

impl<D, S> FirestoreTargetManager for FirestoreListener<D, S>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
    S: FirestoreResumeStateStorage + Clone + Send + Sync + 'static,
{
    fn add_target(&mut self, target_params: FirestoreListenerTargetParams) -> FirestoreResult<()> {
        self.initial_targets_storage.add_target(target_params)
    }

    fn remove_target(&mut self, target: &FirestoreListenerTarget) -> FirestoreResult<()> {
        self.initial_targets_storage.remove_target(target)
    }
}

#[derive(Clone)]
pub struct FirestoreTargetManagerStorage {
    targets: HashMap<FirestoreListenerTarget, FirestoreListenerTargetParams>,
    change_log_mode: bool,
    to_remove: Vec<FirestoreListenerTarget>,
}

impl FirestoreTargetManagerStorage {
    fn new(change_log_mode: bool) -> Self {
        Self {
            targets: HashMap::new(),
            change_log_mode,
            to_remove: Vec::new(),
        }
    }
}

impl FirestoreTargetManager for FirestoreTargetManagerStorage {
    fn add_target(&mut self, target_params: FirestoreListenerTargetParams) -> FirestoreResult<()> {
        target_params.validate()?;
        self.targets
            .insert(target_params.target.clone(), target_params);
        Ok(())
    }

    fn remove_target(&mut self, target: &FirestoreListenerTarget) -> FirestoreResult<()> {
        if self.targets.remove(target).is_none() && self.change_log_mode {
            self.to_remove.push(target.clone());
        }
        Ok(())
    }
}
