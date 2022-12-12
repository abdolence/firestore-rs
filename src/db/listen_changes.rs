use crate::db::safe_document_path;
use crate::errors::*;
use crate::timestamp_utils::to_timestamp;
use crate::{FirestoreDb, FirestoreQueryParams, FirestoreResult};
use async_trait::async_trait;
use chrono::prelude::*;
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
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>>;
}

#[async_trait]
impl FirestoreListenSupport for FirestoreDb {
    async fn listen_doc_changes<'a, 'b>(
        &'a self,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<ListenResponse>>> {
        let listen_request = ListenRequest {
            database: self.get_database_path().to_string(),
            labels: target_params.labels,
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: *target_params.target.value(),
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
                                query_params.to_structured_query(),
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
            })),
        };

        let request = tonic::Request::new(
            futures::stream::iter(vec![listen_request]).chain(futures::stream::pending()),
        );

        let response = self.client.get().listen(request).await?;

        Ok(response.into_inner().map_err(|e| e.into()).boxed())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, ValueStruct)]
pub struct FirestoreListenerTarget(i32);

#[derive(Clone, Debug, ValueStruct)]
pub struct FirestoreListenerToken(Vec<u8>);

type BoxedErrResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait FirestoreResumeStateStorage {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> BoxedErrResult<Option<FirestoreListenerTargetResumeType>>;

    async fn update_resume_token(
        &self,
        target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> BoxedErrResult<()>;
}

pub type FirestoreListenEvent = listen_response::ResponseType;

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
    storage: S,
    listener_params: FirestoreListenerParams,
    target_params: FirestoreListenerTargetParams,
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
        storage: S,
        listener_params: FirestoreListenerParams,
        target_params: FirestoreListenerTargetParams,
    ) -> FirestoreResult<FirestoreListener<D, S>> {
        Ok(FirestoreListener {
            db,
            storage,
            listener_params,
            target_params,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_handle: None,
            shutdown_writer: None,
        })
    }

    pub async fn start<FN, F>(&mut self, cb: FN) -> FirestoreResult<()>
    where
        FN: Fn(FirestoreListenEvent) -> F + Send + Sync + 'static,
        F: Future<Output = BoxedErrResult<()>> + Send + Sync + 'static,
    {
        info!(
            "Starting a Firestore listener for target: {:?}...",
            &self.target_params
        );

        let initial_state = self
            .storage
            .read_resume_state(&self.target_params.target)
            .map_err(|err| {
                FirestoreError::SystemError(FirestoreSystemError::new(
                    FirestoreErrorPublicGenericDetails::new("SystemError".into()),
                    format!("Listener init error: {}", err),
                ))
            })
            .await?;

        let (tx, rx): (UnboundedSender<i8>, UnboundedReceiver<i8>) =
            tokio::sync::mpsc::unbounded_channel();

        self.shutdown_writer = Some(Arc::new(tx));
        self.shutdown_handle = Some(tokio::spawn(Self::listener_loop(
            self.db.clone(),
            self.storage.clone(),
            self.shutdown_flag.clone(),
            self.target_params.clone().opt_resume_type(initial_state),
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
        target_params: FirestoreListenerTargetParams,
        listener_params: FirestoreListenerParams,
        mut shutdown_receiver: UnboundedReceiver<i8>,
        cb: FN,
    ) where
        D: FirestoreListenSupport + Clone + Send + Sync,
        FN: Fn(FirestoreListenEvent) -> F + Send + Sync,
        F: Future<Output = BoxedErrResult<()>> + Send + Sync,
    {
        let mut current_token: Option<FirestoreListenerToken> = None;

        while !shutdown_flag.load(Ordering::Relaxed) {
            let resume_type: Option<FirestoreListenerTargetResumeType> = current_token
                .as_ref()
                .map(|token| FirestoreListenerTargetResumeType::Token(token.clone()))
                .or(target_params.resume_type.clone());

            debug!(
                "Start listening on {:?}... Token: {:?}",
                target_params.target, current_token
            );

            let mut listen_stream = db
                .listen_doc_changes(target_params.clone().opt_resume_type(resume_type))
                .await
                .unwrap();

            loop {
                tokio::select! {
                        _ = shutdown_receiver.recv() => {
                            println!("Exiting from listener...");
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
                                        match event.response_type {
                                            Some(listen_response::ResponseType::TargetChange(ref target_change))
                                                if !target_change.resume_token.is_empty() =>
                                            {
                                                let new_token: FirestoreListenerToken = target_change.resume_token.clone().into();

                                                if let Err(err) = storage.update_resume_token(&target_params.target, new_token.clone()).await {
                                                    error!("Listener token storage error occurred {:?}.", err);
                                                    break;
                                                }
                                                else {
                                                    current_token = Some(new_token)
                                                }
                                            }
                                            Some(response_type) => {
                                                if let Err(err) = cb(response_type).await {
                                                    error!("Listener callback function error occurred {:?}.", err);
                                                    break;
                                                }
                                            }
                                            None  =>  {}
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(err) => {
                                        let effective_delay = listener_params.retry_delay.unwrap_or_else(|| std::time::Duration::from_secs(5));
                                        debug!("Listen error occurred {:?}. Restarting in {:?}...", err, effective_delay);
                                        tokio::time::sleep(effective_delay).await;
                                        break;
                                    }
                                }
                            }
                    }
                }
            }
        }
    }
}
