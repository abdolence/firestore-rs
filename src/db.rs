use crate::errors::*;
use crate::query::*;
use crate::serde::*;
use chrono::prelude::*;
use std::collections::HashMap;
use std::pin::Pin;

use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use futures::{Stream, TryFutureExt};
use futures_util::stream::BoxStream;
use futures_util::{future, StreamExt};
use gcloud_sdk::google::firestore::v1::*;
use gcloud_sdk::*;
use rsb_derive::Builder;
use serde::{Deserialize, Serialize};
use tracing::*;

#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreDbOptions {
    pub google_project_id: String,

    #[default = "3"]
    pub max_retries: usize,
}

pub struct FirestoreDb {
    database_path: String,
    doc_path: String,
    options: FirestoreDbOptions,
    google_firestore_client:
        GoogleApiClientFn<firestore_client::FirestoreClient<GoogleConnectorInterceptedService>>,
}

impl<'a> FirestoreDb {
    pub async fn new(google_project_id: &str) -> Result<Self, FirestoreError> {
        Self::with_options(FirestoreDbOptions::new(google_project_id.to_string())).await
    }

    pub async fn with_options(options: FirestoreDbOptions) -> Result<Self, FirestoreError> {
        let firestore_database_path =
            Self::create_firestore_database_path(&options.google_project_id);
        let firestore_database_doc_path =
            Self::create_firestore_database_documents_path(&options.google_project_id);

        info!("Creating a new DB client: {}", firestore_database_path);

        let client = GoogleApiClient::from_function(
            firestore_client::FirestoreClient::with_interceptor,
            "https://firestore.googleapis.com",
            chrono::Duration::minutes(15),
            Some(firestore_database_path.clone()),
        )
        .await?;

        Ok(Self {
            database_path: firestore_database_path,
            doc_path: firestore_database_doc_path,
            google_firestore_client: client,
            options,
        })
    }

    pub fn deserialize_doc_to<T>(doc: &Document) -> Result<T, FirestoreError>
    where
        for<'de> T: Deserialize<'de>,
    {
        firestore_document_to_serializable(doc)
    }

    pub async fn ping(&self) -> Result<(), FirestoreError> {
        self.google_firestore_client.get().await?;
        Ok(())
    }

    pub fn get_database_path(&self) -> &String {
        &self.database_path
    }

    pub fn get_documents_path(&self) -> &String {
        &self.doc_path
    }

    pub async fn query_doc(
        &'a self,
        params: FirestoreQueryParams,
    ) -> Result<Vec<Document>, FirestoreError> {
        self.query_doc_with_retries(params, 0).await
    }

    pub async fn stream_query_doc<'b>(
        &'a self,
        params: FirestoreQueryParams,
    ) -> Result<BoxStream<'b, Document>, FirestoreError> {
        self.stream_query_doc_with_retries(params, 0).await
    }

    pub async fn query_obj<T>(
        &'a self,
        params: FirestoreQueryParams,
    ) -> Result<Vec<T>, FirestoreError>
    where
        for<'de> T: Deserialize<'de>,
    {
        let doc_vec = self.query_doc(params).await?;
        doc_vec
            .iter()
            .map(|doc| firestore_document_to_serializable(doc))
            .collect()
    }

    pub fn stream_query_obj<'b, T>(
        &'a self,
        params: FirestoreQueryParams,
    ) -> BoxFuture<'a, Result<BoxStream<'b, T>, FirestoreError>>
    where
        for<'de> T: Deserialize<'de>,
    {
        Box::pin(self.stream_query_doc(params).map_ok(|doc_stream| {
            doc_stream
                .map(|doc| firestore_document_to_serializable::<T>(&doc).unwrap())
                .boxed()
        }))
    }

    fn get_doc_by_path(
        &'a self,
        document_path: String,
        retries: usize,
    ) -> BoxFuture<'a, Result<Document, FirestoreError>> {
        let request = tonic::Request::new(GetDocumentRequest {
            name: document_path.clone(),
            consistency_selector: None,
            mask: None,
        });
        async move {
            match self
                .google_firestore_client
                .get()
                .await?
                .get_document(request)
                .map_err(|e| e.into())
                .await
            {
                Ok(doc_response) => Ok(doc_response.into_inner()),
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );
                        self.get_doc_by_path(document_path, retries + 1).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    pub async fn get_doc_by_id(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &'a String,
    ) -> Result<Document, FirestoreError> {
        let document_path = format!("{}/{}/{}", parent, collection_id, document_id);
        self.get_doc_by_path(document_path, 0).await
    }

    pub async fn get_obj<T>(
        &'a self,
        collection_id: &'a str,
        document_id: &'a String,
    ) -> Result<T, FirestoreError>
    where
        for<'de> T: Deserialize<'de>,
    {
        self.get_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
        )
        .await
    }

    pub async fn get_obj_at<T>(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &'a String,
    ) -> Result<T, FirestoreError>
    where
        for<'de> T: Deserialize<'de>,
    {
        let begin_query_utc: DateTime<Utc> = Utc::now();
        let doc: Document = self
            .get_doc_by_id(parent, collection_id, document_id)
            .await?;
        let end_query_utc: DateTime<Utc> = Utc::now();
        let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

        debug!(
            "[DB]: Reading document by id: {}/{} took {}ms",
            collection_id,
            document_id,
            query_duration.num_milliseconds()
        );

        let obj: T = firestore_document_to_serializable(&doc)?;
        Ok(obj)
    }

    pub async fn create_obj<T>(
        &'a self,
        collection_id: &'a str,
        document_id: &'a str,
        obj: &'a T,
    ) -> Result<T, FirestoreError>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        self.create_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
        )
        .await
    }

    pub async fn create_obj_at<T>(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &'a str,
        obj: &'a T,
    ) -> Result<T, FirestoreError>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        let doc = self
            .create_doc(parent, collection_id, document_id, obj)
            .await?;
        firestore_document_to_serializable(&doc)
    }

    pub async fn create_doc<T>(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &str,
        obj: &T,
    ) -> Result<Document, FirestoreError>
    where
        T: Serialize,
    {
        let firestore_doc = firestore_document_from_serializable("", obj).unwrap();
        let create_document_request = tonic::Request::new(CreateDocumentRequest {
            parent: parent.into(),
            document_id: document_id.to_string(),
            mask: None,
            collection_id: collection_id.into(),
            document: Some(firestore_doc),
        });

        let create_response = self
            .google_firestore_client
            .get()
            .await?
            .create_document(create_document_request)
            .await?;
        Ok(create_response.into_inner())
    }

    pub async fn update_obj<T>(
        &'a self,
        collection_id: &'a str,
        document_id: &'a String,
        obj: &'a T,
        update_only: Option<Vec<String>>,
    ) -> Result<T, FirestoreError>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        self.update_obj_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            update_only,
        )
        .await
    }

    pub async fn update_obj_at<T>(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &'a String,
        obj: &'a T,
        update_only: Option<Vec<String>>,
    ) -> Result<T, FirestoreError>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        let doc = self
            .update_doc(parent, collection_id, document_id, obj, update_only)
            .await?;
        firestore_document_to_serializable(&doc)
    }

    pub async fn update_doc<T>(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &String,
        obj: &T,
        update_only: Option<Vec<String>>,
    ) -> Result<Document, FirestoreError>
    where
        T: Serialize,
    {
        let firestore_doc = firestore_document_from_serializable(
            format!("{}/{}/{}", parent, collection_id, document_id).as_str(),
            obj,
        )
        .unwrap();
        let update_document_request = tonic::Request::new(UpdateDocumentRequest {
            mask: None,
            update_mask: update_only.map({
                |vf| DocumentMask {
                    field_paths: vf.iter().map(|f| f.to_string()).collect(),
                }
            }),
            document: Some(firestore_doc),
            current_document: None,
        });

        let update_response = self
            .google_firestore_client
            .get()
            .await?
            .update_document(update_document_request)
            .await?;
        Ok(update_response.into_inner())
    }

    pub async fn delete_by_id(
        &'a self,
        collection_id: &'a str,
        document_id: &'a String,
    ) -> Result<(), FirestoreError> {
        self.delete_by_id_at(
            self.get_documents_path().as_str(),
            collection_id,
            document_id,
        )
        .await
    }

    pub async fn delete_by_id_at(
        &'a self,
        parent: &'a str,
        collection_id: &'a str,
        document_id: &'a String,
    ) -> Result<(), FirestoreError> {
        let document_path = format!("{}/{}/{}", parent, collection_id, document_id);

        let request = tonic::Request::new(DeleteDocumentRequest {
            name: document_path,
            current_document: None,
        });

        self.google_firestore_client
            .get()
            .await?
            .delete_document(request)
            .await?;

        Ok(())
    }

    pub async fn listen_doc<'b>(
        &'a self,
        database_path: &'a str,
        params: &'a FirestoreQueryParams,
        labels: HashMap<String, String>,
        since_token_value: Option<Vec<u8>>,
        target_id: i32,
    ) -> Result<BoxStream<'b, Result<ListenResponse, FirestoreError>>, FirestoreError> {
        use futures::stream;

        let query_request = params.to_structured_query();
        let listen_request = ListenRequest {
            database: database_path.into(),
            labels,
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id,
                once: false,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent: params
                        .parent
                        .as_ref()
                        .unwrap_or_else(|| self.get_documents_path())
                        .clone(),
                    query_type: Some(target::query_target::QueryType::StructuredQuery(
                        query_request,
                    )),
                })),
                resume_type: since_token_value.map(target::ResumeType::ResumeToken),
            })),
        };

        let request = tonic::Request::new(
            futures::stream::iter(vec![listen_request]).chain(stream::pending()),
        );

        let response = self
            .google_firestore_client
            .get()
            .await?
            .listen(request)
            .await?;

        Ok(response.into_inner().map_err(|e| e.into()).boxed())
    }

    fn create_firestore_database_path(google_project_id: &String) -> String {
        format!("projects/{}/databases/(default)", google_project_id)
    }

    fn create_firestore_database_documents_path(google_project_id: &String) -> String {
        format!(
            "{}/documents",
            Self::create_firestore_database_path(google_project_id)
        )
    }

    fn create_query_request(
        &self,
        params: &FirestoreQueryParams,
    ) -> tonic::Request<RunQueryRequest> {
        tonic::Request::new(RunQueryRequest {
            parent: params
                .parent
                .as_ref()
                .unwrap_or_else(|| self.get_documents_path())
                .clone(),
            consistency_selector: None,
            query_type: Some(run_query_request::QueryType::StructuredQuery(
                params.to_structured_query(),
            )),
        })
    }

    fn stream_query_doc_with_retries<'b>(
        &'a self,
        params: FirestoreQueryParams,
        retries: usize,
    ) -> BoxFuture<'a, Result<BoxStream<'b, Document>, FirestoreError>> {
        let query_request = self.create_query_request(&params);
        async move {
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .google_firestore_client
                .get()
                .await?
                .run_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream: Pin<
                        Box<
                            dyn Stream<Item = gcloud_sdk::google::firestore::v1::Document>
                                + std::marker::Send,
                        >,
                    > = query_response
                        .into_inner()
                        .map_ok(|r| r.document)
                        .take_while(|dr| {
                            future::ready(match dr {
                                Ok(Some(_)) => true,
                                Ok(None) => false,
                                Err(err) => {
                                    error!("[DB] Error occurred while consuming query: {}", err);
                                    false
                                }
                            })
                        })
                        .map(|dr| dr.unwrap().unwrap())
                        .boxed();

                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

                    debug!(
                        "[DB]: Querying stream of documents in {:?} took {}ms",
                        params.collection_id,
                        query_duration.num_milliseconds()
                    );

                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );
                        self.stream_query_doc_with_retries(params, retries + 1)
                            .await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }

    fn query_doc_with_retries(
        &'a self,
        params: FirestoreQueryParams,
        retries: usize,
    ) -> BoxFuture<'a, Result<Vec<Document>, FirestoreError>> {
        let query_request = self.create_query_request(&params);
        async move {
            let begin_query_utc: DateTime<Utc> = Utc::now();

            match self
                .google_firestore_client
                .get()
                .await?
                .run_query(query_request)
                .map_err(|e| e.into())
                .await
            {
                Ok(query_response) => {
                    let query_stream = query_response
                        .into_inner()
                        .map_ok(|rs| rs.document)
                        .try_collect::<Vec<Option<Document>>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect();
                    let end_query_utc: DateTime<Utc> = Utc::now();
                    let query_duration = end_query_utc.signed_duration_since(begin_query_utc);

                    debug!(
                        "[DB]: Querying documents in {:?} took {}ms",
                        params.collection_id,
                        query_duration.num_milliseconds()
                    );
                    Ok(query_stream)
                }
                Err(err) => match err {
                    FirestoreError::DatabaseError(ref db_err)
                        if db_err.retry_possible && retries < self.options.max_retries =>
                    {
                        warn!(
                            "[DB]: Failed with {}. Retrying: {}/{}",
                            db_err,
                            retries + 1,
                            self.options.max_retries
                        );
                        self.query_doc_with_retries(params, retries + 1).await
                    }
                    _ => Err(err),
                },
            }
        }
        .boxed()
    }
}
