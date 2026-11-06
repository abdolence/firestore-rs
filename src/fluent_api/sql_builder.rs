use crate::{
    FirestoreDb, FirestoreDocument, FirestoreExplainOptions, FirestorePipelineParams,
    FirestorePipelineSupport, FirestoreRequestOptions, FirestoreRequestTag, FirestoreResult,
    FirestoreSqlOptions, FirestoreSqlParameters, FirestoreSqlStatement,
    FirestoreWithPipelineMetadata,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// A SQL statement, the values of its parameters and the options it runs with. It is translated
/// when it runs, so an error in the text arrives from the terminal method, before any request is
/// sent.
///
/// Created by [`FirestoreExprBuilder::sql()`](crate::FirestoreExprBuilder::sql).
#[derive(Clone, Debug)]
pub struct FirestoreSqlBuilder<'a> {
    db: &'a FirestoreDb,
    sql: String,
    parameters: FirestoreSqlParameters,
    options: FirestoreSqlOptions,
    explain_options: Option<FirestoreExplainOptions>,
    request_options: Option<FirestoreRequestOptions>,
}

impl<'a> FirestoreSqlBuilder<'a> {
    pub(crate) fn new(db: &'a FirestoreDb, sql: String) -> Self {
        Self {
            db,
            sql,
            parameters: FirestoreSqlParameters::new(),
            options: FirestoreSqlOptions::new(),
            explain_options: None,
            request_options: None,
        }
    }

    /// Binds `value` to the parameter `@name`, given without the `@`.
    pub fn param<N, V>(self, name: N, value: V) -> Self
    where
        N: Into<String>,
        V: Serialize,
    {
        Self {
            parameters: self.parameters.param(name, value),
            ..self
        }
    }

    /// Binds `values` to the `?` parameters, in the order the `?`s appear in the text.
    pub fn params<I>(self, values: I) -> Self
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        Self {
            parameters: self.parameters.params(values),
            ..self
        }
    }

    /// Checks the statement against `options`, such as
    /// [`read_only`](FirestoreSqlOptions::read_only) for text from outside the program.
    pub fn options(self, options: FirestoreSqlOptions) -> Self {
        Self { options, ..self }
    }

    /// Runs the statement with Query Explain for its plan only: no document is read or returned,
    /// and the plan arrives in the metadata of the last item of
    /// [`stream_query_with_metadata`](Self::stream_query_with_metadata).
    pub fn explain(self) -> Self {
        self.explain_with_options(FirestoreExplainOptions::new())
    }

    /// Runs the statement with Query Explain. With `analyze` set, the documents are returned
    /// too, and the explain output adds the execution profile.
    pub fn explain_with_options(self, options: FirestoreExplainOptions) -> Self {
        Self {
            explain_options: Some(options),
            ..self
        }
    }

    /// Attaches request options to this statement.
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            request_options: Some(options),
            ..self
        }
    }

    /// Attaches request tags to this statement. They override any session wide default
    /// configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    pub fn request_tags<I>(self, tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(tags))
    }

    /// Deserialises the rows into `T`. A row of chosen columns has no document name unless it
    /// selects `__name__`, so an `_firestore_id` field of `T` must be optional.
    pub fn obj<T>(self) -> FirestoreSqlObjBuilder<'a, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSqlObjBuilder {
            builder: self,
            _pd: PhantomData,
        }
    }

    /// Runs the statement and returns every row.
    pub async fn query(self) -> FirestoreResult<Vec<FirestoreDocument>> {
        let db = self.db;
        db.query_pipeline_doc(self.into_params()?).await
    }

    /// Runs the statement and streams the rows as they arrive, logging and leaving out any
    /// error.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, FirestoreDocument>> {
        let db = self.db;
        db.stream_pipeline_doc(self.into_params()?).await
    }

    /// Runs the statement and streams the rows as they arrive, with an error as an `Err` item.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let db = self.db;
        db.stream_pipeline_doc_with_errors(self.into_params()?)
            .await
    }

    /// Runs the statement and streams each row with the metadata of its response, with an error
    /// as an `Err` item.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<
        BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<FirestoreDocument>>>,
    > {
        let db = self.db;
        db.stream_pipeline_doc_with_metadata(self.into_params()?)
            .await
    }

    /// The translated query and the options it runs with.
    fn into_params(self) -> FirestoreResult<FirestorePipelineParams> {
        match FirestoreSqlStatement::translate(self.db, &self.sql, &self.parameters, &self.options)?
        {
            FirestoreSqlStatement::Query(pipeline) => Ok(FirestorePipelineParams::new(pipeline)
                .opt_explain_options(self.explain_options)
                .opt_request_options(self.request_options)),
        }
    }
}

/// Runs a SQL statement and deserialises its rows into `T`.
#[derive(Clone, Debug)]
pub struct FirestoreSqlObjBuilder<'a, T> {
    builder: FirestoreSqlBuilder<'a>,
    _pd: PhantomData<T>,
}

impl<'a, T> FirestoreSqlObjBuilder<'a, T>
where
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    /// Attaches request options to this statement.
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            builder: self.builder.request_options(options),
            ..self
        }
    }

    /// Attaches request tags to this statement. They override any session wide default
    /// configured with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    pub fn request_tags<I>(self, tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(tags))
    }

    /// Runs the statement and deserialises every row.
    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        let db = self.builder.db;
        db.query_pipeline_obj(self.builder.into_params()?).await
    }

    /// Runs the statement and streams the deserialised rows as they arrive, logging and leaving
    /// out any error.
    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        let db = self.builder.db;
        db.stream_pipeline_obj(self.builder.into_params()?).await
    }

    /// Runs the statement and streams the deserialised rows as they arrive, with an error as an
    /// `Err` item.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        let db = self.builder.db;
        db.stream_pipeline_obj_with_errors(self.builder.into_params()?)
            .await
    }

    /// Runs the statement and streams each deserialised row with the metadata of its response,
    /// with an error as an `Err` item.
    pub async fn stream_query_with_metadata<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreWithPipelineMetadata<T>>>>
    where
        T: 'b,
    {
        let db = self.builder.db;
        db.stream_pipeline_obj_with_metadata(self.builder.into_params()?)
            .await
    }
}
