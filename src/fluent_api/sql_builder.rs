use crate::errors::FirestoreError;
use crate::{
    FirestoreDb, FirestoreDocument, FirestoreExplainOptions, FirestorePipelineParams,
    FirestorePipelineSupport, FirestorePipelineWriteParams, FirestorePipelineWriteResult,
    FirestoreRequestOptions, FirestoreRequestTag, FirestoreResult, FirestoreSqlOptions,
    FirestoreSqlParameters, FirestoreSqlStatement, FirestoreTransaction,
    FirestoreWithPipelineMetadata,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// A SQL statement, the values of its parameters and the options it runs with. It is translated
/// when it runs, so an error in the text arrives from the terminal method, before any request is
/// sent.
///
/// A `SELECT` runs with `query` or one of its streaming variants. An `INSERT`, `UPDATE` or
/// `DELETE` runs with [`execute`](Self::execute) or
/// [`execute_in_transaction`](Self::execute_in_transaction), and fails from the other terminals,
/// as a `SELECT` does from these two.
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

    /// Runs an `INSERT`, `UPDATE` or `DELETE` atomically: it applies in full or not at all.
    ///
    /// A failed write is reported like a failed commit: `retry_possible` is set for `ABORTED`,
    /// which means nothing was applied, and unset for any other code, which can leave the outcome
    /// unknown. The write is sent once and never retried.
    ///
    /// Fails with `InvalidParametersError` before any request for a `SELECT`, and for a statement
    /// with [`explain`](Self::explain), which would run the write as well when it analyses. It
    /// fails the same way on a `db` bound to a transaction or a read time: a write in a
    /// transaction runs with [`execute_in_transaction`](Self::execute_in_transaction).
    pub async fn execute(self) -> FirestoreResult<FirestorePipelineWriteResult> {
        let db = self.db;
        db.execute_pipeline_write(self.into_write_params()?).await
    }

    /// Runs an `INSERT`, `UPDATE` or `DELETE` in `transaction`, which holds it until the
    /// transaction commits and drops it on rollback.
    ///
    /// The transaction takes pipeline writes or buffered writes, not both, and every read comes
    /// before the first pipeline write: see
    /// [`execute_in_transaction`](crate::pipeline_builder::FirestorePipelineBuilder::execute_in_transaction)
    /// of the pipeline write builder.
    pub async fn execute_in_transaction(
        self,
        transaction: &mut FirestoreTransaction<'_>,
    ) -> FirestoreResult<FirestorePipelineWriteResult> {
        let db = self.db;
        db.execute_pipeline_write_in_transaction(self.into_write_params()?, transaction)
            .await
    }

    /// The translated query and the options it runs with.
    fn into_params(self) -> FirestoreResult<FirestorePipelineParams> {
        match FirestoreSqlStatement::translate(self.db, &self.sql, &self.parameters, &self.options)?
        {
            FirestoreSqlStatement::Query(pipeline) => Ok(FirestorePipelineParams::new(pipeline)
                .opt_explain_options(self.explain_options)
                .opt_request_options(self.request_options)),
            FirestoreSqlStatement::Write(_) => Err(FirestoreError::invalid_parameters(
                "sql",
                "an INSERT, UPDATE or DELETE runs with execute() or execute_in_transaction(), and returns a count rather than rows",
            )),
        }
    }

    /// The translated write and the options it runs with.
    fn into_write_params(self) -> FirestoreResult<FirestorePipelineWriteParams> {
        match FirestoreSqlStatement::translate(self.db, &self.sql, &self.parameters, &self.options)?
        {
            FirestoreSqlStatement::Write(_) if self.explain_options.is_some() => {
                Err(FirestoreError::invalid_parameters(
                    "explain",
                    "Firestore rejects explain on a write, or with analyze runs the write as well",
                ))
            }
            FirestoreSqlStatement::Write(write) => {
                Ok(write.opt_request_options(self.request_options))
            }
            FirestoreSqlStatement::Query(_) => Err(FirestoreError::invalid_parameters(
                "sql",
                "a SELECT runs with query() or one of its streaming variants, and returns rows rather than a count",
            )),
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
