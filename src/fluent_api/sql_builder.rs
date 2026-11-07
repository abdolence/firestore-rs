use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreDb, FirestoreDocument, FirestoreExplainOptions,
    FirestoreListingSupport, FirestorePipelineParams, FirestorePipelineSupport,
    FirestorePipelineWriteParams, FirestorePipelineWriteResult, FirestoreRequestOptions,
    FirestoreRequestTag, FirestoreResult, FirestoreSqlOptions, FirestoreSqlOutcome,
    FirestoreSqlParameters, FirestoreSqlStatement, FirestoreTransaction,
    FirestoreWithPipelineMetadata,
};
#[cfg(feature = "admin")]
use crate::{FirestoreIndexSupport, FirestoreOperationWaitOptions};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
#[cfg(feature = "admin")]
use std::time::Duration;

/// A SQL statement, the values of its parameters and the options it runs with. It is translated
/// when it runs, so an error in the text arrives from the terminal method, before any request is
/// sent.
///
/// [`run`](Self::run) runs every statement. A `SELECT` or an `EXPLAIN [ANALYZE] SELECT` also runs
/// with `query` or one of its streaming variants. An `INSERT`, `UPDATE` or `DELETE` also runs with
/// [`execute`](Self::execute) or [`execute_in_transaction`](Self::execute_in_transaction), and
/// fails from the other terminals, as a `SELECT` does from these two. `SHOW` and the index
/// statements run with `run` alone.
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
    #[cfg(feature = "admin")]
    wait: Option<FirestoreOperationWaitOptions>,
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
            #[cfg(feature = "admin")]
            wait: None,
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

    /// Waits for the operation a `CREATE .. INDEX` or an `ALTER COLLECTION` starts to finish
    /// before [`run`](Self::run) returns, polling at the default interval. `timeout` bounds the
    /// whole call: see [`FirestoreOperationWaitOptions`]. Any other statement fails with
    /// [`InvalidParametersError`](FirestoreError::InvalidParametersError) before any request:
    /// `DROP INDEX` finishes at once.
    #[cfg(feature = "admin")]
    pub fn wait_until_ready(self, timeout: Duration) -> Self {
        self.wait_until_ready_with_options(FirestoreOperationWaitOptions::new(timeout))
    }

    /// Like [`wait_until_ready`](Self::wait_until_ready), with the poll interval set too.
    #[cfg(feature = "admin")]
    pub fn wait_until_ready_with_options(self, options: FirestoreOperationWaitOptions) -> Self {
        Self {
            wait: Some(options),
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

    /// Runs any statement and returns its outcome, one variant per kind of statement.
    pub async fn run<'b>(self) -> FirestoreResult<FirestoreSqlOutcome<'b>> {
        let db = self.db;
        let statement = self.statement()?;
        Ok(match statement {
            statement @ (FirestoreSqlStatement::Query(_) | FirestoreSqlStatement::Explain(_)) => {
                FirestoreSqlOutcome::Rows(
                    db.stream_pipeline_doc_with_metadata(self.query_params(statement)?)
                        .await?,
                )
            }
            statement @ FirestoreSqlStatement::Write(_) => FirestoreSqlOutcome::Written(
                db.execute_pipeline_write(self.write_params(statement)?)
                    .await?,
            ),
            FirestoreSqlStatement::ShowCollections(params) => {
                let collection_ids: Vec<String> = db
                    .stream_list_collection_ids_with_errors(
                        params.opt_request_options(self.request_options),
                    )
                    .await?
                    .try_collect()
                    .await?;
                FirestoreSqlOutcome::Collections(
                    collection_ids
                        .into_iter()
                        .map(FirestoreCollectionId::new)
                        .collect::<FirestoreResult<_>>()?,
                )
            }
            #[cfg(feature = "admin")]
            FirestoreSqlStatement::ShowIndexes(collection_group) => {
                FirestoreSqlOutcome::Indexes(db.list_indexes(collection_group).await?)
            }
            #[cfg(feature = "admin")]
            FirestoreSqlStatement::CreateIndex(create) => {
                FirestoreSqlOutcome::IndexCreated(if create.if_not_exists {
                    db.create_index_if_not_exists(create.definition, self.wait)
                        .await?
                } else {
                    db.create_index(create.definition, self.wait).await?
                })
            }
            #[cfg(feature = "admin")]
            FirestoreSqlStatement::DropIndex(drop) => {
                FirestoreSqlOutcome::IndexDropped(if drop.if_exists {
                    db.delete_index_if_exists(drop.target).await?
                } else {
                    Some(db.delete_index(drop.target).await?)
                })
            }
            #[cfg(feature = "admin")]
            FirestoreSqlStatement::AlterCollection(update) => {
                FirestoreSqlOutcome::FieldUpdated(db.update_field(update, self.wait).await?)
            }
        })
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

    /// Translates the statement, and refuses a wait on one that starts no operation to wait for.
    fn statement(&self) -> FirestoreResult<FirestoreSqlStatement> {
        let statement =
            FirestoreSqlStatement::translate(self.db, &self.sql, &self.parameters, &self.options)?;
        #[cfg(feature = "admin")]
        if self.wait.is_some()
            && !matches!(
                statement,
                FirestoreSqlStatement::CreateIndex(_) | FirestoreSqlStatement::AlterCollection(_)
            )
        {
            return Err(FirestoreError::invalid_parameters(
                "wait",
                "only CREATE .. INDEX and ALTER COLLECTION start an operation to wait for",
            ));
        }
        Ok(statement)
    }

    fn into_params(self) -> FirestoreResult<FirestorePipelineParams> {
        let statement = self.statement()?;
        self.query_params(statement)
    }

    fn into_write_params(self) -> FirestoreResult<FirestorePipelineWriteParams> {
        let statement = self.statement()?;
        self.write_params(statement)
    }

    fn query_params(
        self,
        statement: FirestoreSqlStatement,
    ) -> FirestoreResult<FirestorePipelineParams> {
        match statement {
            FirestoreSqlStatement::Query(pipeline) => Ok(FirestorePipelineParams::new(pipeline)
                .opt_explain_options(self.explain_options)
                .opt_request_options(self.request_options)),
            FirestoreSqlStatement::Explain(explain) => {
                Ok(FirestorePipelineParams::new(explain.pipeline)
                    .with_explain_options(explain.options)
                    .opt_request_options(self.request_options))
            }
            FirestoreSqlStatement::Write(_) => Err(FirestoreError::invalid_parameters(
                "sql",
                "an INSERT, UPDATE or DELETE runs with execute() or execute_in_transaction(), and returns a count rather than rows",
            )),
            _ => Err(FirestoreError::sql_runs_with_run()),
        }
    }

    fn write_params(
        self,
        statement: FirestoreSqlStatement,
    ) -> FirestoreResult<FirestorePipelineWriteParams> {
        match statement {
            FirestoreSqlStatement::Write(_) if self.explain_options.is_some() => {
                Err(FirestoreError::invalid_parameters(
                    "explain",
                    "Firestore rejects explain on a write, or with analyze runs the write as well",
                ))
            }
            FirestoreSqlStatement::Write(write) => {
                Ok(write.opt_request_options(self.request_options))
            }
            FirestoreSqlStatement::Query(_) | FirestoreSqlStatement::Explain(_) => {
                Err(FirestoreError::invalid_parameters(
                    "sql",
                    "a SELECT runs with query() or one of its streaming variants, and returns rows rather than a count",
                ))
            }
            _ => Err(FirestoreError::sql_runs_with_run()),
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

#[cfg(all(test, feature = "admin"))]
mod tests {
    use crate::db::fake_firestore::FakeFirestore;
    use crate::errors::FirestoreError;
    use crate::FirestoreResult;
    use std::time::Duration;

    async fn refusing_every_request() -> FakeFirestore {
        FakeFirestore::start(|method, _| panic!("no request may be sent: {method}")).await
    }

    fn refused_field<T>(result: FirestoreResult<T>) -> String {
        match result {
            Err(FirestoreError::InvalidParametersError(error)) => error.public.field,
            Err(other) => panic!("{other:?}"),
            Ok(_) => panic!("the statement ran"),
        }
    }

    #[tokio::test]
    async fn query_refuses_an_index_statement_before_any_request() {
        let fake = refusing_every_request().await;

        let refused = fake.db.fluent().sql("SHOW INDEXES").query().await;

        assert_eq!(refused_field(refused), "sql");
        assert!(fake.calls().is_empty(), "{:?}", fake.calls());
    }

    #[tokio::test]
    async fn wait_until_ready_on_a_statement_without_an_operation_is_refused() {
        let fake = refusing_every_request().await;

        let refused = fake
            .db
            .fluent()
            .sql("SHOW INDEXES FROM orders")
            .wait_until_ready(Duration::from_secs(60))
            .run()
            .await;

        assert_eq!(refused_field(refused), "wait");
        assert!(fake.calls().is_empty(), "{:?}", fake.calls());
    }
}
