use crate::pipeline_builder::{
    FirestorePipelineBuilder, FirestorePipelineCollectionSource, FirestorePipelineInitialBuilder,
    FirestorePipelineStaged,
};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreExpr, FirestoreInsertWrite, FirestorePipeline, FirestorePipelineSupport,
    FirestorePipelineWrite, FirestorePipelineWriteParams, FirestorePipelineWriteResult,
    FirestorePipelineWriteTransaction, FirestoreRequestOptions, FirestoreRequestTag,
    FirestoreResult, FirestoreTransaction, FirestoreUpdateWrite, FirestoreUpsertWrite,
    FirestoreWriteCollection,
};
use serde::Serialize;

/// The context of a top-level pipeline that ends in a write: the database it runs on, the write,
/// and the options it runs with.
///
/// A write has no `explain`, `query` or `obj`. Firestore rejects explain on a write, or with
/// `analyze` runs the write as well, and a write returns a count rather than documents:
///
/// ```compile_fail,E0599
/// use firestore::FirestoreDb;
///
/// fn explained(db: &FirestoreDb) {
///     db.fluent().pipeline().delete().from("books").explain();
/// }
/// ```
#[derive(Clone, Debug)]
pub struct FirestorePipelineWriteContext<'a, D, W> {
    db: &'a D,
    write: W,
    request_options: Option<FirestoreRequestOptions>,
}

impl<'a, D, W> FirestorePipelineWriteContext<'a, D, W> {
    pub(crate) fn new(db: &'a D, write: W) -> Self {
        Self {
            db,
            write,
            request_options: None,
        }
    }

    fn map_write<V>(self, write: impl FnOnce(W) -> V) -> FirestorePipelineWriteContext<'a, D, V> {
        FirestorePipelineWriteContext {
            db: self.db,
            write: write(self.write),
            request_options: self.request_options,
        }
    }
}

type UpdateContext<'a, D> = FirestorePipelineWriteContext<'a, D, FirestoreUpdateWrite>;

/// An insert or upsert whose collection is chosen, which can still take a document ID.
#[derive(Clone, Debug)]
pub struct FirestorePipelineWriteInto<W> {
    write: W,
    target: FirestoreWriteCollection,
}

impl From<FirestorePipelineWriteInto<FirestoreInsertWrite>> for FirestorePipelineWrite {
    fn from(into: FirestorePipelineWriteInto<FirestoreInsertWrite>) -> Self {
        into.write.with_target(into.target).into()
    }
}

impl From<FirestorePipelineWriteInto<FirestoreUpsertWrite>> for FirestorePipelineWrite {
    fn from(into: FirestorePipelineWriteInto<FirestoreUpsertWrite>) -> Self {
        into.write.with_target(into.target).into()
    }
}

/// Picks the documents an update changes. The collection is named with
/// [`in_col`](Self::in_col), as in Core's update; the other sources are those of a read.
///
/// Created by
/// [`update`](crate::pipeline_builder::FirestorePipelineVerbBuilder::update).
#[derive(Clone, Debug)]
pub struct FirestorePipelineUpdateInitialBuilder<'a, D>(
    FirestorePipelineInitialBuilder<UpdateContext<'a, D>>,
);

impl<'a, D> FirestorePipelineUpdateInitialBuilder<'a, D> {
    pub(crate) fn new(context: UpdateContext<'a, D>) -> Self {
        Self(FirestorePipelineInitialBuilder::new(context))
    }

    /// Updates documents of the collection `collection_id`, a root collection unless
    /// [`parent`](FirestorePipelineBuilder::parent) follows, or every collection with that ID when
    /// [`all_descendants`](FirestorePipelineBuilder::all_descendants) follows.
    pub fn in_col<S: AsRef<str>>(
        self,
        collection_id: S,
    ) -> FirestorePipelineBuilder<UpdateContext<'a, D>, FirestorePipelineCollectionSource> {
        self.0.from(collection_id)
    }

    /// Updates every document of the database.
    pub fn database(
        self,
    ) -> FirestorePipelineBuilder<UpdateContext<'a, D>, FirestorePipelineStaged> {
        self.0.database()
    }

    /// Updates the documents at `paths`, relative to the database's documents root, such as
    /// `"books/b1"`.
    pub fn documents<I>(
        self,
        paths: I,
    ) -> FirestorePipelineBuilder<UpdateContext<'a, D>, FirestorePipelineStaged>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.0.documents(paths)
    }

    /// Updates the documents that `rows` name by `__name__`, a full document path. Every row must
    /// serialise to a map.
    pub fn literals<I>(
        self,
        rows: I,
    ) -> FirestorePipelineBuilder<UpdateContext<'a, D>, FirestorePipelineStaged>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        self.0.literals(rows)
    }
}

impl<'a, D>
    FirestorePipelineInitialBuilder<FirestorePipelineWriteContext<'a, D, FirestoreInsertWrite>>
{
    /// Writes the documents to the collection at `collection_path`, relative to the database's
    /// documents root, such as `"archive"` or `"restaurants/r1/reviews"`.
    pub fn into<P: AsRef<str>>(
        self,
        collection_path: P,
    ) -> FirestorePipelineInitialBuilder<
        FirestorePipelineWriteContext<'a, D, FirestorePipelineWriteInto<FirestoreInsertWrite>>,
    > {
        self.into_collection(collection_path)
    }
}

impl<'a, D>
    FirestorePipelineInitialBuilder<FirestorePipelineWriteContext<'a, D, FirestoreUpsertWrite>>
{
    /// Writes the documents to the collection at `collection_path`, relative to the database's
    /// documents root, such as `"archive"` or `"restaurants/r1/reviews"`.
    pub fn into<P: AsRef<str>>(
        self,
        collection_path: P,
    ) -> FirestorePipelineInitialBuilder<
        FirestorePipelineWriteContext<'a, D, FirestorePipelineWriteInto<FirestoreUpsertWrite>>,
    > {
        self.into_collection(collection_path)
    }
}

impl<'a, D, W> FirestorePipelineInitialBuilder<FirestorePipelineWriteContext<'a, D, W>> {
    fn into_collection<P: AsRef<str>>(
        self,
        collection_path: P,
    ) -> FirestorePipelineInitialBuilder<
        FirestorePipelineWriteContext<'a, D, FirestorePipelineWriteInto<W>>,
    > {
        let target = FirestoreWriteCollection::new(collection_path.as_ref().to_string());
        FirestorePipelineInitialBuilder::new(
            self.context
                .map_write(|write| FirestorePipelineWriteInto { write, target }),
        )
    }
}

impl<'a, D, W>
    FirestorePipelineInitialBuilder<
        FirestorePipelineWriteContext<'a, D, FirestorePipelineWriteInto<W>>,
    >
{
    /// Names each document by the ID `document_id` evaluates to on it, such as
    /// `|e| e.field("key")`. The field it reads stays in the stored document. Without it, each
    /// document keeps the ID of its `__name__`, and Firestore assigns one to a row that has none.
    pub fn document_id<FN>(self, document_id: FN) -> Self
    where
        FN: FnOnce(FirestorePipelineExprBuilder) -> FirestoreExpr,
    {
        let document_id = document_id(FirestorePipelineExprBuilder);
        Self::new(self.context.map_write(|into| FirestorePipelineWriteInto {
            target: into.target.with_document_id(document_id),
            ..into
        }))
    }
}

impl<'a, D, W, S> FirestorePipelineBuilder<FirestorePipelineWriteContext<'a, D, W>, S>
where
    D: FirestorePipelineSupport,
    W: Into<FirestorePipelineWrite>,
    S: Into<FirestorePipeline>,
{
    /// Attaches request options to this write.
    pub fn request_options(self, options: FirestoreRequestOptions) -> Self {
        Self {
            context: FirestorePipelineWriteContext {
                request_options: Some(options),
                ..self.context
            },
            ..self
        }
    }

    /// Attaches request tags to this write. They override any session wide default configured
    /// with
    /// [`FirestoreDb::clone_with_request_tags()`](crate::FirestoreDb::clone_with_request_tags).
    pub fn request_tags<I>(self, tags: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreRequestTag>,
    {
        self.request_options(FirestoreRequestOptions::from_tags(tags))
    }

    /// Runs the write in a transaction of its own, which commits as soon as the write is done,
    /// so that it applies in full or not at all.
    ///
    /// Firestore runs an upsert, an insert from any source but `literals`, and a write after
    /// `aggregate`, `distinct`, `unnest`, `find_nearest`, `union`, `search` or a subquery, only
    /// this way or in a transaction.
    ///
    /// A write in an open transaction runs with
    /// [`execute_in_transaction`](Self::execute_in_transaction) instead, since an atomic write
    /// would commit on its own:
    ///
    /// ```compile_fail,E0599
    /// use firestore::*;
    ///
    /// # async fn example(db: &FirestoreDb) -> FirestoreResult<()> {
    /// let mut transaction = db.begin_transaction().await?;
    /// db.fluent()
    ///     .pipeline()
    ///     .delete()
    ///     .from("books")
    ///     .atomic()
    ///     .execute_in_transaction(&mut transaction)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn atomic(self) -> FirestorePipelineAtomicWriteBuilder<'a, D> {
        FirestorePipelineAtomicWriteBuilder {
            db: self.context.db,
            params: FirestorePipelineWriteParams::from(self)
                .with_transaction(FirestorePipelineWriteTransaction::Atomic),
        }
    }

    /// Runs the write, writing each document on its own.
    ///
    /// A failure stops the run and can leave part of the documents written, so the write is
    /// never sent again: a repeated insert would fail as `ALREADY_EXISTS`. The error has
    /// `retry_possible` unset for the same reason, whatever the code. Use
    /// [`atomic`](Self::atomic) for a write that applies in full or not at all.
    ///
    /// Fails with `InvalidParametersError` on a `db` bound to a read time, at which Firestore
    /// runs no write, or to a transaction, such as the one
    /// [`run_transaction`](crate::FirestoreDb::run_transaction) passes: a write in a transaction
    /// runs with [`execute_in_transaction`](Self::execute_in_transaction).
    pub async fn execute(self) -> FirestoreResult<FirestorePipelineWriteResult> {
        let db = self.context.db;
        db.execute_pipeline_write(self.into()).await
    }

    /// Runs the write in `transaction`, which holds it until the transaction commits and drops it
    /// on rollback.
    ///
    /// Firestore adds two rules:
    /// - A transaction takes pipeline writes or buffered writes, such as `add_to_transaction`,
    ///   not both. Firestore fails the commit that mixes them.
    /// - Every read comes before the first pipeline write. Firestore fails a read after it as
    ///   `FAILED_PRECONDITION`.
    ///
    /// ```no_run
    /// use firestore::*;
    /// use futures::FutureExt;
    ///
    /// # async fn example(db: &FirestoreDb) -> FirestoreResult<()> {
    /// db.run_transaction(|db, transaction| {
    ///     async move {
    ///         db.fluent()
    ///             .pipeline()
    ///             .update(|e| [e.constant("shipped").alias("status")])
    ///             .in_col("orders")
    ///             .filter(|q| q.field("status").eq("packed"))
    ///             .execute_in_transaction(transaction)
    ///             .await?;
    ///         Ok(())
    ///     }
    ///     .boxed()
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_in_transaction(
        self,
        transaction: &mut FirestoreTransaction<'_>,
    ) -> FirestoreResult<FirestorePipelineWriteResult> {
        let db = self.context.db;
        db.execute_pipeline_write_in_transaction(self.into(), transaction)
            .await
    }
}

impl<'a, D, W, S> From<FirestorePipelineBuilder<FirestorePipelineWriteContext<'a, D, W>, S>>
    for FirestorePipelineWriteParams
where
    W: Into<FirestorePipelineWrite>,
    S: Into<FirestorePipeline>,
{
    fn from(builder: FirestorePipelineBuilder<FirestorePipelineWriteContext<'a, D, W>, S>) -> Self {
        FirestorePipelineWriteParams::new(builder.state.into(), builder.context.write.into())
            .opt_request_options(builder.context.request_options)
    }
}

/// Runs a pipeline write in a transaction of its own.
///
/// Created by [`atomic`](FirestorePipelineBuilder::atomic).
#[derive(Clone, Debug)]
pub struct FirestorePipelineAtomicWriteBuilder<'a, D> {
    db: &'a D,
    params: FirestorePipelineWriteParams,
}

impl<D> FirestorePipelineAtomicWriteBuilder<'_, D>
where
    D: FirestorePipelineSupport,
{
    /// Runs the write, which applies in full or not at all.
    ///
    /// A failed write is reported like a failed commit: `retry_possible` is set for `ABORTED`,
    /// which means nothing was applied, and unset for any other code, which can leave the outcome
    /// unknown.
    ///
    /// Fails with `InvalidParametersError` on a `db` bound to a transaction or a read time, as
    /// [`execute`](FirestorePipelineBuilder::execute) does.
    pub async fn execute(self) -> FirestoreResult<FirestorePipelineWriteResult> {
        self.db.execute_pipeline_write(self.params).await
    }
}
