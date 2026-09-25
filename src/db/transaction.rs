// Deliberately public: `FirestoreTransactionOps` exists so that transaction operations are
// available on both `FirestoreTransaction` and `FirestoreTransactionData`, which lets callers
// write transaction-agnostic abstractions over the trait. See
// https://github.com/abdolence/firestore-rs/issues/206.
pub use crate::db::transaction_ops::FirestoreTransactionOps;
use crate::errors::*;
use crate::timestamp_utils::from_timestamp;
use crate::{
    FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreRequestOptions,
    FirestoreResult, FirestoreTransactionId, FirestoreTransactionMode, FirestoreTransactionOptions,
    FirestoreTransactionResponse, FirestoreWriteResult,
};
use backoff::future::retry;
use backoff::ExponentialBackoffBuilder;
use futures::future::BoxFuture;
use gcloud_sdk::google::firestore::v1::{BeginTransactionRequest, CommitRequest, RollbackRequest};
use std::time::Duration;
use tracing::*;

#[cfg(test)]
mod commit_tests;

#[derive(Debug, Clone)]
pub struct FirestoreTransactionData {
    transaction_id: FirestoreTransactionId,
    document_path: String,
    transaction_span: Span,
    writes: Vec<gcloud_sdk::google::firestore::v1::Write>,
}

impl FirestoreTransactionData {
    /// Assembles a transaction snapshot from its parts, bypassing `begin_transaction`.
    ///
    /// Most callers get a value of this type from [`FirestoreTransaction::into_data`] instead of
    /// calling this directly.
    pub fn new(
        transaction_id: FirestoreTransactionId,
        document_path: String,
        transaction_span: Span,
        writes: Vec<gcloud_sdk::google::firestore::v1::Write>,
    ) -> Self {
        Self {
            transaction_id,
            document_path,
            transaction_span,
            writes,
        }
    }

    /// Returns the transaction ID Firestore assigned when the transaction began.
    #[inline]
    pub fn transaction_id(&self) -> &FirestoreTransactionId {
        &self.transaction_id
    }

    /// Returns the documents path that this transaction's writes are resolved against.
    #[inline]
    pub fn documents_path(&self) -> &String {
        &self.document_path
    }

    /// Returns the tracing span opened for this transaction.
    #[inline]
    pub fn transaction_span(&self) -> &Span {
        &self.transaction_span
    }

    /// Returns the writes queued on this transaction so far, in the order they were added.
    #[inline]
    pub fn writes(&self) -> &Vec<gcloud_sdk::google::firestore::v1::Write> {
        &self.writes
    }

    /// Returns whether the transaction has no queued writes yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

impl From<FirestoreTransaction<'_>> for FirestoreTransactionData {
    fn from(transaction: FirestoreTransaction) -> Self {
        transaction.into_data()
    }
}

impl FirestoreTransactionOps for FirestoreTransactionData {
    fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>,
    {
        let write = write.try_into()?;
        self.writes.push(write);
        Ok(self)
    }

    fn get_documents_path(&self) -> &String {
        &self.document_path
    }
}

#[derive(Debug)]
pub struct FirestoreTransaction<'a> {
    db: &'a FirestoreDb,
    data: FirestoreTransactionData,
    finished: bool,
    request_options: Option<FirestoreRequestOptions>,
}

impl<'a> FirestoreTransaction<'a> {
    /// Opens a new transaction against `db` via Firestore's `BeginTransaction` RPC.
    ///
    /// Most callers reach this through [`FirestoreDb::begin_transaction`] or
    /// [`FirestoreDb::begin_transaction_with_options`] rather than calling it directly.
    ///
    /// Returns an error if converting a read-only `options.mode`'s consistency selector fails,
    /// or if the `BeginTransaction` request itself fails.
    pub async fn new(
        db: &'a FirestoreDb,
        options: FirestoreTransactionOptions,
    ) -> FirestoreResult<FirestoreTransaction<'a>> {
        let transaction_span = span!(
            Level::DEBUG,
            "Firestore Transaction",
            "/firestore/transaction_id" = field::Empty,
            "/firestore/commit_time" = field::Empty
        );

        let request = gcloud_sdk::tonic::Request::new(BeginTransactionRequest {
            database: db.get_database_path().clone(),
            options: Some(options.clone().try_into()?),
            request_options: db.resolve_request_options(options.request_options.as_ref()),
        });

        let response = db
            .client()
            .get()
            .begin_transaction(request)
            .await?
            .into_inner();

        let mut hex_trans_id = hex::encode(&response.transaction);
        hex_trans_id.truncate(16);

        transaction_span.record("/firestore/transaction_id", hex_trans_id);

        transaction_span.in_scope(|| {
            debug!(mode = ?options.mode, "Created a new transaction.");
        });

        let data = FirestoreTransactionData {
            transaction_id: response.transaction,
            document_path: db.get_documents_path().clone(),
            transaction_span,
            writes: Vec::new(),
        };

        Ok(Self {
            db,
            data,
            finished: false,
            request_options: options.request_options,
        })
    }

    /// Returns the transaction ID Firestore assigned when the transaction began.
    #[inline]
    pub fn transaction_id(&self) -> &FirestoreTransactionId {
        &self.data.transaction_id
    }

    /// Returns the client the transaction was opened on.
    #[inline]
    pub fn db(&self) -> &'a FirestoreDb {
        self.db
    }

    /// Commits every write queued on the transaction and consumes it.
    ///
    /// This is the only one of [`commit`](Self::commit), [`rollback`](Self::rollback) and
    /// [`finish`](Self::finish) that sends the queued writes to Firestore; the other two discard
    /// them. A transaction with no queued writes still commits successfully - useful for a
    /// read-only transaction that only needed a consistent snapshot.
    ///
    /// Returns an error if the `Commit` request fails. Only `ABORTED` sets `retry_possible`:
    /// other failures can leave the commit outcome unknown and must not repeat the transaction.
    pub async fn commit(mut self) -> FirestoreResult<FirestoreTransactionResponse> {
        self.finished = true;

        if self.data.writes.is_empty() {
            self.data.transaction_span.in_scope(|| {
                debug!("Transaction has been committed without any writes.");
            });
        }

        let request = gcloud_sdk::tonic::Request::new(CommitRequest {
            database: self.db.get_database_path().clone(),
            writes: std::mem::take(&mut self.data.writes),
            transaction: self.data.transaction_id.clone(),
            request_options: self
                .db
                .resolve_request_options(self.request_options.as_ref()),
        });

        let response = self
            .db
            .client()
            .get()
            .commit(request)
            .await
            .map_err(|status| {
                // Transport failures can arrive after the writes were committed.
                let retry_possible = status.code() == gcloud_sdk::tonic::Code::Aborted;
                let mut error = FirestoreError::from(status);
                if let FirestoreError::DatabaseError(ref mut error) = error {
                    error.retry_possible = retry_possible;
                }
                error
            })?
            .into_inner();

        let result = FirestoreTransactionResponse::new(
            response
                .write_results
                .into_iter()
                .map(|s| s.try_into())
                .collect::<FirestoreResult<Vec<FirestoreWriteResult>>>()?,
        )
        .opt_commit_time(response.commit_time.map(from_timestamp).transpose()?);

        if let Some(ref commit_time) = result.commit_time {
            self.data
                .transaction_span
                .record("/firestore/commit_time", commit_time.to_string());
        }

        self.data.transaction_span.in_scope(|| {
            debug!("Transaction has been committed.");
        });

        Ok(result)
    }

    /// Discards every write queued on the transaction and consumes it.
    ///
    /// Unlike [`finish`](Self::finish), this tells Firestore to release the transaction's locks
    /// immediately, via the `Rollback` request, rather than waiting for the transaction to
    /// expire on its own.
    ///
    /// Returns an error if the `Rollback` request fails; the queued writes are discarded locally
    /// either way.
    pub async fn rollback(mut self) -> FirestoreResult<()> {
        self.finished = true;
        let request = gcloud_sdk::tonic::Request::new(RollbackRequest {
            database: self.db.get_database_path().clone(),
            transaction: self.data.transaction_id.clone(),
            request_options: self
                .db
                .resolve_request_options(self.request_options.as_ref()),
        });

        self.db.client().get().rollback(request).await?;

        self.data.transaction_span.in_scope(|| {
            debug!("Transaction has been rolled back.");
        });

        Ok(())
    }

    /// Marks the transaction finished locally, without telling Firestore, so it can be retried.
    ///
    /// Unlike [`rollback`](Self::rollback), this sends no request: it exists for the retry loop
    /// in [`FirestoreDb::run_transaction_with_options`], which needs the transaction ID to
    /// survive so the next attempt can reopen it in [`FirestoreTransactionMode::ReadWriteRetry`]
    /// mode. Calling this outside a retry leaves the transaction open on the server until
    /// Firestore expires it on its own; call [`rollback`](Self::rollback) instead.
    ///
    /// # Errors
    /// Never fails; returns `FirestoreResult<()>` for symmetry with [`commit`](Self::commit) and
    /// [`rollback`](Self::rollback).
    pub fn finish(&mut self) -> FirestoreResult<()> {
        self.finished = true;
        self.data.transaction_span.in_scope(|| {
            debug!("Transaction has been finished locally without rolling back to be able to retry it again.");
        });
        Ok(())
    }

    /// Returns whether the transaction has no queued writes yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the transaction's underlying data without consuming it.
    #[inline]
    pub fn transaction_data(&self) -> &FirestoreTransactionData {
        &self.data
    }

    /// Reattaches a [`FirestoreTransactionData`] captured by [`into_data`](Self::into_data) to
    /// `db`, resuming the same transaction.
    ///
    /// The rebuilt value's drop warning is armed again, so commit, rollback or finish it before
    /// it goes out of scope.
    pub fn from_data(
        db: &'a FirestoreDb,
        data: FirestoreTransactionData,
    ) -> FirestoreTransaction<'a> {
        Self {
            db,
            data,
            finished: false,
            request_options: None,
        }
    }

    /// Detaches the transaction's data from its `db` borrow, consuming self.
    ///
    /// Neither commits nor rolls back. Use this to carry a transaction across an `.await`
    /// boundary or a function call that cannot hold the `&FirestoreDb` borrow, and reattach it
    /// later with [`from_data`](Self::from_data).
    #[inline]
    pub fn into_data(mut self) -> FirestoreTransactionData {
        self.finished = true;
        FirestoreTransactionData::new(
            self.data.transaction_id.clone(),
            self.data.document_path.clone(),
            self.data.transaction_span.clone(),
            std::mem::take(&mut self.data.writes),
        )
    }
}

impl FirestoreTransactionOps for FirestoreTransaction<'_> {
    fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>,
    {
        self.data.add(write)?;
        Ok(self)
    }

    fn get_documents_path(&self) -> &String {
        self.data.get_documents_path()
    }
}

impl<'a> Drop for FirestoreTransaction<'a> {
    fn drop(&mut self) {
        if !self.finished {
            self.data
                .transaction_span
                .in_scope(|| warn!("Transaction was neither committed nor rolled back."));
        }
    }
}

impl FirestoreDb {
    /// Opens a new read-write transaction with default options.
    ///
    /// Equivalent to `begin_transaction_with_options(FirestoreTransactionOptions::new())`; see
    /// [`begin_transaction_with_options`](Self::begin_transaction_with_options) for the caller's
    /// obligations and the failure mode, and [`run_transaction`](Self::run_transaction) for a
    /// version that commits and retries for you.
    pub async fn begin_transaction(&self) -> FirestoreResult<FirestoreTransaction<'_>> {
        Self::begin_transaction_with_options(self, FirestoreTransactionOptions::new()).await
    }

    /// Opens a new transaction with `options`, borrowing `self` for the transaction's lifetime.
    ///
    /// The returned [`FirestoreTransaction`] must be committed, rolled back or finished
    /// explicitly - dropping it without doing so only logs a warning and leaves the transaction
    /// open on the server until Firestore's own expiry closes it. Prefer
    /// [`run_transaction_with_options`](Self::run_transaction_with_options) unless you need
    /// direct control over commit and retry.
    ///
    /// Returns an error if converting a read-only `options.mode`'s consistency selector fails,
    /// or if the `BeginTransaction` request itself fails.
    pub async fn begin_transaction_with_options(
        &self,
        options: FirestoreTransactionOptions,
    ) -> FirestoreResult<FirestoreTransaction<'_>> {
        FirestoreTransaction::new(self, options).await
    }

    /// Runs `func` inside a transaction, retrying and committing automatically.
    ///
    /// Equivalent to [`run_transaction_with_options`](Self::run_transaction_with_options) with
    /// [`FirestoreTransactionOptions::new()`]. `func` receives a [`FirestoreDb`] bound to the
    /// transaction's read consistency and the open [`FirestoreTransaction`]; queue writes on the
    /// latter through [`FirestoreTransactionOps`] (`update_object`, `delete_by_id`, `transform`,
    /// ...) or a fluent chain's `add_to_transaction`. A Firestore transaction cannot create a
    /// document with a server-generated ID, so `update_object` with an explicit document ID -
    /// not an insert - is how a transaction creates one.
    ///
    /// On a transient failure - `func` returning [`BackoffError::Transient`], or the commit
    /// returning `ABORTED` - the whole transaction is
    /// retried from the start with exponential backoff, up to `options.max_elapsed_time`. `func`
    /// must therefore be safe to run more than once for the same call: read the state it needs
    /// from the transaction-scoped `db` argument on every invocation rather than closing over
    /// state read before the transaction started, and avoid side effects inside the closure that
    /// are not themselves safe to repeat, such as an external HTTP call. A convenience of this:
    /// `?` on a plain [`FirestoreResult`] inside the closure already converts to
    /// `BackoffError::Transient` for free, through `backoff::Error`'s blanket `From` impl, so the
    /// ordinary error paths already retry; return `Err(BackoffError::Permanent(err))` explicitly
    /// for an error that must not be retried. A permanent error rolls the transaction back and is
    /// returned wrapped in [`FirestoreError::ErrorInTransaction`].
    ///
    /// # Examples
    /// ```rust,no_run
    /// use firestore::*;
    /// use futures::FutureExt;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Clone, Deserialize, Serialize)]
    /// struct Counter {
    ///     value: i64,
    /// }
    ///
    /// # async fn example(db: FirestoreDb) -> FirestoreResult<()> {
    /// db.run_transaction(|db, transaction| {
    ///     async move {
    ///         let mut counter: Counter = db
    ///             .fluent()
    ///             .select()
    ///             .by_id_in("counters")
    ///             .obj()
    ///             .one("visits")
    ///             .await?
    ///             .unwrap_or(Counter { value: 0 });
    ///
    ///         counter.value += 1;
    ///
    ///         db.fluent()
    ///             .update()
    ///             .in_col("counters")
    ///             .document_id("visits")
    ///             .object(&counter)
    ///             .add_to_transaction(transaction)?;
    ///
    ///         Ok(())
    ///     }
    ///     .boxed()
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_transaction<T, FN, E>(&self, func: FN) -> FirestoreResult<T>
    where
        for<'b> FN: Fn(
            FirestoreDb,
            &'b mut FirestoreTransaction,
        ) -> BoxFuture<'b, std::result::Result<T, BackoffError<E>>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.run_transaction_with_options(func, FirestoreTransactionOptions::new())
            .await
    }

    /// Same as [`run_transaction`](Self::run_transaction), with explicit `options`.
    ///
    /// `options.max_elapsed_time` bounds how long the retry loop described on
    /// [`run_transaction`](Self::run_transaction) keeps retrying before giving up.
    ///
    /// Returns an error if the first `BeginTransaction` call fails - the transaction never
    /// opened, so nothing is rolled back - or if `func` returns [`BackoffError::Permanent`], or
    /// if retries are exhausted; in the latter two cases the error is
    /// [`FirestoreError::ErrorInTransaction`] and a rollback is attempted before it is returned.
    pub async fn run_transaction_with_options<T, FN, E>(
        &self,
        func: FN,
        options: FirestoreTransactionOptions,
    ) -> FirestoreResult<T>
    where
        for<'b> FN: Fn(
            FirestoreDb,
            &'b mut FirestoreTransaction,
        ) -> BoxFuture<'b, std::result::Result<T, BackoffError<E>>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Perform our initial attempt. If this fails and the backend tells us we can retry,
        // we'll try again with exponential backoff using the first attempt's transaction ID.
        let (transaction_id, transaction_span, initial_backoff_duration) = {
            let mut transaction = self.begin_transaction_with_options(options.clone()).await?;
            let transaction_id = transaction.transaction_id().clone();
            let transaction_span = transaction.data.transaction_span.clone();
            let mut initial_backoff_duration: Option<Duration> = None;

            let cdb = self.clone_with_consistency_selector(
                FirestoreConsistencySelector::Transaction(transaction_id.clone()),
            );

            match func(cdb, &mut transaction).await {
                Ok(ret_val) => {
                    match transaction.commit().await {
                        Ok(_) => return Ok(ret_val),
                        Err(err) => match err {
                            FirestoreError::DatabaseError(ref db_err) if db_err.retry_possible => {
                                transaction_span.in_scope(|| {
                                    warn!(
                                        %err,
                                        "Transient error occurred while committing transaction.",
                                    )
                                });
                                // Ignore; we'll try again below
                            }
                            other => return Err(other),
                        },
                    }
                }
                Err(err) => match err {
                    BackoffError::Transient { err, retry_after } => {
                        transaction_span.in_scope(|| {
                            warn!(%err, delay = ?retry_after, "Transient error occurred in transaction function. Retrying after the specified delay.");
                        });
                        initial_backoff_duration = retry_after;
                        transaction.finish().ok();
                    }
                    BackoffError::Permanent(err) => {
                        transaction.rollback().await.ok();
                        return Err(FirestoreError::ErrorInTransaction(
                            FirestoreErrorInTransaction::new(transaction_id.clone(), Box::new(err)),
                        ));
                    }
                },
            }

            (transaction_id, transaction_span, initial_backoff_duration)
        };

        // We failed the first time. Now we must change the transaction mode to signal that we're retrying with the original transaction ID.
        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(
                options
                    .max_elapsed_time
                    // Convert to a std `Duration` and clamp any negative durations
                    .map(std::time::Duration::try_from)
                    .transpose()?,
            )
            .with_initial_interval(initial_backoff_duration.unwrap_or(Duration::from_millis(
                backoff::default::INITIAL_INTERVAL_MILLIS,
            )))
            .build();

        let retry_result = retry(backoff, || async {
            let options = FirestoreTransactionOptions {
                mode: FirestoreTransactionMode::ReadWriteRetry(transaction_id.clone()),
                ..options.clone()
            };
            let mut transaction = self
                .begin_transaction_with_options(options)
                .await
                .map_err(firestore_err_to_backoff)?;
            let transaction_id = transaction.transaction_id().clone();

            let cdb = self.clone_with_consistency_selector(
                FirestoreConsistencySelector::Transaction(transaction_id.clone()),
            );

            let ret_val = func(cdb, &mut transaction).await.map_err(|backoff_err| {
                transaction.finish().ok();
                match backoff_err {
                    BackoffError::Transient { err, retry_after } => {
                        transaction_span.in_scope(|| {
                            warn!(%err, delay = ?retry_after, "Transient error occurred in transaction function. Retrying after the specified delay.");
                        });

                        let firestore_err = FirestoreError::ErrorInTransaction(
                            FirestoreErrorInTransaction::new(
                                transaction_id.clone(),
                                Box::new(err)
                            ),
                        );

                        if let Some(retry_after_duration) = retry_after {
                            backoff::Error::retry_after(
                                firestore_err,
                                retry_after_duration
                            )
                        } else {
                            backoff::Error::transient(firestore_err)
                        }
                    }
                    BackoffError::Permanent(err) => {
                        backoff::Error::permanent(
                            FirestoreError::ErrorInTransaction(
                                FirestoreErrorInTransaction::new(
                                    transaction_id.clone(),
                                    Box::new(err)
                                ),
                            )
                        )
                    }
                }
            })?;

            transaction
                .commit()
                .await
                .map_err(firestore_err_to_backoff)?;

            Ok(ret_val)
        })
        .await;

        if let Err(ref err) = retry_result {
            transaction_span.in_scope(|| {
                error!(
                    %err,
                    "Unable to commit transaction. Trying to roll it back.",
                )
            });

            let options = FirestoreTransactionOptions {
                mode: FirestoreTransactionMode::ReadWriteRetry(transaction_id.clone()),
                ..options.clone()
            };
            if let Ok(transaction) = self.begin_transaction_with_options(options).await {
                transaction.rollback().await.ok();
            }
        }

        retry_result
    }
}
