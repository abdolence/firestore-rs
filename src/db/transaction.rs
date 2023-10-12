use crate::errors::*;
use crate::timestamp_utils::from_timestamp;
use crate::{
    FirestoreConsistencySelector, FirestoreDb, FirestoreError, FirestoreResult,
    FirestoreTransactionId, FirestoreTransactionMode, FirestoreTransactionOptions,
    FirestoreTransactionResponse, FirestoreWriteResult,
};
use backoff::future::retry;
use backoff::ExponentialBackoffBuilder;
use futures::future::BoxFuture;
use gcloud_sdk::google::firestore::v1::{BeginTransactionRequest, CommitRequest, RollbackRequest};
use std::time::Duration;
use tracing::*;

pub struct FirestoreTransaction<'a> {
    pub db: &'a FirestoreDb,
    pub transaction_id: FirestoreTransactionId,
    pub transaction_span: Span,
    writes: Vec<gcloud_sdk::google::firestore::v1::Write>,
    finished: bool,
}

impl<'a> FirestoreTransaction<'a> {
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

        Ok(Self {
            db,
            transaction_id: response.transaction,
            transaction_span,
            writes: Vec::new(),
            finished: false,
        })
    }

    #[inline]
    pub fn transaction_id(&self) -> &FirestoreTransactionId {
        &self.transaction_id
    }

    #[inline]
    pub fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>,
    {
        self.writes.push(write.try_into()?);
        Ok(self)
    }

    pub async fn commit(mut self) -> FirestoreResult<FirestoreTransactionResponse> {
        self.finished = true;

        if self.writes.is_empty() {
            self.transaction_span.in_scope(|| {
                debug!("Transaction has been committed without any writes.");
            });

            return Ok(FirestoreTransactionResponse::new(Vec::new()));
        }

        let request = gcloud_sdk::tonic::Request::new(CommitRequest {
            database: self.db.get_database_path().clone(),
            writes: self.writes.drain(..).collect(),
            transaction: self.transaction_id.clone(),
        });

        let response = self.db.client().get().commit(request).await?.into_inner();

        let result = FirestoreTransactionResponse::new(
            response
                .write_results
                .into_iter()
                .map(|s| s.try_into())
                .collect::<FirestoreResult<Vec<FirestoreWriteResult>>>()?,
        )
        .opt_commit_time(response.commit_time.map(from_timestamp).transpose()?);

        if let Some(ref commit_time) = result.commit_time {
            self.transaction_span
                .record("/firestore/commit_time", commit_time.to_rfc3339());
        }

        self.transaction_span.in_scope(|| {
            debug!("Transaction has been committed.");
        });

        Ok(result)
    }

    pub async fn rollback(mut self) -> FirestoreResult<()> {
        self.finished = true;
        let request = gcloud_sdk::tonic::Request::new(RollbackRequest {
            database: self.db.get_database_path().clone(),
            transaction: self.transaction_id.clone(),
        });

        self.db.client().get().rollback(request).await?;

        self.transaction_span.in_scope(|| {
            debug!("Transaction has been rolled back.");
        });

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}

impl<'a> Drop for FirestoreTransaction<'a> {
    fn drop(&mut self) {
        if !self.finished {
            self.transaction_span
                .in_scope(|| warn!("Transaction was neither committed nor rolled back."));
        }
    }
}

impl FirestoreDb {
    pub async fn begin_transaction(&self) -> FirestoreResult<FirestoreTransaction> {
        Self::begin_transaction_with_options(self, FirestoreTransactionOptions::new()).await
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: FirestoreTransactionOptions,
    ) -> FirestoreResult<FirestoreTransaction> {
        FirestoreTransaction::new(self, options).await
    }

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
            let transaction_span = transaction.transaction_span.clone();
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
                    }
                    BackoffError::Permanent(err) => {
                        return Err(FirestoreError::ErrorInTransaction(
                            FirestoreErrorInTransaction::new(transaction_id.clone(), Box::new(err)),
                        ))
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
                    .map(|v| v.to_std())
                    .transpose()?,
            )
            .with_initial_interval(initial_backoff_duration.unwrap_or(Duration::from_millis(
                backoff::default::INITIAL_INTERVAL_MILLIS,
            )))
            .build();

        let retry_result = retry(backoff, || async {
            let options = FirestoreTransactionOptions {
                mode: FirestoreTransactionMode::ReadWriteRetry(transaction_id.clone()),
                ..options
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
                ..options
            };
            if let Ok(transaction) = self.begin_transaction_with_options(options).await {
                transaction.rollback().await.ok();
            }
        }

        retry_result
    }
}
