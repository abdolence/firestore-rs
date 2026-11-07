//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: the declarative `.plan()` and `.sync()` of one
//! collection group, built on the listing, diff, apply and wait steps of
//! [`indexes`](super::indexes).

use crate::db::admin::indexes::SEQUENCING_TIMEOUT;
use crate::db::admin::operation_wait::OperationDeadline;
use crate::db::support::FirestoreIndexSupport;
use crate::errors::FirestoreError;
use crate::{
    FirestoreDb, FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSyncOptions,
    FirestoreIndexSyncReport, FirestoreIndexSyncSkipReason, FirestoreIndexSyncTimings,
    FirestoreInstant, FirestoreOperationWaitOptions, FirestoreResult,
};
use async_trait::async_trait;
use std::time::Instant;
use tracing::*;

#[async_trait]
impl FirestoreIndexSupport for FirestoreDb {
    async fn plan_indexes(
        &self,
        params: FirestoreIndexParams,
        options: FirestoreIndexSyncOptions,
    ) -> FirestoreResult<FirestoreIndexPlan> {
        crate::validate_index_params(&params)?;
        if self.inner.is_emulator {
            info!(
                collection_group = params.collection_group.as_str(),
                "Skipping index plan: the Firestore emulator does not implement the admin API.",
            );
            return Ok(FirestoreIndexPlan {
                skipped: Some(FirestoreIndexSyncSkipReason::Emulator),
                ..Default::default()
            });
        }

        let root = span!(
            Level::INFO,
            "Firestore Index Plan",
            "/firestore/collection_group" = params.collection_group.as_str(),
            "/firestore/prune" = options.prune,
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let plan = async {
            let group = self.read_owned_group(&params.collection_group).await?;
            let (plan, _) = self
                .plan_against_server(&params, &group, options.prune)
                .await?;
            info!(
                collection_group = params.collection_group.as_str(),
                "plan() reports what sync() would change; nothing was applied.",
            );
            Ok::<_, FirestoreError>(plan)
        }
        .instrument(root.clone())
        .await?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        root.record("/firestore/response_time", elapsed.as_millis());
        Ok(plan)
    }

    async fn sync_indexes(
        &self,
        params: FirestoreIndexParams,
        options: FirestoreIndexSyncOptions,
    ) -> FirestoreResult<FirestoreIndexSyncReport> {
        let started = Instant::now();
        crate::validate_index_params(&params)?;
        if self.inner.is_emulator {
            info!(
                collection_group = params.collection_group.as_str(),
                "Skipping index sync: the Firestore emulator does not implement the admin API.",
            );
            return Ok(FirestoreIndexSyncReport {
                skipped: Some(FirestoreIndexSyncSkipReason::Emulator),
                timings: FirestoreIndexSyncTimings {
                    total: started.elapsed(),
                    list: None,
                    apply: None,
                    wait: None,
                },
                ..Default::default()
            });
        }

        let root = span!(
            Level::INFO,
            "Firestore Index Sync",
            "/firestore/collection_group" = params.collection_group.as_str(),
            "/firestore/prune" = options.prune,
            "/firestore/wait" = options.wait.is_some(),
            "/firestore/response_time" = field::Empty,
        );
        let implicit_wait = FirestoreOperationWaitOptions::new(SEQUENCING_TIMEOUT);
        let deadline = OperationDeadline::from_now(options.wait.as_ref().unwrap_or(&implicit_wait));
        let began = FirestoreInstant::now();
        let report = async {
            let group = self.read_owned_group(&params.collection_group).await?;
            let (plan, list_elapsed) = self
                .plan_against_server(&params, &group, options.prune)
                .await?;
            let apply_started = Instant::now();
            let (mut report, applied) = self.apply_plan(&group, &plan, &deadline).await;
            report.timings.list = Some(list_elapsed);
            report.timings.apply = Some(apply_started.elapsed());
            let finished = match (applied, &options.wait) {
                (Ok(pending_operations), Some(_)) => {
                    let wait_started = Instant::now();
                    let waited = self
                        .wait_for_index_operations(&pending_operations, &deadline)
                        .await;
                    report.timings.wait = Some(wait_started.elapsed());
                    waited
                }
                (Ok(_), None) => Ok(()),
                (Err(err), _) => Err(err),
            };
            report.timings.total = started.elapsed();
            if let Err(err) = finished {
                warn!(
                    collection_group = params.collection_group.as_str(),
                    "The sync failed after it started writing; what it applied before that: {report}",
                );
                return Err(err);
            }
            info!(
                collection_group = params.collection_group.as_str(),
                "{report}"
            );
            Ok::<_, FirestoreError>(report)
        }
        .instrument(root.clone())
        .await?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        root.record("/firestore/response_time", elapsed.as_millis());
        Ok(report)
    }
}
