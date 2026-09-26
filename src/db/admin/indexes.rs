//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: lists, plans, applies, prunes and waits on one
//! collection group's composite indexes, single-field overrides and TTL policy against the
//! Firestore Admin API, on the same authenticated channel the data API uses
//! ([`GoogleApiClient::get_with`](gcloud_sdk::GoogleApiClient::get_with)).

use crate::db::admin::index_diff::{plan_index_changes, FirestoreIndexExistingState};
use crate::db::support::FirestoreIndexSupport;
use crate::errors::{FirestoreError, FirestoreErrorPublicGenericDetails, FirestoreSystemError};
use crate::{
    FirestoreCollectionId, FirestoreCompositeIndex, FirestoreDb, FirestoreFieldOverride,
    FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSyncOptions, FirestoreIndexSyncReport,
    FirestoreIndexWait, FirestoreInstant, FirestoreListedCompositeIndex, FirestoreListedField,
    FirestoreOperationWaitOptions, FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::admin::v1::field as proto_field;
use gcloud_sdk::google::firestore::admin::v1::firestore_admin_client::FirestoreAdminClient;
use gcloud_sdk::google::firestore::admin::v1::{
    CreateIndexRequest, DeleteIndexRequest, Field as ProtoField, Index as ProtoIndex,
    ListFieldsRequest, ListIndexesRequest, UpdateFieldRequest,
};
use gcloud_sdk::google::longrunning::operation::Result as LroResult;
use gcloud_sdk::google::longrunning::operations_client::OperationsClient;
use gcloud_sdk::google::longrunning::GetOperationRequest;
use gcloud_sdk::prost_types::FieldMask;
use gcloud_sdk::tonic::Code;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::*;

/// One admin RPC's long-running result that `.sync()` must poll to completion when waiting is
/// requested: the operation's resource name, plus the human-readable action it belongs to, for
/// the wait phase's logging and its timeout error.
struct PendingOperation {
    name: String,
    label: String,
}

/// The result of polling one operation until it is done or the shared wait budget runs out.
/// Distinct from an operation *failing*, which is a [`FirestoreError`] and stops the wait
/// immediately.
enum WaitOutcome {
    Done,
    TimedOut,
}

/// The result of [`FirestoreDb::apply_create_index`]: whether the create actually started a
/// build, or found one already existing (a race with another deployment), which `.sync()` reports
/// as unchanged rather than created.
enum CreateIndexOutcome {
    Created(PendingOperation),
    AlreadyExists,
}

/// Logs one line per listed composite index and field resource in `existing`, before anything is
/// planned, then a summary line. An item this crate's domain model cannot convert is skipped here
/// silently - [`plan_index_changes`] surfaces it in the plan's `unrecognised` list, which
/// [`log_plan`] logs.
fn log_existing_state(group: &FirestoreCollectionId, existing: &FirestoreIndexExistingState) {
    let mut indexes_shown = 0usize;
    for proto in &existing.indexes {
        if let Ok(listed) = FirestoreListedCompositeIndex::try_from(proto.clone()) {
            indexes_shown += 1;
            info!(
                collection_group = group.as_str(),
                "Existing index: {listed}"
            );
        }
    }
    let mut fields_shown = 0usize;
    for proto in &existing.fields {
        let listed = FirestoreListedField::from(proto.clone());
        fields_shown += 1;
        info!(
            collection_group = group.as_str(),
            "Existing field: {listed}"
        );
    }
    info!(
        collection_group = group.as_str(),
        indexes = indexes_shown,
        fields = fields_shown,
        "Existing state summary.",
    );
}

/// Logs one line per planned action - in the same readable form [`Display`](std::fmt::Display)
/// prints - then a summary line of counts. An undeclared item logs as kept when `prune` is unset,
/// or as the action `.sync()` will take on it when `prune` is set.
fn log_plan(group: &FirestoreCollectionId, plan: &FirestoreIndexPlan, prune: bool) {
    for index in &plan.create_indexes {
        info!(
            collection_group = group.as_str(),
            "Plan: create index {index}"
        );
    }
    for declared in &plan.update_fields {
        info!(
            collection_group = group.as_str(),
            "Plan: write field override {declared}"
        );
    }
    for path in &plan.enable_ttl {
        info!(
            collection_group = group.as_str(),
            field_path = path.as_str(),
            "Plan: enable TTL",
        );
    }
    for index in &plan.needs_repair {
        warn!(
            collection_group = group.as_str(),
            "Plan: index needs repair and is left alone: {index}",
        );
    }
    for path in &plan.needs_repair_ttl {
        warn!(
            collection_group = group.as_str(),
            field_path = path.as_str(),
            "Plan: TTL needs repair and is left alone",
        );
    }
    for listed in &plan.undeclared_indexes {
        if prune {
            info!(
                collection_group = group.as_str(),
                "Plan: delete undeclared index {listed}"
            );
        } else {
            info!(
                collection_group = group.as_str(),
                "Plan: keep undeclared index, prune_undeclared() would delete it: {listed}",
            );
        }
    }
    for listed in &plan.undeclared_fields {
        if prune {
            info!(
                collection_group = group.as_str(),
                "Plan: revert undeclared field override {listed}",
            );
        } else {
            info!(
                collection_group = group.as_str(),
                "Plan: keep undeclared field override, prune_undeclared() would revert it: {listed}",
            );
        }
    }
    for listed in &plan.undeclared_ttl {
        if prune {
            info!(
                collection_group = group.as_str(),
                "Plan: disable undeclared TTL {listed}"
            );
        } else {
            info!(
                collection_group = group.as_str(),
                "Plan: keep undeclared TTL, prune_undeclared() would disable it: {listed}",
            );
        }
    }
    for item in &plan.unrecognised {
        info!(
            collection_group = group.as_str(),
            "Plan: unrecognised listed item, never pruned: {item}",
        );
    }
    info!(
        collection_group = group.as_str(),
        create_indexes = plan.create_indexes.len(),
        update_fields = plan.update_fields.len(),
        enable_ttl = plan.enable_ttl.len(),
        unchanged = plan.unchanged.len(),
        pending = plan.pending.len(),
        needs_repair = plan.needs_repair.len(),
        undeclared_indexes = plan.undeclared_indexes.len(),
        undeclared_fields = plan.undeclared_fields.len(),
        undeclared_ttl = plan.undeclared_ttl.len(),
        unrecognised = plan.unrecognised.len(),
        prune,
        "Planned changes summary.",
    );
}

impl FirestoreDb {
    fn admin_client(&self) -> FirestoreAdminClient<gcloud_sdk::GoogleAuthMiddleware> {
        self.inner.client.get_with(FirestoreAdminClient::new)
    }

    fn operations_client(&self) -> OperationsClient<gcloud_sdk::GoogleAuthMiddleware> {
        self.inner.client.get_with(OperationsClient::new)
    }

    fn collection_group_path(&self, group: &FirestoreCollectionId) -> String {
        format!(
            "{}/collectionGroups/{}",
            self.inner.database_path,
            group.as_str()
        )
    }

    async fn list_all_indexes(&self, group_path: &str) -> FirestoreResult<Vec<ProtoIndex>> {
        let mut admin = self.admin_client();
        let mut items = Vec::new();
        let mut page_token = String::new();
        loop {
            let response = admin
                .list_indexes(ListIndexesRequest {
                    parent: group_path.to_string(),
                    filter: String::new(),
                    // ListIndexes rejects any page_size other than 0 ("Invalid page size. Only
                    // 0 is supported."), measured against the real service; still follow
                    // next_page_token, since 0 does not mean "no paging".
                    page_size: 0,
                    page_token: std::mem::take(&mut page_token),
                })
                .await
                .map_err(FirestoreError::from)?
                .into_inner();
            items.extend(response.indexes);
            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }
        Ok(items)
    }

    async fn list_all_fields(
        &self,
        group_path: &str,
        filter: &str,
    ) -> FirestoreResult<Vec<ProtoField>> {
        let mut admin = self.admin_client();
        let mut items = Vec::new();
        let mut page_token = String::new();
        loop {
            let response = admin
                .list_fields(ListFieldsRequest {
                    parent: group_path.to_string(),
                    filter: filter.to_string(),
                    page_size: 0,
                    page_token: std::mem::take(&mut page_token),
                })
                .await
                .map_err(FirestoreError::from)?
                .into_inner();
            items.extend(response.fields);
            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }
        Ok(items)
    }

    /// Lists the owned group's composite indexes and field resources, merging a field that
    /// carries both a listed index override and a listed TTL configuration into one entry.
    ///
    /// `ListFields` also answers with `collectionGroups/__default__/fields/*` alongside the owned
    /// group's own fields (measured against the real service), so membership is decided by a
    /// fixed-prefix match on `{group_path}/fields/` rather than by splitting each resource name on
    /// its last `/`: a backtick-quoted field path can itself contain `/`, and a last-slash split
    /// would cut such a path at the wrong point and either drop it from its group or truncate it.
    async fn list_existing_state(
        &self,
        group: &FirestoreCollectionId,
    ) -> FirestoreResult<(String, FirestoreIndexExistingState)> {
        let group_path = self.collection_group_path(group);
        let span = span!(
            Level::INFO,
            "Firestore Index List",
            "/firestore/collection_group" = group.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let listed = async {
            let (indexes, override_fields, ttl_fields) = tokio::try_join!(
                self.list_all_indexes(&group_path),
                self.list_all_fields(&group_path, "indexConfig.usesAncestorConfig:false"),
                self.list_all_fields(&group_path, "ttlConfig:*"),
            )?;

            let mut merged: HashMap<String, ProtoField> = HashMap::new();
            for f in override_fields {
                merged.insert(f.name.clone(), f);
            }
            for f in ttl_fields {
                merged
                    .entry(f.name.clone())
                    .and_modify(|existing| existing.ttl_config = f.ttl_config)
                    .or_insert(f);
            }
            let fields_prefix = format!("{group_path}/fields/");
            let mut fields: Vec<ProtoField> = merged
                .into_values()
                .filter(|f| f.name.starts_with(fields_prefix.as_str()))
                .collect();
            fields.sort_by(|a, b| a.name.cmp(&b.name));

            debug!(
                indexes = indexes.len(),
                fields = fields.len(),
                "Listed the collection group's indexes and fields.",
            );

            Ok::<_, FirestoreError>(FirestoreIndexExistingState { indexes, fields })
        }
        .instrument(span.clone())
        .await?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        Ok((group_path, listed))
    }

    /// Lists the owned group's existing state, logs it, computes the plan, and logs it - the
    /// step `.plan()` and `.sync()` share. `prune` only changes how [`log_plan`] phrases an
    /// undeclared item; the plan itself does not depend on it.
    async fn plan_against_server(
        &self,
        params: &FirestoreIndexParams,
        prune: bool,
    ) -> FirestoreResult<(String, FirestoreIndexPlan)> {
        let (group_path, existing) = self.list_existing_state(&params.collection_group).await?;
        log_existing_state(&params.collection_group, &existing);

        let diff_span = span!(
            Level::INFO,
            "Firestore Index Diff",
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let plan = diff_span.in_scope(|| plan_index_changes(params, &existing))?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        diff_span.record("/firestore/response_time", elapsed.as_millis());

        log_plan(&params.collection_group, &plan, prune);
        Ok((group_path, plan))
    }

    async fn apply_create_index(
        &self,
        group_path: &str,
        index: &FirestoreCompositeIndex,
    ) -> FirestoreResult<CreateIndexOutcome> {
        let span = span!(
            Level::INFO,
            "Create Index",
            "/firestore/response_time" = field::Empty
        );
        let began = FirestoreInstant::now();
        let proto = ProtoIndex::try_from(index.clone())?;
        let outcome = async {
            let request = CreateIndexRequest {
                parent: group_path.to_string(),
                index: Some(proto),
            };
            match self.admin_client().create_index(request).await {
                Ok(response) => {
                    let operation = response.into_inner();
                    info!(operation = operation.name.as_str(), index = %index, "Created a composite index.");
                    Ok(CreateIndexOutcome::Created(PendingOperation {
                        name: operation.name,
                        label: format!("create index {index}"),
                    }))
                }
                // A race with another deployment: the index already exists, so this counts as
                // unchanged rather than an error.
                Err(status) if status.code() == Code::AlreadyExists => {
                    info!(index = %index, "Index already existed; treating as unchanged.");
                    Ok(CreateIndexOutcome::AlreadyExists)
                }
                Err(status) => {
                    error!(error = %status, index = %index, "Failed to create a composite index.");
                    Err(FirestoreError::from(status))
                }
            }
        }
        .instrument(span.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        outcome
    }

    async fn apply_delete_index(
        &self,
        listed: &FirestoreListedCompositeIndex,
    ) -> FirestoreResult<()> {
        let span = span!(
            Level::INFO,
            "Delete Index",
            "/firestore/response_time" = field::Empty
        );
        let began = FirestoreInstant::now();
        let outcome = async {
            let request = DeleteIndexRequest {
                name: listed.name.clone(),
            };
            match self.admin_client().delete_index(request).await {
                Ok(_) => {
                    info!(index = %listed, "Deleted an undeclared composite index.");
                    Ok(())
                }
                Err(status) => {
                    error!(error = %status, index = %listed, "Failed to delete a composite index.");
                    Err(FirestoreError::from(status))
                }
            }
        }
        .instrument(span.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        outcome
    }

    /// Runs one `UpdateField` request under `span` (already created by the caller with its own
    /// literal name, since `tracing::span!` needs a compile-time name), recording its response
    /// time and logging its outcome.
    async fn run_update_field(
        &self,
        span: Span,
        request: UpdateFieldRequest,
        label: String,
    ) -> FirestoreResult<PendingOperation> {
        let began = FirestoreInstant::now();
        let outcome = async {
            match self.admin_client().update_field(request).await {
                Ok(response) => {
                    let operation = response.into_inner();
                    info!(
                        operation = operation.name.as_str(),
                        label = label.as_str(),
                        "Applied an index management change.",
                    );
                    Ok(PendingOperation {
                        name: operation.name,
                        label,
                    })
                }
                Err(status) => {
                    error!(error = %status, label = label.as_str(), "Failed to apply an index management change.");
                    Err(FirestoreError::from(status))
                }
            }
        }
        .instrument(span.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        outcome
    }

    async fn apply_update_field_override(
        &self,
        group_path: &str,
        declared: &FirestoreFieldOverride,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Update Field",
            "/firestore/response_time" = field::Empty
        );
        let name = format!("{group_path}/fields/{}", declared.target.as_str());
        let index_config = proto_field::IndexConfig::try_from(declared.clone())?;
        let request = UpdateFieldRequest {
            field: Some(ProtoField {
                name,
                index_config: Some(index_config),
                ttl_config: None,
            }),
            update_mask: Some(FieldMask {
                paths: vec!["index_config".to_string()],
            }),
        };
        self.run_update_field(span, request, format!("write field override {declared}"))
            .await
    }

    async fn apply_enable_ttl(
        &self,
        group_path: &str,
        field_path: &str,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Enable TTL",
            "/firestore/response_time" = field::Empty
        );
        let name = format!("{group_path}/fields/{field_path}");
        let request = UpdateFieldRequest {
            field: Some(ProtoField {
                name,
                index_config: None,
                ttl_config: Some(proto_field::TtlConfig::default()),
            }),
            update_mask: Some(FieldMask {
                paths: vec!["ttl_config".to_string()],
            }),
        };
        self.run_update_field(span, request, format!("enable TTL on {field_path}"))
            .await
    }

    async fn apply_revert_field_override(
        &self,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Revert Field Override",
            "/firestore/response_time" = field::Empty,
        );
        let request = UpdateFieldRequest {
            field: Some(ProtoField {
                name: listed.name.clone(),
                index_config: None,
                ttl_config: None,
            }),
            update_mask: Some(FieldMask {
                paths: vec!["index_config".to_string()],
            }),
        };
        self.run_update_field(span, request, format!("revert field override {listed}"))
            .await
    }

    async fn apply_disable_ttl(
        &self,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Disable TTL",
            "/firestore/response_time" = field::Empty
        );
        let request = UpdateFieldRequest {
            field: Some(ProtoField {
                name: listed.name.clone(),
                index_config: None,
                ttl_config: None,
            }),
            update_mask: Some(FieldMask {
                paths: vec!["ttl_config".to_string()],
            }),
        };
        self.run_update_field(
            span,
            request,
            format!("disable TTL on {}", listed.field_path),
        )
        .await
    }

    /// Applies `plan` in the fixed order Firestore requires (create, update fields, enable TTL,
    /// then prune), sequentially: Firestore rejects overlapping field operations on one
    /// collection group. Stops at the first failing action.
    async fn apply_plan(
        &self,
        group_path: &str,
        plan: &FirestoreIndexPlan,
        prune: bool,
    ) -> FirestoreResult<(FirestoreIndexSyncReport, Vec<PendingOperation>)> {
        let mut report = FirestoreIndexSyncReport {
            unchanged: plan.unchanged.clone(),
            pending: plan.pending.clone(),
            needs_repair: plan.needs_repair.clone(),
            pending_ttl: plan.pending_ttl.clone(),
            needs_repair_ttl: plan.needs_repair_ttl.clone(),
            unrecognised: plan.unrecognised.clone(),
            ..Default::default()
        };
        if !prune {
            report.kept_undeclared_indexes = plan.undeclared_indexes.clone();
            report.kept_undeclared_fields = plan.undeclared_fields.clone();
            report.kept_undeclared_ttl = plan.undeclared_ttl.clone();
        }

        let span = span!(
            Level::INFO,
            "Firestore Index Apply",
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let mut pending_operations = Vec::new();

        let apply_result: FirestoreResult<()> = async {
            for index in &plan.create_indexes {
                match self.apply_create_index(group_path, index).await? {
                    CreateIndexOutcome::Created(op) => {
                        pending_operations.push(op);
                        report.created_indexes.push(index.clone());
                    }
                    CreateIndexOutcome::AlreadyExists => report.unchanged.push(index.clone()),
                }
            }
            for declared in &plan.update_fields {
                let op = self
                    .apply_update_field_override(group_path, declared)
                    .await?;
                pending_operations.push(op);
                report.updated_fields.push(declared.clone());
            }
            for path in &plan.enable_ttl {
                let op = self.apply_enable_ttl(group_path, path).await?;
                pending_operations.push(op);
                report.enabled_ttl.push(path.clone());
            }
            if prune {
                for listed in &plan.undeclared_indexes {
                    self.apply_delete_index(listed).await?;
                    report.deleted_indexes.push(listed.clone());
                }
                for listed in &plan.undeclared_fields {
                    let op = self.apply_revert_field_override(listed).await?;
                    pending_operations.push(op);
                    report.reverted_fields.push(listed.clone());
                }
                for listed in &plan.undeclared_ttl {
                    let op = self.apply_disable_ttl(listed).await?;
                    pending_operations.push(op);
                    report.disabled_ttl.push(listed.clone());
                }
            }
            Ok(())
        }
        .instrument(span.clone())
        .await;

        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        apply_result?;

        Ok((report, pending_operations))
    }

    /// Polls one operation until it is done or `deadline` passes, at `poll_interval`.
    async fn wait_for_one_operation(
        &self,
        op: &PendingOperation,
        deadline: Instant,
        poll_interval: Duration,
    ) -> FirestoreResult<WaitOutcome> {
        let span = span!(
            Level::INFO,
            "Wait For Operation",
            "/firestore/operation" = op.name.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let started = Instant::now();
        let mut polls: u32 = 0;
        let outcome = async {
            loop {
                polls += 1;
                let operation = self
                    .operations_client()
                    .get_operation(GetOperationRequest {
                        name: op.name.clone(),
                    })
                    .await
                    .map_err(FirestoreError::from)?
                    .into_inner();
                debug!(
                    poll = polls,
                    operation = op.name.as_str(),
                    label = op.label.as_str(),
                    done = operation.done,
                    "Polled a pending operation.",
                );
                if operation.done {
                    return match operation.result {
                        Some(LroResult::Error(status)) => {
                            error!(
                                operation = op.name.as_str(),
                                label = op.label.as_str(),
                                code = status.code,
                                message = status.message.as_str(),
                                "Operation failed.",
                            );
                            Err(FirestoreError::from(status))
                        }
                        _ => {
                            info!(
                                operation = op.name.as_str(),
                                label = op.label.as_str(),
                                polls,
                                "Operation reached a terminal state.",
                            );
                            Ok(WaitOutcome::Done)
                        }
                    };
                }
                if Instant::now() >= deadline {
                    return Ok(WaitOutcome::TimedOut);
                }
                tokio::time::sleep(poll_interval).await;
            }
        }
        .instrument(span.clone())
        .await;
        let elapsed_ms = started.elapsed().as_millis();
        span.record("/firestore/response_time", elapsed_ms);
        outcome
    }

    /// Waits for every operation `.sync()` started, one after another under one shared deadline:
    /// a timeout on any of them names every operation still pending, not only the one being
    /// polled, since the ones after it were never even checked.
    async fn wait_for_operations(
        &self,
        pending: Vec<PendingOperation>,
        options: &FirestoreOperationWaitOptions,
    ) -> FirestoreResult<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let span = span!(
            Level::INFO,
            "Firestore Index Wait",
            "/firestore/operations" = pending.len(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let deadline = Instant::now() + options.timeout;
        let result: FirestoreResult<()> = async {
            for (position, op) in pending.iter().enumerate() {
                match self
                    .wait_for_one_operation(op, deadline, options.poll_interval)
                    .await?
                {
                    WaitOutcome::Done => {}
                    WaitOutcome::TimedOut => {
                        let remaining: Vec<&str> = pending[position..]
                            .iter()
                            .map(|p| p.label.as_str())
                            .collect();
                        warn!(
                            pending = remaining.join(", "),
                            "Timed out waiting for index operations to finish.",
                        );
                        return Err(FirestoreError::SystemError(FirestoreSystemError::new(
                            FirestoreErrorPublicGenericDetails::new(
                                "OPERATION_WAIT_TIMEOUT".to_string(),
                            ),
                            format!(
                                "timed out after {:?} waiting for: {}",
                                options.timeout,
                                remaining.join(", ")
                            ),
                        )));
                    }
                }
            }
            info!("All index operations reached a terminal state.");
            Ok(())
        }
        .instrument(span.clone())
        .await;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        result
    }
}

#[async_trait]
impl FirestoreIndexSupport for FirestoreDb {
    async fn plan_indexes(
        &self,
        params: FirestoreIndexParams,
    ) -> FirestoreResult<FirestoreIndexPlan> {
        crate::validate_index_params(&params)?;
        if self.inner.is_emulator {
            info!(
                collection_group = params.collection_group.as_str(),
                "Skipping index plan: the Firestore emulator does not implement the admin API.",
            );
            return Ok(FirestoreIndexPlan::default());
        }

        let root = span!(
            Level::INFO,
            "Firestore Index Plan",
            "/firestore/collection_group" = params.collection_group.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let plan = async {
            let (_, plan) = self.plan_against_server(&params, false).await?;
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
        crate::validate_index_params(&params)?;
        if self.inner.is_emulator {
            info!(
                collection_group = params.collection_group.as_str(),
                "Skipping index sync: the Firestore emulator does not implement the admin API.",
            );
            return Ok(FirestoreIndexSyncReport::default());
        }

        let root = span!(
            Level::INFO,
            "Firestore Index Sync",
            "/firestore/collection_group" = params.collection_group.as_str(),
            "/firestore/prune" = options.prune,
            "/firestore/wait" = matches!(options.wait, FirestoreIndexWait::UntilReady(_)),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let report = async {
            let (group_path, plan) = self.plan_against_server(&params, options.prune).await?;
            let (report, pending_operations) =
                self.apply_plan(&group_path, &plan, options.prune).await?;
            if let FirestoreIndexWait::UntilReady(wait_options) = &options.wait {
                self.wait_for_operations(pending_operations, wait_options)
                    .await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake_firestore::{
        done_operation_response, failed_operation_response, list_fields_response,
        list_indexes_response, pending_operation_response, FakeFirestore, FakeResponse,
    };
    use crate::db::FirestoreDbInner;
    use crate::{
        FirestoreCollectionId, FirestoreFieldOverrideIndex, FirestoreFieldOverrideTarget,
        FirestoreIndexField, FirestoreIndexFieldMode, FirestoreQueryDirection,
    };
    use gcloud_sdk::google::firestore::admin::v1::index::{
        ApiScope, IndexField as ProtoIndexField, QueryScope as ProtoQueryScope, State as ProtoState,
    };
    use gcloud_sdk::prost::Message as _;
    use gcloud_sdk::tonic::Code;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc as StdArc, Mutex};

    const LIST_INDEXES: &str = "/google.firestore.admin.v1.FirestoreAdmin/ListIndexes";
    const CREATE_INDEX: &str = "/google.firestore.admin.v1.FirestoreAdmin/CreateIndex";
    const DELETE_INDEX: &str = "/google.firestore.admin.v1.FirestoreAdmin/DeleteIndex";
    const LIST_FIELDS: &str = "/google.firestore.admin.v1.FirestoreAdmin/ListFields";
    const UPDATE_FIELD: &str = "/google.firestore.admin.v1.FirestoreAdmin/UpdateField";
    const GET_OPERATION: &str = "/google.longrunning.Operations/GetOperation";

    const GROUP_PATH: &str = "projects/fake-firestore/databases/(default)/collectionGroups/users";

    fn group() -> FirestoreCollectionId {
        FirestoreCollectionId::from_static("users")
    }

    fn desc(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
        )
    }

    fn order_field(
        path: &str,
        order: gcloud_sdk::google::firestore::admin::v1::index::index_field::Order,
    ) -> ProtoIndexField {
        ProtoIndexField {
            field_path: path.to_string(),
            value_mode: Some(
                gcloud_sdk::google::firestore::admin::v1::index::index_field::ValueMode::Order(
                    order as i32,
                ),
            ),
        }
    }

    /// A listed composite index matching `[a DESC, tags CONTAINS, __name__ ASC]`, in `state`.
    fn listed_index(name: &str, state: ProtoState) -> ProtoIndex {
        use gcloud_sdk::google::firestore::admin::v1::index::index_field::{
            ArrayConfig, ValueMode,
        };
        ProtoIndex {
            name: name.to_string(),
            query_scope: ProtoQueryScope::Collection as i32,
            api_scope: ApiScope::AnyApi as i32,
            fields: vec![
                order_field(
                    "a",
                    gcloud_sdk::google::firestore::admin::v1::index::index_field::Order::Descending,
                ),
                ProtoIndexField {
                    field_path: "tags".to_string(),
                    value_mode: Some(ValueMode::ArrayConfig(ArrayConfig::Contains as i32)),
                },
                order_field(
                    "__name__",
                    gcloud_sdk::google::firestore::admin::v1::index::index_field::Order::Ascending,
                ),
            ],
            state: state as i32,
            density: 0,
            multikey: false,
            shard_count: 0,
            unique: false,
            search_index_options: None,
        }
    }

    fn declared_index() -> FirestoreCompositeIndex {
        FirestoreCompositeIndex::new(vec![
            desc("a"),
            FirestoreIndexField::new("tags".to_string(), FirestoreIndexFieldMode::ArrayContains),
        ])
    }

    fn params_with_index() -> FirestoreIndexParams {
        FirestoreIndexParams::new(group()).with_composite_indexes(vec![declared_index()])
    }

    fn no_writes_allowed(method: &str) -> ! {
        panic!("unexpected write RPC in a read-only scenario: {method}")
    }

    #[tokio::test]
    async fn missing_index_is_created() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => {
                let _ = gcloud_sdk::google::firestore::admin::v1::CreateIndexRequest::decode(bytes)
                    .unwrap();
                (
                    "CreateIndex".to_string(),
                    done_operation_response(&format!("{GROUP_PATH}/operations/op1")),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let report = fake
            .db
            .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert_eq!(report.created_indexes, vec![declared_index()]);
        assert!(fake.calls().contains(&"CreateIndex".to_string()));
    }

    #[tokio::test]
    async fn unchanged_index_causes_no_writes() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_index(
                    &format!("{GROUP_PATH}/indexes/1"),
                    ProtoState::Ready,
                )]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let report = fake
            .db
            .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert_eq!(report.unchanged, vec![declared_index()]);
        assert!(report.created_indexes.is_empty());
    }

    #[tokio::test]
    async fn without_prune_undeclared_index_is_kept_and_reported() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_index(
                    &format!("{GROUP_PATH}/indexes/legacy"),
                    ProtoState::Ready,
                )]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let params = FirestoreIndexParams::new(group());
        let report = fake
            .db
            .sync_indexes(params, FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert_eq!(report.kept_undeclared_indexes.len(), 1);
        assert!(report.deleted_indexes.is_empty());
    }

    #[tokio::test]
    async fn with_prune_undeclared_index_is_deleted() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_index(
                    &format!("{GROUP_PATH}/indexes/legacy"),
                    ProtoState::Ready,
                )]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            DELETE_INDEX => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::DeleteIndexRequest::decode(bytes)
                        .unwrap();
                assert_eq!(request.name, format!("{GROUP_PATH}/indexes/legacy"));
                ("DeleteIndex".to_string(), FakeResponse::empty())
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params = FirestoreIndexParams::new(group());
        let options = FirestoreIndexSyncOptions::new().with_prune(true);
        let report = fake.db.sync_indexes(params, options).await.unwrap();

        assert_eq!(report.deleted_indexes.len(), 1);
        assert!(fake.calls().contains(&"DeleteIndex".to_string()));
    }

    #[tokio::test]
    async fn only_the_owned_group_is_ever_listed() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::ListIndexesRequest::decode(bytes)
                        .unwrap();
                assert_eq!(request.parent, GROUP_PATH);
                assert_eq!(request.page_size, 0);
                ("ListIndexes".to_string(), list_indexes_response(vec![]))
            }
            LIST_FIELDS => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::ListFieldsRequest::decode(bytes)
                        .unwrap();
                assert_eq!(request.parent, GROUP_PATH);
                ("ListFields".to_string(), list_fields_response(vec![]))
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let plan = fake
            .db
            .plan_indexes(FirestoreIndexParams::new(group()))
            .await
            .unwrap();
        assert_eq!(plan, FirestoreIndexPlan::default());
    }

    #[tokio::test]
    async fn wait_polls_until_done() {
        let polls = StdArc::new(AtomicU32::new(0));
        let polls_in_handler = polls.clone();
        let fake = FakeFirestore::start(move |method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&format!("{GROUP_PATH}/operations/op1")),
            ),
            GET_OPERATION => {
                let count = polls_in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                let response = if count < 2 {
                    pending_operation_response(&format!("{GROUP_PATH}/operations/op1"))
                } else {
                    done_operation_response(&format!("{GROUP_PATH}/operations/op1"))
                };
                (format!("GetOperation#{count}"), response)
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(FirestoreIndexWait::UntilReady(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        ));
        let report = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap();

        assert_eq!(report.created_indexes.len(), 1);
        assert!(polls.load(Ordering::SeqCst) >= 2);
        assert!(fake.calls().contains(&"GetOperation#2".to_string()));
    }

    #[tokio::test]
    async fn wait_timeout_returns_an_error_naming_the_operation() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&format!("{GROUP_PATH}/operations/op1")),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                pending_operation_response(&format!("{GROUP_PATH}/operations/op1")),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(FirestoreIndexWait::UntilReady(
            FirestoreOperationWaitOptions::new(Duration::from_millis(20))
                .with_poll_interval(Duration::from_millis(5)),
        ));
        let err = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("create index"));
    }

    #[tokio::test]
    async fn a_failed_operation_surfaces_as_an_error() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&format!("{GROUP_PATH}/operations/op1")),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                failed_operation_response(
                    &format!("{GROUP_PATH}/operations/op1"),
                    9,
                    "index build failed: quota exceeded",
                ),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(FirestoreIndexWait::UntilReady(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5)),
        ));
        let err = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("quota exceeded"));
    }

    #[tokio::test]
    async fn already_exists_on_create_counts_as_unchanged() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex (already exists)".to_string(),
                FakeResponse::Status(Code::AlreadyExists),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let report = fake
            .db
            .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert!(report.created_indexes.is_empty());
        assert_eq!(report.unchanged, vec![declared_index()]);
    }

    #[tokio::test]
    async fn the_emulator_skips_index_management_entirely() {
        let fake = FakeFirestore::start(|method, _| no_writes_allowed(method)).await;

        let emulator_db = FirestoreDb {
            inner: StdArc::new(FirestoreDbInner {
                database_path: fake.db.get_database_path().clone(),
                doc_path: fake.db.get_documents_path().clone(),
                options: fake.db.get_options().clone(),
                client: fake.db.client().clone(),
                is_emulator: true,
            }),
            session_params: fake.db.get_session_params().clone().into(),
        };

        let plan = emulator_db.plan_indexes(params_with_index()).await.unwrap();
        assert_eq!(plan, FirestoreIndexPlan::default());

        let report = emulator_db
            .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
        assert_eq!(report, FirestoreIndexSyncReport::default());

        assert!(
            fake.calls().is_empty(),
            "the emulator path must never contact the server"
        );
    }

    fn field_resource(
        path: &str,
        index_config: Option<gcloud_sdk::google::firestore::admin::v1::field::IndexConfig>,
        ttl_config: Option<gcloud_sdk::google::firestore::admin::v1::field::TtlConfig>,
    ) -> ProtoField {
        ProtoField {
            name: format!("{GROUP_PATH}/fields/{path}"),
            index_config,
            ttl_config,
        }
    }

    #[tokio::test]
    async fn a_field_listed_in_both_filters_is_merged_into_one() {
        use gcloud_sdk::google::firestore::admin::v1::field;
        let override_field = field_resource(
            "tags",
            Some(field::IndexConfig {
                indexes: vec![ProtoIndex {
                    query_scope: ProtoQueryScope::Collection as i32,
                    api_scope: ApiScope::AnyApi as i32,
                    fields: vec![ProtoIndexField {
                        field_path: String::new(),
                        value_mode: Some(
                            gcloud_sdk::google::firestore::admin::v1::index::index_field::ValueMode::Order(
                                gcloud_sdk::google::firestore::admin::v1::index::index_field::Order::Ascending as i32,
                            ),
                        ),
                    }],
                    ..Default::default()
                }],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            None,
        );
        let ttl_field = field_resource(
            "tags",
            None,
            Some(field::TtlConfig {
                state: field::ttl_config::State::Active as i32,
                expiration_offset: None,
            }),
        );

        let fake = FakeFirestore::start(move |method, bytes| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::ListFieldsRequest::decode(bytes)
                        .unwrap();
                if request.filter.contains("ttlConfig") {
                    (
                        "ListFields(ttl)".to_string(),
                        list_fields_response(vec![ttl_field.clone()]),
                    )
                } else {
                    (
                        "ListFields(override)".to_string(),
                        list_fields_response(vec![override_field.clone()]),
                    )
                }
            }
            other => no_writes_allowed(other),
        })
        .await;

        let params = FirestoreIndexParams::new(group())
            .with_field_overrides(vec![FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
                indexes: vec![FirestoreFieldOverrideIndex::new(
                    FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
                )],
            }])
            .with_ttl_fields(vec!["tags".to_string()]);

        let plan = fake.db.plan_indexes(params).await.unwrap();
        assert!(
            plan.update_fields.is_empty(),
            "the override half must already match"
        );
        assert!(
            plan.enable_ttl.is_empty(),
            "the ttl half must already match"
        );
        assert!(plan.unrecognised.is_empty());
    }

    #[tokio::test]
    async fn a_default_group_field_is_ignored() {
        let leaked = ProtoField {
            name:
                "projects/fake-firestore/databases/(default)/collectionGroups/__default__/fields/*"
                    .to_string(),
            index_config: Some(
                gcloud_sdk::google::firestore::admin::v1::field::IndexConfig {
                    indexes: vec![],
                    uses_ancestor_config: false,
                    ancestor_field: String::new(),
                    reverting: false,
                },
            ),
            ttl_config: None,
        };
        let fake = FakeFirestore::start(move |method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => (
                "ListFields".to_string(),
                list_fields_response(vec![leaked.clone()]),
            ),
            other => no_writes_allowed(other),
        })
        .await;

        let plan = fake
            .db
            .plan_indexes(FirestoreIndexParams::new(group()))
            .await
            .unwrap();
        assert!(plan.undeclared_fields.is_empty());
        assert!(plan.undeclared_ttl.is_empty());
        assert!(plan.unrecognised.is_empty());
    }

    #[tokio::test]
    async fn all_fields_apply_and_prune_round_trip() {
        use gcloud_sdk::google::firestore::admin::v1::field;

        let stored: StdArc<Mutex<Option<ProtoField>>> = StdArc::new(Mutex::new(None));

        let stored_for_fields = stored.clone();
        let stored_for_update = stored.clone();
        let fake = FakeFirestore::start(move |method, bytes| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => {
                let existing = stored_for_fields.lock().unwrap().clone();
                (
                    "ListFields".to_string(),
                    list_fields_response(existing.into_iter().collect()),
                )
            }
            UPDATE_FIELD => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::UpdateFieldRequest::decode(bytes)
                        .unwrap();
                let updated_field = request.field.expect("UpdateField always carries a field");
                assert_eq!(updated_field.name, format!("{GROUP_PATH}/fields/*"));
                *stored_for_update.lock().unwrap() =
                    updated_field.index_config.clone().map(|cfg| ProtoField {
                        name: format!("{GROUP_PATH}/fields/*"),
                        index_config: Some(cfg),
                        ttl_config: None,
                    });
                (
                    "UpdateField".to_string(),
                    done_operation_response(&format!("{GROUP_PATH}/operations/op-field")),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let params =
            FirestoreIndexParams::new(group()).with_field_overrides(vec![FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::AllFields,
                indexes: vec![],
            }]);

        let report = fake
            .db
            .sync_indexes(params.clone(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
        assert_eq!(report.updated_fields.len(), 1);
        assert_eq!(
            report.updated_fields[0].target,
            FirestoreFieldOverrideTarget::AllFields
        );

        let replan = fake.db.plan_indexes(params).await.unwrap();
        assert!(
            replan.update_fields.is_empty(),
            "the wildcard override must now match"
        );

        let prune_options = FirestoreIndexSyncOptions::new().with_prune(true);
        let prune_report = fake
            .db
            .sync_indexes(FirestoreIndexParams::new(group()), prune_options)
            .await
            .unwrap();
        assert_eq!(prune_report.reverted_fields.len(), 1);

        assert!(
            stored
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|f| f.index_config.as_ref())
                .is_none()
                || stored.lock().unwrap().is_none(),
            "the wildcard override must be reverted (index_config unset)",
        );
        let _ = field::TtlConfig::default();
    }

    fn capturing_subscriber() -> (
        impl tracing::Subscriber + Send + Sync,
        StdArc<Mutex<Vec<u8>>>,
    ) {
        let buffer = StdArc::new(Mutex::new(Vec::new()));
        let make_writer = {
            let buffer = buffer.clone();
            move || SharedBufferWriter(buffer.clone())
        };
        let subscriber = tracing_subscriber::fmt()
            .with_writer(make_writer)
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_max_level(Level::DEBUG)
            .finish();
        (subscriber, buffer)
    }

    struct SharedBufferWriter(StdArc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedBufferWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn captured_text(buffer: &StdArc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(buffer.lock().unwrap().clone()).unwrap()
    }

    #[tokio::test]
    async fn sync_logs_the_span_tree_and_the_named_lines() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                done_operation_response(&format!("{GROUP_PATH}/operations/op1")),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        let report = {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db
                .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
                .await
                .unwrap()
        };
        let output = captured_text(&buffer);
        assert_eq!(report.created_indexes, vec![declared_index()]);

        for expected in [
            "Firestore Index Sync",
            "Firestore Index List",
            "Firestore Index Diff",
            "Firestore Index Apply",
            "Create Index",
        ] {
            assert!(
                output.contains(expected),
                "missing span {expected:?} in:\n{output}"
            );
        }
        assert!(output.contains("Existing state summary."));
        assert!(output.contains("Plan: create index"));
        assert!(output.contains("Created a composite index."));
        assert!(output.contains("Firestore index sync report"));
        assert!(
            output.contains("/firestore/response_time"),
            "missing recorded response time in:\n{output}"
        );

        let listed_line = output
            .lines()
            .find(|line| line.contains("Listed the collection group's"))
            .unwrap_or_else(|| panic!("no list-summary line in:\n{output}"));
        assert!(listed_line.contains("Firestore Index Sync"));
        assert!(listed_line.contains("Firestore Index List"));
    }

    #[tokio::test]
    async fn plan_logs_no_execution() {
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db.plan_indexes(params_with_index()).await.unwrap();
        }
        let output = captured_text(&buffer);

        assert!(output.contains("nothing was applied"));
        assert!(!output.contains("Firestore Index Apply"));
        assert!(!output.contains("Created a composite index."));
        assert!(!output.contains("Applied an index management change."));
    }
}
