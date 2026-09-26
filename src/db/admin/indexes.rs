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
