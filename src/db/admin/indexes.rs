//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: lists, plans, applies, prunes and waits on one
//! collection group's composite indexes, single-field overrides and TTL policy against the
//! Firestore Admin API, on the same authenticated channel the data API uses
//! ([`GoogleApiClient::get_with`](gcloud_sdk::GoogleApiClient::get_with)).

use crate::db::admin::index_diff::{
    plan_index_changes, FirestoreIndexExistingState, FirestoreIndexListing,
};
use crate::db::admin::index_models::write_section;
use crate::db::admin::operation_wait::{OperationAction, StartedOperation};
use crate::db::support::FirestoreIndexSupport;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreCompositeIndex, FirestoreDb, FirestoreFieldOverride,
    FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSyncOptions, FirestoreIndexSyncReport,
    FirestoreInstant, FirestoreListedCompositeIndex, FirestoreListedField,
    FirestoreOperationWaitOptions, FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::admin::v1::field as proto_field;
use gcloud_sdk::google::firestore::admin::v1::firestore_admin_client::FirestoreAdminClient;
use gcloud_sdk::google::firestore::admin::v1::{
    CreateIndexRequest, DeleteIndexRequest, Field as ProtoField, Index as ProtoIndex,
    ListFieldsRequest, ListIndexesRequest, UpdateFieldRequest,
};
use gcloud_sdk::google::longrunning::operations_client::OperationsClient;
use gcloud_sdk::prost_types::FieldMask;
use gcloud_sdk::tonic::Code;
use std::collections::HashMap;
use tracing::*;

/// One write `.sync()` sends for the owned collection group, holding the declared or listed
/// item it writes. `Display` is the text every log line and wait error uses for the write, such as
/// `enable TTL on expires_at`, so the two never disagree on what an operation was for.
enum IndexAction {
    CreateIndex(FirestoreCompositeIndex),
    UpdateFieldOverride(FirestoreFieldOverride),
    EnableTtl(String),
    RevertFieldOverride(FirestoreListedField),
    DisableTtl(FirestoreListedField),
    DeleteIndex(FirestoreListedCompositeIndex),
}

impl OperationAction for IndexAction {
    fn kind(&self) -> &'static str {
        match self {
            IndexAction::CreateIndex(_) => "create_index",
            IndexAction::UpdateFieldOverride(_) => "update_field_override",
            IndexAction::EnableTtl(_) => "enable_ttl",
            IndexAction::RevertFieldOverride(_) => "revert_field_override",
            IndexAction::DisableTtl(_) => "disable_ttl",
            IndexAction::DeleteIndex(_) => "delete_index",
        }
    }
}

impl std::fmt::Display for IndexAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexAction::CreateIndex(index) => write!(f, "create index {index}"),
            IndexAction::UpdateFieldOverride(declared) => {
                write!(f, "write field override {declared}")
            }
            IndexAction::EnableTtl(field_path) => write!(f, "enable TTL on {field_path}"),
            IndexAction::RevertFieldOverride(listed) => {
                write!(f, "revert field override {listed}")
            }
            IndexAction::DisableTtl(listed) => write!(f, "disable TTL on {}", listed.field_path),
            IndexAction::DeleteIndex(listed) => write!(f, "delete index {listed}"),
        }
    }
}

type PendingOperation = StartedOperation<IndexAction>;

/// The result of [`FirestoreDb::apply_create_index`]: whether the create actually started a
/// build, or found one already existing (a race with another deployment), which `.sync()` reports
/// as unchanged rather than created.
enum CreateIndexOutcome {
    Created(PendingOperation),
    AlreadyExists,
}

/// The owned group's existing state as one grouped block, reusing the per-item `Display` impls
/// the plan and report use (via [`write_section`]), so this listing and those never disagree on
/// how an item reads. A listed index the domain model cannot represent is left out here; the plan
/// lists it, with its reason, under `unrecognised`.
impl std::fmt::Display for FirestoreIndexListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_overrides: Vec<&FirestoreListedField> = self.field_overrides().collect();
        let ttl_fields: Vec<&FirestoreListedField> = self.ttl_fields().collect();
        writeln!(
            f,
            "Existing state: {} indexes, {} field overrides, {} TTL fields",
            self.indexes.len(),
            field_overrides.len(),
            ttl_fields.len(),
        )?;
        write_section(f, "indexes", &self.indexes)?;
        write_section(f, "field_overrides", &field_overrides)?;
        write_section(f, "ttl_fields", &ttl_fields)
    }
}

/// Logs the owned group's existing state as one grouped event, before anything is planned.
fn log_existing_state(group: &FirestoreCollectionId, listing: &FirestoreIndexListing) {
    info!(
        collection_group = group.as_str(),
        composite_indexes = listing.indexes.len(),
        field_overrides = listing.field_overrides().count(),
        ttl_fields = listing.ttl_fields().count(),
        "{listing}",
    );
}

/// Logs `plan` as one grouped event, in the same readable form its own
/// [`Display`](std::fmt::Display) prints, so the log and a caller's own `println!("{plan}")`
/// never disagree. `NEEDS_REPAIR` items additionally get their own `warn!`, since they need
/// attention `info` logging would not draw.
fn log_plan(group: &FirestoreCollectionId, plan: &FirestoreIndexPlan) {
    info!(
        collection_group = group.as_str(),
        create_indexes = plan.create_indexes.len(),
        update_fields = plan.update_fields.len(),
        enable_ttl = plan.enable_ttl.len(),
        unchanged = plan.unchanged.len(),
        pending = plan.pending.len(),
        delete_indexes = plan.delete_indexes.len(),
        revert_fields = plan.revert_fields.len(),
        disable_ttl = plan.disable_ttl.len(),
        kept_undeclared_indexes = plan.kept_undeclared_indexes.len(),
        kept_undeclared_fields = plan.kept_undeclared_fields.len(),
        kept_undeclared_ttl = plan.kept_undeclared_ttl.len(),
        unrecognised = plan.unrecognised.len(),
        "{plan}",
    );

    if !plan.needs_repair.is_empty() || !plan.needs_repair_ttl.is_empty() {
        let items: Vec<String> = plan
            .needs_repair
            .iter()
            .map(ToString::to_string)
            .chain(plan.needs_repair_ttl.iter().cloned())
            .collect();
        warn!(
            collection_group = group.as_str(),
            needs_repair_indexes = plan.needs_repair.len(),
            needs_repair_ttl = plan.needs_repair_ttl.len(),
            "NEEDS_REPAIR, left alone: {}",
            items.join("; "),
        );
    }
}

/// Refuses to touch a resource that is not under `group_path`, independent of the filtering
/// `list_existing_state` already applies to what it reports as undeclared in the first place.
///
/// The last line of defense before a delete or a revert: measured against the real service,
/// 2026-09-26, `ListIndexes` scoped to one collection group's parent still answered with
/// composite indexes belonging to other groups in the same database, so a resource's presence in
/// a listing response is not enough on its own to trust it with `prune_undeclared()`.
fn ensure_owned_resource(group_path: &str, kind: &str, name: &str) -> FirestoreResult<()> {
    let prefix = format!("{group_path}/{kind}/");
    if name.starts_with(prefix.as_str()) {
        Ok(())
    } else {
        Err(FirestoreError::invalid_parameters(
            "resource_name",
            format!("refusing to touch {name}: not under the owned group {group_path}"),
        ))
    }
}

impl FirestoreDb {
    /// Shared with bulk delete ([`crate::db::admin::bulk_delete`]): both build their client on
    /// this same authenticated channel.
    pub(crate) fn admin_client(&self) -> FirestoreAdminClient<gcloud_sdk::GoogleAuthMiddleware> {
        self.inner.client.get_with(FirestoreAdminClient::new)
    }

    /// Shared with [`crate::db::admin::operation_wait`], which both index sync and bulk delete
    /// poll long-running operations through.
    pub(crate) fn operations_client(&self) -> OperationsClient<gcloud_sdk::GoogleAuthMiddleware> {
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
    ) -> FirestoreResult<(String, FirestoreIndexListing)> {
        let group_path = self.collection_group_path(group);
        let span = span!(
            Level::INFO,
            "Firestore Index List",
            "/firestore/collection_group" = group.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let listed = async {
            let (all_indexes, override_fields, ttl_fields) = tokio::try_join!(
                self.list_all_indexes(&group_path),
                self.list_all_fields(&group_path, "indexConfig.usesAncestorConfig:false"),
                self.list_all_fields(&group_path, "ttlConfig:*"),
            )?;

            // Measured against the real service, 2026-09-26: `ListIndexes` scoped to one
            // collection group's parent has still answered with composite indexes belonging to
            // other groups in the same database. Membership is decided here, the same way as for
            // fields below, rather than trusted from the request scope - `sync()`'s prune path
            // must never reach an index this crate did not itself confirm belongs to the owned
            // group.
            let indexes_prefix = format!("{group_path}/indexes/");
            let total_indexes = all_indexes.len();
            let indexes: Vec<ProtoIndex> = all_indexes
                .into_iter()
                .filter(|index| index.name.starts_with(indexes_prefix.as_str()))
                .collect();
            let skipped_indexes = total_indexes - indexes.len();

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
            let total_fields = merged.len();
            let mut fields: Vec<ProtoField> = merged
                .into_values()
                .filter(|f| f.name.starts_with(fields_prefix.as_str()))
                .collect();
            fields.sort_by(|a, b| a.name.cmp(&b.name));
            let skipped_fields = total_fields - fields.len();

            debug!(
                indexes = indexes.len(),
                fields = fields.len(),
                skipped_indexes,
                skipped_fields,
                "Listed the collection group's indexes and fields.",
            );

            Ok::<_, FirestoreError>(FirestoreIndexListing::from(FirestoreIndexExistingState {
                indexes,
                fields,
            }))
        }
        .instrument(span.clone())
        .await?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        Ok((group_path, listed))
    }

    /// Lists the owned group's existing state, logs it, computes the plan, and logs it - the
    /// step `.plan()` and `.sync()` share, so a plan made with the same `prune` is exactly what
    /// the sync applies.
    async fn plan_against_server(
        &self,
        params: &FirestoreIndexParams,
        prune: bool,
    ) -> FirestoreResult<(String, FirestoreIndexPlan)> {
        let (group_path, listing) = self.list_existing_state(&params.collection_group).await?;
        log_existing_state(&params.collection_group, &listing);

        let diff_span = span!(
            Level::INFO,
            "Firestore Index Diff",
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let plan = diff_span.in_scope(|| plan_index_changes(params, &listing, prune))?;
        let elapsed = FirestoreInstant::now().duration_since(began);
        diff_span.record("/firestore/response_time", elapsed.as_millis());

        log_plan(&params.collection_group, &plan);
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
            let action = IndexAction::CreateIndex(index.clone());
            match self.admin_client().create_index(request).await {
                Ok(response) => {
                    let operation = response.into_inner();
                    info!(
                        operation = operation.name.as_str(),
                        action = action.kind(),
                        index = %index,
                        "Created a composite index.",
                    );
                    Ok(CreateIndexOutcome::Created(StartedOperation {
                        name: operation.name,
                        action,
                    }))
                }
                // A race with another deployment: the index already exists, so this counts as
                // unchanged rather than an error.
                Err(status) if status.code() == Code::AlreadyExists => {
                    info!(action = action.kind(), index = %index, "Index already existed; treating as unchanged.");
                    Ok(CreateIndexOutcome::AlreadyExists)
                }
                Err(status) => {
                    error!(error = %status, action = action.kind(), index = %index, "Failed to create a composite index.");
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
        group_path: &str,
        listed: &FirestoreListedCompositeIndex,
    ) -> FirestoreResult<()> {
        ensure_owned_resource(group_path, "indexes", &listed.name)?;
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
            let action = IndexAction::DeleteIndex(listed.clone());
            match self.admin_client().delete_index(request).await {
                Ok(_) => {
                    info!(action = action.kind(), index = %listed, "Deleted an undeclared composite index.");
                    Ok(())
                }
                Err(status) => {
                    error!(error = %status, action = action.kind(), index = %listed, "Failed to delete a composite index.");
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
        action: IndexAction,
    ) -> FirestoreResult<PendingOperation> {
        let began = FirestoreInstant::now();
        let outcome = async {
            match self.admin_client().update_field(request).await {
                Ok(response) => {
                    let operation = response.into_inner();
                    info!(
                        operation = operation.name.as_str(),
                        action = action.kind(),
                        label = %action,
                        "Applied an index management change.",
                    );
                    Ok(StartedOperation {
                        name: operation.name,
                        action,
                    })
                }
                Err(status) => {
                    error!(
                        error = %status,
                        action = action.kind(),
                        label = %action,
                        "Failed to apply an index management change.",
                    );
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
        self.run_update_field(
            span,
            request,
            IndexAction::UpdateFieldOverride(declared.clone()),
        )
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
        self.run_update_field(
            span,
            request,
            IndexAction::EnableTtl(field_path.to_string()),
        )
        .await
    }

    async fn apply_revert_field_override(
        &self,
        group_path: &str,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        ensure_owned_resource(group_path, "fields", &listed.name)?;
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
        self.run_update_field(
            span,
            request,
            IndexAction::RevertFieldOverride(listed.clone()),
        )
        .await
    }

    async fn apply_disable_ttl(
        &self,
        group_path: &str,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        ensure_owned_resource(group_path, "fields", &listed.name)?;
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
        self.run_update_field(span, request, IndexAction::DisableTtl(listed.clone()))
            .await
    }

    /// Applies `plan` in the fixed order Firestore requires (create, update fields, enable TTL,
    /// then prune), sequentially: Firestore rejects overlapping field operations on one
    /// collection group. Stops at the first failing action.
    async fn apply_plan(
        &self,
        group_path: &str,
        plan: &FirestoreIndexPlan,
    ) -> FirestoreResult<(FirestoreIndexSyncReport, Vec<PendingOperation>)> {
        let mut report = FirestoreIndexSyncReport {
            unchanged: plan.unchanged.clone(),
            pending: plan.pending.clone(),
            needs_repair: plan.needs_repair.clone(),
            pending_ttl: plan.pending_ttl.clone(),
            needs_repair_ttl: plan.needs_repair_ttl.clone(),
            unrecognised: plan.unrecognised.clone(),
            kept_undeclared_indexes: plan.kept_undeclared_indexes.clone(),
            kept_undeclared_fields: plan.kept_undeclared_fields.clone(),
            kept_undeclared_ttl: plan.kept_undeclared_ttl.clone(),
            ..Default::default()
        };

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
            for listed in &plan.delete_indexes {
                self.apply_delete_index(group_path, listed).await?;
                report.deleted_indexes.push(listed.clone());
            }
            for listed in &plan.revert_fields {
                let op = self.apply_revert_field_override(group_path, listed).await?;
                pending_operations.push(op);
                report.reverted_fields.push(listed.clone());
            }
            for listed in &plan.disable_ttl {
                let op = self.apply_disable_ttl(group_path, listed).await?;
                pending_operations.push(op);
                report.disabled_ttl.push(listed.clone());
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

    /// Waits for every operation `.sync()` started under one shared deadline (see
    /// [`FirestoreDb::wait_for_operations`]), in a `Firestore Index Wait` span.
    async fn wait_for_index_operations(
        &self,
        pending: &[PendingOperation],
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
        let operations: Vec<&PendingOperation> = pending.iter().collect();
        let result = async {
            self.wait_for_operations(&operations, options, |_, _| {})
                .await?;
            let elapsed = FirestoreInstant::now().duration_since(began);
            info!(
                operations = pending.len(),
                elapsed_ms = elapsed.as_millis(),
                "All index operations reached a terminal state in {} ms.",
                elapsed.as_millis(),
            );
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
        options: FirestoreIndexSyncOptions,
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
            "/firestore/prune" = options.prune,
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let plan = async {
            let (_, plan) = self.plan_against_server(&params, options.prune).await?;
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
            "/firestore/wait" = options.wait.is_some(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let report = async {
            let (group_path, plan) = self.plan_against_server(&params, options.prune).await?;
            let (report, pending_operations) = self.apply_plan(&group_path, &plan).await?;
            if let Some(wait_options) = &options.wait {
                if let Err(err) = self
                    .wait_for_index_operations(&pending_operations, wait_options)
                    .await
                {
                    warn!(
                        collection_group = params.collection_group.as_str(),
                        "Waiting for the applied changes failed; what was applied before it: {report}",
                    );
                    return Err(err);
                }
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
    use crate::db::admin::index_diff::tests::{
        field_resource, listed_index, order_field, USERS_GROUP_PATH,
    };
    use crate::db::fake_firestore::{
        done_operation_response, failed_operation_response, list_fields_response,
        list_indexes_response, pending_operation_response, FakeFirestore, FakeResponse,
    };
    use crate::db::FirestoreDbInner;
    use crate::{
        FirestoreCollectionId, FirestoreFieldOverrideIndex, FirestoreFieldOverrideTarget,
        FirestoreIndexField, FirestoreIndexFieldMode, FirestoreQueryDirection,
        FirestoreVectorIndexConfig,
    };
    use gcloud_sdk::google::firestore::admin::v1::index::{
        ApiScope, IndexField as ProtoIndexField, QueryScope as ProtoQueryScope, State as ProtoState,
    };
    use gcloud_sdk::prost::Message as _;
    use gcloud_sdk::tonic::Code;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    const LIST_INDEXES: &str = "/google.firestore.admin.v1.FirestoreAdmin/ListIndexes";
    const CREATE_INDEX: &str = "/google.firestore.admin.v1.FirestoreAdmin/CreateIndex";
    const DELETE_INDEX: &str = "/google.firestore.admin.v1.FirestoreAdmin/DeleteIndex";
    const LIST_FIELDS: &str = "/google.firestore.admin.v1.FirestoreAdmin/ListFields";
    const UPDATE_FIELD: &str = "/google.firestore.admin.v1.FirestoreAdmin/UpdateField";
    const GET_OPERATION: &str = "/google.longrunning.Operations/GetOperation";

    const GROUP_PATH: &str = USERS_GROUP_PATH;

    fn group() -> FirestoreCollectionId {
        FirestoreCollectionId::from_static("users")
    }

    fn desc(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
        )
    }

    /// A listed `[a DESC, tags CONTAINS, __name__ ASC]` index at `name` in `state`: the stored
    /// shape of [`declared_index`], under a resource name each test picks.
    fn listed_declared_index(name: &str, state: ProtoState) -> ProtoIndex {
        use gcloud_sdk::google::firestore::admin::v1::index::index_field::{
            ArrayConfig, Order, ValueMode,
        };
        ProtoIndex {
            name: name.to_string(),
            ..listed_index(
                vec![
                    order_field("a", Order::Descending),
                    ProtoIndexField {
                        field_path: "tags".to_string(),
                        value_mode: Some(ValueMode::ArrayConfig(ArrayConfig::Contains as i32)),
                    },
                    order_field("__name__", Order::Ascending),
                ],
                ProtoQueryScope::Collection,
                state,
            )
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

    /// Serializes every test in this module against every other. `tracing`'s per-callsite
    /// interest cache is shared process-wide rather than per-thread, and every test here shares
    /// the same "Firestore Index *" span callsites (there is only one `span!(...,
    /// "Firestore Index Sync", ...)` call site in the source, for example); with two of these
    /// tests' `FirestoreDb` calls in flight on different threads at once, that shared cache has
    /// been observed to report one thread's spans as filtered out, dropping a span or an event
    /// this module's tests assert on. No test here is slow enough for the lost parallelism to
    /// matter.
    static MODULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn missing_index_is_created() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
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

    /// One undeclared composite index, one undeclared exempt override on `legacy_field`, and one
    /// undeclared active TTL on `expires_at`, all in the owned group.
    fn undeclared_items_listing(method: &str) -> (String, FakeResponse) {
        use gcloud_sdk::google::firestore::admin::v1::field;
        match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
                    &format!("{GROUP_PATH}/indexes/legacy"),
                    ProtoState::Ready,
                )]),
            ),
            LIST_FIELDS => (
                "ListFields".to_string(),
                list_fields_response(vec![
                    field_resource(
                        "legacy_field",
                        Some(field::IndexConfig {
                            indexes: vec![],
                            uses_ancestor_config: false,
                            ancestor_field: String::new(),
                            reverting: false,
                        }),
                        None,
                    ),
                    field_resource(
                        "expires_at",
                        None,
                        Some(field::TtlConfig {
                            state: field::ttl_config::State::Active as i32,
                            expiration_offset: None,
                        }),
                    ),
                ]),
            ),
            other => no_writes_allowed(other),
        }
    }

    #[tokio::test]
    async fn a_plan_with_prune_lists_what_a_pruning_sync_would_remove() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| undeclared_items_listing(method)).await;

        let pruning = fake
            .db
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new().with_prune(true),
            )
            .await
            .unwrap();
        assert_eq!(pruning.delete_indexes.len(), 1);
        assert_eq!(pruning.revert_fields.len(), 1);
        assert_eq!(pruning.disable_ttl.len(), 1);
        assert!(pruning.kept_undeclared_indexes.is_empty());
        assert!(pruning.kept_undeclared_fields.is_empty());
        assert!(pruning.kept_undeclared_ttl.is_empty());

        let keeping = fake
            .db
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();
        assert!(keeping.delete_indexes.is_empty());
        assert!(keeping.revert_fields.is_empty());
        assert!(keeping.disable_ttl.is_empty());
        assert_eq!(keeping.kept_undeclared_indexes.len(), 1);
        assert_eq!(keeping.kept_undeclared_fields.len(), 1);
        assert_eq!(keeping.kept_undeclared_ttl.len(), 1);
    }

    #[test]
    fn ensure_owned_resource_rejects_a_name_outside_the_group() {
        let err = ensure_owned_resource(
            GROUP_PATH,
            "indexes",
            "projects/fake-firestore/databases/(default)/collectionGroups/other/indexes/x",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not under the owned group"));
    }

    #[test]
    fn ensure_owned_resource_accepts_a_name_inside_the_group() {
        assert!(
            ensure_owned_resource(GROUP_PATH, "indexes", &format!("{GROUP_PATH}/indexes/x"))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn only_the_owned_group_is_ever_listed() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(plan, FirestoreIndexPlan::default());
    }

    #[tokio::test]
    async fn wait_polls_until_done() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        );
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
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

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_millis(20))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let err = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("create index"));
    }

    fn get_operation_name(bytes: &[u8]) -> String {
        gcloud_sdk::google::longrunning::GetOperationRequest::decode(bytes)
            .unwrap()
            .name
    }

    fn operation_name(id: &str) -> String {
        format!("{GROUP_PATH}/operations/{id}")
    }

    /// `[b ASC, c DESC]`, a second declared index distinct from [`declared_index`].
    fn second_declared_index() -> FirestoreCompositeIndex {
        FirestoreCompositeIndex::new(vec![
            FirestoreIndexField::new(
                "b".to_string(),
                FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
            ),
            desc("c"),
        ])
    }

    /// Answers the listing with nothing, and each `CreateIndex` with a pending operation named
    /// after the created index's first field: `op-a` for [`declared_index`], `op-b` for
    /// [`second_declared_index`].
    fn two_creates(method: &str, bytes: &[u8]) -> Option<(String, FakeResponse)> {
        match method {
            LIST_INDEXES => Some(("ListIndexes".to_string(), list_indexes_response(vec![]))),
            LIST_FIELDS => Some(("ListFields".to_string(), list_fields_response(vec![]))),
            CREATE_INDEX => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::CreateIndexRequest::decode(bytes)
                        .unwrap();
                let first = &request.index.unwrap().fields[0].field_path;
                let name = operation_name(&format!("op-{first}"));
                Some((
                    format!("CreateIndex({name})"),
                    pending_operation_response(&name),
                ))
            }
            _ => None,
        }
    }

    fn two_indexes_params() -> FirestoreIndexParams {
        FirestoreIndexParams::new(group())
            .with_composite_indexes(vec![declared_index(), second_declared_index()])
    }

    #[tokio::test]
    async fn a_transient_poll_error_is_retried_within_the_deadline() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let polls = StdArc::new(AtomicU32::new(0));
        let polls_in_handler = polls.clone();
        let fake = FakeFirestore::start(move |method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&operation_name("op1")),
            ),
            GET_OPERATION => {
                let count = polls_in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                if count == 1 {
                    (
                        "GetOperation#1 (unavailable)".to_string(),
                        FakeResponse::Status(Code::Unavailable),
                    )
                } else {
                    (
                        format!("GetOperation#{count}"),
                        done_operation_response(&operation_name("op1")),
                    )
                }
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let report = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .expect("an UNAVAILABLE poll must be retried, not end the wait");

        assert_eq!(report.created_indexes, vec![declared_index()]);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_non_retryable_poll_error_ends_the_wait_at_once() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&operation_name("op1")),
            ),
            GET_OPERATION => (
                "GetOperation (permission denied)".to_string(),
                FakeResponse::Status(Code::PermissionDenied),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let err = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("PermissionDenied"), "{err}");
        assert_eq!(
            fake.calls()
                .iter()
                .filter(|call| call.starts_with("GetOperation"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn every_pending_operation_is_polled_and_only_the_unfinished_one_times_out() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| {
            if let Some(answer) = two_creates(method, bytes) {
                return answer;
            }
            match method {
                GET_OPERATION => {
                    let name = get_operation_name(bytes);
                    let response = if name.ends_with("op-b") {
                        done_operation_response(&name)
                    } else {
                        pending_operation_response(&name)
                    };
                    (format!("GetOperation({name})"), response)
                }
                other => panic!("unexpected RPC: {other}"),
            }
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_millis(100))
                .with_poll_interval(Duration::from_millis(10)),
        );
        let err = fake
            .db
            .sync_indexes(two_indexes_params(), options)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            fake.calls()
                .contains(&format!("GetOperation({})", operation_name("op-b"))),
            "the second operation must be polled while the first is still pending: {:?}",
            fake.calls()
        );
        assert!(err.contains("timed out"), "{err}");
        assert!(
            err.contains("create index [COLLECTION] (a DESC, tags CONTAINS)"),
            "{err}"
        );
        assert!(err.contains(&operation_name("op-a")), "{err}");
        assert!(
            !err.contains("(b ASC") && !err.contains(&operation_name("op-b")),
            "a finished operation must not be reported as pending: {err}"
        );
    }

    #[tokio::test]
    async fn one_deadline_covers_every_operation_of_the_wait() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let polls_of_b = StdArc::new(AtomicU32::new(0));
        let polls_of_b_in_handler = polls_of_b.clone();
        let fake = FakeFirestore::start(move |method, bytes| {
            if let Some(answer) = two_creates(method, bytes) {
                return answer;
            }
            match method {
                GET_OPERATION => {
                    let name = get_operation_name(bytes);
                    let finishes_now = name.ends_with("op-b")
                        && polls_of_b_in_handler.fetch_add(1, Ordering::SeqCst) + 1 >= 8;
                    let response = if finishes_now {
                        done_operation_response(&name)
                    } else {
                        pending_operation_response(&name)
                    };
                    (format!("GetOperation({name})"), response)
                }
                other => panic!("unexpected RPC: {other}"),
            }
        })
        .await;

        // op-b finishes after about 8 polls (~160 ms) and op-a never does. With one deadline the
        // wait ends at ~200 ms; an operation given its own timeout from when the previous one
        // finished would run to ~360 ms.
        let timeout = Duration::from_millis(200);
        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(timeout)
                .with_poll_interval(Duration::from_millis(20)),
        );
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(
            Duration::from_secs(2),
            fake.db.sync_indexes(two_indexes_params(), options),
        )
        .await
        .expect("the wait must end at its deadline")
        .unwrap_err()
        .to_string();
        let elapsed = started.elapsed();

        assert!(polls_of_b.load(Ordering::SeqCst) >= 8);
        assert!(err.contains(&operation_name("op-a")), "{err}");
        assert!(!err.contains(&operation_name("op-b")), "{err}");
        assert!(
            elapsed < Duration::from_millis(300),
            "the wait must end at one shared deadline of {timeout:?}, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_wait_logs_the_report_of_what_was_applied() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&operation_name("op1")),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                pending_operation_response(&operation_name("op1")),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_wait(
            FirestoreOperationWaitOptions::new(Duration::from_millis(20))
                .with_poll_interval(Duration::from_millis(5)),
        );
        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db
                .sync_indexes(params_with_index(), options)
                .await
                .unwrap_err();
        }
        let output = captured_text(&buffer);

        let report_line = output
            .lines()
            .find(|line| line.contains("Firestore index sync report"))
            .unwrap_or_else(|| panic!("no partial report logged in:\n{output}"));
        assert!(report_line.contains("WARN"), "{report_line}");
        assert!(output.contains("created_indexes: 1"), "{output}");
    }

    #[tokio::test]
    async fn a_failed_operation_surfaces_as_an_error() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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

        let options = FirestoreIndexSyncOptions::new()
            .with_wait(FirestoreOperationWaitOptions::new(Duration::from_secs(5)));
        let err = fake
            .db
            .sync_indexes(params_with_index(), options)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("quota exceeded"));
    }

    #[tokio::test]
    async fn already_exists_on_create_counts_as_unchanged() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
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

        let plan = emulator_db
            .plan_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
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

    // The tests below replay `ListIndexes` responses captured read-only from the real `latestbit`
    // project (2026-09-26), rather than hand-written fixtures: a hand-written fixture had already
    // encoded the same wrong assumptions as the code it was meant to check (that `ListIndexes`
    // scoped to one group's parent lists only that group, and that a vector index's `__name__`
    // sits in a single fixed position), so unit tests and review both missed it. The raw JSON
    // lives in `testdata/` and is decoded into the wire types here rather than transcribed by
    // hand, so the fixture is what the service actually returned.

    fn json_str<'a>(value: &'a serde_json::Value, key: &str) -> &'a str {
        value.get(key).and_then(|v| v.as_str()).unwrap_or_default()
    }

    fn decode_captured_query_scope(scope: &str) -> i32 {
        match scope {
            "COLLECTION" => ProtoQueryScope::Collection as i32,
            "COLLECTION_GROUP" => ProtoQueryScope::CollectionGroup as i32,
            "COLLECTION_RECURSIVE" => ProtoQueryScope::CollectionRecursive as i32,
            _ => ProtoQueryScope::Unspecified as i32,
        }
    }

    fn decode_captured_index_state(state: &str) -> i32 {
        match state {
            "CREATING" => ProtoState::Creating as i32,
            "READY" => ProtoState::Ready as i32,
            "NEEDS_REPAIR" => ProtoState::NeedsRepair as i32,
            _ => ProtoState::Unspecified as i32,
        }
    }

    fn decode_captured_index_field(value: &serde_json::Value) -> ProtoIndexField {
        use gcloud_sdk::google::firestore::admin::v1::index::index_field::{
            vector_config, ArrayConfig, Order, ValueMode, VectorConfig,
        };
        let value_mode = if let Some(order) = value.get("order").and_then(|v| v.as_str()) {
            Some(ValueMode::Order(match order {
                "ASCENDING" => Order::Ascending as i32,
                "DESCENDING" => Order::Descending as i32,
                _ => Order::Unspecified as i32,
            }))
        } else if let Some(array_config) = value.get("arrayConfig").and_then(|v| v.as_str()) {
            Some(ValueMode::ArrayConfig(match array_config {
                "CONTAINS" => ArrayConfig::Contains as i32,
                _ => ArrayConfig::Unspecified as i32,
            }))
        } else {
            value.get("vectorConfig").map(|vector| {
                ValueMode::VectorConfig(VectorConfig {
                    dimension: vector
                        .get("dimension")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0) as i32,
                    r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
                })
            })
        };
        ProtoIndexField {
            field_path: json_str(value, "fieldPath").to_string(),
            value_mode,
        }
    }

    /// Decodes a captured `ListIndexesResponse` REST JSON body into the wire types, translating
    /// its camelCase field names to the ones the proto messages use. Resource names are rewritten
    /// from the project they were captured under to [`FakeFirestore`]'s fixed `fake-firestore`
    /// project, so the fixture lines up with the group path the code under test computes.
    fn decode_captured_indexes(json: &str) -> Vec<ProtoIndex> {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        value
            .get("indexes")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .map(|index| ProtoIndex {
                name: json_str(index, "name")
                    .replace("projects/latestbit/", "projects/fake-firestore/"),
                query_scope: decode_captured_query_scope(json_str(index, "queryScope")),
                api_scope: ApiScope::AnyApi as i32,
                fields: index
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
                    .map(decode_captured_index_field)
                    .collect(),
                state: decode_captured_index_state(json_str(index, "state")),
                density: 0,
                multikey: false,
                shard_count: 0,
                unique: false,
                search_index_options: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn only_the_owned_groups_index_is_ever_considered_among_real_database_indexes() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let indexes = decode_captured_indexes(include_str!("testdata/latestbit-list-indexes.json"));
        assert_eq!(
            indexes.len(),
            8,
            "fixture sanity check: 8 real indexes were captured, spanning six other groups"
        );

        let fake = FakeFirestore::start(move |method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(indexes.clone()),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let plan = fake
            .db
            .plan_indexes(
                FirestoreIndexParams::new(FirestoreCollectionId::from_static(
                    "firestore-rs-index-sync-test",
                )),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            plan.kept_undeclared_indexes.len(),
            1,
            "only the owned group's own index must be considered, not the other seven"
        );
        let only = &plan.kept_undeclared_indexes[0];
        assert!(only
            .name
            .contains("firestore-rs-index-sync-test/indexes/CICAgJiHlpgK"));
        for other_group in [
            "versions",
            "/test/",
            "integration-test-query",
            "test-query-vec",
            "test-camel-case",
        ] {
            assert!(
                !only.name.contains(other_group),
                "no index from {other_group} may be considered, got {}",
                only.name
            );
        }
    }

    #[tokio::test]
    async fn prune_never_deletes_anything_outside_the_owned_group() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let indexes = decode_captured_indexes(include_str!("testdata/latestbit-list-indexes.json"));
        let fake = FakeFirestore::start(move |method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(indexes.clone()),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            DELETE_INDEX => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::DeleteIndexRequest::decode(bytes)
                        .unwrap();
                assert!(
                    request.name.contains("firestore-rs-index-sync-test"),
                    "must never delete a resource outside the owned group: {}",
                    request.name
                );
                ("DeleteIndex".to_string(), FakeResponse::empty())
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let options = FirestoreIndexSyncOptions::new().with_prune(true);
        let report = fake
            .db
            .sync_indexes(
                FirestoreIndexParams::new(FirestoreCollectionId::from_static(
                    "firestore-rs-index-sync-test",
                )),
                options,
            )
            .await
            .unwrap();

        assert_eq!(report.deleted_indexes.len(), 1);
        assert!(report.deleted_indexes[0]
            .name
            .contains("firestore-rs-index-sync-test"));
    }

    #[test]
    fn vector_index_shape_from_the_real_service_matches_when_replayed_as_the_owned_group() {
        let captured =
            decode_captured_indexes(include_str!("testdata/latestbit-list-indexes.json"));
        let vector_index = captured
            .into_iter()
            .find(|index| index.name.contains("test-query-vec"))
            .expect("fixture must contain the captured vector index");
        assert_eq!(
            vector_index.fields[0].field_path, "__name__",
            "fixture sanity check: __name__ precedes the vector field in the real listing"
        );

        // Replay the real field shape as if it belonged to the owned group, isolating the
        // matching rule from the group-membership filter proven above.
        let replayed = ProtoIndex {
            name: format!("{GROUP_PATH}/indexes/replayed-vector"),
            ..vector_index
        };
        let declared = FirestoreCompositeIndex::new(vec![FirestoreIndexField::new(
            "some_vec".to_string(),
            FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(3)),
        )]);
        let params =
            FirestoreIndexParams::new(group()).with_composite_indexes(vec![declared.clone()]);
        let existing = FirestoreIndexExistingState {
            indexes: vec![replayed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing.into(), false).unwrap();
        assert_eq!(plan.unchanged, vec![declared]);
        assert!(plan.kept_undeclared_indexes.is_empty());
    }

    #[tokio::test]
    async fn a_field_listed_in_both_filters_is_merged_into_one() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
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

        let plan = fake
            .db
            .plan_indexes(params, FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
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
            .plan_indexes(
                FirestoreIndexParams::new(group()),
                FirestoreIndexSyncOptions::new(),
            )
            .await
            .unwrap();
        assert!(plan.kept_undeclared_fields.is_empty());
        assert!(plan.kept_undeclared_ttl.is_empty());
        assert!(plan.unrecognised.is_empty());
    }

    #[tokio::test]
    async fn all_fields_apply_and_prune_round_trip() {
        let _serialize = MODULE_TEST_LOCK.lock().await;

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

        let replan = fake
            .db
            .plan_indexes(params, FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
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
            stored.lock().unwrap().is_none(),
            "the wildcard override must be reverted (index_config unset)",
        );
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
        // Scoped to this crate's own target: `cargo test` runs many tests concurrently, and an
        // unfiltered DEBUG level captures every other test's h2/tower/hyper transport chatter
        // into this buffer too, which has been observed to crowd out or reorder the lines this
        // test asserts on.
        let subscriber = tracing_subscriber::fmt()
            .with_writer(make_writer)
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_env_filter(tracing_subscriber::EnvFilter::new("firestore=debug"))
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
        let _serialize = MODULE_TEST_LOCK.lock().await;
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
        assert!(output.contains("Existing state:"));
        assert!(output.contains("Firestore index plan:"));
        assert!(output.contains("create_indexes: 1"));
        assert!(output.contains("Created a composite index."));
        assert!(output.contains("Firestore index sync report"));
        assert!(
            output.contains("/firestore/response_time"),
            "missing recorded response time in:\n{output}"
        );
        assert_eq!(
            output.matches("Existing state:").count(),
            1,
            "the existing state must be one grouped event, not one per item:\n{output}"
        );
        assert_eq!(
            output.matches("Firestore index plan:").count(),
            1,
            "the plan must be one grouped event, not one per item:\n{output}"
        );

        let listed_line = output
            .lines()
            .find(|line| line.contains("Listed the collection group's"))
            .unwrap_or_else(|| panic!("no list-summary line in:\n{output}"));
        assert!(listed_line.contains("Firestore Index Sync"));
        assert!(listed_line.contains("Firestore Index List"));
    }

    #[tokio::test]
    async fn log_events_are_grouped_not_per_item() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        // Two listed indexes, neither declared: enough items in one category to tell "one event
        // holding N items" apart from "N events".
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![
                    listed_declared_index(
                        &format!("{GROUP_PATH}/indexes/legacy-1"),
                        ProtoState::Ready,
                    ),
                    listed_declared_index(
                        &format!("{GROUP_PATH}/indexes/legacy-2"),
                        ProtoState::Ready,
                    ),
                ]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db
                .plan_indexes(
                    FirestoreIndexParams::new(group()),
                    FirestoreIndexSyncOptions::new(),
                )
                .await
                .unwrap();
        }
        let output = captured_text(&buffer);

        assert!(output.contains("legacy-1"));
        assert!(output.contains("legacy-2"));
        assert_eq!(
            output.matches("Existing state:").count(),
            1,
            "two listed indexes must still be one existing-state event:\n{output}"
        );
        assert_eq!(
            output.matches("Firestore index plan:").count(),
            1,
            "two undeclared indexes must still be one plan event:\n{output}"
        );
    }

    #[tokio::test]
    async fn needs_repair_items_get_their_own_warning() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_declared_index(
                    &format!("{GROUP_PATH}/indexes/1"),
                    ProtoState::NeedsRepair,
                )]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db
                .plan_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
                .await
                .unwrap();
        }
        let output = captured_text(&buffer);

        assert_eq!(
            output.matches("NEEDS_REPAIR, left alone:").count(),
            1,
            "needs_repair must be one grouped warning, not one per item:\n{output}"
        );
        assert!(output.contains("WARN"));
    }

    #[tokio::test]
    async fn plan_logs_no_execution() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => no_writes_allowed(other),
        })
        .await;

        let (subscriber, buffer) = capturing_subscriber();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            fake.db
                .plan_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
                .await
                .unwrap();
        }
        let output = captured_text(&buffer);

        assert!(output.contains("nothing was applied"));
        assert!(!output.contains("Firestore Index Apply"));
        assert!(!output.contains("Created a composite index."));
        assert!(!output.contains("Applied an index management change."));
    }
}
