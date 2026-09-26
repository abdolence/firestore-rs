//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: lists, plans, applies, prunes and waits on one
//! collection group's composite indexes, single-field overrides and TTL policy against the
//! Firestore Admin API, on the same authenticated channel the data API uses
//! ([`GoogleApiClient::get_with`](gcloud_sdk::GoogleApiClient::get_with)).

use crate::db::admin::index_diff::{
    plan_index_changes, FirestoreIndexExistingState, FirestoreIndexListing,
};
use crate::db::admin::index_models::{validate_collection_group, write_section};
use crate::db::admin::operation_wait::{OperationAction, StartedOperation};
use crate::db::support::FirestoreIndexSupport;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreCompositeIndex, FirestoreDb, FirestoreFieldOverride,
    FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSyncOptions, FirestoreIndexSyncReport,
    FirestoreIndexSyncSkipReason, FirestoreIndexSyncTimings, FirestoreInstant,
    FirestoreListedCompositeIndex, FirestoreListedField, FirestoreOperationWaitOptions,
    FirestoreResult,
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
use std::time::{Duration, Instant};
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

/// The one collection group a statement owns: its ID, and its resource path in the database.
struct OwnedGroup<'a> {
    id: &'a FirestoreCollectionId,
    path: String,
}

impl OwnedGroup<'_> {
    /// The resource name of field `path` in this group: the key writes to one field are
    /// sequenced on, and the name an `UpdateField` request carries.
    fn field_resource(&self, path: &str) -> String {
        format!("{}/fields/{path}", self.path)
    }

    /// Refuses to touch a resource that is not under this group, independent of the filtering
    /// `list_existing_state` already applies to what it reports as undeclared in the first place.
    ///
    /// The last line of defense before a delete or a revert: measured against the real service,
    /// 2026-09-26, `ListIndexes` scoped to one collection group's parent still answered with
    /// composite indexes belonging to other groups in the same database, so a resource's presence
    /// in a listing response is not enough on its own to trust it with `prune_undeclared()`. A
    /// group Firestore reserves, such as `__default__`, owns nothing this crate may touch, even
    /// though validation already rejects a statement naming one.
    fn ensure_owns(&self, kind: &str, name: &str) -> FirestoreResult<()> {
        validate_collection_group(self.id)?;
        let prefix = format!("{}/{kind}/", self.path);
        if name.starts_with(prefix.as_str()) {
            Ok(())
        } else {
            Err(FirestoreError::invalid_parameters(
                "resource_name",
                format!(
                    "refusing to touch {name}: not under the owned group {}",
                    self.path
                ),
            ))
        }
    }
}

/// Bounds each wait `.sync()` makes between two dependent writes (see
/// [`FirestoreDb::apply_plan`]) when the caller did not ask to wait: long enough for a field
/// override's single-field index build, so a sync that was not asked to wait only fails when a
/// write it depends on is stuck.
const SEQUENCING_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// The operations one sync has started, which of them are known to have finished, and which one
/// last wrote each field resource.
#[derive(Default)]
struct StartedWrites {
    operations: Vec<PendingOperation>,
    settled: Vec<bool>,
    last_write_to_field: HashMap<String, usize>,
}

impl StartedWrites {
    /// Records `operation`, as the latest write to `field` when it writes one, and returns its
    /// position.
    fn push(&mut self, field: Option<String>, operation: PendingOperation) -> usize {
        let position = self.operations.len();
        self.operations.push(operation);
        self.settled.push(false);
        if let Some(field) = field {
            self.last_write_to_field.insert(field, position);
        }
        position
    }

    fn into_unsettled(self) -> Vec<PendingOperation> {
        self.operations
            .into_iter()
            .zip(self.settled)
            .filter(|(_, settled)| !settled)
            .map(|(operation, _)| operation)
            .collect()
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

    fn owned_group<'a>(&self, group: &'a FirestoreCollectionId) -> OwnedGroup<'a> {
        OwnedGroup {
            id: group,
            path: format!(
                "{}/collectionGroups/{}",
                self.inner.database_path,
                group.as_str()
            ),
        }
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
        group: &OwnedGroup<'_>,
    ) -> FirestoreResult<FirestoreIndexListing> {
        let group_path = &group.path;
        let span = span!(
            Level::INFO,
            "Firestore Index List",
            "/firestore/collection_group" = group.id.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let listed = async {
            let (all_indexes, override_fields, ttl_fields) = tokio::try_join!(
                self.list_all_indexes(group_path),
                self.list_all_fields(group_path, "indexConfig.usesAncestorConfig:false"),
                self.list_all_fields(group_path, "ttlConfig:*"),
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
        Ok(listed)
    }

    /// Lists the owned group's existing state, logs it, computes the plan, and logs it - the
    /// step `.plan()` and `.sync()` share, so a plan made with the same `prune` is exactly what
    /// the sync applies.
    async fn plan_against_server(
        &self,
        params: &FirestoreIndexParams,
        group: &OwnedGroup<'_>,
        prune: bool,
    ) -> FirestoreResult<(FirestoreIndexPlan, Duration)> {
        let list_started = Instant::now();
        let listing = self.list_existing_state(group).await?;
        let list_elapsed = list_started.elapsed();
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
        Ok((plan, list_elapsed))
    }

    async fn apply_create_index(
        &self,
        group: &OwnedGroup<'_>,
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
                parent: group.path.clone(),
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
        group: &OwnedGroup<'_>,
        listed: &FirestoreListedCompositeIndex,
    ) -> FirestoreResult<()> {
        group.ensure_owns("indexes", &listed.name)?;
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
        group: &OwnedGroup<'_>,
        declared: &FirestoreFieldOverride,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Update Field",
            "/firestore/response_time" = field::Empty
        );
        let name = group.field_resource(declared.target.as_str());
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
        group: &OwnedGroup<'_>,
        field_path: &str,
    ) -> FirestoreResult<PendingOperation> {
        let span = span!(
            Level::INFO,
            "Enable TTL",
            "/firestore/response_time" = field::Empty
        );
        let name = group.field_resource(field_path);
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
        group: &OwnedGroup<'_>,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        group.ensure_owns("fields", &listed.name)?;
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
        group: &OwnedGroup<'_>,
        listed: &FirestoreListedField,
    ) -> FirestoreResult<PendingOperation> {
        group.ensure_owns("fields", &listed.name)?;
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

    /// Waits for the given started writes to finish, then marks them settled so neither a later
    /// write nor the final wait polls them again.
    async fn settle_writes(
        &self,
        writes: &mut StartedWrites,
        positions: &[usize],
        options: &FirestoreOperationWaitOptions,
    ) -> FirestoreResult<()> {
        let unsettled: Vec<usize> = positions
            .iter()
            .copied()
            .filter(|position| !writes.settled[*position])
            .collect();
        if unsettled.is_empty() {
            return Ok(());
        }
        let operations: Vec<&PendingOperation> = unsettled
            .iter()
            .map(|position| &writes.operations[*position])
            .collect();
        info!(
            waiting_for = operations
                .iter()
                .map(|operation| operation.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            "Waiting for earlier writes to finish before the next write that depends on them.",
        );
        self.wait_for_operations(&operations, options, |_, _| {})
            .await?;
        for position in unsettled {
            writes.settled[position] = true;
        }
        Ok(())
    }

    /// Waits for the last write to `field_resource`, if this sync sent one that has not settled.
    async fn settle_field(
        &self,
        writes: &mut StartedWrites,
        field_resource: &str,
        options: &FirestoreOperationWaitOptions,
    ) -> FirestoreResult<()> {
        match writes.last_write_to_field.get(field_resource).copied() {
            Some(position) => self.settle_writes(writes, &[position], options).await,
            None => Ok(()),
        }
    }

    /// Applies `plan`, stopping at the first failing write, in this order: create indexes,
    /// write declared field overrides, disable undeclared TTL, enable declared TTL, delete
    /// undeclared indexes, revert undeclared field overrides.
    ///
    /// Creates come before deletes so a replaced index is never missing in between. A TTL
    /// disable must finish before any TTL enable is sent, because Firestore allows one TTL field
    /// per collection group (<https://firebase.google.com/docs/firestore/ttl>). A write to a field
    /// waits for this sync's previous write to that same field, because overlapping writes to one
    /// field are not known to be safe; writes to different fields are sent back to back, which
    /// Firestore accepts. These waits use `sequencing`, whether or not the caller asked to wait.
    ///
    /// Returns the report and the started operations not already waited for.
    async fn apply_plan(
        &self,
        group: &OwnedGroup<'_>,
        plan: &FirestoreIndexPlan,
        sequencing: &FirestoreOperationWaitOptions,
    ) -> FirestoreResult<(FirestoreIndexSyncReport, Vec<PendingOperation>)> {
        let mut report = FirestoreIndexSyncReport {
            unchanged: plan.unchanged.clone(),
            pending: plan.pending.clone(),
            needs_repair: plan.needs_repair.clone(),
            unchanged_ttl: plan.unchanged_ttl.clone(),
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
        let mut writes = StartedWrites::default();

        let apply_result: FirestoreResult<()> = async {
            for index in &plan.create_indexes {
                match self.apply_create_index(group, index).await? {
                    CreateIndexOutcome::Created(op) => {
                        writes.push(None, op);
                        report.created_indexes.push(index.clone());
                    }
                    CreateIndexOutcome::AlreadyExists => report.unchanged.push(index.clone()),
                }
            }
            for declared in &plan.update_fields {
                let field = group.field_resource(declared.target.as_str());
                self.settle_field(&mut writes, &field, sequencing).await?;
                let op = self.apply_update_field_override(group, declared).await?;
                writes.push(Some(field), op);
                report.updated_fields.push(declared.clone());
            }
            let mut ttl_disables = Vec::new();
            for listed in &plan.disable_ttl {
                self.settle_field(&mut writes, &listed.name, sequencing)
                    .await?;
                let op = self.apply_disable_ttl(group, listed).await?;
                ttl_disables.push(writes.push(Some(listed.name.clone()), op));
                report.disabled_ttl.push(listed.clone());
            }
            if !plan.enable_ttl.is_empty() {
                self.settle_writes(&mut writes, &ttl_disables, sequencing)
                    .await?;
            }
            for path in &plan.enable_ttl {
                let field = group.field_resource(path);
                self.settle_field(&mut writes, &field, sequencing).await?;
                let op = self.apply_enable_ttl(group, path).await?;
                writes.push(Some(field), op);
                report.enabled_ttl.push(path.clone());
            }
            for listed in &plan.delete_indexes {
                self.apply_delete_index(group, listed).await?;
                report.deleted_indexes.push(listed.clone());
            }
            for listed in &plan.revert_fields {
                self.settle_field(&mut writes, &listed.name, sequencing)
                    .await?;
                let op = self.apply_revert_field_override(group, listed).await?;
                writes.push(Some(listed.name.clone()), op);
                report.reverted_fields.push(listed.clone());
            }
            Ok(())
        }
        .instrument(span.clone())
        .await;

        let elapsed = FirestoreInstant::now().duration_since(began);
        span.record("/firestore/response_time", elapsed.as_millis());
        apply_result?;

        Ok((report, writes.into_unsettled()))
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
            let group = self.owned_group(&params.collection_group);
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
        let began = FirestoreInstant::now();
        let report = async {
            let group = self.owned_group(&params.collection_group);
            let (plan, list_elapsed) = self
                .plan_against_server(&params, &group, options.prune)
                .await?;
            let sequencing = options
                .wait
                .clone()
                .unwrap_or_else(|| FirestoreOperationWaitOptions::new(SEQUENCING_TIMEOUT));
            let apply_started = Instant::now();
            let (mut report, pending_operations) =
                self.apply_plan(&group, &plan, &sequencing).await?;
            report.timings.list = Some(list_elapsed);
            report.timings.apply = Some(apply_started.elapsed());
            if let Some(wait_options) = &options.wait {
                let wait_started = Instant::now();
                let waited = self
                    .wait_for_index_operations(&pending_operations, wait_options)
                    .await;
                report.timings.wait = Some(wait_started.elapsed());
                if let Err(err) = waited {
                    report.timings.total = started.elapsed();
                    warn!(
                        collection_group = params.collection_group.as_str(),
                        "Waiting for the applied changes failed; what was applied before it: {report}",
                    );
                    return Err(err);
                }
            }
            report.timings.total = started.elapsed();
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
    async fn an_already_active_declared_ttl_is_reported_unchanged() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => (
                "ListFields".to_string(),
                list_fields_response(vec![field_resource("expires_at", None, Some(active_ttl()))]),
            ),
            other => no_writes_allowed(other),
        })
        .await;

        let params = FirestoreIndexParams::new(group()).with_ttl_fields(vec!["expires_at".into()]);
        let report = fake
            .db
            .sync_indexes(params, FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert_eq!(report.unchanged_ttl, vec!["expires_at".to_string()]);
    }

    #[tokio::test]
    async fn the_report_carries_the_time_of_each_phase_that_ran() {
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
                done_operation_response(&operation_name("op1")),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let waited = fake
            .db
            .sync_indexes(
                params_with_index(),
                FirestoreIndexSyncOptions::new()
                    .with_wait(FirestoreOperationWaitOptions::new(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        assert_eq!(waited.skipped, None);
        let (list, apply, wait) = (
            waited.timings.list.expect("the listing ran"),
            waited.timings.apply.expect("the apply ran"),
            waited.timings.wait.expect("the wait ran"),
        );
        assert!(waited.timings.total >= list + apply + wait);
        assert!(waited.to_string().contains("timings: total"));

        let not_waited = fake
            .db
            .sync_indexes(params_with_index(), FirestoreIndexSyncOptions::new())
            .await
            .unwrap();
        assert!(not_waited.timings.apply.is_some());
        assert_eq!(not_waited.timings.wait, None);
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

    /// Answers each `UpdateField` with a pending operation named after its mask and field path,
    /// e.g. `op-ttl_config-expires_at`, each `GetOperation` with that operation done, each
    /// `CreateIndex` with a pending `op-create` and each `DeleteIndex` with success. Every call is
    /// logged with what it targeted, so a test can assert the exact write sequence.
    fn recording_writes(method: &str, bytes: &[u8]) -> (String, FakeResponse) {
        match method {
            UPDATE_FIELD => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::UpdateFieldRequest::decode(bytes)
                        .unwrap();
                let mask = request.update_mask.unwrap().paths.join(",");
                let field = request.field.unwrap().name;
                let path = field.rsplit("/fields/").next().unwrap().to_string();
                (
                    format!("UpdateField({mask} {path})"),
                    pending_operation_response(&operation_name(&format!("op-{mask}-{path}"))),
                )
            }
            GET_OPERATION => {
                let name = get_operation_name(bytes);
                let id = name.rsplit('/').next().unwrap().to_string();
                (
                    format!("GetOperation({id})"),
                    done_operation_response(&name),
                )
            }
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                pending_operation_response(&operation_name("op-create")),
            ),
            DELETE_INDEX => {
                let request =
                    gcloud_sdk::google::firestore::admin::v1::DeleteIndexRequest::decode(bytes)
                        .unwrap();
                let id = request.name.rsplit('/').next().unwrap().to_string();
                (format!("DeleteIndex({id})"), FakeResponse::empty())
            }
            other => panic!("unexpected RPC: {other}"),
        }
    }

    /// Every call after the listing, whose three RPCs run concurrently in no fixed order.
    fn writes_and_polls(fake: &FakeFirestore) -> Vec<String> {
        fake.calls()
            .into_iter()
            .filter(|call| !call.starts_with("List"))
            .collect()
    }

    fn active_ttl() -> gcloud_sdk::google::firestore::admin::v1::field::TtlConfig {
        use gcloud_sdk::google::firestore::admin::v1::field;
        field::TtlConfig {
            state: field::ttl_config::State::Active as i32,
            expiration_offset: None,
        }
    }

    fn exempt_override() -> gcloud_sdk::google::firestore::admin::v1::field::IndexConfig {
        gcloud_sdk::google::firestore::admin::v1::field::IndexConfig {
            indexes: vec![],
            uses_ancestor_config: false,
            ancestor_field: String::new(),
            reverting: false,
        }
    }

    #[tokio::test]
    async fn a_ttl_move_disables_the_old_field_and_waits_before_enabling_the_new_one() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => (
                "ListFields".to_string(),
                list_fields_response(vec![field_resource("old_expiry", None, Some(active_ttl()))]),
            ),
            other => recording_writes(other, bytes),
        })
        .await;

        let params = FirestoreIndexParams::new(group()).with_ttl_fields(vec!["new_expiry".into()]);
        let report = fake
            .db
            .sync_indexes(params, FirestoreIndexSyncOptions::new().with_prune(true))
            .await
            .unwrap();

        assert_eq!(
            writes_and_polls(&fake),
            vec![
                "UpdateField(ttl_config old_expiry)",
                "GetOperation(op-ttl_config-old_expiry)",
                "UpdateField(ttl_config new_expiry)",
            ]
        );
        assert_eq!(report.disabled_ttl.len(), 1);
        assert_eq!(report.enabled_ttl, vec!["new_expiry".to_string()]);
    }

    #[tokio::test]
    async fn a_write_to_a_field_waits_for_that_fields_previous_write_only() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => ("ListIndexes".to_string(), list_indexes_response(vec![])),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => recording_writes(other, bytes),
        })
        .await;

        let exempt = |path: &str| FirestoreFieldOverride {
            target: FirestoreFieldOverrideTarget::Field(path.to_string()),
            indexes: vec![],
        };
        let params = FirestoreIndexParams::new(group())
            .with_field_overrides(vec![exempt("expires_at"), exempt("bio")])
            .with_ttl_fields(vec!["expires_at".into()]);
        fake.db
            .sync_indexes(params, FirestoreIndexSyncOptions::new())
            .await
            .unwrap();

        assert_eq!(
            writes_and_polls(&fake),
            vec![
                "UpdateField(index_config expires_at)",
                "UpdateField(index_config bio)",
                "GetOperation(op-index_config-expires_at)",
                "UpdateField(ttl_config expires_at)",
            ]
        );
    }

    #[tokio::test]
    async fn a_pruning_sync_sends_its_writes_in_one_fixed_order() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![listed_index(
                    vec![order_field(
                        "legacy",
                        gcloud_sdk::google::firestore::admin::v1::index::index_field::Order::Ascending,
                    ), order_field(
                        "other",
                        gcloud_sdk::google::firestore::admin::v1::index::index_field::Order::Ascending,
                    )],
                    ProtoQueryScope::Collection,
                    ProtoState::Ready,
                )]),
            ),
            LIST_FIELDS => (
                "ListFields".to_string(),
                list_fields_response(vec![field_resource(
                    "old_expiry",
                    Some(exempt_override()),
                    Some(active_ttl()),
                )]),
            ),
            other => recording_writes(other, bytes),
        })
        .await;

        let params = params_with_index().with_ttl_fields(vec!["new_expiry".into()]);
        fake.db
            .sync_indexes(params, FirestoreIndexSyncOptions::new().with_prune(true))
            .await
            .unwrap();

        // Creates come before deletes, so a replaced index is never missing in between; a TTL
        // disable finishes before the enable, since a group has one TTL field; the revert of
        // `old_expiry`'s override follows its TTL disable, already finished, without a poll.
        assert_eq!(
            writes_and_polls(&fake),
            vec![
                "CreateIndex",
                "UpdateField(ttl_config old_expiry)",
                "GetOperation(op-ttl_config-old_expiry)",
                "UpdateField(ttl_config new_expiry)",
                "DeleteIndex(1)",
                "UpdateField(index_config old_expiry)",
            ]
        );
    }

    fn owned_users_group(users: &FirestoreCollectionId) -> OwnedGroup<'_> {
        OwnedGroup {
            id: users,
            path: GROUP_PATH.to_string(),
        }
    }

    #[test]
    fn ensure_owns_rejects_a_name_outside_the_group() {
        let users = group();
        let err = owned_users_group(&users)
            .ensure_owns(
                "indexes",
                "projects/fake-firestore/databases/(default)/collectionGroups/other/indexes/x",
            )
            .unwrap_err();
        assert!(err.to_string().contains("not under the owned group"));
    }

    #[test]
    fn ensure_owns_refuses_every_resource_of_a_reserved_group() {
        for reserved in ["__default__", "-", "__system__"] {
            let id = FirestoreCollectionId::new(reserved).unwrap();
            let owned = OwnedGroup {
                id: &id,
                path: format!(
                    "projects/fake-firestore/databases/(default)/collectionGroups/{reserved}"
                ),
            };
            let name = format!("{}/fields/*", owned.path);
            assert!(
                owned.ensure_owns("fields", &name).is_err(),
                "{reserved} is reserved; nothing under it may be touched"
            );
        }
    }

    #[tokio::test]
    async fn applying_a_plan_refuses_every_prune_target_outside_the_owned_group() {
        let _serialize = MODULE_TEST_LOCK.lock().await;
        let fake = FakeFirestore::start(|method, _| no_writes_allowed(method)).await;
        let foreign = "projects/fake-firestore/databases/(default)/collectionGroups/other";
        let foreign_index: FirestoreListedCompositeIndex =
            listed_declared_index(&format!("{foreign}/indexes/x"), ProtoState::Ready)
                .try_into()
                .unwrap();
        let foreign_field: FirestoreListedField = ProtoField {
            name: format!("{foreign}/fields/expires_at"),
            index_config: Some(exempt_override()),
            ttl_config: Some(active_ttl()),
        }
        .into();

        let users = group();
        let owned = owned_users_group(&users);
        let sequencing = FirestoreOperationWaitOptions::new(Duration::from_secs(1));
        for plan in [
            FirestoreIndexPlan {
                delete_indexes: vec![foreign_index],
                ..Default::default()
            },
            FirestoreIndexPlan {
                revert_fields: vec![foreign_field.clone()],
                ..Default::default()
            },
            FirestoreIndexPlan {
                disable_ttl: vec![foreign_field],
                ..Default::default()
            },
        ] {
            let err = match fake.db.apply_plan(&owned, &plan, &sequencing).await {
                Ok(_) => panic!("a foreign prune target was applied: {plan}"),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains("not under the owned group"),
                "{err}"
            );
        }
        assert!(fake.calls().is_empty(), "{:?}", fake.calls());
    }

    #[test]
    fn ensure_owns_accepts_a_name_inside_the_group() {
        let users = group();
        assert!(owned_users_group(&users)
            .ensure_owns("indexes", &format!("{GROUP_PATH}/indexes/x"))
            .is_ok());
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
        assert_eq!(report.skipped, Some(FirestoreIndexSyncSkipReason::Emulator));
        assert_eq!(report.timings.list, None);
        assert_eq!(report.timings.apply, None);
        assert!(report
            .to_string()
            .contains("skipped: the Firestore emulator"));

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
