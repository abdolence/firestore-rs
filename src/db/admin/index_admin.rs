//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: the declarative `.plan()` and `.sync()` of one
//! collection group, built on the listing, diff, apply and wait steps of
//! [`indexes`](super::indexes), and the four calls that each list, create, delete or update one
//! index or field.
//!
//! The four calls run no edition rule and no emulator check of their own: Firestore's own
//! `INVALID_ARGUMENT` and the emulator's `UNIMPLEMENTED` are their errors.

use crate::db::admin::index_models::{validate_collection_group, CanonicalFieldPath};
use crate::db::admin::indexes::PendingOperation;
use crate::db::admin::indexes::{IndexAction, RefusedFieldWrite, SEQUENCING_TIMEOUT};
use crate::db::admin::operation_wait::{OperationDeadline, StartedOperation};
use crate::db::support::FirestoreIndexSupport;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreDb, FirestoreFieldOverride, FirestoreFieldOverrideUpdate,
    FirestoreFieldTtlUpdate, FirestoreFieldUpdate, FirestoreIndexDefinition,
    FirestoreIndexDeleteTarget, FirestoreIndexIdTarget, FirestoreIndexListing,
    FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexState, FirestoreIndexSyncOptions,
    FirestoreIndexSyncReport, FirestoreIndexSyncSkipReason, FirestoreIndexSyncTimings,
    FirestoreInstant, FirestoreListedCompositeIndex, FirestoreListedField,
    FirestoreOperationWaitOptions, FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::admin::v1::field::{IndexConfig, TtlConfig};
use gcloud_sdk::google::firestore::admin::v1::{
    Field as ProtoField, GetFieldRequest, GetIndexRequest, Index as ProtoIndex,
    IndexOperationMetadata, UpdateFieldRequest,
};
use gcloud_sdk::google::longrunning::Operation;
use gcloud_sdk::prost::Message as _;
use gcloud_sdk::prost_types::FieldMask;
use gcloud_sdk::tonic::{Code, Status};
use gcloud_sdk::RequestTimeouts;
use std::time::Instant;
use tracing::*;

/// The code of the [`DataNotFoundError`](FirestoreError::DataNotFoundError) a delete by ID fails
/// with when the index belongs to another group than the caller named.
const INDEX_IN_OTHER_GROUP: &str = "INDEX_IN_OTHER_GROUP";

/// The code of the [`DataNotFoundError`](FirestoreError::DataNotFoundError) a delete fails with
/// when no index has the ID, or no listed index matches the definition.
const NO_MATCHING_INDEX: &str = "NO_MATCHING_INDEX";

/// The code of the [`DataConflictError`](FirestoreError::DataConflictError) a delete by
/// definition fails with when more than one listed index matches it.
const SEVERAL_MATCHING_INDEXES: &str = "SEVERAL_MATCHING_INDEXES";

/// The code of the [`SystemError`](FirestoreError::SystemError) a wait fails with when the index
/// it waited for ended in `NEEDS_REPAIR`.
const INDEX_NEEDS_REPAIR: &str = "INDEX_NEEDS_REPAIR";

/// A `CreateIndex` Firestore accepted: its operation, and the resource name of the index it
/// builds.
struct StartedIndexBuild {
    operation: PendingOperation,
    index_name: String,
}

impl StartedIndexBuild {
    /// Firestore names the new index in the metadata of its first answer, which is why a build
    /// without that name is refused: nothing else says which index to read back.
    fn new(operation: Operation, action: IndexAction) -> FirestoreResult<Self> {
        let index_name = operation
            .metadata
            .as_ref()
            .map(|metadata| IndexOperationMetadata::decode(metadata.value.as_slice()))
            .transpose()?
            .map(|metadata| metadata.index)
            .filter(|index| !index.is_empty())
            .ok_or_else(|| {
                FirestoreError::system_error(format!(
                    "operation {} started to {action} without naming the index",
                    operation.name
                ))
            })?;
        Ok(Self {
            operation: StartedOperation {
                name: operation.name,
                action,
            },
            index_name,
        })
    }
}

impl FirestoreDb {
    async fn get_index(&self, name: &str) -> Result<ProtoIndex, Status> {
        let request = GetIndexRequest {
            name: name.to_string(),
        };
        Ok(self
            .admin_client()
            .get_index(self.request_with_timeout(request, RequestTimeouts::with_total))
            .await?
            .into_inner())
    }

    async fn get_field(&self, name: &str) -> FirestoreResult<FirestoreListedField> {
        let request = GetFieldRequest {
            name: name.to_string(),
        };
        let field: ProtoField = self
            .admin_client()
            .get_field(self.request_with_timeout(request, RequestTimeouts::with_total))
            .await?
            .into_inner();
        FirestoreListedField::try_from(field)
    }

    /// Reads the index `name` as one poll of a wait: bounded by `deadline`'s poll limit, and
    /// retryable when it does not answer in time, as `GetOperation` is in
    /// [`wait_for_operations`](Self::wait_for_operations).
    async fn poll_index(
        &self,
        name: &str,
        deadline: &OperationDeadline,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        let request = GetIndexRequest {
            name: name.to_string(),
        };
        let polled = self
            .admin_client()
            .get_index(self.poll_request(request, deadline))
            .await
            .map_err(FirestoreError::from_read_status)?;
        FirestoreListedCompositeIndex::try_from(polled.into_inner())
    }

    /// Reads `index` again until it has left `CREATING`, then returns it unless it ended in
    /// `NEEDS_REPAIR`, which fails the call as a build of its own that failed does. No operation
    /// exists for a build another caller started, so the index itself is polled. Fails once
    /// `deadline` has passed with the index still building.
    async fn wait_while_creating(
        &self,
        mut index: FirestoreListedCompositeIndex,
        deadline: &OperationDeadline,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        while index.state == FirestoreIndexState::Creating {
            if deadline.has_passed() {
                return Err(deadline.timed_out(&format!("index {} to finish building", index.id)));
            }
            deadline.sleep_until_next_round().await;
            match self.poll_index(&index.name, deadline).await {
                Ok(polled) => index = polled,
                Err(err) if err.retry_possible() => {
                    warn!(
                        %err,
                        index = %index,
                        "Polling an index failed with a retryable error; polling it again.",
                    );
                }
                Err(err) => return Err(err),
            }
        }
        match index.state {
            FirestoreIndexState::NeedsRepair => Err(FirestoreError::system_error_with_code(
                INDEX_NEEDS_REPAIR,
                format!("index {} ended in NEEDS_REPAIR", index.id),
            )),
            _ => Ok(index),
        }
    }

    /// What `IF NOT EXISTS` returns for a `CreateIndex` Firestore refused with `refused`, which
    /// is `ALREADY_EXISTS`: the listed index `definition` matches, built when `deadline` is set.
    /// Firestore keeps answering that status for an index deleted minutes ago, so it counts only
    /// when the listing shows the index too; otherwise `refused` is the error.
    async fn existing_index(
        &self,
        definition: &FirestoreIndexDefinition,
        refused: Status,
        deadline: Option<&OperationDeadline>,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        let Some(existing) = self.matching_index(definition).await? else {
            error!(
                error = %refused,
                index = %definition.index,
                "Firestore answered ALREADY_EXISTS for an index its listing does not show.",
            );
            return Err(refused.into());
        };
        match deadline {
            Some(deadline) => self.wait_while_creating(existing, deadline).await,
            None => Ok(existing),
        }
    }

    /// The index `target` names, as listed, or `None` when nothing has the ID or no listed index
    /// matches the definition.
    async fn index_to_delete(
        &self,
        target: &FirestoreIndexDeleteTarget,
    ) -> FirestoreResult<Option<FirestoreListedCompositeIndex>> {
        match target {
            FirestoreIndexDeleteTarget::Id(target) => self.index_by_id(target).await,
            FirestoreIndexDeleteTarget::Definition(definition) => {
                self.matching_index(definition).await
            }
        }
    }

    /// The listed index with the ID `target` names, `None` when Firestore has none, refused when
    /// the domain model cannot represent it or when it belongs to another group than `target`
    /// names.
    async fn index_by_id(
        &self,
        target: &FirestoreIndexIdTarget,
    ) -> FirestoreResult<Option<FirestoreListedCompositeIndex>> {
        if let Some(group) = &target.collection_group {
            validate_collection_group(group)?;
        }
        // Firestore reads an index by its ID alone and ignores the group of the name, so `-`
        // reads it wherever it is.
        let name = format!("{}/indexes/{}", self.collection_group_path("-"), target.id);
        let proto = match self.get_index(&name).await {
            Ok(proto) => proto,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(status.into()),
        };
        let listed_name = proto.name.clone();
        let listed = FirestoreListedCompositeIndex::try_from(proto).map_err(|err| {
            FirestoreError::invalid_parameters(
                "index",
                format!(
                    "{listed_name} is an index this crate cannot represent ({}); nothing was \
                     deleted",
                    err.unrecognised_reason()
                ),
            )
        })?;
        validate_collection_group(&listed.collection_group)?;
        match &target.collection_group {
            Some(group) if *group != listed.collection_group => {
                Err(FirestoreError::data_not_found(
                    INDEX_IN_OTHER_GROUP,
                    format!(
                        "index {} belongs to group {}, not {group}; nothing was deleted",
                        target.id, listed.collection_group
                    ),
                ))
            }
            _ => Ok(Some(listed)),
        }
    }

    /// The one listed index of its group that `definition` matches, under the database's
    /// edition, or `None` when it matches none. Fails with
    /// [`DataConflictError`](FirestoreError::DataConflictError) when it matches several.
    async fn matching_index(
        &self,
        definition: &FirestoreIndexDefinition,
    ) -> FirestoreResult<Option<FirestoreListedCompositeIndex>> {
        let group_id = &definition.collection_group;
        validate_collection_group(group_id)?;
        let group = self.read_owned_group(group_id).await?;
        let matching = self.listed_matches(&group, &definition.index).await?;
        let listed = match <[_; 1]>::try_from(matching) {
            Ok([listed]) => listed,
            Err(matching) if matching.is_empty() => return Ok(None),
            Err(matching) => {
                let ids: Vec<String> = matching
                    .iter()
                    .map(|listed| listed.id.to_string())
                    .collect();
                return Err(FirestoreError::data_conflict(
                    SEVERAL_MATCHING_INDEXES,
                    format!(
                        "{} indexes of group {group_id} match {}: {}",
                        ids.len(),
                        definition.index,
                        ids.join(", ")
                    ),
                ));
            }
        };
        group.ensure_owns("indexes", &listed.name)?;
        Ok(Some(listed))
    }
}

impl FirestoreIndexDeleteTarget {
    /// The error of a delete that found no index to delete.
    pub(crate) fn not_found(&self) -> FirestoreError {
        match self {
            Self::Id(target) => FirestoreError::data_not_found(
                NO_MATCHING_INDEX,
                format!("no index has the ID {}; nothing was deleted", target.id),
            ),
            Self::Definition(definition) => FirestoreError::data_not_found(
                NO_MATCHING_INDEX,
                format!(
                    "no index of group {} matches {}; nothing was deleted",
                    definition.collection_group, definition.index
                ),
            ),
        }
    }
}

impl FirestoreFieldUpdate {
    /// The `UpdateField` request for this update of the field `name`. Its mask holds each part
    /// the update sets; a revert or a TTL disable leaves its part out of the body, so Firestore
    /// clears it.
    fn into_request(self, name: String) -> FirestoreResult<UpdateFieldRequest> {
        let mut paths = Vec::new();
        let index_config = match self.index_override {
            Some(FirestoreFieldOverrideUpdate::Set(indexes)) => {
                paths.push("index_config".to_string());
                Some(IndexConfig::try_from(FirestoreFieldOverride {
                    target: self.target,
                    indexes,
                })?)
            }
            Some(FirestoreFieldOverrideUpdate::Revert) => {
                paths.push("index_config".to_string());
                None
            }
            None => None,
        };
        let ttl_config = match self.ttl {
            Some(FirestoreFieldTtlUpdate::Enable) => {
                paths.push("ttl_config".to_string());
                Some(TtlConfig::default())
            }
            Some(FirestoreFieldTtlUpdate::Disable) => {
                paths.push("ttl_config".to_string());
                None
            }
            None => None,
        };
        Ok(UpdateFieldRequest {
            field: Some(ProtoField {
                name,
                index_config,
                ttl_config,
            }),
            update_mask: Some(FieldMask { paths }),
        })
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

    async fn list_indexes(
        &self,
        collection_group: Option<FirestoreCollectionId>,
    ) -> FirestoreResult<FirestoreIndexListing> {
        if let Some(group) = &collection_group {
            validate_collection_group(group)?;
        }
        self.list_existing_state(collection_group.as_ref()).await
    }

    async fn create_index(
        &self,
        definition: FirestoreIndexDefinition,
        wait: Option<FirestoreOperationWaitOptions>,
        if_not_exists: bool,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        validate_collection_group(&definition.collection_group)?;
        let deadline = wait.as_ref().map(OperationDeadline::from_now);
        let action = IndexAction::CreateIndex(definition.index.clone());
        let parent = self.collection_group_path(definition.collection_group.as_str());
        let proto = ProtoIndex::try_from(definition.index.clone())?;
        let operation = match self.run_create_index(parent, proto, &action).await {
            Ok(operation) => operation,
            Err(status) if if_not_exists && status.code() == Code::AlreadyExists => {
                return self
                    .existing_index(&definition, status, deadline.as_ref())
                    .await;
            }
            Err(status) => {
                error!(error = %status, label = %action, "Failed to create a composite index.");
                return Err(status.into());
            }
        };
        let build = StartedIndexBuild::new(operation, action)?;
        if let Some(deadline) = &deadline {
            self.wait_for_operations(&[&build.operation], deadline, |_, _| {})
                .await?;
        }
        FirestoreListedCompositeIndex::try_from(self.get_index(&build.index_name).await?)
    }

    async fn delete_index(
        &self,
        target: FirestoreIndexDeleteTarget,
        if_exists: bool,
    ) -> FirestoreResult<Option<FirestoreListedCompositeIndex>> {
        let Some(listed) = self.index_to_delete(&target).await? else {
            return if if_exists {
                Ok(None)
            } else {
                Err(target.not_found())
            };
        };
        let action = IndexAction::DeleteIndex(listed.clone());
        if let Err(status) = self.run_delete_index(&listed.name, &action).await {
            error!(error = %status, label = %action, "Failed to delete a composite index.");
            return Err(status.into());
        }
        Ok(Some(listed))
    }

    async fn update_field(
        &self,
        update: FirestoreFieldUpdate,
        wait: Option<FirestoreOperationWaitOptions>,
    ) -> FirestoreResult<FirestoreListedField> {
        update.validate()?;
        let deadline = wait.as_ref().map(OperationDeadline::from_now);
        let name = CanonicalFieldPath::from(update.target.as_str())
            .field_resource_name(&self.collection_group_path(update.collection_group.as_str()));
        let request = update.clone().into_request(name.clone())?;
        let operation = self
            .run_update_field(request, IndexAction::UpdateField(update))
            .await
            .map_err(RefusedFieldWrite::into_logged_error)?;
        if let Some(deadline) = &deadline {
            self.wait_for_operations(&[&operation], deadline, |_, _| {})
                .await?;
        }
        // The field is read back rather than taken from the finished operation, which misreads
        // a revert as an explicit override, leaves out an unchanged index configuration, and can
        // hold a state a later write to the field already replaced.
        self.get_field(&name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::admin::indexes::tests::{
        decode_captured_field, decode_captured_fields, decode_captured_index,
        decode_captured_indexes, decode_captured_operation, start_on, CREATE_INDEX, DELETE_INDEX,
        GET_FIELD, GET_OPERATION, LIST_FIELDS, LIST_INDEXES, UPDATE_FIELD,
    };
    use crate::db::fake_firestore::{
        list_fields_response, list_indexes_response, FakeFirestore, FakeResponse,
    };
    use crate::{
        FirestoreCompositeIndex, FirestoreFieldOverrideOutcome, FirestoreFieldOverrideTarget,
        FirestoreIndexDensity, FirestoreIndexField, FirestoreIndexFieldMode, FirestoreIndexId,
        FirestoreIndexIdTarget, FirestoreQueryDirection,
    };
    use gcloud_sdk::google::firestore::admin::v1::database::DatabaseEdition;
    use gcloud_sdk::google::firestore::admin::v1::index::State as ProtoIndexState;
    use gcloud_sdk::google::firestore::admin::v1::{DeleteIndexRequest, ListFieldsRequest};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const GET_INDEX: &str = "/google.firestore.admin.v1.FirestoreAdmin/GetIndex";

    /// The group every capture was made in, as [`FakeFirestore`]'s database names it.
    const CAPTURE_GROUP_PATH: &str =
        "projects/fake-firestore/databases/(default)/collectionGroups/sql-ddl-capture";

    fn capture_group() -> FirestoreCollectionId {
        FirestoreCollectionId::from_static("sql-ddl-capture")
    }

    fn captured_index(json: &str) -> ProtoIndex {
        decode_captured_index(&serde_json::from_str(json).unwrap())
    }

    fn captured_field(json: &str) -> ProtoField {
        decode_captured_field(&serde_json::from_str(json).unwrap())
    }

    fn encoded(message: impl gcloud_sdk::prost::Message) -> FakeResponse {
        FakeResponse::Message(message.encode_to_vec())
    }

    fn field(path: &str, direction: FirestoreQueryDirection) -> FirestoreIndexField {
        FirestoreIndexField::new(path.to_string(), FirestoreIndexFieldMode::Order(direction))
    }

    /// The index the Standard capture created: `customer_id ASC, placed_at DESC`.
    fn captured_definition() -> FirestoreIndexDefinition {
        FirestoreIndexDefinition::new(
            capture_group(),
            FirestoreCompositeIndex::new(vec![
                field("customer_id", FirestoreQueryDirection::Ascending),
                field("placed_at", FirestoreQueryDirection::Descending),
            ]),
        )
    }

    fn fast_wait() -> Option<FirestoreOperationWaitOptions> {
        Some(
            FirestoreOperationWaitOptions::new(Duration::from_secs(5))
                .with_poll_interval(Duration::from_millis(5)),
        )
    }

    /// A fake database whose listing is the captured Enterprise one, which holds the unique
    /// `SPARSE_ANY` and the plain `DENSE` index on `email` side by side, and which logs the name
    /// of every `DeleteIndex` it is sent.
    async fn enterprise_listing_recording_deletes() -> FakeFirestore {
        start_on(DatabaseEdition::Enterprise, |method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(decode_captured_indexes(include_str!(
                    "testdata/enterprise-list-indexes-scratch-group-before-delete.json"
                ))),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            DELETE_INDEX => {
                let request = DeleteIndexRequest::decode(bytes).unwrap();
                (
                    format!("DeleteIndex({})", request.name),
                    FakeResponse::empty(),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await
    }

    fn email_index() -> FirestoreCompositeIndex {
        FirestoreCompositeIndex::new(vec![field("email", FirestoreQueryDirection::Ascending)])
    }

    #[tokio::test]
    async fn create_returns_the_id_from_the_operation_metadata_and_the_listed_state() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                encoded(decode_captured_operation(include_str!(
                    "testdata/standard-create-index-operation.json"
                ))),
            ),
            GET_INDEX => (
                format!("GetIndex({})", GetIndexRequest::decode(bytes).unwrap().name),
                encoded(captured_index(include_str!(
                    "testdata/standard-get-index-creating.json"
                ))),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let created = fake
            .db
            .create_index(captured_definition(), None, false)
            .await
            .unwrap();

        assert_eq!(
            (created.collection_group, created.id, created.state),
            (
                capture_group(),
                FirestoreIndexId::new("CICAgPj-z4oK").unwrap(),
                FirestoreIndexState::Creating
            )
        );
        assert_eq!(
            fake.calls(),
            vec![
                "CreateIndex".to_string(),
                format!("GetIndex({CAPTURE_GROUP_PATH}/indexes/CICAgPj-z4oK)")
            ]
        );
    }

    #[tokio::test]
    async fn create_with_a_wait_polls_the_operation_then_reads_the_index() {
        let done = include_str!("testdata/standard-create-index-operation-done.json");
        let fake = FakeFirestore::start(move |method, _| match method {
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                encoded(decode_captured_operation(include_str!(
                    "testdata/standard-create-index-operation.json"
                ))),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                encoded(decode_captured_operation(done)),
            ),
            GET_INDEX => {
                let done: serde_json::Value = serde_json::from_str(done).unwrap();
                (
                    "GetIndex".to_string(),
                    encoded(decode_captured_index(&done["response"])),
                )
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let created = fake
            .db
            .create_index(captured_definition(), fast_wait(), false)
            .await
            .unwrap();

        assert_eq!(created.state, FirestoreIndexState::Ready);
        assert_eq!(
            fake.calls(),
            vec!["CreateIndex", "GetOperation", "GetIndex"]
        );
    }

    #[tokio::test]
    async fn delete_by_id_refuses_an_index_of_another_group_without_deleting() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            GET_INDEX => (
                format!("GetIndex({})", GetIndexRequest::decode(bytes).unwrap().name),
                encoded(captured_index(include_str!(
                    "testdata/standard-get-index-wrong-group.json"
                ))),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let refused = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Id(
                    FirestoreIndexIdTarget::new(FirestoreIndexId::new("CICAgOi39IkJ").unwrap())
                        .with_collection_group(FirestoreCollectionId::from_static("versions")),
                ),
                false,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(&refused, FirestoreError::DataNotFoundError(error)
                if error.public.code == INDEX_IN_OTHER_GROUP),
            "{refused:?}"
        );
        assert_eq!(
            fake.calls(),
            vec![
                "GetIndex(projects/fake-firestore/databases/(default)/collectionGroups/-/indexes/CICAgOi39IkJ)"
            ]
        );
    }

    #[tokio::test]
    async fn delete_by_definition_on_enterprise_deletes_only_the_index_with_matching_options() {
        let fake = enterprise_listing_recording_deletes().await;

        let deleted = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Definition(FirestoreIndexDefinition::new(
                    capture_group(),
                    email_index()
                        .unique()
                        .density(FirestoreIndexDensity::SparseAny),
                )),
                false,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(deleted.id, FirestoreIndexId::new("CICAgLjyuogJ").unwrap());
        let deletes: Vec<String> = fake
            .calls()
            .into_iter()
            .filter(|call| call.starts_with("DeleteIndex"))
            .collect();
        assert_eq!(
            deletes,
            vec![format!(
                "DeleteIndex({CAPTURE_GROUP_PATH}/indexes/CICAgLjyuogJ)"
            )]
        );
    }

    #[tokio::test]
    async fn delete_by_definition_without_a_match_fails_without_deleting() {
        let fake = enterprise_listing_recording_deletes().await;

        let refused = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Definition(FirestoreIndexDefinition::new(
                    capture_group(),
                    FirestoreCompositeIndex::new(vec![field(
                        "email",
                        FirestoreQueryDirection::Descending,
                    )]),
                )),
                false,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(&refused, FirestoreError::DataNotFoundError(error)
                if error.public.code == NO_MATCHING_INDEX),
            "{refused:?}"
        );
        assert!(
            !fake
                .calls()
                .iter()
                .any(|call| call.starts_with("DeleteIndex")),
            "{:?}",
            fake.calls()
        );
    }

    #[tokio::test]
    async fn list_of_every_group_leaves_out_reserved_groups() {
        let fake = FakeFirestore::start(|method, bytes| match method {
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(decode_captured_indexes(include_str!(
                    "testdata/standard-list-indexes-all-groups.json"
                ))),
            ),
            LIST_FIELDS if ListFieldsRequest::decode(bytes).unwrap().filter == "ttlConfig:*" => {
                ("ListFields(ttl)".to_string(), list_fields_response(vec![]))
            }
            LIST_FIELDS => (
                "ListFields(overrides)".to_string(),
                list_fields_response(decode_captured_fields(include_str!(
                    "testdata/standard-list-fields-overrides-all-groups.json"
                ))),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let listing = fake.db.list_indexes(None).await.unwrap();

        let groups: BTreeSet<&str> = listing
            .indexes
            .iter()
            .map(|listed| listed.collection_group.as_str())
            .chain(
                listing
                    .fields
                    .iter()
                    .map(|listed| listed.collection_group.as_str()),
            )
            .collect();
        assert_eq!(
            groups,
            BTreeSet::from([
                "integration-test-query",
                "test",
                "test-camel-case",
                "test-childs",
                "test-partitions",
                "test-query-vec",
                "versions",
            ])
        );
        assert!(
            listing.unrecognised.is_empty(),
            "{:?}",
            listing.unrecognised
        );
    }

    #[tokio::test]
    async fn field_update_with_ttl_and_override_sends_one_write_with_both_masks() {
        let sent: Arc<Mutex<Vec<UpdateFieldRequest>>> = Arc::default();
        let recorded = sent.clone();
        let fake = FakeFirestore::start(move |method, bytes| match method {
            UPDATE_FIELD => {
                recorded
                    .lock()
                    .unwrap()
                    .push(UpdateFieldRequest::decode(bytes).unwrap());
                (
                    "UpdateField".to_string(),
                    encoded(decode_captured_operation(include_str!(
                        "testdata/standard-update-field-override-and-ttl-operation.json"
                    ))),
                )
            }
            GET_FIELD => (
                "GetField".to_string(),
                encoded(captured_field(include_str!(
                    "testdata/standard-get-field-after-update.json"
                ))),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        fake.db
            .update_field(
                FirestoreFieldUpdate::new(
                    capture_group(),
                    FirestoreFieldOverrideTarget::Field("expires_at".to_string()),
                )
                .with_index_override(FirestoreFieldOverrideUpdate::Set(vec![]))
                .with_ttl(FirestoreFieldTtlUpdate::Enable),
                None,
            )
            .await
            .unwrap();

        let sent = sent.lock().unwrap();
        let [request] = sent.as_slice() else {
            panic!("one UpdateField was expected, {} were sent", sent.len())
        };
        let written = request.field.as_ref().unwrap();
        assert_eq!(
            (
                request.update_mask.as_ref().unwrap().paths.as_slice(),
                written.name.as_str(),
                written
                    .index_config
                    .as_ref()
                    .map(|config| config.indexes.len()),
                written.ttl_config.is_some(),
            ),
            (
                ["index_config".to_string(), "ttl_config".to_string()].as_slice(),
                format!("{CAPTURE_GROUP_PATH}/fields/expires_at").as_str(),
                Some(0),
                true,
            )
        );
    }

    #[tokio::test]
    async fn field_update_with_a_wait_reports_the_field_as_read_after_the_operation() {
        let fake = FakeFirestore::start(|method, _| match method {
            UPDATE_FIELD => (
                "UpdateField".to_string(),
                encoded(decode_captured_operation(include_str!(
                    "testdata/standard-update-field-revert-and-disable-ttl-operation.json"
                ))),
            ),
            GET_OPERATION => (
                "GetOperation".to_string(),
                encoded(decode_captured_operation(include_str!(
                    "testdata/standard-update-field-revert-and-disable-ttl-operation-done.json"
                ))),
            ),
            GET_FIELD => (
                "GetField".to_string(),
                encoded(captured_field(include_str!(
                    "testdata/standard-get-field-after-revert.json"
                ))),
            ),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let updated = fake
            .db
            .update_field(
                FirestoreFieldUpdate::new(
                    capture_group(),
                    FirestoreFieldOverrideTarget::Field("expires_at".to_string()),
                )
                .with_index_override(FirestoreFieldOverrideUpdate::Revert)
                .with_ttl(FirestoreFieldTtlUpdate::Disable),
                fast_wait(),
            )
            .await
            .unwrap();

        assert_eq!(
            updated.index_override,
            Some(FirestoreFieldOverrideOutcome::Inherited)
        );
        assert_eq!(
            fake.calls(),
            vec!["UpdateField", "GetOperation", "GetField"]
        );
    }

    /// A fake Standard database that answers `CreateIndex` with `ALREADY_EXISTS` and lists
    /// `listed`.
    async fn already_exists_on_create(listed: Vec<ProtoIndex>) -> FakeFirestore {
        start_on(DatabaseEdition::Standard, move |method, _| match method {
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                FakeResponse::Status(Code::AlreadyExists),
            ),
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(listed.clone()),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            other => panic!("unexpected RPC: {other}"),
        })
        .await
    }

    #[tokio::test]
    async fn create_of_an_existing_index_with_if_not_exists_returns_the_listed_index() {
        let fake = already_exists_on_create(vec![captured_index(include_str!(
            "testdata/standard-get-index-creating.json"
        ))])
        .await;

        let existing = fake
            .db
            .create_index(captured_definition(), None, true)
            .await
            .unwrap();

        assert_eq!(existing.id, FirestoreIndexId::new("CICAgPj-z4oK").unwrap());
    }

    /// A fake Standard database that answers `CreateIndex` with `ALREADY_EXISTS`, lists the index
    /// of the capture as `CREATING`, and answers the `n`th `GetIndex` with `poll(n)`.
    async fn another_caller_is_building(
        poll: impl Fn(u32) -> FakeResponse + Send + Sync + 'static,
    ) -> FakeFirestore {
        let creating = captured_index(include_str!("testdata/standard-get-index-creating.json"));
        let polls = AtomicU32::new(0);
        start_on(DatabaseEdition::Standard, move |method, _| match method {
            CREATE_INDEX => (
                "CreateIndex".to_string(),
                FakeResponse::Status(Code::AlreadyExists),
            ),
            LIST_INDEXES => (
                "ListIndexes".to_string(),
                list_indexes_response(vec![creating.clone()]),
            ),
            LIST_FIELDS => ("ListFields".to_string(), list_fields_response(vec![])),
            GET_INDEX => {
                let poll_number = polls.fetch_add(1, Ordering::SeqCst) + 1;
                (format!("GetIndex#{poll_number}"), poll(poll_number))
            }
            other => panic!("unexpected RPC: {other}"),
        })
        .await
    }

    fn ready_index() -> ProtoIndex {
        let ready: serde_json::Value = serde_json::from_str(include_str!(
            "testdata/standard-create-index-operation-done.json"
        ))
        .unwrap();
        decode_captured_index(&ready["response"])
    }

    #[tokio::test]
    async fn create_of_an_index_another_caller_is_building_waits_until_it_is_ready() {
        let ready = ready_index();
        let fake = another_caller_is_building(move |_| encoded(ready.clone())).await;

        let existing = fake
            .db
            .create_index(captured_definition(), fast_wait(), true)
            .await
            .unwrap();

        assert_eq!(existing.state, FirestoreIndexState::Ready);
        assert!(fake.calls().contains(&"GetIndex#1".to_string()));
    }

    #[tokio::test]
    async fn a_poll_of_an_index_another_caller_is_building_is_sent_again_after_a_timeout() {
        let ready = ready_index();
        let fake = another_caller_is_building(move |poll| match poll {
            1 => FakeResponse::Status(Code::DeadlineExceeded),
            _ => encoded(ready.clone()),
        })
        .await;

        let existing = fake
            .db
            .create_index(captured_definition(), fast_wait(), true)
            .await
            .unwrap();

        assert_eq!(existing.state, FirestoreIndexState::Ready);
        assert!(fake.calls().contains(&"GetIndex#2".to_string()));
    }

    #[tokio::test]
    async fn a_hung_poll_of_an_index_another_caller_is_building_ends_at_the_deadline() {
        let fake = another_caller_is_building(|_| FakeResponse::Hang).await;
        let wait = FirestoreOperationWaitOptions::new(Duration::from_millis(300))
            .with_poll_interval(Duration::from_millis(5));

        let refused = tokio::time::timeout(
            Duration::from_secs(5),
            fake.db
                .create_index(captured_definition(), Some(wait), true),
        )
        .await
        .expect("the wait is bounded by its own deadline")
        .unwrap_err();

        assert!(
            matches!(&refused, FirestoreError::SystemError(error)
                if error.public.code == "OPERATION_WAIT_TIMEOUT"),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn an_index_another_caller_built_into_needs_repair_fails_the_wait() {
        let mut broken = ready_index();
        broken.set_state(ProtoIndexState::NeedsRepair);
        let fake = another_caller_is_building(move |_| encoded(broken.clone())).await;

        let refused = fake
            .db
            .create_index(captured_definition(), fast_wait(), true)
            .await
            .unwrap_err();

        assert!(
            matches!(&refused, FirestoreError::SystemError(error)
                if error.public.code == INDEX_NEEDS_REPAIR),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn already_exists_for_an_index_the_listing_does_not_show_stays_an_error() {
        let fake = already_exists_on_create(vec![]).await;

        let refused = fake
            .db
            .create_index(captured_definition(), None, true)
            .await
            .unwrap_err();

        assert!(
            matches!(refused, FirestoreError::DataConflictError(_)),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn delete_by_id_of_a_missing_index_fails_with_not_found() {
        let fake = FakeFirestore::start(|method, _| match method {
            GET_INDEX => ("GetIndex".to_string(), FakeResponse::Status(Code::NotFound)),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let refused = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Id(FirestoreIndexIdTarget::new(
                    FirestoreIndexId::new("CICAgOi39IkJ").unwrap(),
                )),
                false,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(refused, FirestoreError::DataNotFoundError(_)),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn delete_of_a_missing_id_with_if_exists_deletes_nothing() {
        let fake = FakeFirestore::start(|method, _| match method {
            GET_INDEX => ("GetIndex".to_string(), FakeResponse::Status(Code::NotFound)),
            other => panic!("unexpected RPC: {other}"),
        })
        .await;

        let deleted = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Id(FirestoreIndexIdTarget::new(
                    FirestoreIndexId::new("CICAgOi39IkJ").unwrap(),
                )),
                true,
            )
            .await
            .unwrap();

        assert!(deleted.is_none(), "{deleted:?}");
        assert_eq!(fake.calls(), vec!["GetIndex"]);
    }

    #[tokio::test]
    async fn delete_of_an_unmatched_definition_with_if_exists_deletes_nothing() {
        let fake = enterprise_listing_recording_deletes().await;

        let deleted = fake
            .db
            .delete_index(
                FirestoreIndexDeleteTarget::Definition(FirestoreIndexDefinition::new(
                    capture_group(),
                    FirestoreCompositeIndex::new(vec![field(
                        "email",
                        FirestoreQueryDirection::Descending,
                    )]),
                )),
                true,
            )
            .await
            .unwrap();

        assert!(deleted.is_none(), "{deleted:?}");
        assert!(
            !fake
                .calls()
                .iter()
                .any(|call| call.starts_with("DeleteIndex")),
            "{:?}",
            fake.calls()
        );
    }
}
