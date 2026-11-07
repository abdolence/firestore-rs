//! [`FirestoreIndexSupport`] for [`FirestoreDb`]: the declarative `.plan()` and `.sync()` of one
//! collection group, built on the listing, diff, apply and wait steps of
//! [`indexes`](super::indexes), and the four calls that each list, create, delete or update one
//! index or field.
//!
//! The four calls run no edition rule and no emulator check of their own: Firestore's own
//! `INVALID_ARGUMENT` and the emulator's `UNIMPLEMENTED` are their errors.

use crate::db::admin::index_diff::composite_index_matches;
use crate::db::admin::index_models::{validate_collection_group, CanonicalFieldPath};
use crate::db::admin::indexes::{IndexAction, RefusedFieldWrite, SEQUENCING_TIMEOUT};
use crate::db::admin::operation_wait::{OperationAction, OperationDeadline, StartedOperation};
use crate::db::support::FirestoreIndexSupport;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCollectionId, FirestoreDb, FirestoreFieldOverride, FirestoreFieldOverrideUpdate,
    FirestoreFieldTtlUpdate, FirestoreFieldUpdate, FirestoreIndexDefinition,
    FirestoreIndexDeleteTarget, FirestoreIndexIdTarget, FirestoreIndexListing,
    FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSyncOptions, FirestoreIndexSyncReport,
    FirestoreIndexSyncSkipReason, FirestoreIndexSyncTimings, FirestoreInstant,
    FirestoreListedCompositeIndex, FirestoreListedField, FirestoreOperationWaitOptions,
    FirestoreResult,
};
use async_trait::async_trait;
use gcloud_sdk::google::firestore::admin::v1::field::{IndexConfig, TtlConfig};
use gcloud_sdk::google::firestore::admin::v1::{
    CreateIndexRequest, DeleteIndexRequest, Field as ProtoField, GetFieldRequest, GetIndexRequest,
    Index as ProtoIndex, IndexOperationMetadata, UpdateFieldRequest,
};
use gcloud_sdk::prost::Message as _;
use gcloud_sdk::prost_types::FieldMask;
use gcloud_sdk::RequestTimeouts;
use std::time::Instant;
use tracing::*;

/// The code of the [`DataNotFoundError`](FirestoreError::DataNotFoundError) a delete by ID fails
/// with when the index belongs to another group than the caller named.
const INDEX_IN_OTHER_GROUP: &str = "INDEX_IN_OTHER_GROUP";

/// The code of the [`DataNotFoundError`](FirestoreError::DataNotFoundError) a delete by
/// definition fails with when no listed index matches it.
const NO_MATCHING_INDEX: &str = "NO_MATCHING_INDEX";

/// The code of the [`DataConflictError`](FirestoreError::DataConflictError) a delete by
/// definition fails with when more than one listed index matches it.
const SEVERAL_MATCHING_INDEXES: &str = "SEVERAL_MATCHING_INDEXES";

impl FirestoreDb {
    async fn get_index(&self, name: &str) -> FirestoreResult<ProtoIndex> {
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

    /// Sends one `CreateIndex` and returns its operation, under the resource name of the index
    /// it builds: Firestore names the new index in the metadata of its first answer.
    async fn start_create_index(
        &self,
        definition: FirestoreIndexDefinition,
    ) -> FirestoreResult<(StartedOperation<IndexAction>, String)> {
        let action = IndexAction::CreateIndex(definition.index.clone());
        let span = span!(
            Level::INFO,
            "Create Index",
            "/firestore/action" = action.kind(),
            "/firestore/target" = action.target().as_str(),
            "/firestore/operation" = field::Empty,
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let request = CreateIndexRequest {
            parent: self.collection_group_path(definition.collection_group.as_str()),
            index: Some(ProtoIndex::try_from(definition.index)?),
        };
        let started = self
            .admin_client()
            .create_index(self.request_with_timeout(request, RequestTimeouts::with_total))
            .instrument(span.clone())
            .await;
        span.record(
            "/firestore/response_time",
            FirestoreInstant::now().duration_since(began).as_millis(),
        );
        let operation = started
            .inspect_err(|status| {
                span.in_scope(|| {
                    error!(error = %status, label = %action, "Failed to create a composite index.")
                })
            })?
            .into_inner();
        span.record("/firestore/operation", operation.name.as_str());
        let index_name = operation
            .metadata
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
        span.in_scope(|| {
            info!(
                operation = operation.name.as_str(),
                index = index_name.as_str(),
                label = %action,
                "Created a composite index.",
            )
        });
        Ok((
            StartedOperation {
                name: operation.name,
                action,
            },
            index_name,
        ))
    }

    async fn send_delete_index(
        &self,
        listed: &FirestoreListedCompositeIndex,
    ) -> FirestoreResult<()> {
        let span = span!(
            Level::INFO,
            "Delete Index",
            "/firestore/action" = "delete_index",
            "/firestore/target" = listed.name.as_str(),
            "/firestore/response_time" = field::Empty,
        );
        let began = FirestoreInstant::now();
        let request = DeleteIndexRequest {
            name: listed.name.clone(),
        };
        let deleted = self
            .admin_client()
            .delete_index(self.request_with_timeout(request, RequestTimeouts::with_total))
            .instrument(span.clone())
            .await;
        span.record(
            "/firestore/response_time",
            FirestoreInstant::now().duration_since(began).as_millis(),
        );
        span.in_scope(|| match &deleted {
            Ok(_) => info!(index = %listed, "Deleted a composite index."),
            Err(status) => {
                error!(error = %status, index = %listed, "Failed to delete a composite index.")
            }
        });
        deleted?;
        Ok(())
    }

    /// The listed index with the ID `target` names, refused when the domain model cannot
    /// represent it or when it belongs to another group than `target` names.
    async fn index_by_id(
        &self,
        target: FirestoreIndexIdTarget,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        if let Some(group) = &target.collection_group {
            validate_collection_group(group)?;
        }
        // Firestore reads an index by its ID alone and ignores the group of the name, so `-`
        // reads it wherever it is.
        let name = format!("{}/indexes/{}", self.collection_group_path("-"), target.id);
        let proto = self.get_index(&name).await?;
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
        match target.collection_group {
            Some(group) if group != listed.collection_group => Err(FirestoreError::data_not_found(
                INDEX_IN_OTHER_GROUP,
                format!(
                    "index {} belongs to group {}, not {group}; nothing was deleted",
                    target.id, listed.collection_group
                ),
            )),
            _ => Ok(listed),
        }
    }

    /// The one listed index of its group that `definition` matches, under the database's
    /// edition.
    async fn index_by_definition(
        &self,
        definition: &FirestoreIndexDefinition,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        let group_id = &definition.collection_group;
        validate_collection_group(group_id)?;
        let group = self.read_owned_group(group_id).await?;
        let listing = self.list_existing_state(Some(group_id)).await?;
        let matching: Vec<FirestoreListedCompositeIndex> = listing
            .indexes
            .into_iter()
            .filter(|listed| {
                composite_index_matches(&definition.index, &listed.index, group.edition)
            })
            .collect();
        let listed = match <[_; 1]>::try_from(matching) {
            Ok([listed]) => listed,
            Err(matching) if matching.is_empty() => {
                return Err(FirestoreError::data_not_found(
                    NO_MATCHING_INDEX,
                    format!(
                        "no index of group {group_id} matches {}; nothing was deleted",
                        definition.index
                    ),
                ))
            }
            Err(matching) => {
                let ids: Vec<String> = matching
                    .iter()
                    .map(|listed| listed.id.to_string())
                    .collect();
                return Err(FirestoreError::data_conflict(
                    SEVERAL_MATCHING_INDEXES,
                    format!(
                        "{} indexes of group {group_id} match {}: {}; nothing was deleted",
                        ids.len(),
                        definition.index,
                        ids.join(", ")
                    ),
                ));
            }
        };
        group.ensure_owns("indexes", &listed.name)?;
        Ok(listed)
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
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        validate_collection_group(&definition.collection_group)?;
        let deadline = wait.as_ref().map(OperationDeadline::from_now);
        let (operation, index_name) = self.start_create_index(definition).await?;
        if let Some(deadline) = &deadline {
            self.wait_for_operations(&[&operation], deadline, |_, _| {})
                .await?;
        }
        FirestoreListedCompositeIndex::try_from(self.get_index(&index_name).await?)
    }

    async fn delete_index(
        &self,
        target: FirestoreIndexDeleteTarget,
    ) -> FirestoreResult<FirestoreListedCompositeIndex> {
        let listed = match target {
            FirestoreIndexDeleteTarget::Id(target) => self.index_by_id(target).await?,
            FirestoreIndexDeleteTarget::Definition(definition) => {
                self.index_by_definition(&definition).await?
            }
        };
        self.send_delete_index(&listed).await?;
        Ok(listed)
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
        FirestoreIndexIdTarget, FirestoreIndexState, FirestoreQueryDirection,
    };
    use gcloud_sdk::google::firestore::admin::v1::database::DatabaseEdition;
    use gcloud_sdk::google::firestore::admin::v1::ListFieldsRequest;
    use std::collections::BTreeSet;
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
            .create_index(captured_definition(), None)
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
            .create_index(captured_definition(), fast_wait())
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
            .delete_index(FirestoreIndexDeleteTarget::Id(
                FirestoreIndexIdTarget::new(FirestoreIndexId::new("CICAgOi39IkJ").unwrap())
                    .with_collection_group(FirestoreCollectionId::from_static("versions")),
            ))
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
            .delete_index(FirestoreIndexDeleteTarget::Definition(
                FirestoreIndexDefinition::new(
                    capture_group(),
                    email_index()
                        .unique()
                        .density(FirestoreIndexDensity::SparseAny),
                ),
            ))
            .await
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
            .delete_index(FirestoreIndexDeleteTarget::Definition(
                FirestoreIndexDefinition::new(
                    capture_group(),
                    FirestoreCompositeIndex::new(vec![field(
                        "email",
                        FirestoreQueryDirection::Descending,
                    )]),
                ),
            ))
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
}
