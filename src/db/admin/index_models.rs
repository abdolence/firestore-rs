//! Declarative index, single-field override and TTL model for one collection group.
//!
//! Nothing here performs I/O. [`FirestoreIndexParams`] is what a fluent
//! `db.fluent().indexes()...` chain assembles, and what `FirestoreDb`'s index management passes
//! on to Firestore.

use crate::errors::FirestoreError;
use crate::{FirestoreCollectionId, FirestoreQueryDirection, FirestoreResult};
use rsb_derive::Builder;
use std::collections::HashSet;
use std::time::Duration;

/// The reserved field path Firestore appends to every composite index at creation time.
pub(crate) const IMPLIED_NAME_FIELD: &str = "__name__";

/// Whether an index serves queries against one collection or a collection-group query across
/// every collection with the same ID anywhere under the database.
///
/// This is the same distinction [`.all_descendants()`](crate::select_builder::FirestoreSelectDocBuilder::all_descendants)
/// makes for a query: a `CollectionGroup`-scoped index is exactly what such a query needs.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Default)]
pub enum FirestoreIndexQueryScope {
    /// Serves queries against a single collection at a known path.
    #[default]
    Collection,
    /// Serves collection-group queries: every collection with this ID, anywhere under the
    /// database.
    AllDescendants,
}

/// How a single field participates in an index.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum FirestoreIndexFieldMode {
    /// Sortable and comparable in the given direction.
    Order(FirestoreQueryDirection),
    /// Queryable with array-containment operators.
    ArrayContains,
    /// A flat vector index of a fixed dimension, for nearest-neighbor search.
    Vector {
        /// The dimension every indexed vector in this field must have.
        dimension: u32,
    },
}

/// One field of a composite index: its path and how it participates.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreIndexField {
    /// The field's path within a document.
    pub field_path: String,
    /// How the field is indexed.
    pub mode: FirestoreIndexFieldMode,
}

impl FirestoreIndexField {
    pub(crate) fn new(field_path: String, mode: FirestoreIndexFieldMode) -> Self {
        Self { field_path, mode }
    }
}

/// A declared composite index: an ordered list of fields plus the scope it serves.
///
/// Build one with [`FirestoreCompositeIndexBuilder::index`](crate::index_builder::FirestoreCompositeIndexBuilder::index);
/// it defaults to [`FirestoreIndexQueryScope::Collection`] until [`all_descendants`](Self::all_descendants)
/// is chained onto it.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreCompositeIndex {
    /// The indexed fields, in the order Firestore evaluates them.
    pub fields: Vec<FirestoreIndexField>,
    /// Whether this index serves a single collection or every collection with the same ID.
    pub query_scope: FirestoreIndexQueryScope,
}

impl FirestoreCompositeIndex {
    pub(crate) fn new(fields: Vec<FirestoreIndexField>) -> Self {
        Self {
            fields,
            query_scope: FirestoreIndexQueryScope::Collection,
        }
    }

    /// Scopes this index to a collection-group query instead of a single collection.
    #[inline]
    pub fn all_descendants(mut self) -> Self {
        self.query_scope = FirestoreIndexQueryScope::AllDescendants;
        self
    }
}

/// One single-field index a [`FirestoreFieldOverride`] declares explicitly.
///
/// Build one from [`FirestoreFieldOverrideBuilder`](crate::index_builder::FirestoreFieldOverrideBuilder)'s
/// `.ascending()`, `.descending()` or `.array_contains()`; it defaults to
/// [`FirestoreIndexQueryScope::Collection`] until [`all_descendants`](Self::all_descendants) is
/// chained onto it.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreFieldOverrideIndex {
    /// Whether this single-field index serves a single collection or every collection with the
    /// same ID.
    pub query_scope: FirestoreIndexQueryScope,
    /// How the field is indexed.
    pub mode: FirestoreIndexFieldMode,
}

impl FirestoreFieldOverrideIndex {
    pub(crate) fn new(mode: FirestoreIndexFieldMode) -> Self {
        Self {
            query_scope: FirestoreIndexQueryScope::Collection,
            mode,
        }
    }

    /// Scopes this single-field index to a collection-group query instead of a single
    /// collection.
    #[inline]
    pub fn all_descendants(mut self) -> Self {
        self.query_scope = FirestoreIndexQueryScope::AllDescendants;
        self
    }
}

/// A single-field index override: the automatic indexes Firestore would otherwise build for
/// `field_path`, replaced by exactly the indexes listed here.
///
/// **This replaces the field's whole automatic index set, it does not add to it.** Declaring
/// only `array_contains()` on a field that is also compared with `==` and ordered removes the
/// automatic equality and ordering support for that field, because Firestore does not merge an
/// override with its defaults - neither does `firestore.indexes.json`. An empty index list
/// (equivalently, [`exempt()`](crate::index_builder::FirestoreFieldOverrideFieldBuilder::exempt))
/// excludes the field from single-field indexing entirely.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreFieldOverride {
    /// The field's path within a document.
    pub field_path: String,
    /// The exact set of single-field indexes to maintain for this field. Empty means exempt.
    pub indexes: Vec<FirestoreFieldOverrideIndex>,
}

/// The lifecycle state Firestore reports for a listed composite index.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
pub enum FirestoreIndexState {
    /// There is an active long-running operation building the index.
    Creating,
    /// The index is fully built and serving queries.
    Ready,
    /// The most recent build failed and left no active operation. A declared index matched to
    /// one in this state is reported only; `.sync()` never deletes or recreates it. An
    /// *undeclared* index in this state is pruned like any other undeclared index when `prune`
    /// is set - Firestore does not exempt it.
    NeedsRepair,
}

/// The lifecycle state Firestore reports for a listed field's TTL configuration.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
pub enum FirestoreFieldTtlState {
    /// There is an active long-running operation applying TTL to existing documents.
    Creating,
    /// TTL is active for all documents that carry the field.
    Active,
    /// The long-running operation that last tried to enable TTL failed.
    NeedsRepair,
}

/// One composite index Firestore has listed for the owned collection group.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreListedCompositeIndex {
    /// The resource name Firestore assigned this index, for logging or a delete call.
    pub name: String,
    /// The index's current lifecycle state.
    pub state: FirestoreIndexState,
    /// The index in the same shape a declaration would take.
    pub index: FirestoreCompositeIndex,
}

/// One field resource Firestore has listed for the owned collection group, because it carries an
/// explicit single-field index override, a TTL configuration, or both.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreListedField {
    /// The resource name Firestore assigned this field, for logging or an update call.
    pub name: String,
    /// The field's path within a document, parsed from `name`.
    pub field_path: String,
    /// The field's explicit single-field index set, or `None` when this resource carries no
    /// index configuration at all, or its index configuration is inherited from an ancestor
    /// field (`uses_ancestor_config`). `Some(vec![])` is an explicit exemption, the same as
    /// [`exempt()`](crate::index_builder::FirestoreFieldOverrideFieldBuilder::exempt).
    pub indexes: Option<Vec<FirestoreFieldOverrideIndex>>,
    /// Whether Firestore is already reverting `indexes` to the ancestor's configuration (an
    /// in-progress `reverting` long-running change). Always `false` when `indexes` is `None`.
    /// The planner treats a field with this set as already handled and never plans reverting it
    /// a second time.
    pub reverting: bool,
    /// The field's TTL configuration state, or `None` when TTL is not configured on this field.
    pub ttl: Option<FirestoreFieldTtlState>,
}

/// One listed index or field resource this crate's domain model cannot represent, kept for
/// visibility instead of being silently dropped.
///
/// Covers a search index, a MongoDB-compat or Datastore-mode API scope, a `COLLECTION_RECURSIVE`
/// query scope, and an unspecified field order - none of which a declared [`FirestoreCompositeIndex`]
/// or [`FirestoreFieldOverride`] can express. The planner never proposes deleting or reverting
/// one of these, even when pruning, because it cannot know that doing so is what the declaration
/// intends.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreUnrecognisedIndexItem {
    /// The resource name Firestore assigned it.
    pub name: String,
    /// Why this crate could not convert it into a domain value.
    pub reason: String,
}

/// One collection group's declared composite indexes, single-field overrides and TTL policy.
///
/// A statement owns exactly this one collection group: [`FirestoreIndexSyncOptions::prune`]
/// never reaches an index, override or TTL field on any other group. Several groups need
/// several statements.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreIndexParams {
    /// The collection group this statement owns.
    pub collection_group: FirestoreCollectionId,
    /// The declared composite indexes.
    #[default = "Vec::new()"]
    pub composite_indexes: Vec<FirestoreCompositeIndex>,
    /// The declared single-field index overrides.
    #[default = "Vec::new()"]
    pub field_overrides: Vec<FirestoreFieldOverride>,
    /// The field paths whose timestamp should enable document TTL.
    #[default = "Vec::new()"]
    pub ttl_fields: Vec<String>,
}

/// How long a long-running Firestore operation is polled for before giving up.
///
/// Not index-specific: any long-running admin operation that waits for a terminal state - index
/// and TTL sync today - can reuse this same options type.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreOperationWaitOptions {
    /// The maximum time to wait before returning an error naming the operations still pending.
    pub timeout: Duration,
    /// The interval between polls.
    #[default = "Duration::from_secs(5)"]
    pub poll_interval: Duration,
}

/// Whether, and how long, `.sync()` waits for changes it starts to finish.
#[derive(Debug, PartialEq, Clone, Default)]
pub enum FirestoreIndexWait {
    /// Returns as soon as changes are requested, without waiting for them to complete.
    #[default]
    NoWait,
    /// Waits for every started change to reach a terminal state.
    UntilReady(FirestoreOperationWaitOptions),
}

/// Options controlling how `.sync()` reconciles a declared [`FirestoreIndexParams`] with
/// Firestore.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreIndexSyncOptions {
    /// When set, also deletes undeclared composite indexes, reverts undeclared field overrides,
    /// and disables undeclared TTL fields in the owned collection group. Otherwise undeclared
    /// items are only reported.
    #[default = "false"]
    pub prune: bool,
    /// Whether, and how long, to wait for started changes to finish.
    #[default = "FirestoreIndexWait::NoWait"]
    pub wait: FirestoreIndexWait,
}

/// The outcome of comparing a declared [`FirestoreIndexParams`] against one collection group's
/// existing indexes, field overrides and TTL configuration.
///
/// Returned by the fluent `.plan()` terminal, and by `.sync()`'s internal planning step before it
/// writes anything.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FirestoreIndexPlan {
    /// Declared composite indexes that match nothing listed; `.sync()` creates these.
    pub create_indexes: Vec<FirestoreCompositeIndex>,
    /// Declared composite indexes matched to a listed index in state `READY`.
    pub unchanged: Vec<FirestoreCompositeIndex>,
    /// Declared composite indexes matched to a listed index in state `CREATING`.
    pub pending: Vec<FirestoreCompositeIndex>,
    /// Declared composite indexes matched to a listed index in state `NEEDS_REPAIR`. These are
    /// reported only; `.sync()` never deletes or recreates them.
    pub needs_repair: Vec<FirestoreCompositeIndex>,
    /// Listed composite indexes with no declared match, in the owned collection group.
    pub undeclared_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Declared field overrides whose listed configuration differs from the declaration, or is
    /// absent; `.sync()` writes these.
    pub update_fields: Vec<FirestoreFieldOverride>,
    /// Listed field overrides with no declared match, in the owned collection group.
    pub undeclared_fields: Vec<FirestoreListedField>,
    /// Declared TTL fields with no TTL configuration listed; `.sync()` enables these.
    pub enable_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration still in state `CREATING`.
    pub pending_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration in state `NEEDS_REPAIR`.
    pub needs_repair_ttl: Vec<String>,
    /// Listed TTL fields with no declaration, in the owned collection group.
    pub undeclared_ttl: Vec<FirestoreListedField>,
    /// Listed indexes or fields this crate's domain model cannot represent; never planned for
    /// deletion or revert, even when pruning.
    pub unrecognised: Vec<FirestoreUnrecognisedIndexItem>,
}

/// The result of `.sync()`: what it changed, what it left alone, and what it found undeclared.
///
/// Every list carries the resource a caller would log or use to fail a deploy on; filled in by
/// [`FirestoreDb`](crate::FirestoreDb)'s index management.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FirestoreIndexSyncReport {
    /// Composite indexes created by this sync.
    pub created_indexes: Vec<FirestoreCompositeIndex>,
    /// Field overrides written by this sync.
    pub updated_fields: Vec<FirestoreFieldOverride>,
    /// TTL fields enabled by this sync.
    pub enabled_ttl: Vec<String>,
    /// Composite indexes matched to a listed index already in state `READY`.
    pub unchanged: Vec<FirestoreCompositeIndex>,
    /// Composite indexes matched to a listed index still in state `CREATING`.
    pub pending: Vec<FirestoreCompositeIndex>,
    /// Composite indexes matched to a listed index in state `NEEDS_REPAIR`, reported only.
    pub needs_repair: Vec<FirestoreCompositeIndex>,
    /// Declared TTL fields matched to a listed TTL configuration still in state `CREATING`.
    pub pending_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration in state `NEEDS_REPAIR`,
    /// reported only.
    pub needs_repair_ttl: Vec<String>,
    /// Undeclared composite indexes this sync deleted, because `prune` was set.
    pub deleted_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Undeclared field overrides this sync reverted to automatic indexing, because `prune` was
    /// set.
    pub reverted_fields: Vec<FirestoreListedField>,
    /// Undeclared TTL fields this sync disabled, because `prune` was set.
    pub disabled_ttl: Vec<FirestoreListedField>,
    /// Undeclared composite indexes left alone, because `prune` was not set.
    pub kept_undeclared_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Undeclared field overrides left alone, because `prune` was not set.
    pub kept_undeclared_fields: Vec<FirestoreListedField>,
    /// Undeclared TTL fields left alone, because `prune` was not set.
    pub kept_undeclared_ttl: Vec<FirestoreListedField>,
    /// Listed indexes or fields this crate's domain model cannot represent; never deleted or
    /// reverted, even when pruning.
    pub unrecognised: Vec<FirestoreUnrecognisedIndexItem>,
}

/// The maximum number of fields Firestore allows on one composite index, `__name__` included.
const MAX_INDEX_FIELDS_WITH_IMPLIED_NAME: usize = 100;

/// Checks the structural rules a declared [`FirestoreIndexParams`] must satisfy, independent of
/// what Firestore currently has.
///
/// # Errors
/// Returns [`FirestoreError::InvalidParametersError`] if a composite index has fewer than two
/// fields (a lone vector field excepted), more than 100 fields counting an implied `__name__`,
/// more than one vector field, a vector field that is not last, a vector dimension outside
/// `1..=2048`, an empty field path, or duplicates an earlier declared index once a trailing
/// `__name__` is normalised away; if a field override path is declared more than once, declares
/// the same single-field index twice, declares a vector index, or has an empty path; or if more
/// than one TTL field is declared, a TTL path is empty, or a TTL path repeats.
pub(crate) fn validate_index_params(params: &FirestoreIndexParams) -> FirestoreResult<()> {
    validate_composite_indexes(&params.composite_indexes)?;
    validate_field_overrides(&params.field_overrides)?;
    validate_ttl_fields(&params.ttl_fields)?;
    Ok(())
}

fn validate_composite_indexes(indexes: &[FirestoreCompositeIndex]) -> FirestoreResult<()> {
    for (position, index) in indexes.iter().enumerate() {
        let vector_positions: Vec<usize> = index
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| matches!(field.mode, FirestoreIndexFieldMode::Vector { .. }))
            .map(|(field_position, _)| field_position)
            .collect();

        let is_lone_vector_field = index.fields.len() == 1 && vector_positions == [0];
        if index.fields.len() < 2 && !is_lone_vector_field {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} needs at least two fields, or exactly one vector field, has {}",
                    index.fields.len()
                ),
            ));
        }

        if index.fields.iter().any(|field| field.field_path.is_empty()) {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!("index {position} has a field with an empty path"),
            ));
        }

        let declares_implied_name = index
            .fields
            .iter()
            .any(|field| field.field_path == IMPLIED_NAME_FIELD);
        let max_fields = if declares_implied_name {
            MAX_INDEX_FIELDS_WITH_IMPLIED_NAME
        } else {
            MAX_INDEX_FIELDS_WITH_IMPLIED_NAME - 1
        };
        if index.fields.len() > max_fields {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} declares {} fields; Firestore allows at most {max_fields} \
                     (100 including the __name__ field it appends)",
                    index.fields.len()
                ),
            ));
        }

        for (earlier_position, earlier) in indexes.iter().enumerate().take(position) {
            if super::index_diff::composite_index_matches(index, earlier) {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!(
                        "index {position} duplicates index {earlier_position} once a trailing \
                         implied __name__ field is normalised away"
                    ),
                ));
            }
        }

        match vector_positions.as_slice() {
            [] => {}
            [only] if *only == index.fields.len() - 1 => {
                if let FirestoreIndexFieldMode::Vector { dimension } = index.fields[*only].mode {
                    validate_vector_dimension(dimension, "composite_indexes")?;
                }
            }
            [only] => {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!(
                        "index {position}'s vector field must be last, was at position {only} of {}",
                        index.fields.len()
                    ),
                ));
            }
            multiple => {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!(
                        "index {position} has {} vector fields, at most one is allowed",
                        multiple.len()
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_vector_dimension(dimension: u32, field: &'static str) -> FirestoreResult<()> {
    if dimension == 0 || dimension > 2048 {
        return Err(FirestoreError::invalid_parameters(
            field,
            format!("vector dimension must be between 1 and 2048, was {dimension}"),
        ));
    }
    Ok(())
}

fn validate_field_overrides(overrides: &[FirestoreFieldOverride]) -> FirestoreResult<()> {
    let mut seen_paths: HashSet<&str> = HashSet::new();
    for field_override in overrides {
        if field_override.field_path.is_empty() {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                "a field override's field path must not be empty",
            ));
        }
        if !seen_paths.insert(field_override.field_path.as_str()) {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "field path \"{}\" is declared more than once",
                    field_override.field_path
                ),
            ));
        }
        if super::index_diff::override_index_set(&field_override.indexes).len()
            != field_override.indexes.len()
        {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "field path \"{}\" declares the same single-field index more than once",
                    field_override.field_path
                ),
            ));
        }
        for index in &field_override.indexes {
            if matches!(index.mode, FirestoreIndexFieldMode::Vector { .. }) {
                return Err(FirestoreError::invalid_parameters(
                    "field_overrides",
                    format!(
                        "field path \"{}\" declares a vector index, which Firestore does not \
                         support as a single-field override",
                        field_override.field_path
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Firestore allows at most one TTL field per collection group.
///
/// <https://firebase.google.com/docs/firestore/ttl>: "You can mark only one field per collection
/// group as a TTL field."
fn validate_ttl_fields(ttl_fields: &[String]) -> FirestoreResult<()> {
    for path in ttl_fields {
        if path.is_empty() {
            return Err(FirestoreError::invalid_parameters(
                "ttl_fields",
                "a TTL field path must not be empty",
            ));
        }
    }
    if ttl_fields.len() > 1 {
        return Err(FirestoreError::invalid_parameters(
            "ttl_fields",
            format!(
                "a collection group may have at most one TTL field, {} were declared",
                ttl_fields.len()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(path: &str, mode: FirestoreIndexFieldMode) -> FirestoreIndexField {
        FirestoreIndexField::new(path.to_string(), mode)
    }

    fn asc(path: &str) -> FirestoreIndexField {
        field(
            path,
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
        )
    }

    #[test]
    fn composite_index_needs_at_least_two_fields() {
        let index = FirestoreCompositeIndex::new(vec![asc("a")]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("at least two fields"));
    }

    #[test]
    fn composite_index_with_two_fields_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn vector_field_must_be_last() {
        let index = FirestoreCompositeIndex::new(vec![
            field("v", FirestoreIndexFieldMode::Vector { dimension: 8 }),
            asc("b"),
        ]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("must be last"));
    }

    #[test]
    fn vector_field_last_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![
            asc("a"),
            field("v", FirestoreIndexFieldMode::Vector { dimension: 8 }),
        ]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn at_most_one_vector_field() {
        let index = FirestoreCompositeIndex::new(vec![
            field("v1", FirestoreIndexFieldMode::Vector { dimension: 8 }),
            field("v2", FirestoreIndexFieldMode::Vector { dimension: 8 }),
        ]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("at most one is allowed"));
    }

    #[test]
    fn vector_dimension_must_be_in_range() {
        for bad_dimension in [0u32, 2049] {
            let index = FirestoreCompositeIndex::new(vec![
                asc("a"),
                field(
                    "v",
                    FirestoreIndexFieldMode::Vector {
                        dimension: bad_dimension,
                    },
                ),
            ]);
            let err = validate_composite_indexes(&[index]).unwrap_err();
            assert!(err.to_string().contains("between 1 and 2048"));
        }

        let index = FirestoreCompositeIndex::new(vec![
            asc("a"),
            field("v", FirestoreIndexFieldMode::Vector { dimension: 2048 }),
        ]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn duplicate_field_override_path_is_rejected() {
        let overrides = vec![
            FirestoreFieldOverride {
                field_path: "tags".to_string(),
                indexes: vec![],
            },
            FirestoreFieldOverride {
                field_path: "tags".to_string(),
                indexes: vec![],
            },
        ];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("declared more than once"));
    }

    #[test]
    fn exempt_and_empty_indexes_are_the_same_and_accepted() {
        let overrides = vec![FirestoreFieldOverride {
            field_path: "bio".to_string(),
            indexes: vec![],
        }];
        assert!(validate_field_overrides(&overrides).is_ok());
    }

    #[test]
    fn empty_declaration_is_valid() {
        let params = FirestoreIndexParams::new(FirestoreCollectionId::from_static("users"));
        assert!(validate_index_params(&params).is_ok());
        assert!(params.composite_indexes.is_empty());
        assert!(params.field_overrides.is_empty());
        assert!(params.ttl_fields.is_empty());
    }

    #[test]
    fn lone_vector_field_composite_index_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![field(
            "embedding",
            FirestoreIndexFieldMode::Vector { dimension: 8 },
        )]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn lone_vector_field_dimension_is_still_validated() {
        let index = FirestoreCompositeIndex::new(vec![field(
            "embedding",
            FirestoreIndexFieldMode::Vector { dimension: 0 },
        )]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("between 1 and 2048"));
    }

    #[test]
    fn composite_index_with_empty_field_path_is_rejected() {
        let index = FirestoreCompositeIndex::new(vec![asc(""), asc("b")]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("empty path"));
    }

    #[test]
    fn composite_index_exceeding_max_declared_fields_is_rejected() {
        let fields: Vec<_> = (0..100).map(|i| asc(&format!("f{i}"))).collect();
        let index = FirestoreCompositeIndex::new(fields);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("at most 99"));
    }

    #[test]
    fn composite_index_at_max_declared_fields_is_valid() {
        let fields: Vec<_> = (0..99).map(|i| asc(&format!("f{i}"))).collect();
        let index = FirestoreCompositeIndex::new(fields);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn duplicate_composite_index_after_name_normalisation_is_rejected() {
        let index_a = FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        let index_b =
            FirestoreCompositeIndex::new(vec![asc("a"), asc("b"), asc(IMPLIED_NAME_FIELD)]);
        let err = validate_composite_indexes(&[index_a, index_b]).unwrap_err();
        assert!(err.to_string().contains("index 1 duplicates index 0"));
    }

    #[test]
    fn field_override_with_duplicate_index_entries_is_rejected() {
        let overrides = vec![FirestoreFieldOverride {
            field_path: "tags".to_string(),
            indexes: vec![
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains),
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains),
            ],
        }];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("more than once"));
    }

    #[test]
    fn field_override_with_vector_mode_is_rejected() {
        let overrides = vec![FirestoreFieldOverride {
            field_path: "embedding".to_string(),
            indexes: vec![FirestoreFieldOverrideIndex::new(
                FirestoreIndexFieldMode::Vector { dimension: 8 },
            )],
        }];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("vector index"));
    }

    #[test]
    fn field_override_with_empty_path_is_rejected() {
        let overrides = vec![FirestoreFieldOverride {
            field_path: String::new(),
            indexes: vec![],
        }];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn single_ttl_field_is_valid() {
        assert!(validate_ttl_fields(&["expires_at".to_string()]).is_ok());
    }

    #[test]
    fn more_than_one_ttl_field_is_rejected() {
        let err = validate_ttl_fields(&["a".to_string(), "b".to_string()]).unwrap_err();
        assert!(err.to_string().contains("at most one TTL field"));
    }

    #[test]
    fn duplicate_ttl_field_is_rejected() {
        let err =
            validate_ttl_fields(&["expires_at".to_string(), "expires_at".to_string()]).unwrap_err();
        assert!(err.to_string().contains("at most one TTL field"));
    }

    #[test]
    fn empty_ttl_field_path_is_rejected() {
        let err = validate_ttl_fields(&[String::new()]).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }
}
