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
#[derive(Debug, PartialEq, Clone)]
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
/// Not index-specific: anything that starts a long-running operation and waits for it to reach a
/// terminal state (index/TTL sync today, a planned bulk-delete API) takes this same options type.
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
    pub undeclared_indexes: Vec<gcloud_sdk::google::firestore::admin::v1::Index>,
    /// Declared field overrides whose listed configuration differs from the declaration, or is
    /// absent; `.sync()` writes these.
    pub update_fields: Vec<FirestoreFieldOverride>,
    /// Listed field overrides with no declared match, in the owned collection group.
    pub undeclared_fields: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
    /// Declared TTL fields with no TTL configuration listed; `.sync()` enables these.
    pub enable_ttl: Vec<String>,
    /// Listed TTL fields with no declaration, in the owned collection group.
    pub undeclared_ttl: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
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
    /// Undeclared composite indexes this sync deleted, because `prune` was set.
    pub deleted_indexes: Vec<gcloud_sdk::google::firestore::admin::v1::Index>,
    /// Undeclared field overrides this sync reverted to automatic indexing, because `prune` was
    /// set.
    pub reverted_fields: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
    /// Undeclared TTL fields this sync disabled, because `prune` was set.
    pub disabled_ttl: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
    /// Undeclared composite indexes left alone, because `prune` was not set.
    pub kept_undeclared_indexes: Vec<gcloud_sdk::google::firestore::admin::v1::Index>,
    /// Undeclared field overrides left alone, because `prune` was not set.
    pub kept_undeclared_fields: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
    /// Undeclared TTL fields left alone, because `prune` was not set.
    pub kept_undeclared_ttl: Vec<gcloud_sdk::google::firestore::admin::v1::Field>,
}

/// Checks the structural rules a declared [`FirestoreIndexParams`] must satisfy, independent of
/// what Firestore currently has.
///
/// # Errors
/// Returns [`FirestoreError::InvalidParametersError`] if a composite index has fewer than two
/// fields, has more than one vector field, has a vector field that is not last, has a vector
/// dimension outside `1..=2048`, or if a field path is declared more than once across
/// `field_overrides`.
pub(crate) fn validate_index_params(params: &FirestoreIndexParams) -> FirestoreResult<()> {
    validate_composite_indexes(&params.composite_indexes)?;
    validate_field_overrides(&params.field_overrides)?;
    Ok(())
}

fn validate_composite_indexes(indexes: &[FirestoreCompositeIndex]) -> FirestoreResult<()> {
    for (position, index) in indexes.iter().enumerate() {
        if index.fields.len() < 2 {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} needs at least two fields, has {}",
                    index.fields.len()
                ),
            ));
        }

        let vector_positions: Vec<usize> = index
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| matches!(field.mode, FirestoreIndexFieldMode::Vector { .. }))
            .map(|(field_position, _)| field_position)
            .collect();

        match vector_positions.as_slice() {
            [] => {}
            [only] if *only == index.fields.len() - 1 => {
                if let FirestoreIndexFieldMode::Vector { dimension } = index.fields[*only].mode {
                    validate_vector_dimension(dimension)?;
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

fn validate_vector_dimension(dimension: u32) -> FirestoreResult<()> {
    if dimension == 0 || dimension > 2048 {
        return Err(FirestoreError::invalid_parameters(
            "composite_indexes",
            format!("vector dimension must be between 1 and 2048, was {dimension}"),
        ));
    }
    Ok(())
}

fn validate_field_overrides(overrides: &[FirestoreFieldOverride]) -> FirestoreResult<()> {
    let mut seen_paths: HashSet<&str> = HashSet::new();
    for field_override in overrides {
        if !seen_paths.insert(field_override.field_path.as_str()) {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "field path \"{}\" is declared more than once",
                    field_override.field_path
                ),
            ));
        }
        for index in &field_override.indexes {
            if let FirestoreIndexFieldMode::Vector { dimension } = index.mode {
                validate_vector_dimension(dimension)?;
            }
        }
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
}
