//! Declarative index, single-field override and TTL model for one collection group.
//!
//! Nothing here performs I/O. [`FirestoreIndexParams`] is what a fluent
//! `db.fluent().indexes()...` chain assembles, and what `FirestoreDb`'s index management passes
//! on to Firestore.

use crate::errors::FirestoreError;
use crate::{FirestoreCollectionId, FirestoreQueryDirection, FirestoreResult};
use rsb_derive::Builder;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
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

/// The Firestore special field path (`*`) that targets every field in a collection group, in
/// place of one named field. Reachable only through
/// [`all_fields()`](crate::index_builder::FirestoreFieldOverrideBuilder::all_fields), never as a
/// literal string: see [`FirestoreFieldOverrideTarget`].
pub(crate) const ALL_FIELDS_PATH: &str = "*";

/// What a [`FirestoreFieldOverride`] applies to: one named field, or every field in the owned
/// collection group.
///
/// A bare `field_path: String` would let `"*"` arrive through
/// [`field()`](crate::index_builder::FirestoreFieldOverrideBuilder::field) indistinguishably from
/// [`all_fields()`](crate::index_builder::FirestoreFieldOverrideBuilder::all_fields), so validation
/// could never tell "the caller meant the wildcard" from "the caller has a field literally named
/// `*`". Splitting the two into their own variants makes `field("*")` a value validation can
/// reject outright, pointing the caller at `all_fields()`.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum FirestoreFieldOverrideTarget {
    /// One field's path within a document.
    Field(String),
    /// Every field in the owned collection group that has no more specific, named override.
    AllFields,
}

impl FirestoreFieldOverrideTarget {
    /// The resource path segment this target corresponds to: the field path itself, or `*` for
    /// [`AllFields`](Self::AllFields).
    pub(crate) fn as_str(&self) -> &str {
        match self {
            FirestoreFieldOverrideTarget::Field(path) => path.as_str(),
            FirestoreFieldOverrideTarget::AllFields => ALL_FIELDS_PATH,
        }
    }
}

/// A single-field index override: the automatic indexes Firestore would otherwise build for
/// `target`, replaced by exactly the indexes listed here.
///
/// **This replaces the field's whole automatic index set, it does not add to it.** Declaring
/// only `array_contains()` on a field that is also compared with `==` and ordered removes the
/// automatic equality and ordering support for that field, because Firestore does not merge an
/// override with its defaults - neither does `firestore.indexes.json`. An empty index list
/// (equivalently, [`exempt()`](crate::index_builder::FirestoreFieldOverrideFieldBuilder::exempt))
/// excludes the field from single-field indexing entirely.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreFieldOverride {
    /// What this override applies to.
    pub target: FirestoreFieldOverrideTarget,
    /// The exact set of single-field indexes to maintain for this target. Empty means exempt.
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

/// A field resource's single-field index configuration, as read from a listed [`ProtoField`]
/// (`gcloud_sdk::google::firestore::admin::v1::Field`).
///
/// Carries its own outcome rather than failing the whole field's conversion: a field resource can
/// carry a valid override alongside an unrecognisable TTL state (or the reverse), and failing on
/// one bad half would hide the other, valid half from the diff and re-plan a change to it on
/// every sync.
///
/// [`ProtoField`]: gcloud_sdk::google::firestore::admin::v1::Field
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFieldOverrideOutcome {
    /// The field carries an `index_config`, but it is inherited from an ancestor field
    /// (`uses_ancestor_config`), not an explicit override. Not an override a declaration can be
    /// compared against - the same as [`FirestoreListedField::index_override`] being `None`
    /// (no `index_config` at all), which is why the diff treats the two alike.
    Inherited,
    /// An explicit, non-inherited override.
    Explicit {
        /// The field's explicit single-field index set. Empty is an explicit exemption, the same
        /// as [`exempt()`](crate::index_builder::FirestoreFieldOverrideFieldBuilder::exempt).
        indexes: Vec<FirestoreFieldOverrideIndex>,
        /// Whether Firestore is already reverting this override to the ancestor's configuration
        /// (an in-progress `reverting` long-running change). The planner treats a field with this
        /// set as already handled and never plans reverting it a second time.
        reverting: bool,
    },
    /// The listed `index_config` carries data this crate's domain model cannot represent (an
    /// unspecified or unknown order, array config, vector dimension, query scope or API scope).
    /// Carries why, for [`FirestoreUnrecognisedIndexItem::reason`].
    Unrecognised(String),
}

/// A field resource's TTL configuration, as read from a listed [`ProtoField`] that carries one.
///
/// [`ProtoField`]: gcloud_sdk::google::firestore::admin::v1::Field
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFieldTtlOutcome {
    /// TTL is configured, in the given lifecycle state.
    Configured(FirestoreFieldTtlState),
    /// The listed `ttl_config` carries a state this crate's domain model cannot represent.
    /// Carries why, for [`FirestoreUnrecognisedIndexItem::reason`]. Report-only, the same as any
    /// other unrecognised item: the planner never enables, disables or otherwise acts on it.
    Unrecognised(String),
}

/// One field resource Firestore has listed for the owned collection group, because it carries an
/// explicit single-field index override, a TTL configuration, or both.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreListedField {
    /// The resource name Firestore assigned this field, for logging or an update call.
    pub name: String,
    /// The field's path within a document, parsed from `name`.
    pub field_path: String,
    /// The field's single-field index configuration, or `None` when the field carries no
    /// `index_config` at all.
    pub index_override: Option<FirestoreFieldOverrideOutcome>,
    /// The field's TTL configuration, or `None` when the field carries no `ttl_config` at all.
    pub ttl: Option<FirestoreFieldTtlOutcome>,
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

/// Options controlling how `.sync()` reconciles a declared [`FirestoreIndexParams`] with
/// Firestore.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreIndexSyncOptions {
    /// When set, also deletes undeclared composite indexes, reverts undeclared field overrides,
    /// and disables undeclared TTL fields in the owned collection group. Otherwise undeclared
    /// items are only reported.
    #[default = "false"]
    pub prune: bool,
    /// Whether, and how long, to wait for started changes to finish. `None` returns as soon as
    /// changes are requested; `Some` waits for every started change to reach a terminal state.
    #[default = "None"]
    pub wait: Option<FirestoreOperationWaitOptions>,
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
            // Checked both ways: `composite_index_matches` only ignores `__name__` on its
            // second (`listed`) argument, and either of the two declarations may be the one
            // that states it explicitly.
            if super::index_diff::composite_index_matches(index, earlier)
                || super::index_diff::composite_index_matches(earlier, index)
            {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!(
                        "index {position} duplicates index {earlier_position} once an implicit \
                         __name__ field is ignored"
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
    let mut seen_targets: HashSet<&FirestoreFieldOverrideTarget> = HashSet::new();
    for field_override in overrides {
        match &field_override.target {
            FirestoreFieldOverrideTarget::Field(path) if path.is_empty() => {
                return Err(FirestoreError::invalid_parameters(
                    "field_overrides",
                    "a field override's field path must not be empty",
                ));
            }
            FirestoreFieldOverrideTarget::Field(path) if path == ALL_FIELDS_PATH => {
                return Err(FirestoreError::invalid_parameters(
                    "field_overrides",
                    "field path \"*\" is reserved; declare it with all_fields() instead of \
                     field(\"*\")",
                ));
            }
            _ => {}
        }
        if !seen_targets.insert(&field_override.target) {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "{} is declared more than once",
                    field_override.target.as_str()
                ),
            ));
        }
        if super::index_diff::override_index_set(&field_override.indexes).len()
            != field_override.indexes.len()
        {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "{} declares the same single-field index more than once",
                    field_override.target.as_str()
                ),
            ));
        }
        for index in &field_override.indexes {
            if matches!(index.mode, FirestoreIndexFieldMode::Vector { .. }) {
                return Err(FirestoreError::invalid_parameters(
                    "field_overrides",
                    format!(
                        "{} declares a vector index, which Firestore does not support as a \
                         single-field override",
                        field_override.target.as_str()
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

/// Writes `label: <count>`, then one indented line per item's [`Display`], or `label: none` when
/// `items` is empty. Shared by every section of [`Display for FirestoreIndexPlan`] and
/// [`Display for FirestoreIndexSyncReport`], so a log line built from the same items never
/// disagrees with what these types print.
fn write_section<T: Display>(f: &mut Formatter<'_>, label: &str, items: &[T]) -> fmt::Result {
    if items.is_empty() {
        return writeln!(f, "  {label}: none");
    }
    writeln!(f, "  {label}: {}", items.len())?;
    for item in items {
        writeln!(f, "    {item}")?;
    }
    Ok(())
}

impl Display for FirestoreIndexQueryScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FirestoreIndexQueryScope::Collection => "COLLECTION",
            FirestoreIndexQueryScope::AllDescendants => "COLLECTION_GROUP",
        })
    }
}

impl Display for FirestoreIndexFieldMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending) => {
                f.write_str("ASC")
            }
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending) => {
                f.write_str("DESC")
            }
            FirestoreIndexFieldMode::ArrayContains => f.write_str("CONTAINS"),
            FirestoreIndexFieldMode::Vector { dimension } => write!(f, "VECTOR({dimension})"),
        }
    }
}

impl Display for FirestoreIndexField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field_path, self.mode)
    }
}

impl Display for FirestoreCompositeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] (", self.query_scope)?;
        for (position, field) in self.fields.iter().enumerate() {
            if position > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}")?;
        }
        write!(f, ")")
    }
}

impl Display for FirestoreIndexState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FirestoreIndexState::Creating => "CREATING",
            FirestoreIndexState::Ready => "READY",
            FirestoreIndexState::NeedsRepair => "NEEDS_REPAIR",
        })
    }
}

impl Display for FirestoreListedCompositeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} ({})", self.index, self.state, self.name)
    }
}

impl Display for FirestoreFieldOverrideIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.query_scope, self.mode)
    }
}

impl Display for FirestoreFieldOverrideTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Display for FirestoreFieldOverride {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.target)?;
        if self.indexes.is_empty() {
            return f.write_str(" EXEMPT");
        }
        write!(f, " [")?;
        for (position, index) in self.indexes.iter().enumerate() {
            if position > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{index}")?;
        }
        write!(f, "]")
    }
}

impl Display for FirestoreFieldTtlState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FirestoreFieldTtlState::Creating => "CREATING",
            FirestoreFieldTtlState::Active => "ACTIVE",
            FirestoreFieldTtlState::NeedsRepair => "NEEDS_REPAIR",
        })
    }
}

impl Display for FirestoreListedField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.field_path)?;
        match &self.index_override {
            None | Some(FirestoreFieldOverrideOutcome::Inherited) => {}
            Some(FirestoreFieldOverrideOutcome::Explicit { indexes, reverting }) => {
                if indexes.is_empty() {
                    write!(f, " EXEMPT")?;
                } else {
                    write!(f, " [")?;
                    for (position, index) in indexes.iter().enumerate() {
                        if position > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{index}")?;
                    }
                    write!(f, "]")?;
                }
                if *reverting {
                    write!(f, " (reverting)")?;
                }
            }
            Some(FirestoreFieldOverrideOutcome::Unrecognised(reason)) => {
                write!(f, " override unrecognised: {reason}")?;
            }
        }
        match &self.ttl {
            None => {}
            Some(FirestoreFieldTtlOutcome::Configured(state)) => write!(f, " ttl={state}")?,
            Some(FirestoreFieldTtlOutcome::Unrecognised(reason)) => {
                write!(f, " ttl unrecognised: {reason}")?;
            }
        }
        write!(f, " ({})", self.name)
    }
}

impl Display for FirestoreUnrecognisedIndexItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.reason)
    }
}

impl Display for FirestoreIndexPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Firestore index plan:")?;
        write_section(f, "create_indexes", &self.create_indexes)?;
        write_section(f, "unchanged", &self.unchanged)?;
        write_section(f, "pending", &self.pending)?;
        write_section(f, "needs_repair", &self.needs_repair)?;
        write_section(f, "undeclared_indexes", &self.undeclared_indexes)?;
        write_section(f, "update_fields", &self.update_fields)?;
        write_section(f, "undeclared_fields", &self.undeclared_fields)?;
        write_section(f, "enable_ttl", &self.enable_ttl)?;
        write_section(f, "pending_ttl", &self.pending_ttl)?;
        write_section(f, "needs_repair_ttl", &self.needs_repair_ttl)?;
        write_section(f, "undeclared_ttl", &self.undeclared_ttl)?;
        write_section(f, "unrecognised", &self.unrecognised)
    }
}

impl Display for FirestoreIndexSyncReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Firestore index sync report:")?;
        write_section(f, "created_indexes", &self.created_indexes)?;
        write_section(f, "updated_fields", &self.updated_fields)?;
        write_section(f, "enabled_ttl", &self.enabled_ttl)?;
        write_section(f, "unchanged", &self.unchanged)?;
        write_section(f, "pending", &self.pending)?;
        write_section(f, "needs_repair", &self.needs_repair)?;
        write_section(f, "pending_ttl", &self.pending_ttl)?;
        write_section(f, "needs_repair_ttl", &self.needs_repair_ttl)?;
        write_section(f, "deleted_indexes", &self.deleted_indexes)?;
        write_section(f, "reverted_fields", &self.reverted_fields)?;
        write_section(f, "disabled_ttl", &self.disabled_ttl)?;
        write_section(f, "kept_undeclared_indexes", &self.kept_undeclared_indexes)?;
        write_section(f, "kept_undeclared_fields", &self.kept_undeclared_fields)?;
        write_section(f, "kept_undeclared_ttl", &self.kept_undeclared_ttl)?;
        write_section(f, "unrecognised", &self.unrecognised)
    }
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
                target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
                indexes: vec![],
            },
            FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
                indexes: vec![],
            },
        ];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("declared more than once"));
    }

    #[test]
    fn exempt_and_empty_indexes_are_the_same_and_accepted() {
        let overrides = vec![FirestoreFieldOverride {
            target: FirestoreFieldOverrideTarget::Field("bio".to_string()),
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
            target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
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
            target: FirestoreFieldOverrideTarget::Field("embedding".to_string()),
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
            target: FirestoreFieldOverrideTarget::Field(String::new()),
            indexes: vec![],
        }];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn field_named_star_is_rejected_pointing_at_all_fields() {
        let overrides = vec![FirestoreFieldOverride {
            target: FirestoreFieldOverrideTarget::Field(ALL_FIELDS_PATH.to_string()),
            indexes: vec![],
        }];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("all_fields()"));
    }

    #[test]
    fn all_fields_target_is_accepted() {
        let overrides = vec![FirestoreFieldOverride {
            target: FirestoreFieldOverrideTarget::AllFields,
            indexes: vec![],
        }];
        assert!(validate_field_overrides(&overrides).is_ok());
    }

    #[test]
    fn all_fields_declared_twice_is_rejected_as_a_duplicate() {
        let overrides = vec![
            FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::AllFields,
                indexes: vec![],
            },
            FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::AllFields,
                indexes: vec![FirestoreFieldOverrideIndex::new(
                    FirestoreIndexFieldMode::ArrayContains,
                )],
            },
        ];
        let err = validate_field_overrides(&overrides).unwrap_err();
        assert!(err.to_string().contains("declared more than once"));
    }

    #[test]
    fn all_fields_and_a_named_field_are_both_accepted() {
        let overrides = vec![
            FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::AllFields,
                indexes: vec![],
            },
            FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::Field("country".to_string()),
                indexes: vec![FirestoreFieldOverrideIndex::new(
                    FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
                )],
            },
        ];
        assert!(validate_field_overrides(&overrides).is_ok());
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
