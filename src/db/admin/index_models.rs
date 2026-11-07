//! Declarative index, single-field override and TTL model for one collection group.
//!
//! Nothing here performs I/O. [`FirestoreIndexParams`] is what a fluent
//! `db.fluent().indexes()...` chain assembles, and what `FirestoreDb`'s index management passes
//! on to Firestore.

use crate::errors::FirestoreError;
use crate::field_path_segment;
use crate::{FirestoreCollectionId, FirestoreIndexId, FirestoreQueryDirection, FirestoreResult};
use rsb_derive::Builder;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

/// The reserved field path a Standard edition database appends to every composite index at
/// creation time.
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

/// Which documents an index holds an entry for.
///
/// A *sparse* index leaves out the documents that lack its fields; a *dense* index holds an entry
/// for every document of the collection, with a missing field stored as unset.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
pub enum FirestoreIndexDensity {
    /// An entry for each document that has every indexed field. The only density a Standard
    /// edition database builds.
    SparseAll,
    /// An entry for each document that has at least one indexed field. Enterprise edition only,
    /// and the only density of a text or geo search index.
    SparseAny,
    /// An entry for every document. Enterprise edition only, where it is the default for an index
    /// that is not a search index.
    Dense,
}

/// The edition of the database an index statement manages, read once per `.plan()` or `.sync()`
/// call: it decides which indexes Firestore accepts and how it lists the ones it builds.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(crate) enum FirestoreDatabaseEdition {
    Standard,
    Enterprise,
}

/// A flat vector index's configuration, for nearest-neighbor search.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Builder)]
pub struct FirestoreVectorIndexConfig {
    /// The dimension every indexed vector in this field must have.
    pub dimension: u32,
}

/// A geospatial search index's configuration.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Builder)]
pub struct FirestoreGeoIndexConfig {
    /// Leaves GeoJSON points in the field out of the index. Firestore indexes them by default.
    #[default = "false"]
    pub geo_json_indexing_disabled: bool,
}

/// How a single field participates in an index.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum FirestoreIndexFieldMode {
    /// Sortable and comparable in the given direction.
    Order(FirestoreQueryDirection),
    /// Queryable with array-containment operators.
    ArrayContains,
    /// A flat vector index of a fixed dimension, for nearest-neighbor search.
    Vector(FirestoreVectorIndexConfig),
    /// A text search index: the field's text is tokenised and matched anywhere in the field,
    /// the one kind of text index Firestore offers. Enterprise edition only.
    Text,
    /// A geospatial search index. Enterprise edition only.
    Geo(FirestoreGeoIndexConfig),
}

impl FirestoreIndexFieldMode {
    /// Whether the field is part of a text or geo search index.
    pub(crate) fn is_search(&self) -> bool {
        matches!(
            self,
            FirestoreIndexFieldMode::Text | FirestoreIndexFieldMode::Geo(_)
        )
    }
}

/// The index-level options of a search index.
#[derive(Debug, PartialEq, Eq, Clone, Builder)]
pub struct FirestoreSearchIndexOptions {
    /// The BCP 47 language code text fields are indexed in, unless a document overrides it.
    /// When this is unset, Firestore detects each document's language and lists the option as
    /// `und`, the BCP 47 code for an undetermined language.
    pub text_language: Option<String>,
    /// The field that holds a document's own language code, overriding `text_language` for
    /// that document. When this is unset, Firestore reads `language` and lists that.
    pub text_language_override_field_path: Option<String>,
}

impl FirestoreSearchIndexOptions {
    /// The text language Firestore lists for a search index created without one.
    pub(crate) const DEFAULT_TEXT_LANGUAGE: &'static str = "und";
    /// The override field Firestore lists for a search index created without one.
    pub(crate) const DEFAULT_TEXT_LANGUAGE_OVERRIDE_FIELD_PATH: &'static str = "language";
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
    /// The index-level options of a search index, `None` for Firestore's defaults.
    pub search_options: Option<FirestoreSearchIndexOptions>,
    /// Whether Firestore refuses a write that would give two documents the same entry in this
    /// index. Enterprise edition only.
    pub unique: bool,
    /// Which documents the index holds an entry for, `None` for the database's default: see
    /// [`FirestoreIndexDensity`].
    pub density: Option<FirestoreIndexDensity>,
}

impl FirestoreCompositeIndex {
    pub(crate) fn new(fields: Vec<FirestoreIndexField>) -> Self {
        Self {
            fields,
            query_scope: FirestoreIndexQueryScope::Collection,
            search_options: None,
            unique: false,
            density: None,
        }
    }

    /// Scopes this index to a collection-group query instead of a single collection.
    #[inline]
    pub fn all_descendants(mut self) -> Self {
        self.query_scope = FirestoreIndexQueryScope::AllDescendants;
        self
    }

    /// Sets the options of a search index, an index of [`Text`](FirestoreIndexFieldMode::Text)
    /// or [`Geo`](FirestoreIndexFieldMode::Geo) fields.
    #[inline]
    pub fn search_options(mut self, options: FirestoreSearchIndexOptions) -> Self {
        self.search_options = Some(options);
        self
    }

    /// Makes this a unique index: Firestore refuses a write that would give two documents the
    /// same entry in it, and the write fails with
    /// [`DataConflictError`](crate::errors::FirestoreError::DataConflictError). Enterprise edition
    /// only.
    #[inline]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Sets which documents this index holds an entry for, in place of the database's default.
    #[inline]
    pub fn density(mut self, density: FirestoreIndexDensity) -> Self {
        self.density = Some(density);
        self
    }

    /// Whether this is a text or geo search index.
    pub(crate) fn is_search_index(&self) -> bool {
        self.fields.iter().any(|field| field.mode.is_search())
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

/// A field path in the one spelling Firestore's grammar allows for it, so two spellings of the
/// same field compare and hash equal: `` `x` `` and `x`, or `` a.`b` `` and `` `a`.b ``.
///
/// A segment is written bare when it is a simple identifier (`[A-Za-z_][A-Za-z0-9_]*`), and in
/// backticks otherwise, with `` \` `` and `\\` escaping a backtick and a backslash inside them.
/// The all-fields wildcard `*` is kept as it is. A path that does not parse, such as one with an
/// unclosed backtick, is kept exactly as written, so it only ever equals the same text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalFieldPath(String);

impl CanonicalFieldPath {
    /// The resource name of this field in the collection group at `group_path`, the name an
    /// `UpdateField` request carries.
    pub(crate) fn field_resource_name(&self, group_path: &str) -> String {
        format!("{group_path}/fields/{self}")
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// The unescaped name of every segment of `path`, or `None` when `path` does not follow the
    /// field path grammar.
    fn segments(path: &str) -> Option<Vec<String>> {
        let mut segments = Vec::new();
        let mut chars = path.chars();
        loop {
            let mut segment = String::new();
            let mut rest = chars.clone();
            let ended = if rest.next() == Some('`') {
                chars = rest;
                loop {
                    match chars.next()? {
                        '\\' => segment.push(chars.next()?),
                        '`' => break,
                        c => segment.push(c),
                    }
                }
                match chars.next() {
                    None => true,
                    Some('.') => false,
                    Some(_) => return None,
                }
            } else {
                loop {
                    match chars.next() {
                        None => break true,
                        Some('.') => break false,
                        Some('`') => return None,
                        Some(c) => segment.push(c),
                    }
                }
            };
            if segment.is_empty() {
                return None;
            }
            segments.push(segment);
            if ended {
                return Some(segments);
            }
        }
    }
}

impl From<&str> for CanonicalFieldPath {
    fn from(path: &str) -> Self {
        if path == ALL_FIELDS_PATH {
            return Self(path.to_string());
        }
        let Some(segments) = Self::segments(path) else {
            return Self(path.to_string());
        };
        let mut canonical = String::with_capacity(path.len());
        for (position, segment) in segments.iter().enumerate() {
            if position > 0 {
                canonical.push('.');
            }
            canonical.push_str(&field_path_segment(segment));
        }
        Self(canonical)
    }
}

impl Display for CanonicalFieldPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The parts of a resource name under a collection group,
/// `projects/{p}/databases/{d}/collectionGroups/{group}/{kind}/{item}`.
///
/// Anchored on the first `/collectionGroups/` rather than split on the last `/`: a quoted field
/// path such as `` `a/b` `` can itself contain `/`, and a database ID can contain the word
/// `fields`, but neither a database ID nor a group ID contains `/`.
pub(crate) struct CollectionGroupResourceName<'n> {
    pub(crate) collection_group: &'n str,
    pub(crate) item: &'n str,
}

impl<'n> CollectionGroupResourceName<'n> {
    pub(crate) fn parse(name: &'n str, kind: &str) -> Option<Self> {
        const COLLECTION_GROUPS_MARKER: &str = "/collectionGroups/";
        let groups_at = name.find(COLLECTION_GROUPS_MARKER)?;
        let (collection_group, rest) =
            name[groups_at + COLLECTION_GROUPS_MARKER.len()..].split_once('/')?;
        let item = rest.strip_prefix(kind)?.strip_prefix('/')?;
        Some(Self {
            collection_group,
            item,
        })
    }
}

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

/// One composite index as Firestore lists it.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreListedCompositeIndex {
    /// The resource name Firestore assigned this index, for logging or a delete call.
    pub name: String,
    /// The collection group the index belongs to, parsed from `name`.
    pub collection_group: FirestoreCollectionId,
    /// The ID Firestore assigned the index, parsed from `name`. Unique in its database, and the
    /// ID a pipeline's [`force_index`](crate::FirestoreIndexHint::Index) hint takes.
    pub id: FirestoreIndexId,
    /// The index's current lifecycle state.
    pub state: FirestoreIndexState,
    /// The index in the same shape a declaration would take.
    pub index: FirestoreCompositeIndex,
}

/// An explicit, non-inherited single-field index override, as listed for one field resource.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreExplicitFieldOverride {
    /// The field's explicit single-field index set. Empty is an explicit exemption, the same as
    /// [`exempt()`](crate::index_builder::FirestoreFieldOverrideFieldBuilder::exempt).
    pub indexes: Vec<FirestoreFieldOverrideIndex>,
    /// Whether Firestore is already reverting this override to the ancestor's configuration (an
    /// in-progress `reverting` long-running change). The planner treats a field with this set as
    /// already handled and never plans reverting it a second time.
    pub reverting: bool,
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
    Explicit(FirestoreExplicitFieldOverride),
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

/// One field resource as Firestore lists it: one that carries an explicit single-field index
/// override, a TTL configuration, or both, or the one field a field update read back.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreListedField {
    /// The resource name Firestore assigned this field, for logging or an update call.
    pub name: String,
    /// The collection group the field belongs to, parsed from `name`.
    pub collection_group: FirestoreCollectionId,
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
/// Covers a search config other than one text or geo spec, a MongoDB-compat or Datastore-mode API
/// scope, a `COLLECTION_RECURSIVE` query scope, an unspecified field order, and a multikey or
/// sharded index - none of which a declared [`FirestoreCompositeIndex`] or
/// [`FirestoreFieldOverride`] can express. The planner never proposes deleting or reverting one of
/// these, even when pruning, because it cannot know that doing so is what the declaration
/// intends.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreUnrecognisedIndexItem {
    /// The resource name Firestore assigned it.
    pub name: String,
    /// Why this crate could not convert it into a domain value.
    pub reason: String,
}

/// The composite indexes, field resources and unrecognised items of one listing, each converted
/// into the domain model once.
///
/// A listed index or field the domain model cannot represent is carried in `unrecognised`
/// rather than dropped. A listed field converts whenever its resource name has the field shape,
/// because each of its halves carries its own outcome (see [`FirestoreListedField`]).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FirestoreIndexListing {
    /// The listed composite indexes.
    pub indexes: Vec<FirestoreListedCompositeIndex>,
    /// The listed field resources that carry an explicit single-field index override, a TTL
    /// configuration, or both.
    pub fields: Vec<FirestoreListedField>,
    /// The listed indexes and fields this crate's domain model cannot represent.
    pub unrecognised: Vec<FirestoreUnrecognisedIndexItem>,
}

/// A composite index and the collection group it belongs to.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreIndexDefinition {
    /// The collection group the index belongs to, whatever its query scope.
    pub collection_group: FirestoreCollectionId,
    /// The index itself.
    pub index: FirestoreCompositeIndex,
}

/// Which index a delete removes.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreIndexDeleteTarget {
    /// The index with this ID; an ID is unique in its database.
    Id(FirestoreIndexIdTarget),
    /// The one listed index this definition matches, as `.sync()` matches a declaration.
    Definition(FirestoreIndexDefinition),
}

/// An index named by its ID, for a delete.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreIndexIdTarget {
    /// The ID Firestore assigned the index.
    pub id: FirestoreIndexId,
    /// When set, the delete fails unless the index belongs to this group.
    pub collection_group: Option<FirestoreCollectionId>,
}

/// One `UpdateField` write: a field's single-field index override, its TTL, or both.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreFieldUpdate {
    /// The collection group the field belongs to.
    pub collection_group: FirestoreCollectionId,
    /// The field written, or every field of the group without its own override.
    pub target: FirestoreFieldOverrideTarget,
    /// The change to the field's single-field indexes, `None` to leave them as they are.
    pub index_override: Option<FirestoreFieldOverrideUpdate>,
    /// The change to the field's TTL, `None` to leave it as it is.
    pub ttl: Option<FirestoreFieldTtlUpdate>,
}

/// The change a [`FirestoreFieldUpdate`] makes to a field's single-field indexes.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreFieldOverrideUpdate {
    /// Replaces the field's single-field indexes with exactly these; empty exempts the field.
    Set(Vec<FirestoreFieldOverrideIndex>),
    /// Removes the override, so the field inherits its ancestor's configuration.
    Revert,
}

/// The change a [`FirestoreFieldUpdate`] makes to a field's TTL.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FirestoreFieldTtlUpdate {
    /// Expires each document at the timestamp the field holds.
    Enable,
    /// Stops expiring documents by the field.
    Disable,
}

impl FirestoreFieldUpdate {
    /// Checks the update before it is sent: its group must not be one Firestore reserves, its
    /// target must be a named field or [`AllFields`](FirestoreFieldOverrideTarget::AllFields),
    /// it must change something, and TTL applies to a named field only.
    ///
    /// # Errors
    /// [`FirestoreError::InvalidParametersError`] for each of those cases, and for a
    /// [`Set`](FirestoreFieldOverrideUpdate::Set) that `.sync()` would refuse in a declared
    /// override.
    pub(crate) fn validate(&self) -> FirestoreResult<()> {
        validate_collection_group(&self.collection_group)?;
        let indexes = match &self.index_override {
            Some(FirestoreFieldOverrideUpdate::Set(indexes)) => indexes.clone(),
            Some(FirestoreFieldOverrideUpdate::Revert) | None => Vec::new(),
        };
        validate_field_overrides(&[FirestoreFieldOverride {
            target: self.target.clone(),
            indexes,
        }])?;
        match (&self.index_override, self.ttl, &self.target) {
            (None, None, _) => Err(FirestoreError::invalid_parameters(
                "field_update",
                format!(
                    "the update of {} changes neither its single-field indexes nor its TTL",
                    self.target
                ),
            )),
            (_, Some(_), FirestoreFieldOverrideTarget::AllFields) => {
                Err(FirestoreError::invalid_parameters(
                    "ttl",
                    "TTL applies to one named field, not to every field of a collection group",
                ))
            }
            _ => Ok(()),
        }
    }
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

/// How long long-running Firestore operations are polled for before giving up, shared by index
/// sync and bulk delete.
///
/// `timeout` sets one deadline, fixed when the call starts: every wait of the call ends by it,
/// including an index sync's waits between writes that depend on each other. Until then, each
/// round polls every pending operation and then sleeps `poll_interval`, or only until the
/// deadline when that comes first, so the last round goes out at the deadline itself. One poll
/// may take up to 10 seconds to answer, or the whole `timeout` when that is shorter; a poll that
/// takes longer is abandoned and sent again next round. So a wait that times out returns at most
/// that long after the deadline, with an error naming the operations still pending. A `timeout`
/// too long to represent, such as `Duration::MAX`, means no practical deadline.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct FirestoreOperationWaitOptions {
    /// How long after the call starts every wait of it must end.
    pub timeout: Duration,
    /// The pause between two rounds of polls.
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
    /// Whether, and how long, to wait for started changes to finish. `Some` waits for every
    /// started change to reach a terminal state, and its timeout bounds the whole sync. `None`
    /// returns once the last change is requested; the waits between writes that depend on each
    /// other still happen, under a 30-minute deadline for the whole sync.
    #[default = "None"]
    pub wait: Option<FirestoreOperationWaitOptions>,
}

/// The outcome of comparing a declared [`FirestoreIndexParams`] against one collection group's
/// existing indexes, field overrides and TTL configuration, under the given
/// [`FirestoreIndexSyncOptions::prune`] setting.
///
/// Returned by the fluent `.plan()` terminal, and by `.sync()`'s internal planning step before it
/// writes anything. Every undeclared item lands in exactly one of two lists: the one `.sync()`
/// acts on when pruning (`delete_indexes`, `revert_fields`, `disable_ttl`), or the one it keeps
/// and only reports otherwise (`kept_undeclared_*`). So a plan made with `.prune_undeclared()`
/// shows exactly what a pruning sync would do.
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
    /// Listed composite indexes with no declared match, in the owned collection group, that
    /// `.sync()` deletes because `prune` is set.
    pub delete_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Listed composite indexes with no declared match, in the owned collection group, kept
    /// because `prune` is not set.
    pub kept_undeclared_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Declared field overrides whose listed configuration differs from the declaration, or is
    /// absent; `.sync()` writes these.
    pub update_fields: Vec<FirestoreFieldOverride>,
    /// Declared field overrides matched to a listed override Firestore is already reverting
    /// (`reverting: true`, an in-flight change back to the ancestor's configuration). Classified
    /// as pending, not unchanged: once the revert completes the field will no longer carry this
    /// override, and only the *next* sync writes it back. `.sync()` never writes over one of
    /// these - doing so would race Firestore's own in-flight change.
    pub reverting_fields: Vec<FirestoreListedField>,
    /// Listed field overrides with no declared match, in the owned collection group, that
    /// `.sync()` reverts to the ancestor's configuration because `prune` is set.
    pub revert_fields: Vec<FirestoreListedField>,
    /// Listed field overrides with no declared match, in the owned collection group, kept because
    /// `prune` is not set.
    pub kept_undeclared_fields: Vec<FirestoreListedField>,
    /// Declared TTL fields with no TTL configuration listed; `.sync()` enables these.
    pub enable_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration already in state `ACTIVE`.
    pub unchanged_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration still in state `CREATING`.
    pub pending_ttl: Vec<String>,
    /// Declared TTL fields matched to a listed TTL configuration in state `NEEDS_REPAIR`.
    pub needs_repair_ttl: Vec<String>,
    /// Listed TTL fields with no declaration, in the owned collection group, that `.sync()`
    /// disables because `prune` is set.
    pub disable_ttl: Vec<FirestoreListedField>,
    /// Listed TTL fields with no declaration, in the owned collection group, kept because `prune`
    /// is not set.
    pub kept_undeclared_ttl: Vec<FirestoreListedField>,
    /// Listed indexes or fields this crate's domain model cannot represent; never planned for
    /// deletion or revert, even when pruning.
    pub unrecognised: Vec<FirestoreUnrecognisedIndexItem>,
    /// Why this plan did nothing without contacting Firestore; `None` when it ran. Mirrors
    /// [`FirestoreIndexSyncReport::skipped`], so a plan made against the emulator carries the
    /// same explicit marker a sync against it reports, rather than the plain default a caller
    /// could otherwise mistake for "nothing to do".
    pub skipped: Option<FirestoreIndexSyncSkipReason>,
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
    /// Declared TTL fields already active before this sync ran.
    pub unchanged_ttl: Vec<String>,
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
    /// Undeclared composite indexes this sync deleted, because `prune` was set. Includes one
    /// another writer deleted first: the two converge on the same absence, and the log line for
    /// that delete, not this list, is where the two are told apart.
    pub deleted_indexes: Vec<FirestoreListedCompositeIndex>,
    /// Undeclared composite indexes a pruning sync planned to delete but left in place, and why;
    /// `None` when it withheld no delete.
    pub withheld_deletes: Option<FirestoreIndexDeletesWithheld>,
    /// Undeclared field overrides this sync reverted to automatic indexing, because `prune` was
    /// set. Includes one another writer reverted first: the two converge on the same state, and
    /// the log line for that write, not this list, is where the two are told apart.
    pub reverted_fields: Vec<FirestoreListedField>,
    /// Undeclared TTL fields this sync disabled, because `prune` was set. Includes one another
    /// writer disabled first: the two converge on the same state, and the log line for that
    /// write, not this list, is where the two are told apart.
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
    /// Why this sync did nothing without contacting Firestore; `None` when it ran.
    pub skipped: Option<FirestoreIndexSyncSkipReason>,
    /// How long the sync and each of its phases took.
    pub timings: FirestoreIndexSyncTimings,
}

/// Undeclared composite indexes a pruning `.sync()` planned to delete but left in place, because
/// deleting them could have left queries without an index.
#[derive(Debug, PartialEq, Clone)]
pub struct FirestoreIndexDeletesWithheld {
    /// The undeclared indexes left in place.
    pub indexes: Vec<FirestoreListedCompositeIndex>,
    /// Why they were left in place.
    pub reason: FirestoreIndexDeletesWithheldReason,
}

/// Why a pruning `.sync()` sent none of its planned index deletes.
#[derive(Debug, PartialEq, Clone)]
pub enum FirestoreIndexDeletesWithheldReason {
    /// `CreateIndex` answered `ALREADY_EXISTS` for these declared indexes, which the listing the
    /// sync planned from did not show. A fresh listing does, so another writer created them since,
    /// and it may be changing the indexes planned for deletion too.
    AlreadyExists(Vec<FirestoreCompositeIndex>),
    /// These indexes, created by the same sync, had not finished building: an operation failed,
    /// or the sync's deadline passed first. A delete sent then could remove the index queries use
    /// while its replacement cannot serve them yet.
    CreatesUnfinished(Vec<FirestoreCompositeIndex>),
}

/// Why a `.sync()` returned without contacting Firestore.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FirestoreIndexSyncSkipReason {
    /// The client talks to the Firestore emulator, which does not implement the admin API.
    Emulator,
}

/// How long a `.sync()` took, in total and per phase. A phase is `None` when it did not run: all
/// of them for a skipped sync, and `wait` when the caller did not ask to wait.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct FirestoreIndexSyncTimings {
    /// The whole call.
    pub total: Duration,
    /// Listing the group's indexes and fields.
    pub list: Option<Duration>,
    /// Sending the changes, including any wait between two writes that depend on each other and
    /// the wait for new indexes before a pruning sync deletes old ones.
    pub apply: Option<Duration>,
    /// Waiting for the started changes to finish.
    pub wait: Option<Duration>,
}

/// The maximum number of fields Firestore allows on one composite index, `__name__` included.
const MAX_INDEX_FIELDS: usize = 100;

/// Checks the structural rules a declared [`FirestoreIndexParams`] must satisfy, independent of
/// what Firestore currently has and of the database's edition (see
/// [`validate_index_params_for_edition`] for the rules that depend on it).
///
/// # Errors
/// Returns [`FirestoreError::InvalidParametersError`] if a composite index has no fields, mixes
/// text or geo fields with other fields, has more than one `array_contains` field, more than one
/// vector field, a vector field that is not last, a vector dimension outside `1..=2048`, or an
/// empty field path; if a field override path is declared more than once, declares the same
/// single-field index twice, declares a vector index, or has an empty path; or if more than one
/// TTL field is declared, a TTL path is empty, or a TTL path repeats.
pub(crate) fn validate_index_params(params: &FirestoreIndexParams) -> FirestoreResult<()> {
    validate_collection_group(&params.collection_group)?;
    validate_composite_indexes(&params.composite_indexes)?;
    validate_field_overrides(&params.field_overrides)?;
    validate_ttl_fields(&params.ttl_fields)?;
    Ok(())
}

/// Checks the rules a declared [`FirestoreIndexParams`] must satisfy on a database of `edition`,
/// on top of [`validate_index_params`].
///
/// # Errors
/// Returns [`FirestoreError::InvalidParametersError`] if a composite index duplicates an earlier
/// declared one, as Firestore tells indexes apart on `edition`, or has more than 100 fields,
/// counting the `__name__` field a Standard edition database appends; if its density is one the
/// edition refuses (see [`FirestoreIndexDensity`]); on a Standard edition database, if
/// a composite index is unique, is a text or geo search index, or has a single field that is not
/// a vector field; on an Enterprise edition database, if a field override lists single-field
/// indexes.
pub(crate) fn validate_index_params_for_edition(
    params: &FirestoreIndexParams,
    edition: FirestoreDatabaseEdition,
) -> FirestoreResult<()> {
    let indexes = &params.composite_indexes;
    for (position, index) in indexes.iter().enumerate() {
        if edition == FirestoreDatabaseEdition::Standard {
            let enterprise_only = if index.unique {
                Some("is unique")
            } else if index.is_search_index() {
                Some("is a text or geo search index")
            } else if index.fields.len() == 1
                && !matches!(index.fields[0].mode, FirestoreIndexFieldMode::Vector(_))
            {
                Some("has a single field, which Standard indexes on its own (see .field_overrides())")
            } else {
                None
            };
            if let Some(enterprise_only) = enterprise_only {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!(
                        "index {position} {enterprise_only}; only an Enterprise edition database \
                         supports that"
                    ),
                ));
            }
        }

        let appended_name = edition == FirestoreDatabaseEdition::Standard
            && !index
                .fields
                .iter()
                .any(|field| field.field_path == IMPLIED_NAME_FIELD);
        let max_fields = MAX_INDEX_FIELDS - usize::from(appended_name);
        if index.fields.len() > max_fields {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} declares {} fields; Firestore allows at most {max_fields} \
                     on this edition",
                    index.fields.len()
                ),
            ));
        }

        if let Some(density) = index.density {
            let accepted = match edition {
                FirestoreDatabaseEdition::Standard => density == FirestoreIndexDensity::SparseAll,
                FirestoreDatabaseEdition::Enterprise if index.is_search_index() => {
                    density == FirestoreIndexDensity::SparseAny
                }
                FirestoreDatabaseEdition::Enterprise => density != FirestoreIndexDensity::SparseAll,
            };
            if !accepted {
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!("index {position} cannot be built {density} on this edition"),
                ));
            }
        }

        for (earlier_position, earlier) in indexes.iter().enumerate().take(position) {
            // Checked both ways: on Standard, `composite_index_matches` only ignores `__name__` on
            // its second (`listed`) argument, and either declaration may be the one that states
            // it explicitly.
            if super::index_diff::composite_index_matches(index, earlier, edition)
                || super::index_diff::composite_index_matches(earlier, index, edition)
            {
                let ignoring = match edition {
                    FirestoreDatabaseEdition::Standard => {
                        " once the __name__ field Firestore appends is ignored"
                    }
                    FirestoreDatabaseEdition::Enterprise => "",
                };
                return Err(FirestoreError::invalid_parameters(
                    "composite_indexes",
                    format!("index {position} duplicates index {earlier_position}{ignoring}"),
                ));
            }
        }
    }

    if edition == FirestoreDatabaseEdition::Enterprise {
        if let Some(field_override) = params
            .field_overrides
            .iter()
            .find(|field_override| !field_override.indexes.is_empty())
        {
            return Err(FirestoreError::invalid_parameters(
                "field_overrides",
                format!(
                    "{} lists single-field indexes, which an Enterprise edition database does \
                     not support; declare a composite index of that one field instead",
                    field_override.target
                ),
            ));
        }
    }
    Ok(())
}

/// Rejects the collection group IDs Firestore reserves for itself: the literal `-` (a wildcard
/// segment in a document path, never a real collection group), `__default__` (the database-wide
/// configuration every collection group inherits from unless overridden), and any other ID of the
/// shape `__*__` (Firestore's reserved namespace for system collection groups). A statement that
/// named one of these would list and modify the reserved group's own resources instead of the
/// caller's collection: a `.collection_group("__default__")` sync would issue
/// `UpdateField .../collectionGroups/__default__/fields/*`, changing database-wide defaults no
/// statement should be able to reach.
///
/// [`FirestoreCollectionId::new`] accepts these IDs on purpose (see its module docs), because a
/// bare document or collection reference to them is legitimate; only naming one as the group a
/// whole statement *owns* is not.
pub(crate) fn validate_collection_group(
    collection_group: &FirestoreCollectionId,
) -> FirestoreResult<()> {
    let id = collection_group.as_str();
    let is_reserved = id == "-" || (id.len() >= 4 && id.starts_with("__") && id.ends_with("__"));
    if is_reserved {
        return Err(FirestoreError::invalid_parameters(
            "collection_group",
            format!(
                "\"{id}\" is a collection group ID Firestore reserves (\"-\", or __*__ such as \
                 __default__); index management must not target it"
            ),
        ));
    }
    Ok(())
}

fn validate_composite_indexes(indexes: &[FirestoreCompositeIndex]) -> FirestoreResult<()> {
    for (position, index) in indexes.iter().enumerate() {
        let vector_positions: Vec<usize> = index
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| matches!(field.mode, FirestoreIndexFieldMode::Vector(_)))
            .map(|(field_position, _)| field_position)
            .collect();

        // Firestore refuses the mix: "Mixing search and non-search indexes is not supported."
        let search_fields = index
            .fields
            .iter()
            .filter(|field| field.mode.is_search())
            .count();
        if search_fields > 0 && search_fields < index.fields.len() {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} mixes {search_fields} text or geo fields with {} other \
                     fields; a search index holds only text and geo fields",
                    index.fields.len() - search_fields
                ),
            ));
        }

        if index.fields.is_empty() {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!("index {position} has no fields"),
            ));
        }

        let array_contains_fields = index
            .fields
            .iter()
            .filter(|field| field.mode == FirestoreIndexFieldMode::ArrayContains)
            .count();
        if array_contains_fields > 1 {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!(
                    "index {position} has {array_contains_fields} array_contains fields; \
                     Firestore allows one per index"
                ),
            ));
        }

        if index.fields.iter().any(|field| field.field_path.is_empty()) {
            return Err(FirestoreError::invalid_parameters(
                "composite_indexes",
                format!("index {position} has a field with an empty path"),
            ));
        }

        match vector_positions.as_slice() {
            [] => {}
            [only] if *only == index.fields.len() - 1 => {
                if let FirestoreIndexFieldMode::Vector(vector) = &index.fields[*only].mode {
                    validate_vector_dimension(vector.dimension, "composite_indexes")?;
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
            if matches!(index.mode, FirestoreIndexFieldMode::Vector(_)) {
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

/// Writes `label: <count>`, then one indented line per item's [`Display`], or nothing at all when
/// `items` is empty. Shared by every section of [`Display for FirestoreIndexPlan`],
/// [`Display for FirestoreIndexSyncReport`] and the existing-state summary in
/// `db::admin::indexes`, so a log line built from the same items never disagrees with what these
/// types print, and an empty section never crowds out the ones that have something to say.
pub(crate) fn write_section<T: Display>(
    f: &mut Formatter<'_>,
    label: &str,
    items: &[T],
) -> fmt::Result {
    if items.is_empty() {
        return Ok(());
    }
    writeln!(f, "  {label}: {}", items.len())?;
    for item in items {
        writeln!(f, "    {item}")?;
    }
    Ok(())
}

/// Writes each item's [`Display`], separated by `", "`, with no wrapping punctuation of its own -
/// callers add whatever brackets their own format uses. Shared by every place that prints an
/// index or override's field list, so the separator never drifts between call sites.
fn write_comma_joined<T: Display>(f: &mut Formatter<'_>, items: &[T]) -> fmt::Result {
    for (position, item) in items.iter().enumerate() {
        if position > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{item}")?;
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
            FirestoreIndexFieldMode::Vector(vector) => write!(f, "VECTOR({})", vector.dimension),
            FirestoreIndexFieldMode::Text => f.write_str("TEXT"),
            FirestoreIndexFieldMode::Geo(geo) if geo.geo_json_indexing_disabled => {
                f.write_str("GEO(geo_json_indexing_disabled)")
            }
            FirestoreIndexFieldMode::Geo(_) => f.write_str("GEO"),
        }
    }
}

impl Display for FirestoreIndexField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field_path, self.mode)
    }
}

impl Display for FirestoreIndexDensity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FirestoreIndexDensity::SparseAll => "SPARSE_ALL",
            FirestoreIndexDensity::SparseAny => "SPARSE_ANY",
            FirestoreIndexDensity::Dense => "DENSE",
        })
    }
}

impl Display for FirestoreCompositeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] (", self.query_scope)?;
        write_comma_joined(f, &self.fields)?;
        write!(f, ")")?;
        if self.unique {
            write!(f, " unique")?;
        }
        if let Some(density) = &self.density {
            write!(f, " density={density}")?;
        }
        let Some(options) = &self.search_options else {
            return Ok(());
        };
        if let Some(language) = &options.text_language {
            write!(f, " text_language={language}")?;
        }
        if let Some(path) = &options.text_language_override_field_path {
            write!(f, " text_language_override_field_path={path}")?;
        }
        Ok(())
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
        write_comma_joined(f, &self.indexes)?;
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
            Some(FirestoreFieldOverrideOutcome::Explicit(explicit)) => {
                if explicit.indexes.is_empty() {
                    write!(f, " EXEMPT")?;
                } else {
                    write!(f, " [")?;
                    write_comma_joined(f, &explicit.indexes)?;
                    write!(f, "]")?;
                }
                if explicit.reverting {
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
        let anything_to_report = !self.create_indexes.is_empty()
            || !self.update_fields.is_empty()
            || !self.reverting_fields.is_empty()
            || !self.enable_ttl.is_empty()
            || !self.delete_indexes.is_empty()
            || !self.revert_fields.is_empty()
            || !self.disable_ttl.is_empty()
            || !self.kept_undeclared_indexes.is_empty()
            || !self.kept_undeclared_fields.is_empty()
            || !self.kept_undeclared_ttl.is_empty()
            || !self.needs_repair.is_empty()
            || !self.needs_repair_ttl.is_empty()
            || !self.unrecognised.is_empty();

        if !anything_to_report {
            writeln!(
                f,
                "Firestore index plan: no changes ({} unchanged, {} pending, {} ttl unchanged)",
                self.unchanged.len(),
                self.pending.len(),
                self.unchanged_ttl.len()
            )?;
            if let Some(reason) = &self.skipped {
                writeln!(f, "  skipped: {reason}")?;
            }
            return Ok(());
        }

        writeln!(f, "Firestore index plan:")?;
        write_section(f, "create_indexes", &self.create_indexes)?;
        write_section(f, "update_field_overrides", &self.update_fields)?;
        write_section(
            f,
            "reverting_field_overrides (revert in progress, no write planned)",
            &self.reverting_fields,
        )?;
        write_section(f, "disable_ttl", &self.disable_ttl)?;
        write_section(f, "enable_ttl", &self.enable_ttl)?;
        write_section(f, "delete_indexes", &self.delete_indexes)?;
        write_section(f, "revert_field_overrides", &self.revert_fields)?;
        write_section(
            f,
            "kept_undeclared_indexes (prune_undeclared() would delete)",
            &self.kept_undeclared_indexes,
        )?;
        write_section(
            f,
            "kept_undeclared_fields (prune_undeclared() would revert)",
            &self.kept_undeclared_fields,
        )?;
        write_section(
            f,
            "kept_undeclared_ttl (prune_undeclared() would disable)",
            &self.kept_undeclared_ttl,
        )?;
        write_section(f, "needs_repair", &self.needs_repair)?;
        write_section(f, "needs_repair_ttl", &self.needs_repair_ttl)?;
        write_section(f, "unrecognised, never pruned", &self.unrecognised)?;
        if !self.unchanged.is_empty() || !self.pending.is_empty() || !self.unchanged_ttl.is_empty()
        {
            writeln!(
                f,
                "  unchanged: {}, pending: {}, ttl unchanged: {}",
                self.unchanged.len(),
                self.pending.len(),
                self.unchanged_ttl.len()
            )?;
        }
        if let Some(reason) = &self.skipped {
            writeln!(f, "  skipped: {reason}")?;
        }
        Ok(())
    }
}

impl Display for FirestoreIndexSyncReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Firestore index sync report:")?;
        write_section(f, "created_indexes", &self.created_indexes)?;
        write_section(f, "updated_fields", &self.updated_fields)?;
        write_section(f, "enabled_ttl", &self.enabled_ttl)?;
        write_section(f, "unchanged_ttl", &self.unchanged_ttl)?;
        write_section(f, "unchanged", &self.unchanged)?;
        write_section(f, "pending", &self.pending)?;
        write_section(f, "needs_repair", &self.needs_repair)?;
        write_section(f, "pending_ttl", &self.pending_ttl)?;
        write_section(f, "needs_repair_ttl", &self.needs_repair_ttl)?;
        write_section(f, "deleted_indexes", &self.deleted_indexes)?;
        if let Some(withheld) = &self.withheld_deletes {
            writeln!(
                f,
                "  withheld_deletes: {}, because {}",
                withheld.indexes.len(),
                withheld.reason
            )?;
            for index in &withheld.indexes {
                writeln!(f, "    {index}")?;
            }
        }
        write_section(f, "reverted_fields", &self.reverted_fields)?;
        write_section(f, "disabled_ttl", &self.disabled_ttl)?;
        write_section(f, "kept_undeclared_indexes", &self.kept_undeclared_indexes)?;
        write_section(f, "kept_undeclared_fields", &self.kept_undeclared_fields)?;
        write_section(f, "kept_undeclared_ttl", &self.kept_undeclared_ttl)?;
        write_section(f, "unrecognised", &self.unrecognised)?;
        if let Some(reason) = &self.skipped {
            writeln!(f, "  skipped: {reason}")?;
        }
        writeln!(f, "  timings: {}", self.timings)
    }
}

impl Display for FirestoreIndexDeletesWithheldReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FirestoreIndexDeletesWithheldReason::AlreadyExists(indexes) => {
                write!(f, "CreateIndex answered ALREADY_EXISTS for ")?;
                write_comma_joined(f, indexes)
            }
            FirestoreIndexDeletesWithheldReason::CreatesUnfinished(indexes) => {
                write!(f, "these new indexes had not finished building: ")?;
                write_comma_joined(f, indexes)
            }
        }
    }
}

impl Display for FirestoreIndexSyncSkipReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FirestoreIndexSyncSkipReason::Emulator => {
                write!(f, "the Firestore emulator does not implement the admin API")
            }
        }
    }
}

impl Display for FirestoreIndexSyncTimings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "total {} ms", self.total.as_millis())?;
        for (phase, duration) in [
            ("list", self.list),
            ("apply", self.apply),
            ("wait", self.wait),
        ] {
            if let Some(duration) = duration {
                write!(f, ", {phase} {} ms", duration.as_millis())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::admin::index_diff::tests::is_invalid_parameter;

    #[test]
    fn a_field_path_has_one_canonical_spelling() {
        for (written, canonical) in [
            ("x", "x"),
            ("`x`", "x"),
            ("a.`b`", "a.b"),
            ("`a`.b", "a.b"),
            ("`my-field`", "`my-field`"),
            ("my-field", "`my-field`"),
            ("`a/b`.c", "`a/b`.c"),
            ("`a\\`b`", "`a\\`b`"),
            ("`back\\\\slash`", "`back\\\\slash`"),
            ("`_ok9`", "_ok9"),
            ("`9lives`", "`9lives`"),
            ("`__name__`", "__name__"),
            ("*", "*"),
        ] {
            assert_eq!(
                CanonicalFieldPath::from(written).as_str(),
                canonical,
                "{written}"
            );
        }
    }

    #[test]
    fn a_field_path_outside_the_grammar_is_kept_as_written() {
        for written in ["`open", "a..b", "a.", "``", "a`b"] {
            assert_eq!(CanonicalFieldPath::from(written).as_str(), written);
        }
    }

    #[test]
    fn a_field_resource_name_yields_its_group_and_field_path() {
        let name = "projects/p/databases/fields/collectionGroups/users/fields/`a/b`.`c`";
        let resource = CollectionGroupResourceName::parse(name, "fields").unwrap();
        assert_eq!(
            (resource.collection_group, resource.item),
            ("users", "`a/b`.`c`")
        );
        assert!(CollectionGroupResourceName::parse("projects/p/other/x", "fields").is_none());
    }

    fn field(path: &str, mode: FirestoreIndexFieldMode) -> FirestoreIndexField {
        FirestoreIndexField::new(path.to_string(), mode)
    }

    fn asc(path: &str) -> FirestoreIndexField {
        field(
            path,
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
        )
    }

    fn users_params(indexes: Vec<FirestoreCompositeIndex>) -> FirestoreIndexParams {
        FirestoreIndexParams::new(FirestoreCollectionId::from_static("users"))
            .with_composite_indexes(indexes)
    }

    #[test]
    fn composite_index_without_fields_is_rejected() {
        let err = validate_composite_indexes(&[FirestoreCompositeIndex::new(vec![])]).unwrap_err();
        assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
    }

    #[test]
    fn an_index_with_two_array_contains_fields_is_rejected() {
        let index = FirestoreCompositeIndex::new(vec![
            field("tags", FirestoreIndexFieldMode::ArrayContains),
            field("labels", FirestoreIndexFieldMode::ArrayContains),
        ]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
    }

    #[test]
    fn unique_and_search_indexes_are_refused_on_standard_only() {
        for index in [
            FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]).unique(),
            FirestoreCompositeIndex::new(vec![field("body", FirestoreIndexFieldMode::Text)]),
        ] {
            let params = users_params(vec![index]);
            let err =
                validate_index_params_for_edition(&params, FirestoreDatabaseEdition::Standard)
                    .unwrap_err();
            assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
            assert!(validate_index_params_for_edition(
                &params,
                FirestoreDatabaseEdition::Enterprise
            )
            .is_ok());
        }
    }

    #[test]
    fn a_density_the_edition_refuses_is_rejected_before_any_write() {
        use FirestoreIndexDensity::{Dense, SparseAll, SparseAny};
        let ordered = || FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        let search =
            || FirestoreCompositeIndex::new(vec![field("body", FirestoreIndexFieldMode::Text)]);
        let refused = [
            (
                FirestoreDatabaseEdition::Standard,
                ordered().density(SparseAny),
            ),
            (FirestoreDatabaseEdition::Standard, ordered().density(Dense)),
            (
                FirestoreDatabaseEdition::Enterprise,
                ordered().density(SparseAll),
            ),
            (
                FirestoreDatabaseEdition::Enterprise,
                search().density(Dense),
            ),
        ];
        for (edition, index) in refused {
            let err =
                validate_index_params_for_edition(&users_params(vec![index]), edition).unwrap_err();
            assert!(
                is_invalid_parameter(&err, "composite_indexes"),
                "{edition:?}: {err:?}"
            );
        }

        let accepted = [
            (
                FirestoreDatabaseEdition::Standard,
                ordered().density(SparseAll),
            ),
            (
                FirestoreDatabaseEdition::Enterprise,
                ordered().density(SparseAny),
            ),
            (
                FirestoreDatabaseEdition::Enterprise,
                ordered().density(Dense),
            ),
            (
                FirestoreDatabaseEdition::Enterprise,
                search().density(SparseAny),
            ),
        ];
        for (edition, index) in accepted {
            validate_index_params_for_edition(&users_params(vec![index]), edition).unwrap();
        }
    }

    #[test]
    fn composite_index_with_two_fields_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn vector_field_must_be_last() {
        let index = FirestoreCompositeIndex::new(vec![
            field(
                "v",
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
            ),
            asc("b"),
        ]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(err.to_string().contains("must be last"));
    }

    #[test]
    fn vector_field_last_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![
            asc("a"),
            field(
                "v",
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
            ),
        ]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn at_most_one_vector_field() {
        let index = FirestoreCompositeIndex::new(vec![
            field(
                "v1",
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
            ),
            field(
                "v2",
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
            ),
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
                    FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(bad_dimension)),
                ),
            ]);
            let err = validate_composite_indexes(&[index]).unwrap_err();
            assert!(err.to_string().contains("between 1 and 2048"));
        }

        let index = FirestoreCompositeIndex::new(vec![
            asc("a"),
            field(
                "v",
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(2048)),
            ),
        ]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn a_search_index_must_not_mix_search_and_non_search_fields() {
        let index = FirestoreCompositeIndex::new(vec![
            field("menu", FirestoreIndexFieldMode::Text),
            asc("rating"),
        ]);
        let err = validate_composite_indexes(&[index]).unwrap_err();
        assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
    }

    #[test]
    fn a_search_index_displays_its_field_modes_and_options() {
        let index = FirestoreCompositeIndex::new(vec![
            field("menu", FirestoreIndexFieldMode::Text),
            field(
                "location",
                FirestoreIndexFieldMode::Geo(
                    FirestoreGeoIndexConfig::new().with_geo_json_indexing_disabled(true),
                ),
            ),
            field(
                "area",
                FirestoreIndexFieldMode::Geo(FirestoreGeoIndexConfig::new()),
            ),
        ])
        .search_options(
            FirestoreSearchIndexOptions::new()
                .with_text_language("en".to_string())
                .with_text_language_override_field_path("lang".to_string()),
        );
        assert_eq!(
            index.to_string(),
            "[COLLECTION] (menu TEXT, location GEO(geo_json_indexing_disabled), area GEO) \
             text_language=en text_language_override_field_path=lang"
        );
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
    fn default_collection_group_is_rejected() {
        // Firestore's database-wide defaults every collection group inherits from unless
        // overridden - a statement must never be able to reach them.
        let params = FirestoreIndexParams::new(FirestoreCollectionId::from_static("__default__"));
        let err = validate_index_params(&params).unwrap_err();
        assert!(err.to_string().contains("__default__"));
    }

    #[test]
    fn dash_collection_group_is_rejected() {
        let params = FirestoreIndexParams::new(FirestoreCollectionId::from_static("-"));
        let err = validate_index_params(&params).unwrap_err();
        assert!(err.to_string().contains("reserves"));
    }

    #[test]
    fn other_reserved_shaped_collection_group_is_rejected() {
        let params = FirestoreIndexParams::new(FirestoreCollectionId::from_static("__x__"));
        let err = validate_index_params(&params).unwrap_err();
        assert!(err.to_string().contains("__x__"));
    }

    #[test]
    fn ordinary_collection_group_is_accepted() {
        let params = FirestoreIndexParams::new(FirestoreCollectionId::from_static("users"));
        assert!(validate_index_params(&params).is_ok());
    }

    #[test]
    fn lone_vector_field_composite_index_is_valid() {
        let index = FirestoreCompositeIndex::new(vec![field(
            "embedding",
            FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
        )]);
        assert!(validate_composite_indexes(&[index]).is_ok());
    }

    #[test]
    fn lone_vector_field_dimension_is_still_validated() {
        let index = FirestoreCompositeIndex::new(vec![field(
            "embedding",
            FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(0)),
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
    fn composite_index_exceeding_max_declared_fields_is_rejected_on_standard() {
        let fields: Vec<_> = (0..100).map(|i| asc(&format!("f{i}"))).collect();
        let params = users_params(vec![FirestoreCompositeIndex::new(fields)]);
        let err = validate_index_params_for_edition(&params, FirestoreDatabaseEdition::Standard)
            .unwrap_err();
        assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
    }

    #[test]
    fn composite_index_at_max_declared_fields_is_valid_on_standard() {
        let fields: Vec<_> = (0..99).map(|i| asc(&format!("f{i}"))).collect();
        let params = users_params(vec![FirestoreCompositeIndex::new(fields)]);
        validate_index_params_for_edition(&params, FirestoreDatabaseEdition::Standard).unwrap();
    }

    #[test]
    fn duplicate_composite_index_after_name_normalisation_is_rejected_on_standard() {
        let index_a = FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        let index_b =
            FirestoreCompositeIndex::new(vec![asc("a"), asc("b"), asc(IMPLIED_NAME_FIELD)]);
        let err = validate_index_params_for_edition(
            &users_params(vec![index_a, index_b]),
            FirestoreDatabaseEdition::Standard,
        )
        .unwrap_err();
        assert!(is_invalid_parameter(&err, "composite_indexes"), "{err:?}");
    }

    #[test]
    fn composite_index_with_name_field_mid_position_is_not_a_duplicate() {
        // Firestore never places an implicit `__name__` mid-index, so a declaration that states
        // it there explicitly is a different index from one that omits `__name__` altogether,
        // not a duplicate of it.
        let index_a = FirestoreCompositeIndex::new(vec![asc("a"), asc("b")]);
        let index_b =
            FirestoreCompositeIndex::new(vec![asc("a"), asc(IMPLIED_NAME_FIELD), asc("b")]);
        assert!(validate_index_params_for_edition(
            &users_params(vec![index_a, index_b]),
            FirestoreDatabaseEdition::Standard
        )
        .is_ok());
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
                FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(8)),
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
