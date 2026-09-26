//! Converts Firestore's listed protos into this crate's domain types, and diffs the result
//! against a declared [`FirestoreIndexParams`].
//!
//! Everything here is a pure function of its inputs, deliberately, so it stays unit-testable
//! without a server: [`plan_index_changes`] takes the state a `ListIndexes`/`ListFields` call
//! already fetched and never performs I/O of its own.

use crate::db::admin::index_models::IMPLIED_NAME_FIELD;
use crate::db::split_document_path;
use crate::errors::FirestoreError;
use crate::{
    FirestoreCompositeIndex, FirestoreFieldOverride, FirestoreFieldOverrideIndex,
    FirestoreFieldTtlState, FirestoreIndexField, FirestoreIndexFieldMode, FirestoreIndexParams,
    FirestoreIndexPlan, FirestoreIndexQueryScope, FirestoreIndexState,
    FirestoreListedCompositeIndex, FirestoreListedField, FirestoreQueryDirection, FirestoreResult,
    FirestoreUnrecognisedIndexItem,
};
use gcloud_sdk::google::firestore::admin::v1::index::index_field::{
    vector_config, ArrayConfig as ProtoArrayConfig, Order as ProtoOrder, ValueMode,
    VectorConfig as ProtoVectorConfig,
};
use gcloud_sdk::google::firestore::admin::v1::index::{
    ApiScope, IndexField as ProtoIndexField, QueryScope as ProtoQueryScope, State as ProtoState,
};
use gcloud_sdk::google::firestore::admin::v1::{field, Field as ProtoField, Index as ProtoIndex};
use std::collections::HashSet;

/// The state Firestore already has for one collection group: its listed composite indexes and
/// the field resources that carry an explicit index or TTL configuration.
///
/// Built from a `ListIndexes` call and two `ListFields` calls
/// (`indexConfig.usesAncestorConfig:false` and `ttlConfig:*`); `fields` is the union of both.
/// Only the first `ListFields` call is filtered on `usesAncestorConfig`: a field reached only
/// through the `ttlConfig:*` listing can still carry an inherited (`uses_ancestor_config: true`)
/// index configuration, and [`plan_index_changes`] reads that as no override at all rather than
/// as one to revert. An entry this crate's domain model cannot convert (see
/// [`FirestoreUnrecognisedIndexItem`]) is not dropped: it is carried into the plan's
/// `unrecognised` list instead.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FirestoreIndexExistingState {
    /// The collection group's currently listed composite indexes.
    pub indexes: Vec<ProtoIndex>,
    /// The collection group's field resources that carry an explicit index override, a TTL
    /// configuration, or both.
    pub fields: Vec<ProtoField>,
}

impl From<FirestoreIndexQueryScope> for ProtoQueryScope {
    fn from(scope: FirestoreIndexQueryScope) -> Self {
        match scope {
            FirestoreIndexQueryScope::Collection => ProtoQueryScope::Collection,
            FirestoreIndexQueryScope::AllDescendants => ProtoQueryScope::CollectionGroup,
        }
    }
}

/// Converts a listed query scope. `Unspecified` and `CollectionRecursive` (a Datastore-mode-only
/// scope no declaration can express) are rejected with distinct reasons, rather than folded into
/// "does not match", so a caller can tell a real mismatch from an item this crate cannot represent
/// at all.
impl TryFrom<ProtoQueryScope> for FirestoreIndexQueryScope {
    type Error = FirestoreError;

    fn try_from(scope: ProtoQueryScope) -> Result<Self, Self::Error> {
        match scope {
            ProtoQueryScope::Collection => Ok(FirestoreIndexQueryScope::Collection),
            ProtoQueryScope::CollectionGroup => Ok(FirestoreIndexQueryScope::AllDescendants),
            ProtoQueryScope::Unspecified => Err(FirestoreError::invalid_parameters(
                "query_scope",
                "query scope is unspecified",
            )),
            ProtoQueryScope::CollectionRecursive => Err(FirestoreError::invalid_parameters(
                "query_scope",
                "COLLECTION_RECURSIVE query scope has no domain equivalent",
            )),
        }
    }
}

/// Parses a listed `i32` query scope, going through the proto's own `TryFrom<i32>` before
/// [`TryFrom<ProtoQueryScope> for FirestoreIndexQueryScope`] so an out-of-range value and a
/// known-but-unsupported one are reported the same way this module reports everything else.
fn parse_query_scope(scope: i32) -> FirestoreResult<FirestoreIndexQueryScope> {
    let scope = ProtoQueryScope::try_from(scope).map_err(|_| {
        FirestoreError::invalid_parameters(
            "query_scope",
            format!("{scope} is not a known query scope"),
        )
    })?;
    FirestoreIndexQueryScope::try_from(scope)
}

/// Rejects any API scope other than `ANY_API`. A declared index is always `ANY_API` (see
/// `TryFrom<FirestoreCompositeIndex> for ProtoIndex` below), so a listed MongoDB-compat or
/// Datastore-mode index can never be the domain equivalent of a declaration; it is reported as
/// unrecognised rather than compared field-by-field and found merely "different".
fn ensure_any_api_scope(api_scope: i32) -> FirestoreResult<()> {
    match ApiScope::try_from(api_scope) {
        Ok(ApiScope::AnyApi) => Ok(()),
        Ok(other) => Err(FirestoreError::invalid_parameters(
            "api_scope",
            format!("API scope {} is not supported", other.as_str_name()),
        )),
        Err(_) => Err(FirestoreError::invalid_parameters(
            "api_scope",
            format!("{api_scope} is not a known API scope"),
        )),
    }
}

impl TryFrom<FirestoreIndexFieldMode> for ValueMode {
    type Error = FirestoreError;

    fn try_from(mode: FirestoreIndexFieldMode) -> Result<Self, Self::Error> {
        Ok(match mode {
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending) => {
                ValueMode::Order(ProtoOrder::Ascending.into())
            }
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending) => {
                ValueMode::Order(ProtoOrder::Descending.into())
            }
            FirestoreIndexFieldMode::ArrayContains => {
                ValueMode::ArrayConfig(ProtoArrayConfig::Contains.into())
            }
            FirestoreIndexFieldMode::Vector { dimension } => {
                ValueMode::VectorConfig(ProtoVectorConfig {
                    dimension: i32::try_from(dimension).map_err(|_| {
                        FirestoreError::invalid_parameters(
                            "dimension",
                            format!(
                                "vector dimension {dimension} exceeds the maximum representable value {}",
                                i32::MAX
                            ),
                        )
                    })?,
                    r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
                })
            }
        })
    }
}

/// Converts a listed value mode. `SearchConfig` has no domain equivalent (search indexes are
/// MongoDB-compat only) and an unspecified order is rejected rather than guessed at; both make
/// the containing index or field unrecognised instead of silently mismatched.
impl TryFrom<ValueMode> for FirestoreIndexFieldMode {
    type Error = FirestoreError;

    fn try_from(mode: ValueMode) -> Result<Self, Self::Error> {
        match mode {
            ValueMode::Order(order) => match ProtoOrder::try_from(order) {
                Ok(ProtoOrder::Ascending) => Ok(FirestoreIndexFieldMode::Order(
                    FirestoreQueryDirection::Ascending,
                )),
                Ok(ProtoOrder::Descending) => Ok(FirestoreIndexFieldMode::Order(
                    FirestoreQueryDirection::Descending,
                )),
                Ok(ProtoOrder::Unspecified) | Err(_) => Err(FirestoreError::invalid_parameters(
                    "value_mode",
                    format!("order {order} is unspecified or unknown"),
                )),
            },
            ValueMode::ArrayConfig(array_config) => {
                match ProtoArrayConfig::try_from(array_config) {
                    Ok(ProtoArrayConfig::Contains) => Ok(FirestoreIndexFieldMode::ArrayContains),
                    Ok(ProtoArrayConfig::Unspecified) | Err(_) => {
                        Err(FirestoreError::invalid_parameters(
                            "value_mode",
                            format!("array config {array_config} is unspecified or unknown"),
                        ))
                    }
                }
            }
            ValueMode::VectorConfig(vector) => {
                let dimension = u32::try_from(vector.dimension).map_err(|_| {
                    FirestoreError::invalid_parameters(
                        "dimension",
                        format!("listed vector dimension {} is negative", vector.dimension),
                    )
                })?;
                Ok(FirestoreIndexFieldMode::Vector { dimension })
            }
            ValueMode::SearchConfig(_) => Err(FirestoreError::invalid_parameters(
                "value_mode",
                "search config is not a comparable index mode",
            )),
        }
    }
}

impl TryFrom<FirestoreIndexField> for ProtoIndexField {
    type Error = FirestoreError;

    fn try_from(field: FirestoreIndexField) -> Result<Self, Self::Error> {
        Ok(ProtoIndexField {
            field_path: field.field_path,
            value_mode: Some(ValueMode::try_from(field.mode)?),
        })
    }
}

/// Converts a listed index field. A field that carries no value mode at all is rejected rather
/// than treated as a plain mismatch.
impl TryFrom<ProtoIndexField> for FirestoreIndexField {
    type Error = FirestoreError;

    fn try_from(field: ProtoIndexField) -> Result<Self, Self::Error> {
        let value_mode = field.value_mode.ok_or_else(|| {
            FirestoreError::invalid_parameters("value_mode", "field carries no value mode")
        })?;
        Ok(FirestoreIndexField::new(
            field.field_path,
            FirestoreIndexFieldMode::try_from(value_mode)?,
        ))
    }
}

/// Converts a declared composite index to the proto shape `CreateIndex` sends. The result never
/// carries a `__name__` field - the server appends it on creation with the implied direction.
impl TryFrom<FirestoreCompositeIndex> for ProtoIndex {
    type Error = FirestoreError;

    fn try_from(index: FirestoreCompositeIndex) -> Result<Self, Self::Error> {
        Ok(ProtoIndex {
            query_scope: ProtoQueryScope::from(index.query_scope).into(),
            api_scope: ApiScope::AnyApi.into(),
            fields: index
                .fields
                .into_iter()
                .map(ProtoIndexField::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }
}

/// Converts a listed composite index into the same shape a declaration takes, so the two can be
/// compared as domain values. A non-`ANY_API` scope, an unrepresentable query scope or a field
/// this crate cannot express fails the whole conversion, so the caller reports the index as
/// unrecognised instead of matching it field-by-field against a declaration it could never equal.
impl TryFrom<ProtoIndex> for FirestoreCompositeIndex {
    type Error = FirestoreError;

    fn try_from(index: ProtoIndex) -> Result<Self, Self::Error> {
        ensure_any_api_scope(index.api_scope)?;
        let query_scope = parse_query_scope(index.query_scope)?;
        let fields = index
            .fields
            .into_iter()
            .map(FirestoreIndexField::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FirestoreCompositeIndex {
            fields,
            query_scope,
        })
    }
}

/// Converts a listed index resource: its resource name and lifecycle state, plus the composite
/// index itself.
impl TryFrom<ProtoIndex> for FirestoreListedCompositeIndex {
    type Error = FirestoreError;

    fn try_from(index: ProtoIndex) -> Result<Self, Self::Error> {
        let name = index.name.clone();
        let state =
            FirestoreIndexState::try_from(ProtoState::try_from(index.state).map_err(|_| {
                FirestoreError::invalid_parameters(
                    "state",
                    format!("{} is not a known index state", index.state),
                )
            })?)?;
        let index = FirestoreCompositeIndex::try_from(index)?;
        Ok(FirestoreListedCompositeIndex { name, state, index })
    }
}

/// Converts a listed index state. `Unspecified` is rejected rather than read as `Ready`: an
/// unknown or unspecified server state must not be treated as healthy, and a rejected state
/// makes the whole index unrecognised rather than eligible for pruning.
impl TryFrom<ProtoState> for FirestoreIndexState {
    type Error = FirestoreError;

    fn try_from(state: ProtoState) -> Result<Self, Self::Error> {
        match state {
            ProtoState::Creating => Ok(FirestoreIndexState::Creating),
            ProtoState::Ready => Ok(FirestoreIndexState::Ready),
            ProtoState::NeedsRepair => Ok(FirestoreIndexState::NeedsRepair),
            ProtoState::Unspecified => Err(FirestoreError::invalid_parameters(
                "state",
                "index state is unspecified",
            )),
        }
    }
}

/// Converts a single-field index entry to the proto shape a [`field::IndexConfig`] carries.
impl TryFrom<FirestoreFieldOverrideIndex> for ProtoIndex {
    type Error = FirestoreError;

    fn try_from(entry: FirestoreFieldOverrideIndex) -> Result<Self, Self::Error> {
        Ok(ProtoIndex {
            query_scope: ProtoQueryScope::from(entry.query_scope).into(),
            api_scope: ApiScope::AnyApi.into(),
            // The field path is the owning `Field` resource's own path and may be omitted here,
            // per the admin API docs for single-field indexes.
            fields: vec![ProtoIndexField {
                field_path: String::new(),
                value_mode: Some(ValueMode::try_from(entry.mode)?),
            }],
            ..Default::default()
        })
    }
}

/// Converts one listed entry of a `field::IndexConfig` (a single-field index). Shares
/// `ensure_any_api_scope` and the query-scope/value-mode conversions with the composite-index
/// path, since a single-field index is the same `Index` message with exactly one field.
impl TryFrom<ProtoIndex> for FirestoreFieldOverrideIndex {
    type Error = FirestoreError;

    fn try_from(index: ProtoIndex) -> Result<Self, Self::Error> {
        ensure_any_api_scope(index.api_scope)?;
        let query_scope = parse_query_scope(index.query_scope)?;
        let value_mode = index
            .fields
            .into_iter()
            .next()
            .and_then(|field| field.value_mode)
            .ok_or_else(|| {
                FirestoreError::invalid_parameters(
                    "value_mode",
                    "single-field index carries no field entry",
                )
            })?;
        let mode = FirestoreIndexFieldMode::try_from(value_mode)?;
        Ok(FirestoreFieldOverrideIndex { query_scope, mode })
    }
}

/// Converts a declared field override to the `Field.index_config` shape `UpdateField` sends.
/// `uses_ancestor_config` and `reverting` are left at their `Default` (`false`): a declaration is
/// always an explicit, non-inherited override, never a request to revert one.
impl TryFrom<FirestoreFieldOverride> for field::IndexConfig {
    type Error = FirestoreError;

    fn try_from(field_override: FirestoreFieldOverride) -> Result<Self, Self::Error> {
        Ok(field::IndexConfig {
            indexes: field_override
                .indexes
                .into_iter()
                .map(ProtoIndex::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }
}

/// Converts a listed field's TTL configuration state. An unspecified state is rejected: a field
/// resource only carries a `ttl_config` once TTL has been requested for it, so `UNSPECIFIED`
/// there is itself unrecognised data, not "no TTL".
impl TryFrom<field::TtlConfig> for FirestoreFieldTtlState {
    type Error = FirestoreError;

    fn try_from(config: field::TtlConfig) -> Result<Self, Self::Error> {
        match field::ttl_config::State::try_from(config.state) {
            Ok(field::ttl_config::State::Creating) => Ok(FirestoreFieldTtlState::Creating),
            Ok(field::ttl_config::State::Active) => Ok(FirestoreFieldTtlState::Active),
            Ok(field::ttl_config::State::NeedsRepair) => Ok(FirestoreFieldTtlState::NeedsRepair),
            Ok(field::ttl_config::State::Unspecified) | Err(_) => {
                Err(FirestoreError::invalid_parameters(
                    "ttl_config.state",
                    format!("ttl state {} is unspecified or unknown", config.state),
                ))
            }
        }
    }
}

/// Converts one field resource's index-override half: `None` for both "no `index_config` at all"
/// and "`index_config` is inherited from an ancestor field" (`uses_ancestor_config`), since
/// neither carries an explicit override for `plan_index_changes` to compare against a
/// declaration. Returns the override's `reverting` flag alongside it.
fn convert_listed_field_indexes(
    config: Option<field::IndexConfig>,
) -> FirestoreResult<(Option<Vec<FirestoreFieldOverrideIndex>>, bool)> {
    let Some(config) = config else {
        return Ok((None, false));
    };
    if config.uses_ancestor_config {
        return Ok((None, false));
    }
    let indexes = config
        .indexes
        .into_iter()
        .map(FirestoreFieldOverrideIndex::try_from)
        .collect::<FirestoreResult<Vec<_>>>()?;
    Ok((Some(indexes), config.reverting))
}

/// Converts a listed field resource. The field path is the resource name's last segment
/// (`.../collectionGroups/{group}/fields/{path}`), the same shape `split_document_path` already
/// splits a document path on, so that helper is reused rather than a second last-segment parser.
///
/// The index-override half and the TTL half are converted independently rather than through one
/// `TryFrom`: a field resource can carry a valid override alongside an unrecognisable TTL state
/// (or the reverse), and failing the whole field on one bad half would hide the other, valid one
/// from the diff and re-plan a change to it on every sync. [`plan_index_changes`] calls this
/// directly and folds each half's failure into its own `unrecognised` entry; [`TryFrom<ProtoField>
/// for FirestoreListedField`] below still fails the whole field, for callers that want a single
/// fully-converted value.
fn convert_listed_field(
    field: &ProtoField,
) -> (FirestoreListedField, Vec<FirestoreUnrecognisedIndexItem>) {
    let field_path = split_document_path(&field.name).1.to_string();
    let mut unrecognised = Vec::new();

    let (indexes, reverting) = match convert_listed_field_indexes(field.index_config.clone()) {
        Ok(result) => result,
        Err(err) => {
            unrecognised.push(FirestoreUnrecognisedIndexItem {
                name: field.name.clone(),
                reason: describe_error(&err),
            });
            (None, false)
        }
    };

    let ttl = match field.ttl_config.map(FirestoreFieldTtlState::try_from) {
        None => None,
        Some(Ok(state)) => Some(state),
        Some(Err(err)) => {
            unrecognised.push(FirestoreUnrecognisedIndexItem {
                name: field.name.clone(),
                reason: describe_error(&err),
            });
            None
        }
    };

    (
        FirestoreListedField {
            name: field.name.clone(),
            field_path,
            indexes,
            reverting,
            ttl,
        },
        unrecognised,
    )
}

impl TryFrom<ProtoField> for FirestoreListedField {
    type Error = FirestoreError;

    fn try_from(field: ProtoField) -> Result<Self, Self::Error> {
        let (listed, unrecognised) = convert_listed_field(&field);
        match unrecognised.into_iter().next() {
            None => Ok(listed),
            Some(item) => Err(FirestoreError::invalid_parameters("field", item.reason)),
        }
    }
}

/// The direction Firestore assigns `__name__` when the index does not specify one: the direction
/// of the last declared field when it is directional (`Order`), or ascending otherwise. This is
/// the direction of the *last* field, not the last *directional* one - `[a DESC, tags CONTAINS]`
/// implies ascending, not descending, because `tags` (not `a`) is last.
fn implied_name_direction(fields: &[FirestoreIndexField]) -> FirestoreQueryDirection {
    match fields.last().map(|field| &field.mode) {
        Some(FirestoreIndexFieldMode::Order(direction)) => direction.clone(),
        _ => FirestoreQueryDirection::Ascending,
    }
}

impl FirestoreCompositeIndex {
    /// The field list with a trailing `__name__` field removed, when its direction equals the
    /// implied direction of the fields before it. The server always appends `__name__` to a
    /// listed index and a declaration never states it, so a declared index and its listed form
    /// compare equal only after this normalisation.
    pub(crate) fn comparable_fields(&self) -> &[FirestoreIndexField] {
        match self.fields.split_last() {
            Some((last, rest)) if last.field_path == IMPLIED_NAME_FIELD => {
                let implied = FirestoreIndexFieldMode::Order(implied_name_direction(rest));
                if last.mode == implied {
                    rest
                } else {
                    &self.fields
                }
            }
            _ => &self.fields,
        }
    }
}

/// Whether two composite indexes are the same declaration, after normalising away a trailing
/// implied `__name__` field on either side. Used both to match a declaration against a listed
/// index and, in [`validate_index_params`](crate::validate_index_params), to reject two
/// declarations that are really the same index.
pub(crate) fn composite_index_matches(
    declared: &FirestoreCompositeIndex,
    listed: &FirestoreCompositeIndex,
) -> bool {
    declared.query_scope == listed.query_scope
        && declared.comparable_fields() == listed.comparable_fields()
}

pub(crate) fn override_index_set(
    indexes: &[FirestoreFieldOverrideIndex],
) -> HashSet<(FirestoreIndexQueryScope, FirestoreIndexFieldMode)> {
    indexes
        .iter()
        .map(|entry| (entry.query_scope, entry.mode.clone()))
        .collect()
}

/// Whether `listed`'s index configuration already matches `declared`, as sets of
/// `(query_scope, mode)`.
///
/// A field with no listed index configuration at all never matches: even a declared `exempt()`
/// must be written explicitly, because the server's un-configured default is the automatic index
/// set, not exemption.
fn field_override_matches(
    declared: &FirestoreFieldOverride,
    listed: &FirestoreListedField,
) -> bool {
    let Some(listed_indexes) = &listed.indexes else {
        return false;
    };
    override_index_set(&declared.indexes) == override_index_set(listed_indexes)
}

/// Describes a [`FirestoreError`] the way an unrecognised listed item's `reason` should read: a
/// plain description of what was wrong with the server data, not a client-facing "invalid
/// parameters" message (the item was never a parameter the caller supplied).
fn describe_error(err: &FirestoreError) -> String {
    match err {
        FirestoreError::InvalidParametersError(details) => details.public.error.clone(),
        other => other.to_string(),
    }
}

/// Compares a declared [`FirestoreIndexParams`] against `existing`, the already-fetched listed
/// state of the one collection group it owns.
///
/// Re-validates `params` itself, the same structural check the fluent `.plan()`/`.sync()`
/// terminals already run before calling in, so every caller gets it - including a
/// `FirestoreIndexSupport` implementation that reaches this function some other way.
///
/// A listed index or field this crate's domain model cannot represent - search config, a
/// MongoDB-compat or Datastore-mode API scope, `COLLECTION_RECURSIVE`, an unspecified or unknown
/// order, array config, index state or TTL state - is reported in the returned plan's
/// `unrecognised` list. It is never matched against a declaration and never planned for deletion
/// or revert, even when pruning: this crate cannot know that removing it is what the declaration
/// intends.
pub fn plan_index_changes(
    params: &FirestoreIndexParams,
    existing: &FirestoreIndexExistingState,
) -> FirestoreResult<FirestoreIndexPlan> {
    crate::validate_index_params(params)?;

    let mut plan = FirestoreIndexPlan::default();

    let listed_indexes: Vec<FirestoreListedCompositeIndex> = existing
        .indexes
        .iter()
        .filter_map(
            |proto| match FirestoreListedCompositeIndex::try_from(proto.clone()) {
                Ok(listed) => Some(listed),
                Err(err) => {
                    plan.unrecognised.push(FirestoreUnrecognisedIndexItem {
                        name: proto.name.clone(),
                        reason: describe_error(&err),
                    });
                    None
                }
            },
        )
        .collect();

    let listed_fields: Vec<FirestoreListedField> = existing
        .fields
        .iter()
        .map(|proto| {
            let (listed, unrecognised) = convert_listed_field(proto);
            plan.unrecognised.extend(unrecognised);
            listed
        })
        .collect();

    let mut matched_listed_index = vec![false; listed_indexes.len()];
    for declared in &params.composite_indexes {
        let mut found = None;
        for (position, listed) in listed_indexes.iter().enumerate() {
            if matched_listed_index[position] {
                continue;
            }
            if composite_index_matches(declared, &listed.index) {
                found = Some(position);
                break;
            }
        }

        match found {
            None => plan.create_indexes.push(declared.clone()),
            Some(position) => {
                matched_listed_index[position] = true;
                match listed_indexes[position].state {
                    FirestoreIndexState::Creating => plan.pending.push(declared.clone()),
                    FirestoreIndexState::NeedsRepair => plan.needs_repair.push(declared.clone()),
                    FirestoreIndexState::Ready => plan.unchanged.push(declared.clone()),
                }
            }
        }
    }
    for (position, listed) in listed_indexes.into_iter().enumerate() {
        if !matched_listed_index[position] {
            plan.undeclared_indexes.push(listed);
        }
    }

    let declared_override_paths: HashSet<&str> = params
        .field_overrides
        .iter()
        .map(|f| f.field_path.as_str())
        .collect();
    for declared in &params.field_overrides {
        let listed = listed_fields
            .iter()
            .find(|f| f.indexes.is_some() && f.field_path == declared.field_path);
        let up_to_date = listed
            .map(|f| field_override_matches(declared, f))
            .unwrap_or(false);
        if !up_to_date {
            plan.update_fields.push(declared.clone());
        }
    }
    for listed in &listed_fields {
        // A field already `reverting` has an in-flight change back to the ancestor's config;
        // planning another revert for it would just repeat a change already under way.
        if listed.indexes.is_some()
            && !listed.reverting
            && !declared_override_paths.contains(listed.field_path.as_str())
        {
            plan.undeclared_fields.push(listed.clone());
        }
    }

    let declared_ttl_paths: HashSet<&str> =
        params.ttl_fields.iter().map(|path| path.as_str()).collect();
    for declared_path in &params.ttl_fields {
        let listed_ttl = listed_fields
            .iter()
            .find(|f| f.field_path == *declared_path)
            .and_then(|f| f.ttl);
        match listed_ttl {
            None => plan.enable_ttl.push(declared_path.clone()),
            Some(FirestoreFieldTtlState::Creating) => plan.pending_ttl.push(declared_path.clone()),
            Some(FirestoreFieldTtlState::NeedsRepair) => {
                plan.needs_repair_ttl.push(declared_path.clone())
            }
            Some(FirestoreFieldTtlState::Active) => {}
        }
    }
    for listed in &listed_fields {
        if listed.ttl.is_some() && !declared_ttl_paths.contains(listed.field_path.as_str()) {
            plan.undeclared_ttl.push(listed.clone());
        }
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FirestoreCollectionId;

    fn users_params() -> FirestoreIndexParams {
        FirestoreIndexParams::new(FirestoreCollectionId::from_static("users"))
    }

    fn asc_field(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
        )
    }

    fn desc_field(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
        )
    }

    fn listed_index(
        fields: Vec<ProtoIndexField>,
        scope: ProtoQueryScope,
        state: ProtoState,
    ) -> ProtoIndex {
        ProtoIndex {
            name: "projects/p/databases/(default)/collectionGroups/users/indexes/1".to_string(),
            query_scope: scope as i32,
            api_scope: ApiScope::AnyApi as i32,
            fields,
            state: state as i32,
            density: 0,
            multikey: false,
            shard_count: 0,
            unique: false,
            search_index_options: None,
        }
    }

    fn proto_field(path: &str, value_mode: ValueMode) -> ProtoIndexField {
        ProtoIndexField {
            field_path: path.to_string(),
            value_mode: Some(value_mode),
        }
    }

    fn order_field(path: &str, order: ProtoOrder) -> ProtoIndexField {
        proto_field(path, ValueMode::Order(order as i32))
    }

    fn active_ttl_config() -> field::TtlConfig {
        field::TtlConfig {
            state: field::ttl_config::State::Active as i32,
            expiration_offset: None,
        }
    }

    #[test]
    fn declared_composite_index_converts_to_the_create_index_proto_shape() {
        let declared =
            FirestoreCompositeIndex::new(vec![asc_field("country"), desc_field("created_at")])
                .all_descendants();
        let proto = ProtoIndex::try_from(declared).unwrap();
        assert_eq!(proto.query_scope, ProtoQueryScope::CollectionGroup as i32);
        assert_eq!(proto.api_scope, ApiScope::AnyApi as i32);
        assert_eq!(
            proto.fields,
            vec![
                order_field("country", ProtoOrder::Ascending),
                order_field("created_at", ProtoOrder::Descending),
            ]
        );
        assert!(proto.name.is_empty());
    }

    #[test]
    fn declared_override_converts_to_an_explicit_non_inherited_index_config() {
        let field_override = FirestoreFieldOverride {
            field_path: "tags".to_string(),
            indexes: vec![
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
                    .all_descendants(),
            ],
        };
        let config = field::IndexConfig::try_from(field_override).unwrap();
        assert!(!config.uses_ancestor_config);
        assert_eq!(config.indexes.len(), 1);
        assert_eq!(
            config.indexes[0].query_scope,
            ProtoQueryScope::CollectionGroup as i32
        );
        assert_eq!(
            config.indexes[0].fields[0].value_mode,
            Some(ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32))
        );
    }

    #[test]
    fn empty_declaration_against_no_listed_state_plans_nothing() {
        let params = users_params();
        let existing = FirestoreIndexExistingState::default();
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan, FirestoreIndexPlan::default());
    }

    #[test]
    fn missing_index_is_planned_for_creation() {
        let params =
            users_params().with_composite_indexes(vec![FirestoreCompositeIndex::new(vec![
                asc_field("country"),
                desc_field("created_at"),
            ])]);
        let plan = plan_index_changes(&params, &FirestoreIndexExistingState::default()).unwrap();
        assert_eq!(plan.create_indexes, params.composite_indexes);
        assert!(plan.unchanged.is_empty());
    }

    #[test]
    fn listed_index_with_implied_trailing_name_matches_declared_without_it() {
        let declared =
            FirestoreCompositeIndex::new(vec![asc_field("country"), desc_field("created_at")]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        // The server appends `__name__` with the direction of the last directional field
        // (descending here), which must still compare equal to the declaration.
        let listed = listed_index(
            vec![
                order_field("country", ProtoOrder::Ascending),
                order_field("created_at", ProtoOrder::Descending),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Descending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.unchanged, vec![declared]);
        assert!(plan.create_indexes.is_empty());
        assert!(plan.undeclared_indexes.is_empty());
    }

    #[test]
    fn implied_name_direction_is_ascending_when_the_last_field_is_not_directional() {
        // "a" is descending, but the *last* field ("tags", ArrayContains) is not directional,
        // so Firestore implies `__name__` ascending. Picking the last *directional* field's
        // direction instead - the bug this pins - would wrongly read this as descending.
        let fields = vec![
            desc_field("a"),
            FirestoreIndexField::new("tags".to_string(), FirestoreIndexFieldMode::ArrayContains),
        ];
        assert_eq!(
            implied_name_direction(&fields),
            FirestoreQueryDirection::Ascending
        );
    }

    #[test]
    fn implied_name_direction_is_ascending_with_no_directional_field_at_all() {
        let fields = vec![FirestoreIndexField::new(
            "tags".to_string(),
            FirestoreIndexFieldMode::ArrayContains,
        )];
        assert_eq!(
            implied_name_direction(&fields),
            FirestoreQueryDirection::Ascending
        );
    }

    #[test]
    fn declared_index_ending_non_directional_matches_listed_ascending_implied_name() {
        // Regression: an earlier version implied `__name__`'s direction from the last
        // *directional* field ("a", descending) instead of the actual last field ("tags", not
        // directional), so it expected a descending `__name__` here. That mismatch would plan a
        // duplicate `create_indexes` entry and leave the live listed index in
        // `undeclared_indexes` - exactly what `prune_undeclared()` deletes.
        let declared = FirestoreCompositeIndex::new(vec![
            desc_field("a"),
            FirestoreIndexField::new("tags".to_string(), FirestoreIndexFieldMode::ArrayContains),
        ]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Descending),
                proto_field(
                    "tags",
                    ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32),
                ),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.unchanged, vec![declared]);
        assert!(plan.create_indexes.is_empty());
        assert!(
            plan.undeclared_indexes.is_empty(),
            "the live listed index must not be left eligible for prune"
        );
    }

    #[test]
    fn declared_index_ending_in_vector_matches_listed_ascending_implied_name() {
        let declared = FirestoreCompositeIndex::new(vec![
            desc_field("a"),
            FirestoreIndexField::new(
                "v".to_string(),
                FirestoreIndexFieldMode::Vector { dimension: 8 },
            ),
        ]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Descending),
                proto_field(
                    "v",
                    ValueMode::VectorConfig(ProtoVectorConfig {
                        dimension: 8,
                        r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
                    }),
                ),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.unchanged, vec![declared]);
        assert!(
            plan.undeclared_indexes.is_empty(),
            "the live listed index must not be left eligible for prune"
        );
    }

    #[test]
    fn scope_mismatch_is_not_a_match() {
        let declared =
            FirestoreCompositeIndex::new(vec![asc_field("country"), desc_field("created_at")])
                .all_descendants();
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("country", ProtoOrder::Ascending),
                order_field("created_at", ProtoOrder::Descending),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Descending),
            ],
            // Listed as Collection-scoped, declared as AllDescendants: must not match.
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.create_indexes, vec![declared]);
        assert_eq!(plan.undeclared_indexes.len(), 1);
    }

    #[test]
    fn vector_dimension_mismatch_is_not_a_match() {
        let declared = FirestoreCompositeIndex::new(vec![
            asc_field("country"),
            FirestoreIndexField::new(
                "embedding".to_string(),
                FirestoreIndexFieldMode::Vector { dimension: 768 },
            ),
        ]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("country", ProtoOrder::Ascending),
                proto_field(
                    "embedding",
                    ValueMode::VectorConfig(ProtoVectorConfig {
                        dimension: 256,
                        r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
                    }),
                ),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.create_indexes, vec![declared]);
        assert_eq!(plan.undeclared_indexes.len(), 1);
    }

    #[test]
    fn creating_index_is_classified_pending() {
        let declared = FirestoreCompositeIndex::new(vec![asc_field("a"), asc_field("b")]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Creating,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.pending, vec![declared]);
        assert!(plan.unchanged.is_empty());
    }

    #[test]
    fn needs_repair_index_is_classified_needs_repair_and_never_deleted() {
        let declared = FirestoreCompositeIndex::new(vec![asc_field("a"), asc_field("b")]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::NeedsRepair,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.needs_repair, vec![declared]);
        assert!(plan.undeclared_indexes.is_empty());
    }

    fn field_resource(
        path: &str,
        index_config: Option<field::IndexConfig>,
        ttl_config: Option<field::TtlConfig>,
    ) -> ProtoField {
        ProtoField {
            name: format!("projects/p/databases/(default)/collectionGroups/users/fields/{path}"),
            index_config,
            ttl_config,
        }
    }

    fn single_field_index(scope: ProtoQueryScope, value_mode: ValueMode) -> ProtoIndex {
        ProtoIndex {
            name: String::new(),
            query_scope: scope as i32,
            api_scope: ApiScope::AnyApi as i32,
            fields: vec![ProtoIndexField {
                field_path: String::new(),
                value_mode: Some(value_mode),
            }],
            state: 0,
            density: 0,
            multikey: false,
            shard_count: 0,
            unique: false,
            search_index_options: None,
        }
    }

    #[test]
    fn declared_exempt_matches_listed_empty_override() {
        let declared = FirestoreFieldOverride {
            field_path: "bio".to_string(),
            indexes: vec![],
        };
        let params = users_params().with_field_overrides(vec![declared]);
        let listed_field = field_resource(
            "bio",
            Some(field::IndexConfig {
                indexes: vec![],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            None,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert!(plan.update_fields.is_empty());
        assert!(plan.undeclared_fields.is_empty());
    }

    #[test]
    fn declared_exempt_with_no_listed_override_needs_update() {
        let declared = FirestoreFieldOverride {
            field_path: "bio".to_string(),
            indexes: vec![],
        };
        let params = users_params().with_field_overrides(vec![declared.clone()]);
        let existing = FirestoreIndexExistingState::default();
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.update_fields, vec![declared]);
    }

    #[test]
    fn undeclared_override_is_reported() {
        let listed_field = field_resource(
            "legacy",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::Collection,
                    ValueMode::Order(ProtoOrder::Ascending as i32),
                )],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            None,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field.clone()],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(
            plan.undeclared_fields,
            vec![FirestoreListedField::try_from(listed_field).unwrap()]
        );
    }

    #[test]
    fn matching_override_is_unchanged() {
        let declared = FirestoreFieldOverride {
            field_path: "tags".to_string(),
            indexes: vec![
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
                    .all_descendants(),
            ],
        };
        let params = users_params().with_field_overrides(vec![declared]);
        let listed_field = field_resource(
            "tags",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::CollectionGroup,
                    ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32),
                )],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            None,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert!(plan.update_fields.is_empty());
    }

    #[test]
    fn ttl_field_with_no_listed_config_is_enabled() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let plan = plan_index_changes(&params, &FirestoreIndexExistingState::default()).unwrap();
        assert_eq!(plan.enable_ttl, vec!["expires_at".to_string()]);
    }

    #[test]
    fn ttl_field_already_configured_needs_no_change() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let listed_field = field_resource("expires_at", None, Some(active_ttl_config()));
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert!(plan.enable_ttl.is_empty());
    }

    #[test]
    fn undeclared_ttl_field_is_reported() {
        let listed_field = field_resource("legacy_expiry", None, Some(active_ttl_config()));
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field.clone()],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(
            plan.undeclared_ttl,
            vec![FirestoreListedField::try_from(listed_field).unwrap()]
        );
    }

    #[test]
    fn vector_dimension_above_i32_range_is_rejected() {
        let field = FirestoreIndexField::new(
            "embedding".to_string(),
            FirestoreIndexFieldMode::Vector {
                dimension: u32::MAX,
            },
        );
        let err = ProtoIndexField::try_from(field).unwrap_err();
        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn negative_listed_vector_dimension_is_rejected() {
        let mode = ValueMode::VectorConfig(ProtoVectorConfig {
            dimension: -1,
            r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
        });
        let err = FirestoreIndexFieldMode::try_from(mode).unwrap_err();
        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn mongodb_compat_listed_index_is_unrecognised_and_never_pruned() {
        let mut listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        listed.api_scope = ApiScope::MongodbCompatibleApi as i32;
        listed.name = "projects/p/databases/(default)/collectionGroups/users/indexes/9".to_string();
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed.clone()],
            fields: vec![],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(plan.unrecognised.len(), 1);
        assert_eq!(plan.unrecognised[0].name, listed.name);
        assert!(plan.undeclared_indexes.is_empty());
        // The reason must describe the listed data, not read as a client parameter error.
        assert_eq!(
            plan.unrecognised[0].reason,
            "API scope MONGODB_COMPATIBLE_API is not supported"
        );
        assert!(!plan.unrecognised[0].reason.contains("Invalid parameters"));
    }

    #[test]
    fn field_with_unspecified_ttl_state_is_unrecognised_and_never_pruned() {
        let listed_field = field_resource(
            "legacy_expiry",
            None,
            Some(field::TtlConfig {
                state: field::ttl_config::State::Unspecified as i32,
                expiration_offset: None,
            }),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field.clone()],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(plan.unrecognised.len(), 1);
        assert_eq!(plan.unrecognised[0].name, listed_field.name);
        assert!(plan.undeclared_ttl.is_empty());
    }

    #[test]
    fn lone_vector_composite_index_matches_listed_with_implied_name() {
        let declared = FirestoreCompositeIndex::new(vec![FirestoreIndexField::new(
            "embedding".to_string(),
            FirestoreIndexFieldMode::Vector { dimension: 8 },
        )]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                proto_field(
                    "embedding",
                    ValueMode::VectorConfig(ProtoVectorConfig {
                        dimension: 8,
                        r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
                    }),
                ),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.unchanged, vec![declared]);
    }

    #[test]
    fn field_with_inherited_index_config_is_not_an_override_to_revert() {
        let listed_field = field_resource(
            "created_at",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::Collection,
                    ValueMode::Order(ProtoOrder::Ascending as i32),
                )],
                uses_ancestor_config: true,
                ancestor_field:
                    "projects/p/databases/(default)/collectionGroups/__default__/fields/*"
                        .to_string(),
                reverting: false,
            }),
            Some(active_ttl_config()),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert!(
            plan.undeclared_fields.is_empty(),
            "an inherited index config is not an explicit override to prune"
        );
        assert_eq!(
            plan.undeclared_ttl.len(),
            1,
            "the TTL half converts independently of the inherited index config"
        );
    }

    #[test]
    fn reverting_override_is_not_planned_for_revert_again() {
        let listed_field = field_resource(
            "legacy",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::Collection,
                    ValueMode::Order(ProtoOrder::Ascending as i32),
                )],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: true,
            }),
            None,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert!(
            plan.undeclared_fields.is_empty(),
            "a field already reverting must not be planned for another revert"
        );
    }

    #[test]
    fn unspecified_index_state_is_unrecognised_and_never_treated_as_ready() {
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Unspecified,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed.clone()],
            fields: vec![],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(plan.unrecognised.len(), 1);
        assert_eq!(plan.unrecognised[0].name, listed.name);
        assert!(plan.undeclared_indexes.is_empty());
    }

    #[test]
    fn unknown_index_state_value_is_unrecognised() {
        let mut listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        listed.state = 99;
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(plan.unrecognised.len(), 1);
    }

    #[test]
    fn unspecified_array_config_is_rejected() {
        let mode = ValueMode::ArrayConfig(ProtoArrayConfig::Unspecified as i32);
        let err = FirestoreIndexFieldMode::try_from(mode).unwrap_err();
        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn unknown_array_config_value_is_rejected() {
        let mode = ValueMode::ArrayConfig(7);
        let err = FirestoreIndexFieldMode::try_from(mode).unwrap_err();
        assert!(matches!(err, FirestoreError::InvalidParametersError(_)));
    }

    #[test]
    fn valid_override_survives_an_unrecognised_ttl_half() {
        let declared = FirestoreFieldOverride {
            field_path: "tags".to_string(),
            indexes: vec![
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
                    .all_descendants(),
            ],
        };
        let params = users_params().with_field_overrides(vec![declared]);
        let listed_field = field_resource(
            "tags",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::CollectionGroup,
                    ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32),
                )],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            Some(field::TtlConfig {
                state: field::ttl_config::State::Unspecified as i32,
                expiration_offset: None,
            }),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert!(
            plan.update_fields.is_empty(),
            "the valid override half must still be matched"
        );
        assert_eq!(
            plan.unrecognised.len(),
            1,
            "only the bad TTL half is reported"
        );
    }

    #[test]
    fn valid_ttl_survives_an_unrecognised_override_half() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let listed_field = field_resource(
            "expires_at",
            Some(field::IndexConfig {
                indexes: vec![single_field_index(
                    ProtoQueryScope::Collection,
                    ValueMode::Order(ProtoOrder::Unspecified as i32),
                )],
                uses_ancestor_config: false,
                ancestor_field: String::new(),
                reverting: false,
            }),
            Some(active_ttl_config()),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert!(
            plan.enable_ttl.is_empty(),
            "the valid TTL half must still be matched"
        );
        assert_eq!(
            plan.unrecognised.len(),
            1,
            "only the bad override half is reported"
        );
    }

    #[test]
    fn declared_ttl_field_still_creating_is_classified_pending() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let listed_field = field_resource(
            "expires_at",
            None,
            Some(field::TtlConfig {
                state: field::ttl_config::State::Creating as i32,
                expiration_offset: None,
            }),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.pending_ttl, vec!["expires_at".to_string()]);
        assert!(plan.enable_ttl.is_empty());
    }

    #[test]
    fn declared_ttl_field_needing_repair_is_classified_needs_repair() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let listed_field = field_resource(
            "expires_at",
            None,
            Some(field::TtlConfig {
                state: field::ttl_config::State::NeedsRepair as i32,
                expiration_offset: None,
            }),
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing).unwrap();
        assert_eq!(plan.needs_repair_ttl, vec!["expires_at".to_string()]);
        assert!(plan.enable_ttl.is_empty());
    }

    #[test]
    fn undeclared_needs_repair_index_is_pruned_like_any_other_undeclared_index() {
        // Only a *declared* NEEDS_REPAIR match is left alone (see `needs_repair_index_is_
        // classified_needs_repair_and_never_deleted` above); an undeclared one is pruned like any
        // other undeclared index.
        let listed = listed_index(
            vec![
                order_field("a", ProtoOrder::Ascending),
                order_field("b", ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::NeedsRepair,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed.clone()],
            fields: vec![],
        };
        let plan = plan_index_changes(&users_params(), &existing).unwrap();
        assert_eq!(plan.undeclared_indexes.len(), 1);
        assert_eq!(plan.undeclared_indexes[0].name, listed.name);
    }

    #[test]
    fn validation_runs_inside_plan_index_changes_too() {
        // Round B's `FirestoreIndexSupport` implementations call `plan_index_changes` directly;
        // this pins that it validates `params` itself rather than trusting a caller to have done
        // so already.
        let params = users_params()
            .with_composite_indexes(vec![FirestoreCompositeIndex::new(vec![asc_field("a")])]);
        let err = plan_index_changes(&params, &FirestoreIndexExistingState::default()).unwrap_err();
        assert!(err.to_string().contains("at least two fields"));
    }
}
