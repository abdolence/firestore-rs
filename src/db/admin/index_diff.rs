//! Converts declared indexes to proto form, and diffs them against Firestore's listed state.
//!
//! Everything here is a pure function of its inputs, deliberately, so it stays unit-testable
//! without a server: [`plan_index_changes`] takes the state a `ListIndexes`/`ListFields` call
//! already fetched and never performs I/O of its own.

use crate::{
    FirestoreCompositeIndex, FirestoreFieldOverride, FirestoreFieldOverrideIndex,
    FirestoreIndexField, FirestoreIndexFieldMode, FirestoreIndexParams, FirestoreIndexPlan,
    FirestoreIndexQueryScope, FirestoreQueryDirection,
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

/// The reserved field path Firestore appends to every composite index at creation time.
const IMPLIED_NAME_FIELD: &str = "__name__";

/// The state Firestore already has for one collection group: its listed composite indexes and
/// the field resources that carry an explicit index or TTL configuration.
///
/// Built from package 3's `ListIndexes` call and its two `ListFields` calls
/// (`indexConfig.usesAncestorConfig:false` and `ttlConfig:*`); `fields` is the union of both.
/// [`plan_index_changes`] assumes every entry in `fields` already carries an explicit,
/// non-inherited configuration - that filtering happens server-side, not here.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FirestoreIndexExistingState {
    /// The collection group's currently listed composite indexes.
    pub indexes: Vec<ProtoIndex>,
    /// The collection group's field resources that carry an explicit index override, a TTL
    /// configuration, or both.
    pub fields: Vec<ProtoField>,
}

fn proto_query_scope(scope: FirestoreIndexQueryScope) -> ProtoQueryScope {
    match scope {
        FirestoreIndexQueryScope::Collection => ProtoQueryScope::Collection,
        FirestoreIndexQueryScope::AllDescendants => ProtoQueryScope::CollectionGroup,
    }
}

fn domain_query_scope(scope: i32) -> Option<FirestoreIndexQueryScope> {
    match ProtoQueryScope::try_from(scope).ok()? {
        ProtoQueryScope::Collection => Some(FirestoreIndexQueryScope::Collection),
        ProtoQueryScope::CollectionGroup => Some(FirestoreIndexQueryScope::AllDescendants),
        ProtoQueryScope::Unspecified | ProtoQueryScope::CollectionRecursive => None,
    }
}

fn value_mode_of(mode: &FirestoreIndexFieldMode) -> ValueMode {
    match mode {
        FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending) => {
            ValueMode::Order(ProtoOrder::Ascending as i32)
        }
        FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending) => {
            ValueMode::Order(ProtoOrder::Descending as i32)
        }
        FirestoreIndexFieldMode::ArrayContains => {
            ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32)
        }
        FirestoreIndexFieldMode::Vector { dimension } => {
            ValueMode::VectorConfig(ProtoVectorConfig {
                dimension: *dimension as i32,
                r#type: Some(vector_config::Type::Flat(vector_config::FlatIndex {})),
            })
        }
    }
}

fn declared_field_to_proto(field: &FirestoreIndexField) -> ProtoIndexField {
    ProtoIndexField {
        field_path: field.field_path.clone(),
        value_mode: Some(value_mode_of(&field.mode)),
    }
}

/// Converts a declared composite index to the proto shape `CreateIndex` sends. The result never
/// carries a `__name__` field - the server appends it on creation with the implied direction.
pub(crate) fn declared_index_to_proto(index: &FirestoreCompositeIndex) -> ProtoIndex {
    ProtoIndex {
        name: String::new(),
        query_scope: proto_query_scope(index.query_scope) as i32,
        api_scope: ApiScope::AnyApi as i32,
        fields: index.fields.iter().map(declared_field_to_proto).collect(),
        state: 0,
        density: 0,
        multikey: false,
        shard_count: 0,
        unique: false,
        search_index_options: None,
    }
}

/// Converts a declared field override to the `Field.index_config` shape `UpdateField` sends.
///
/// Not called from this crate yet: the `FirestoreDb` implementation of `FirestoreIndexSupport`
/// that builds `UpdateField` requests from it is added separately, once that trait has a real
/// caller.
#[allow(dead_code)]
pub(crate) fn declared_override_to_index_config(
    field_override: &FirestoreFieldOverride,
) -> field::IndexConfig {
    field::IndexConfig {
        indexes: field_override
            .indexes
            .iter()
            .map(single_field_override_to_proto)
            .collect(),
        uses_ancestor_config: false,
        ancestor_field: String::new(),
        reverting: false,
    }
}

#[allow(dead_code)]
fn single_field_override_to_proto(entry: &FirestoreFieldOverrideIndex) -> ProtoIndex {
    ProtoIndex {
        name: String::new(),
        query_scope: proto_query_scope(entry.query_scope) as i32,
        api_scope: ApiScope::AnyApi as i32,
        // The field path is the owning `Field` resource's own path and may be omitted here, per
        // the admin API docs for single-field indexes.
        fields: vec![ProtoIndexField {
            field_path: String::new(),
            value_mode: Some(value_mode_of(&entry.mode)),
        }],
        state: 0,
        density: 0,
        multikey: false,
        shard_count: 0,
        unique: false,
        search_index_options: None,
    }
}

/// The `Field.ttl_config` shape that enables TTL: an active configuration with no expiration
/// offset, so the field's own timestamp value is the expiration time.
///
/// Not called from this crate yet; see [`declared_override_to_index_config`] for why.
#[allow(dead_code)]
pub(crate) fn ttl_field_config() -> field::TtlConfig {
    field::TtlConfig::default()
}

fn field_order(field: &ProtoIndexField) -> Option<ProtoOrder> {
    match field.value_mode {
        Some(ValueMode::Order(order)) => ProtoOrder::try_from(order).ok(),
        _ => None,
    }
}

/// The direction Firestore assigns `__name__` when the index does not specify one: the last
/// directional field's direction, or ascending when there is none.
fn implied_name_direction(fields: &[ProtoIndexField]) -> ProtoOrder {
    fields
        .iter()
        .rev()
        .find_map(field_order)
        .unwrap_or(ProtoOrder::Ascending)
}

/// Strips a trailing `__name__` field from `fields` when its direction equals the implied
/// direction of the fields before it, so a declared index (which never states `__name__`)
/// compares equal to a listed index (which always carries it).
fn strip_implied_name_field(fields: &[ProtoIndexField]) -> &[ProtoIndexField] {
    match fields.split_last() {
        Some((last, rest)) if last.field_path == IMPLIED_NAME_FIELD => {
            if field_order(last) == Some(implied_name_direction(rest)) {
                rest
            } else {
                fields
            }
        }
        _ => fields,
    }
}

fn composite_index_matches(declared: &FirestoreCompositeIndex, listed: &ProtoIndex) -> bool {
    let declared_proto = declared_index_to_proto(declared);
    declared_proto.query_scope == listed.query_scope
        && declared_proto.api_scope == listed.api_scope
        && strip_implied_name_field(&declared_proto.fields)
            == strip_implied_name_field(&listed.fields)
}

fn field_path_of(field: &ProtoField) -> String {
    field
        .name
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum ComparableFieldMode {
    Ascending,
    Descending,
    ArrayContains,
    Vector(u32),
}

fn comparable_declared_mode(mode: &FirestoreIndexFieldMode) -> ComparableFieldMode {
    match mode {
        FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending) => {
            ComparableFieldMode::Ascending
        }
        FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending) => {
            ComparableFieldMode::Descending
        }
        FirestoreIndexFieldMode::ArrayContains => ComparableFieldMode::ArrayContains,
        FirestoreIndexFieldMode::Vector { dimension } => ComparableFieldMode::Vector(*dimension),
    }
}

fn comparable_listed_mode(mode: &ValueMode) -> Option<ComparableFieldMode> {
    match mode {
        ValueMode::Order(order) => match ProtoOrder::try_from(*order).ok()? {
            ProtoOrder::Ascending => Some(ComparableFieldMode::Ascending),
            ProtoOrder::Descending => Some(ComparableFieldMode::Descending),
            ProtoOrder::Unspecified => None,
        },
        ValueMode::ArrayConfig(_) => Some(ComparableFieldMode::ArrayContains),
        ValueMode::VectorConfig(vector) => {
            Some(ComparableFieldMode::Vector(vector.dimension as u32))
        }
        ValueMode::SearchConfig(_) => None,
    }
}

fn declared_override_set(
    field_override: &FirestoreFieldOverride,
) -> HashSet<(FirestoreIndexQueryScope, ComparableFieldMode)> {
    field_override
        .indexes
        .iter()
        .map(|entry| (entry.query_scope, comparable_declared_mode(&entry.mode)))
        .collect()
}

fn listed_override_set(
    config: &field::IndexConfig,
) -> HashSet<(FirestoreIndexQueryScope, ComparableFieldMode)> {
    config
        .indexes
        .iter()
        .filter_map(|index| {
            let scope = domain_query_scope(index.query_scope)?;
            let mode = index
                .fields
                .first()
                .and_then(|f| f.value_mode.as_ref())
                .and_then(comparable_listed_mode)?;
            Some((scope, mode))
        })
        .collect()
}

/// Whether `listed`'s index configuration already matches `declared`, as sets of
/// `(query_scope, mode)`.
///
/// A field with no listed `index_config` at all never matches: even a declared `exempt()` must
/// be written explicitly, because the server's un-configured default is the automatic index set,
/// not exemption.
fn field_override_matches(declared: &FirestoreFieldOverride, listed: &ProtoField) -> bool {
    let Some(config) = &listed.index_config else {
        return false;
    };
    declared_override_set(declared) == listed_override_set(config)
}

/// Compares a declared [`FirestoreIndexParams`] against `existing`, the already-fetched listed
/// state of the one collection group it owns.
pub fn plan_index_changes(
    params: &FirestoreIndexParams,
    existing: &FirestoreIndexExistingState,
) -> FirestoreIndexPlan {
    let mut plan = FirestoreIndexPlan::default();

    let mut matched_listed_index = vec![false; existing.indexes.len()];
    for declared in &params.composite_indexes {
        let found = existing
            .indexes
            .iter()
            .enumerate()
            .find(|(position, listed)| {
                !matched_listed_index[*position] && composite_index_matches(declared, listed)
            })
            .map(|(position, _)| position);

        match found {
            None => plan.create_indexes.push(declared.clone()),
            Some(position) => {
                matched_listed_index[position] = true;
                let state = ProtoState::try_from(existing.indexes[position].state)
                    .unwrap_or(ProtoState::Unspecified);
                match state {
                    ProtoState::Creating => plan.pending.push(declared.clone()),
                    ProtoState::NeedsRepair => plan.needs_repair.push(declared.clone()),
                    ProtoState::Ready | ProtoState::Unspecified => {
                        plan.unchanged.push(declared.clone())
                    }
                }
            }
        }
    }
    for (position, listed) in existing.indexes.iter().enumerate() {
        if !matched_listed_index[position] {
            plan.undeclared_indexes.push(listed.clone());
        }
    }

    let declared_override_paths: HashSet<&str> = params
        .field_overrides
        .iter()
        .map(|f| f.field_path.as_str())
        .collect();
    for declared in &params.field_overrides {
        let listed = existing
            .fields
            .iter()
            .find(|f| f.index_config.is_some() && field_path_of(f) == declared.field_path);
        let up_to_date = listed
            .map(|f| field_override_matches(declared, f))
            .unwrap_or(false);
        if !up_to_date {
            plan.update_fields.push(declared.clone());
        }
    }
    for listed in &existing.fields {
        if listed.index_config.is_some()
            && !declared_override_paths.contains(field_path_of(listed).as_str())
        {
            plan.undeclared_fields.push(listed.clone());
        }
    }

    let declared_ttl_paths: HashSet<&str> =
        params.ttl_fields.iter().map(|path| path.as_str()).collect();
    for declared_path in &params.ttl_fields {
        let has_ttl = existing
            .fields
            .iter()
            .any(|f| f.ttl_config.is_some() && field_path_of(f) == *declared_path);
        if !has_ttl {
            plan.enable_ttl.push(declared_path.clone());
        }
    }
    for listed in &existing.fields {
        if listed.ttl_config.is_some()
            && !declared_ttl_paths.contains(field_path_of(listed).as_str())
        {
            plan.undeclared_ttl.push(listed.clone());
        }
    }

    plan
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

    #[test]
    fn declared_override_converts_to_an_explicit_non_inherited_index_config() {
        let field_override = FirestoreFieldOverride {
            field_path: "tags".to_string(),
            indexes: vec![
                FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
                    .all_descendants(),
            ],
        };
        let config = declared_override_to_index_config(&field_override);
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
        let plan = plan_index_changes(&params, &existing);
        assert_eq!(plan, FirestoreIndexPlan::default());
    }

    #[test]
    fn missing_index_is_planned_for_creation() {
        let params =
            users_params().with_composite_indexes(vec![FirestoreCompositeIndex::new(vec![
                asc_field("country"),
                desc_field("created_at"),
            ])]);
        let plan = plan_index_changes(&params, &FirestoreIndexExistingState::default());
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
        let plan = plan_index_changes(&params, &existing);
        assert_eq!(plan.unchanged, vec![declared]);
        assert!(plan.create_indexes.is_empty());
        assert!(plan.undeclared_indexes.is_empty());
    }

    #[test]
    fn implied_name_direction_defaults_to_ascending_with_no_directional_field() {
        // A single-field-mode-only index (e.g. all array-contains) implies an ascending
        // `__name__`, the other direction this normalisation must handle.
        let declared = FirestoreCompositeIndex::new(vec![
            FirestoreIndexField::new("tags".to_string(), FirestoreIndexFieldMode::ArrayContains),
            asc_field("age"),
        ]);
        let params = users_params().with_composite_indexes(vec![declared.clone()]);
        let listed = listed_index(
            vec![
                proto_field(
                    "tags",
                    ValueMode::ArrayConfig(ProtoArrayConfig::Contains as i32),
                ),
                order_field("age", ProtoOrder::Ascending),
                order_field(IMPLIED_NAME_FIELD, ProtoOrder::Ascending),
            ],
            ProtoQueryScope::Collection,
            ProtoState::Ready,
        );
        let existing = FirestoreIndexExistingState {
            indexes: vec![listed],
            fields: vec![],
        };
        let plan = plan_index_changes(&params, &existing);
        assert_eq!(plan.unchanged, vec![declared]);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&params, &existing);
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
        let plan = plan_index_changes(&users_params(), &existing);
        assert_eq!(plan.undeclared_fields, vec![listed_field]);
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
        let plan = plan_index_changes(&params, &existing);
        assert!(plan.update_fields.is_empty());
    }

    #[test]
    fn ttl_field_with_no_listed_config_is_enabled() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let plan = plan_index_changes(&params, &FirestoreIndexExistingState::default());
        assert_eq!(plan.enable_ttl, vec!["expires_at".to_string()]);
    }

    #[test]
    fn ttl_field_already_configured_needs_no_change() {
        let params = users_params().with_ttl_fields(vec!["expires_at".to_string()]);
        let listed_field = field_resource("expires_at", None, Some(ttl_field_config()));
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field],
        };
        let plan = plan_index_changes(&params, &existing);
        assert!(plan.enable_ttl.is_empty());
    }

    #[test]
    fn undeclared_ttl_field_is_reported() {
        let listed_field = field_resource("legacy_expiry", None, Some(ttl_field_config()));
        let existing = FirestoreIndexExistingState {
            indexes: vec![],
            fields: vec![listed_field.clone()],
        };
        let plan = plan_index_changes(&users_params(), &existing);
        assert_eq!(plan.undeclared_ttl, vec![listed_field]);
    }
}
