//! Builder for declaring and syncing one collection group's composite indexes, single-field
//! overrides and TTL policy.
//!
//! The entry point is [`FirestoreIndexesInitialBuilder`](crate::index_builder::FirestoreIndexesInitialBuilder),
//! reached from [`FirestoreExprBuilder::indexes()`](crate::FirestoreExprBuilder::indexes).

use crate::{
    FirestoreCollectionId, FirestoreCompositeIndex, FirestoreFieldOverride,
    FirestoreFieldOverrideIndex, FirestoreFieldOverrideTarget, FirestoreIndexField,
    FirestoreIndexFieldMode, FirestoreIndexParams, FirestoreIndexPlan, FirestoreIndexSupport,
    FirestoreIndexSyncOptions, FirestoreIndexSyncReport, FirestoreOperationWaitOptions,
    FirestoreQueryDirection, FirestoreResult, FirestoreVectorIndexConfig,
};
use std::time::Duration;

/// The initial builder for declaring one collection group's indexes.
///
/// Created by [`FirestoreExprBuilder::indexes()`](crate::fluent_api::FirestoreExprBuilder::indexes).
#[derive(Clone, Debug)]
pub struct FirestoreIndexesInitialBuilder<'a, D>
where
    D: FirestoreIndexSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreIndexesInitialBuilder<'a, D>
where
    D: FirestoreIndexSupport + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Names the collection group this statement owns.
    ///
    /// A fluent chain owns exactly one collection group: `.prune_undeclared()` never reaches an
    /// index, override or TTL field on any other group. Several groups need several statements.
    /// The ID is validated at `.plan()` or `.sync()`, not here.
    #[inline]
    pub fn collection_group<S: AsRef<str>>(
        self,
        collection_group: S,
    ) -> FirestoreIndexesBuilder<'a, D> {
        FirestoreIndexesBuilder::new(self.db, collection_group.as_ref().to_string())
    }
}

/// A builder for one collection group's composite indexes, field overrides and TTL policy.
///
/// Reach this from [`FirestoreIndexesInitialBuilder::collection_group`], and end the chain with
/// [`plan`](Self::plan) or [`sync`](Self::sync).
#[derive(Clone, Debug)]
pub struct FirestoreIndexesBuilder<'a, D>
where
    D: FirestoreIndexSupport,
{
    db: &'a D,
    collection_group: String,
    composite_indexes: Vec<FirestoreCompositeIndex>,
    field_overrides: Vec<FirestoreFieldOverride>,
    ttl_fields: Vec<String>,
    prune: bool,
    wait: Option<FirestoreOperationWaitOptions>,
}

impl<'a, D> FirestoreIndexesBuilder<'a, D>
where
    D: FirestoreIndexSupport + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_group: String) -> Self {
        Self {
            db,
            collection_group,
            composite_indexes: Vec::new(),
            field_overrides: Vec::new(),
            ttl_fields: Vec::new(),
            prune: false,
            wait: None,
        }
    }

    /// Declares the collection group's composite indexes.
    ///
    /// The closure receives a [`FirestoreCompositeIndexBuilder`] and returns the list built with
    /// [`indexes`](FirestoreCompositeIndexBuilder::indexes). Each call replaces any composite
    /// indexes set by a previous call.
    #[inline]
    pub fn composite<FN>(self, composite: FN) -> Self
    where
        FN: FnOnce(FirestoreCompositeIndexBuilder) -> Vec<FirestoreCompositeIndex>,
    {
        Self {
            composite_indexes: composite(FirestoreCompositeIndexBuilder::new()),
            ..self
        }
    }

    /// Declares the collection group's single-field index overrides.
    ///
    /// The closure receives a [`FirestoreFieldOverrideBuilder`] and returns the list built with
    /// [`fields`](FirestoreFieldOverrideBuilder::fields). Each call replaces any overrides set by
    /// a previous call. See [`FirestoreFieldOverride`] for what an override replaces.
    #[inline]
    pub fn field_overrides<FN>(self, overrides: FN) -> Self
    where
        FN: FnOnce(FirestoreFieldOverrideBuilder) -> Vec<FirestoreFieldOverride>,
    {
        Self {
            field_overrides: overrides(FirestoreFieldOverrideBuilder::new()),
            ..self
        }
    }

    /// Declares the fields whose timestamp value enables document TTL. Each call replaces any
    /// TTL fields set by a previous call.
    #[inline]
    pub fn ttl<I>(self, fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            ttl_fields: fields
                .into_iter()
                .map(|field| field.as_ref().to_string())
                .collect(),
            ..self
        }
    }

    /// Also deletes or reverts anything listed for this group that this statement does not
    /// declare: undeclared composite indexes are deleted, undeclared field overrides are
    /// reverted to automatic indexing, and undeclared TTL fields are disabled. An undeclared
    /// index in state `NEEDS_REPAIR` is deleted like any other undeclared index.
    ///
    /// Without this, undeclared items are only reported. Either way, a *declared* index matched
    /// to a listed `NEEDS_REPAIR` index is only ever reported: `.sync()` never deletes or
    /// recreates it automatically.
    #[inline]
    pub fn prune_undeclared(self) -> Self {
        Self {
            prune: true,
            ..self
        }
    }

    /// Waits for created indexes and enabled TTL to reach a terminal state before `.sync()`
    /// returns, up to `timeout`, polling at the default interval. Without this, `.sync()` returns
    /// once changes are requested.
    #[inline]
    pub fn wait_until_ready(self, timeout: Duration) -> Self {
        self.wait_until_ready_with_options(FirestoreOperationWaitOptions::new(timeout))
    }

    /// Waits for created indexes and enabled TTL to reach a terminal state before `.sync()`
    /// returns, per `options`. Without this, `.sync()` returns once changes are requested.
    #[inline]
    pub fn wait_until_ready_with_options(self, options: FirestoreOperationWaitOptions) -> Self {
        Self {
            wait: Some(options),
            ..self
        }
    }

    /// Validates the declaration and assembles the params and options the trait methods take.
    ///
    /// Kept separate from `plan`/`sync` so the two terminals share one validation path.
    fn build_params(
        self,
    ) -> FirestoreResult<(&'a D, FirestoreIndexParams, FirestoreIndexSyncOptions)> {
        let db = self.db;
        let collection_group = FirestoreCollectionId::new(self.collection_group)?;
        let params = FirestoreIndexParams::new(collection_group)
            .with_composite_indexes(self.composite_indexes)
            .with_field_overrides(self.field_overrides)
            .with_ttl_fields(self.ttl_fields);
        crate::validate_index_params(&params)?;

        let options = FirestoreIndexSyncOptions::new().with_prune(self.prune);
        let options = match self.wait {
            Some(wait) => options.with_wait(wait),
            None => options,
        };

        Ok((db, params, options))
    }

    /// Reports what [`sync`](Self::sync) would change, without writing anything.
    pub async fn plan(self) -> FirestoreResult<FirestoreIndexPlan> {
        let (db, params, _options) = self.build_params()?;
        db.plan_indexes(params).await
    }

    /// Reconciles Firestore with this declaration.
    pub async fn sync(self) -> FirestoreResult<FirestoreIndexSyncReport> {
        let (db, params, options) = self.build_params()?;
        db.sync_indexes(params, options).await
    }
}

/// A stateless helper for building a collection group's composite indexes, passed into the
/// closure given to [`FirestoreIndexesBuilder::composite`].
pub struct FirestoreCompositeIndexBuilder {}

impl FirestoreCompositeIndexBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Collects composite index entries - typically built with [`index`](Self::index) - into the
    /// list [`composite`](FirestoreIndexesBuilder::composite) stores, dropping `None` entries.
    #[inline]
    pub fn indexes<I>(&self, entries: I) -> Vec<FirestoreCompositeIndex>
    where
        I: IntoIterator,
        I::Item: FirestoreCompositeIndexExpr,
    {
        entries
            .into_iter()
            .filter_map(FirestoreCompositeIndexExpr::build_composite_index)
            .collect()
    }

    /// Builds one composite index from an ordered list of fields - typically built with
    /// [`field`](Self::field) - defaulting to
    /// [`FirestoreIndexQueryScope::Collection`](crate::FirestoreIndexQueryScope::Collection);
    /// chain `.all_descendants()` on the result for a collection-group index.
    #[inline]
    pub fn index<I>(&self, fields: I) -> FirestoreCompositeIndex
    where
        I: IntoIterator,
        I::Item: FirestoreIndexFieldExpr,
    {
        FirestoreCompositeIndex::new(
            fields
                .into_iter()
                .filter_map(FirestoreIndexFieldExpr::build_index_field)
                .collect(),
        )
    }

    /// Targets `field_path` for a field within a composite index.
    #[inline]
    pub fn field<S: AsRef<str>>(&self, field_path: S) -> FirestoreIndexFieldExprBuilder {
        FirestoreIndexFieldExprBuilder::new(field_path.as_ref().to_string())
    }
}

/// A trait for types that can be converted into a [`FirestoreCompositeIndex`], implemented for
/// `Option<T>` so a declaration can be conditional, the same way a filter or order can.
pub trait FirestoreCompositeIndexExpr {
    /// Builds the [`FirestoreCompositeIndex`]. Returns `None` if the expression declares no
    /// index.
    fn build_composite_index(self) -> Option<FirestoreCompositeIndex>;
}

impl FirestoreCompositeIndexExpr for FirestoreCompositeIndex {
    #[inline]
    fn build_composite_index(self) -> Option<FirestoreCompositeIndex> {
        Some(self)
    }
}

impl<T> FirestoreCompositeIndexExpr for Option<T>
where
    T: FirestoreCompositeIndexExpr,
{
    #[inline]
    fn build_composite_index(self) -> Option<FirestoreCompositeIndex> {
        self.and_then(FirestoreCompositeIndexExpr::build_composite_index)
    }
}

/// A trait for types that can be converted into a [`FirestoreIndexField`], implemented for
/// `Option<T>` so a field can be conditional, the same way a filter or order can.
pub trait FirestoreIndexFieldExpr {
    /// Builds the [`FirestoreIndexField`]. Returns `None` if the expression declares no field.
    fn build_index_field(self) -> Option<FirestoreIndexField>;
}

impl FirestoreIndexFieldExpr for FirestoreIndexField {
    #[inline]
    fn build_index_field(self) -> Option<FirestoreIndexField> {
        Some(self)
    }
}

impl<T> FirestoreIndexFieldExpr for Option<T>
where
    T: FirestoreIndexFieldExpr,
{
    #[inline]
    fn build_index_field(self) -> Option<FirestoreIndexField> {
        self.and_then(FirestoreIndexFieldExpr::build_index_field)
    }
}

/// A field targeted for inclusion in a composite index.
///
/// Obtained from [`FirestoreCompositeIndexBuilder::field`].
pub struct FirestoreIndexFieldExprBuilder {
    field_path: String,
}

impl FirestoreIndexFieldExprBuilder {
    pub(crate) fn new(field_path: String) -> Self {
        Self { field_path }
    }

    /// Includes this field ordered ascending. Alias for [`ascending`](Self::ascending).
    #[inline]
    pub fn asc(self) -> Option<FirestoreIndexField> {
        self.ascending()
    }

    /// Includes this field ordered descending. Alias for [`descending`](Self::descending).
    #[inline]
    pub fn desc(self) -> Option<FirestoreIndexField> {
        self.descending()
    }

    /// Includes this field ordered ascending.
    #[inline]
    pub fn ascending(self) -> Option<FirestoreIndexField> {
        self.direction(FirestoreQueryDirection::Ascending)
    }

    /// Includes this field ordered descending.
    #[inline]
    pub fn descending(self) -> Option<FirestoreIndexField> {
        self.direction(FirestoreQueryDirection::Descending)
    }

    /// Includes this field ordered in a direction chosen at runtime.
    #[inline]
    pub fn direction(self, direction: FirestoreQueryDirection) -> Option<FirestoreIndexField> {
        Some(FirestoreIndexField::new(
            self.field_path,
            FirestoreIndexFieldMode::Order(direction),
        ))
    }

    /// Includes this field for array-containment queries.
    #[inline]
    pub fn array_contains(self) -> Option<FirestoreIndexField> {
        Some(FirestoreIndexField::new(
            self.field_path,
            FirestoreIndexFieldMode::ArrayContains,
        ))
    }

    /// Includes this field as a flat vector index of `dimension`. Only valid as the last field
    /// of a composite index; validated at `.plan()`/`.sync()`, not here.
    #[inline]
    pub fn vector(self, dimension: u32) -> Option<FirestoreIndexField> {
        Some(FirestoreIndexField::new(
            self.field_path,
            FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(dimension)),
        ))
    }
}

/// A stateless helper for building a collection group's single-field index overrides, passed
/// into the closure given to [`FirestoreIndexesBuilder::field_overrides`].
pub struct FirestoreFieldOverrideBuilder {}

impl FirestoreFieldOverrideBuilder {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Collects field-override entries - typically built with [`field`](Self::field) - into the
    /// list [`field_overrides`](FirestoreIndexesBuilder::field_overrides) stores, dropping `None`
    /// entries.
    #[inline]
    pub fn fields<I>(&self, entries: I) -> Vec<FirestoreFieldOverride>
    where
        I: IntoIterator,
        I::Item: FirestoreFieldOverrideExpr,
    {
        entries
            .into_iter()
            .filter_map(FirestoreFieldOverrideExpr::build_field_override)
            .collect()
    }

    /// Targets `field_path` for a single-field index override.
    ///
    /// `field_path` must not be the literal string `"*"` - that special path is only reachable
    /// through [`all_fields`](Self::all_fields), and is rejected here at `.plan()`/`.sync()` with
    /// a message pointing at it.
    #[inline]
    pub fn field<S: AsRef<str>>(&self, field_path: S) -> FirestoreFieldOverrideFieldBuilder {
        FirestoreFieldOverrideFieldBuilder::new(FirestoreFieldOverrideTarget::Field(
            field_path.as_ref().to_string(),
        ))
    }

    /// Targets every field in the owned collection group that has no more specific, named
    /// override - Firestore's special `*` field.
    ///
    /// A named [`field`](Self::field) declared alongside this one wins for that field, since it
    /// is more specific: this is how "everything except these fields" is expressed. Composite
    /// indexes are unaffected. There is no database-wide equivalent: a collection-group statement
    /// never reaches the `__default__` group's own `*` field.
    #[inline]
    pub fn all_fields(&self) -> FirestoreFieldOverrideFieldBuilder {
        FirestoreFieldOverrideFieldBuilder::new(FirestoreFieldOverrideTarget::AllFields)
    }

    /// An ascending single-field index, for use inside
    /// [`FirestoreFieldOverrideFieldBuilder::indexes`]. Alias for
    /// [`ascending`](Self::ascending).
    #[inline]
    pub fn asc(&self) -> FirestoreFieldOverrideIndex {
        self.ascending()
    }

    /// A descending single-field index, for use inside
    /// [`FirestoreFieldOverrideFieldBuilder::indexes`]. Alias for
    /// [`descending`](Self::descending).
    #[inline]
    pub fn desc(&self) -> FirestoreFieldOverrideIndex {
        self.descending()
    }

    /// An ascending single-field index, for use inside
    /// [`FirestoreFieldOverrideFieldBuilder::indexes`].
    #[inline]
    pub fn ascending(&self) -> FirestoreFieldOverrideIndex {
        FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::Order(
            FirestoreQueryDirection::Ascending,
        ))
    }

    /// A descending single-field index, for use inside
    /// [`FirestoreFieldOverrideFieldBuilder::indexes`].
    #[inline]
    pub fn descending(&self) -> FirestoreFieldOverrideIndex {
        FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::Order(
            FirestoreQueryDirection::Descending,
        ))
    }

    /// An array-containment single-field index, for use inside
    /// [`FirestoreFieldOverrideFieldBuilder::indexes`].
    #[inline]
    pub fn array_contains(&self) -> FirestoreFieldOverrideIndex {
        FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
    }
}

/// A trait for types that can be converted into a [`FirestoreFieldOverride`], implemented for
/// `Option<T>` so a declaration can be conditional, the same way a filter or order can.
pub trait FirestoreFieldOverrideExpr {
    /// Builds the [`FirestoreFieldOverride`]. Returns `None` if the expression declares no
    /// override.
    fn build_field_override(self) -> Option<FirestoreFieldOverride>;
}

impl FirestoreFieldOverrideExpr for FirestoreFieldOverride {
    #[inline]
    fn build_field_override(self) -> Option<FirestoreFieldOverride> {
        Some(self)
    }
}

impl<T> FirestoreFieldOverrideExpr for Option<T>
where
    T: FirestoreFieldOverrideExpr,
{
    #[inline]
    fn build_field_override(self) -> Option<FirestoreFieldOverride> {
        self.and_then(FirestoreFieldOverrideExpr::build_field_override)
    }
}

/// A field targeted for a single-field index override.
///
/// Obtained from [`FirestoreFieldOverrideBuilder::field`].
pub struct FirestoreFieldOverrideFieldBuilder {
    target: FirestoreFieldOverrideTarget,
}

impl FirestoreFieldOverrideFieldBuilder {
    pub(crate) fn new(target: FirestoreFieldOverrideTarget) -> Self {
        Self { target }
    }

    /// Excludes this field from automatic single-field indexing entirely. Equivalent to
    /// `.indexes([])`.
    #[inline]
    pub fn exempt(self) -> FirestoreFieldOverride {
        FirestoreFieldOverride {
            target: self.target,
            indexes: Vec::new(),
        }
    }

    /// Replaces this field's automatic indexes with exactly the ones listed - typically built
    /// with [`FirestoreFieldOverrideBuilder::ascending`],
    /// [`descending`](FirestoreFieldOverrideBuilder::descending) or
    /// [`array_contains`](FirestoreFieldOverrideBuilder::array_contains). Entries drop `None`,
    /// the same as a composite index's fields, so one can be conditional. See
    /// [`FirestoreFieldOverride`] for why this replaces rather than extends the default set.
    #[inline]
    pub fn indexes<I>(self, indexes: I) -> FirestoreFieldOverride
    where
        I: IntoIterator,
        I::Item: FirestoreFieldOverrideIndexExpr,
    {
        FirestoreFieldOverride {
            target: self.target,
            indexes: indexes
                .into_iter()
                .filter_map(FirestoreFieldOverrideIndexExpr::build_field_override_index)
                .collect(),
        }
    }
}

/// A trait for types that can be converted into a [`FirestoreFieldOverrideIndex`], implemented
/// for `Option<T>` so one entry can be conditional, the same way a composite index's field is.
pub trait FirestoreFieldOverrideIndexExpr {
    /// Builds the [`FirestoreFieldOverrideIndex`]. Returns `None` if the expression declares no
    /// index.
    fn build_field_override_index(self) -> Option<FirestoreFieldOverrideIndex>;
}

impl FirestoreFieldOverrideIndexExpr for FirestoreFieldOverrideIndex {
    #[inline]
    fn build_field_override_index(self) -> Option<FirestoreFieldOverrideIndex> {
        Some(self)
    }
}

impl<T> FirestoreFieldOverrideIndexExpr for Option<T>
where
    T: FirestoreFieldOverrideIndexExpr,
{
    #[inline]
    fn build_field_override_index(self) -> Option<FirestoreFieldOverrideIndex> {
        self.and_then(FirestoreFieldOverrideIndexExpr::build_field_override_index)
    }
}

#[cfg(test)]
mod tests {
    use crate::fluent_api::tests::mockdb::MockIndexDatabase;
    use crate::fluent_api::FirestoreExprBuilder;
    use crate::{
        path, FirestoreCollectionId, FirestoreCompositeIndex, FirestoreFieldOverride,
        FirestoreFieldOverrideIndex, FirestoreFieldOverrideTarget, FirestoreIndexField,
        FirestoreIndexFieldMode, FirestoreIndexSyncOptions, FirestoreOperationWaitOptions,
        FirestoreQueryDirection, FirestoreVectorIndexConfig,
    };
    use std::time::Duration;

    // Test structure used only for its field names via `path!()`; never constructed.
    struct User {
        pub country: String,
        pub created_at: String,
        pub tags: Vec<String>,
        pub age: u32,
        pub embedding: Vec<f64>,
        pub bio: String,
        pub expires_at: String,
    }

    fn asc(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending),
        )
    }

    fn desc(path: &str) -> FirestoreIndexField {
        FirestoreIndexField::new(
            path.to_string(),
            FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Descending),
        )
    }

    #[tokio::test]
    async fn full_chain_produces_the_expected_params_and_options() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .composite(|i| {
                i.indexes([
                    i.index([
                        i.field(path!(User::country)).asc(),
                        i.field(path!(User::created_at)).desc(),
                    ]),
                    i.index([
                        i.field(path!(User::tags)).array_contains(),
                        i.field(path!(User::age)).asc(),
                    ])
                    .all_descendants(),
                    i.index([
                        i.field(path!(User::country)).asc(),
                        i.field(path!(User::embedding)).vector(768),
                    ]),
                ])
            })
            .field_overrides(|f| {
                f.fields([
                    f.field(path!(User::bio)).exempt(),
                    f.field(path!(User::tags))
                        .indexes([f.ascending(), f.array_contains().all_descendants()]),
                ])
            })
            .ttl([path!(User::expires_at)])
            .prune_undeclared()
            .wait_until_ready(Duration::from_secs(600))
            .sync()
            .await
            .expect("capturing mock always succeeds");

        let (params, options) = mock.captured().expect("sync_indexes was called");

        assert_eq!(
            params.collection_group,
            FirestoreCollectionId::new("users").unwrap()
        );
        assert_eq!(
            params.composite_indexes,
            vec![
                FirestoreCompositeIndex::new(vec![asc("country"), desc("created_at")]),
                FirestoreCompositeIndex::new(vec![
                    FirestoreIndexField::new(
                        "tags".to_string(),
                        FirestoreIndexFieldMode::ArrayContains
                    ),
                    asc("age"),
                ])
                .all_descendants(),
                FirestoreCompositeIndex::new(vec![
                    asc("country"),
                    FirestoreIndexField::new(
                        "embedding".to_string(),
                        FirestoreIndexFieldMode::Vector(FirestoreVectorIndexConfig::new(768))
                    ),
                ]),
            ]
        );
        assert_eq!(
            params.field_overrides,
            vec![
                FirestoreFieldOverride {
                    target: FirestoreFieldOverrideTarget::Field("bio".to_string()),
                    indexes: vec![],
                },
                FirestoreFieldOverride {
                    target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
                    indexes: vec![
                        FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::Order(
                            FirestoreQueryDirection::Ascending
                        )),
                        FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains)
                            .all_descendants(),
                    ],
                },
            ]
        );
        assert_eq!(params.ttl_fields, vec!["expires_at".to_string()]);
        assert_eq!(
            options,
            FirestoreIndexSyncOptions::new()
                .with_prune(true)
                .with_wait(FirestoreOperationWaitOptions::new(Duration::from_secs(600)))
        );
    }

    #[tokio::test]
    async fn without_wait_until_ready_options_default_to_no_wait_and_no_prune() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .sync()
            .await
            .unwrap();

        let (_, options) = mock.captured().unwrap();
        assert_eq!(options, FirestoreIndexSyncOptions::new().with_prune(false));
    }

    #[tokio::test]
    async fn optional_composite_index_entries_drop_out() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .composite(|i| {
                i.indexes([
                    Some(i.index([
                        i.field(path!(User::country)).asc(),
                        i.field(path!(User::age)).asc(),
                    ])),
                    None,
                ])
            })
            .sync()
            .await
            .unwrap();

        let (params, _) = mock.captured().unwrap();
        assert_eq!(
            params.composite_indexes,
            vec![FirestoreCompositeIndex::new(vec![
                asc("country"),
                asc("age")
            ])]
        );
    }

    #[tokio::test]
    async fn optional_index_field_entries_drop_out() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .composite(|i| {
                i.indexes([i.index([
                    i.field(path!(User::country)).asc(),
                    None,
                    i.field(path!(User::age)).asc(),
                ])])
            })
            .sync()
            .await
            .unwrap();

        let (params, _) = mock.captured().unwrap();
        assert_eq!(
            params.composite_indexes,
            vec![FirestoreCompositeIndex::new(vec![
                asc("country"),
                asc("age")
            ])]
        );
    }

    #[tokio::test]
    async fn optional_field_override_entries_drop_out() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .field_overrides(|f| f.fields([Some(f.field(path!(User::bio)).exempt()), None]))
            .sync()
            .await
            .unwrap();

        let (params, _) = mock.captured().unwrap();
        assert_eq!(
            params.field_overrides,
            vec![FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::Field("bio".to_string()),
                indexes: vec![],
            }]
        );
    }

    #[tokio::test]
    async fn optional_field_override_index_entries_drop_out() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .field_overrides(|f| {
                f.fields([f.field(path!(User::tags)).indexes([
                    Some(f.ascending()),
                    None,
                    Some(f.array_contains()),
                ])])
            })
            .sync()
            .await
            .unwrap();

        let (params, _) = mock.captured().unwrap();
        assert_eq!(
            params.field_overrides,
            vec![FirestoreFieldOverride {
                target: FirestoreFieldOverrideTarget::Field("tags".to_string()),
                indexes: vec![
                    FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::Order(
                        FirestoreQueryDirection::Ascending
                    )),
                    FirestoreFieldOverrideIndex::new(FirestoreIndexFieldMode::ArrayContains),
                ],
            }]
        );
    }

    #[tokio::test]
    async fn all_fields_targets_the_wildcard_and_named_fields_win_over_it() {
        let mock = MockIndexDatabase::default();
        FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .field_overrides(|f| {
                f.fields([
                    f.all_fields().exempt(),
                    f.field(path!(User::country)).indexes([f.ascending()]),
                ])
            })
            .sync()
            .await
            .unwrap();

        let (params, _) = mock.captured().unwrap();
        assert_eq!(
            params.field_overrides,
            vec![
                FirestoreFieldOverride {
                    target: FirestoreFieldOverrideTarget::AllFields,
                    indexes: vec![],
                },
                FirestoreFieldOverride {
                    target: FirestoreFieldOverrideTarget::Field("country".to_string()),
                    indexes: vec![FirestoreFieldOverrideIndex::new(
                        FirestoreIndexFieldMode::Order(FirestoreQueryDirection::Ascending)
                    )],
                },
            ]
        );
    }

    #[tokio::test]
    async fn field_named_star_is_rejected_at_the_terminal() {
        let mock = MockIndexDatabase::default();
        let err = FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .field_overrides(|f| f.fields([f.field("*").exempt()]))
            .plan()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("all_fields()"));
        assert!(mock.captured().is_none());
    }

    #[tokio::test]
    async fn invalid_collection_id_is_rejected_at_the_terminal() {
        let mock = MockIndexDatabase::default();
        let err = FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("a/b")
            .plan()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("collection_id"));
        assert!(mock.captured().is_none());
    }

    #[tokio::test]
    async fn composite_index_with_one_field_is_rejected_at_the_terminal() {
        let mock = MockIndexDatabase::default();
        let err = FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .composite(|i| i.indexes([i.index([i.field(path!(User::country)).asc()])]))
            .plan()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at least two fields"));
        assert!(mock.captured().is_none());
    }

    #[tokio::test]
    async fn vector_field_not_last_is_rejected_at_the_terminal() {
        let mock = MockIndexDatabase::default();
        let err = FirestoreExprBuilder { db: &mock }
            .indexes()
            .collection_group("users")
            .composite(|i| {
                i.indexes([i.index([
                    i.field(path!(User::embedding)).vector(8),
                    i.field(path!(User::country)).asc(),
                ])])
            })
            .plan()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be last"));
    }
}
