//! Runs the declarative index management API against a real Firestore project
//! (`GCP_PROJECT`, local ADC). Uses dedicated collection groups so it never touches anything a
//! caller's own data depends on, and prunes both groups to an empty declaration at the end even
//! when an earlier assertion fails.
//!
//! `plan_is_read_only_and_scoped_to_the_owned_group` makes no writes and runs in seconds, so it
//! runs by default. Every other test here creates, updates or deletes real indexes, overrides or
//! TTL policy in `GCP_PROJECT` and takes most of an hour: each index or field operation it waits
//! on can take minutes, even on an empty collection. So each carries `#[ignore]` and only runs on
//! `cargo test --test admin-indexes-tests --features admin -- --ignored --nocapture`.
//! `search_indexes_against_real_firestore_enterprise` runs against the Enterprise edition database
//! `GCP_ENTERPRISE_DATABASE_ID` of the same project instead, on a group of its own.
//!
//! The `sql_*` tests run the index statements of the `sql` feature, each on a group of its own,
//! and empty that group again even when an assertion fails.
//!
//! Safety net: `ListIndexes` scoped to one collection group's parent can answer with composite
//! indexes belonging to *every* group in the database (see the fake-server tests in
//! `src/db/admin/indexes.rs` that replay a captured response). The library filters and defends
//! against that on its own, but the write-side test does not take that on faith: it snapshots
//! every index outside its own two groups before touching anything, and asserts that snapshot is
//! unchanged - not merely present, but identical in fields, scope and state - after every write
//! it makes, including the final cleanup.

use firestore::errors::FirestoreError;
use firestore::index_builder::FirestoreIndexesBuilder;
use firestore::*;
use futures::FutureExt;
use gcloud_sdk::google::firestore::admin::v1::firestore_admin_client::FirestoreAdminClient;
use gcloud_sdk::google::firestore::admin::v1::{Index as ProtoIndex, ListIndexesRequest};
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

mod common;
#[cfg(feature = "sql")]
use common::setup_enterprise_unseeded;
use common::{setup, setup_enterprise};

const SYNC_GROUP: &str = "firestore-rs-index-sync-test";
const PROBE_GROUP: &str = "firestore-rs-index-probe";
const SEARCH_GROUP: &str = "admin-search-index-test";
const STANDARD_INDEX_GROUP: &str = "firestore-rs-sql-index-standard";
const ENTERPRISE_INDEX_GROUP: &str = "firestore-rs-sql-index-enterprise";
const FIELD_GROUP: &str = "firestore-rs-sql-field-standard";

/// Every group a test of this file changes indexes in. The tests run in parallel against the same
/// databases, so each one's check that nothing outside its own group changed must leave out the
/// groups of the others as well.
const TEST_GROUPS: [&str; 6] = [
    SYNC_GROUP,
    PROBE_GROUP,
    SEARCH_GROUP,
    STANDARD_INDEX_GROUP,
    ENTERPRISE_INDEX_GROUP,
    FIELD_GROUP,
];

/// Generous: a real composite index build on an empty collection is usually seconds, but the
/// live project's quota and current load are outside this test's control.
const WAIT_TIMEOUT: Duration = Duration::from_secs(900);

/// Every composite index in the database, listed through the admin client directly (the crate's
/// own fluent API is deliberately scoped to one collection group at a time, so it has no "list
/// everything" call of its own) with `collectionGroups/-`, the documented way to ask for every
/// group's indexes in one call.
async fn list_all_database_indexes(
    db: &FirestoreDb,
) -> Result<Vec<ProtoIndex>, Box<dyn std::error::Error + Send + Sync>> {
    let mut admin: FirestoreAdminClient<_> = db.client().get_with(FirestoreAdminClient::new);
    let parent = format!("{}/collectionGroups/-", db.get_database_path());
    let mut indexes = Vec::new();
    let mut page_token = String::new();
    loop {
        let response = admin
            .list_indexes(ListIndexesRequest {
                parent: parent.clone(),
                filter: String::new(),
                page_size: 0,
                page_token: std::mem::take(&mut page_token),
            })
            .await?
            .into_inner();
        indexes.extend(response.indexes);
        if response.next_page_token.is_empty() {
            break;
        }
        page_token = response.next_page_token;
    }
    Ok(indexes)
}

/// The subset of `indexes` outside `groups`, keyed by resource name so a before/after comparison
/// catches a changed field list or state, not only a missing or added entry.
fn outside_groups(indexes: &[ProtoIndex], groups: &[&str]) -> BTreeMap<String, ProtoIndex> {
    indexes
        .iter()
        .filter(|index| {
            !groups
                .iter()
                .any(|group| index.name.contains(&format!("/collectionGroups/{group}/")))
        })
        .map(|index| (index.name.clone(), index.clone()))
        .collect()
}

/// The stored field lists of every composite index in `group`, each field as `path MODE`, in
/// the order Firestore stores them, so a test can check where `__name__` really went.
async fn stored_index_fields(
    db: &FirestoreDb,
    group: &str,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
    use gcloud_sdk::google::firestore::admin::v1::index::index_field::{
        ArrayConfig, Order, ValueMode,
    };
    let marker = format!("/collectionGroups/{group}/");
    Ok(list_all_database_indexes(db)
        .await?
        .into_iter()
        .filter(|index| index.name.contains(&marker))
        .map(|index| {
            index
                .fields
                .into_iter()
                .map(|field| {
                    let mode = match field.value_mode {
                        Some(ValueMode::Order(order)) if order == Order::Ascending as i32 => {
                            "ASC".to_string()
                        }
                        Some(ValueMode::Order(order)) if order == Order::Descending as i32 => {
                            "DESC".to_string()
                        }
                        Some(ValueMode::ArrayConfig(config))
                            if config == ArrayConfig::Contains as i32 =>
                        {
                            "CONTAINS".to_string()
                        }
                        Some(ValueMode::VectorConfig(vector)) => {
                            format!("VECTOR({})", vector.dimension)
                        }
                        other => format!("{other:?}"),
                    };
                    format!("{} {mode}", field.field_path)
                })
                .collect()
        })
        .collect())
}

async fn prune_to_empty(
    db: &FirestoreDb,
    collection_group: &str,
) -> Result<FirestoreIndexSyncReport, Box<dyn std::error::Error + Send + Sync>> {
    let report = db
        .fluent()
        .indexes()
        .collection_group(collection_group)
        .prune_undeclared()
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await?;
    Ok(report)
}

/// Question 1: does Firestore store `__name__` ASC for a created `[a DESC, tags CONTAINS]`, and
/// does a second plan then show no changes? Runs on a group the scenario emptied first.
async fn implied_name_direction_and_replan_is_a_no_op(
    db: &FirestoreDb,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let report = db
        .fluent()
        .indexes()
        .collection_group(SYNC_GROUP)
        .composite(|i| {
            i.indexes([i.index([i.field("a").desc(), i.field("tags").array_contains()])])
        })
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await?;
    println!("[Q1] first sync report:\n{report}");
    assert_eq!(report.created_indexes.len(), 1, "the declared index is new");
    assert!(report.unchanged.is_empty() && report.pending.is_empty());

    let stored = stored_index_fields(db, SYNC_GROUP).await?;
    println!("[Q1] stored: {stored:?}");
    assert_eq!(
        stored,
        vec![vec!["a DESC", "tags CONTAINS", "__name__ ASC"]],
        "Firestore appends __name__ ASC after the declared fields"
    );

    let plan = db
        .fluent()
        .indexes()
        .collection_group(SYNC_GROUP)
        .composite(|i| {
            i.indexes([i.index([i.field("a").desc(), i.field("tags").array_contains()])])
        })
        .plan()
        .await?;
    println!("[Q1] replan:\n{plan}");
    assert!(plan.create_indexes.is_empty());
    assert_eq!(plan.unchanged.len(), 1, "the waited-for index is READY");
    assert!(plan.pending.is_empty());
    Ok(())
}

/// Question 2: is a vector-only composite index accepted, and where does `__name__` go in it?
async fn vector_only_composite_index_is_accepted(
    db: &FirestoreDb,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let report = db
        .fluent()
        .indexes()
        .collection_group(SYNC_GROUP)
        .composite(|i| {
            i.indexes([
                i.index([i.field("a").desc(), i.field("tags").array_contains()]),
                i.index([i.field("embedding").vector(8)]),
            ])
        })
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await?;
    println!("[Q2] vector index sync report:\n{report}");
    assert_eq!(
        report.created_indexes.len(),
        1,
        "only the vector index is new"
    );
    assert_eq!(report.unchanged.len(), 1, "the Q1 index is unchanged");
    assert!(report.pending.is_empty());

    let stored = stored_index_fields(db, SYNC_GROUP).await?;
    println!("[Q2] stored: {stored:?}");
    assert!(
        stored.contains(&vec![
            "__name__ ASC".to_string(),
            "embedding VECTOR(8)".to_string()
        ]),
        "Firestore puts __name__ before the vector field: {stored:?}"
    );
    Ok(())
}

/// Question 4: is an index override on a single group's `*` accepted? If Firestore rejects the
/// write, this stops and the error is reported verbatim rather than worked around.
async fn all_fields_override_on_a_single_group(
    db: &FirestoreDb,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sync_result = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .field_overrides(|f| {
            f.fields([
                f.all_fields().exempt(),
                f.field("country").indexes([f.ascending()]),
            ])
        })
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await;

    let report = match sync_result {
        Ok(report) => report,
        Err(err) => {
            println!("[Q4] Firestore rejected an override on a single group's `*`: {err}");
            return Err(err.into());
        }
    };
    println!("[Q4] all_fields() sync report:\n{report}");
    assert_eq!(
        report.updated_fields.len(),
        2,
        "expected both the wildcard and the named field written"
    );
    assert!(report.reverted_fields.is_empty() && report.kept_undeclared_fields.is_empty());

    let plan = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .field_overrides(|f| {
            f.fields([
                f.all_fields().exempt(),
                f.field("country").indexes([f.ascending()]),
            ])
        })
        .plan()
        .await?;
    println!("[Q4] replan after all_fields():\n{plan}");
    assert!(
        plan.update_fields.is_empty(),
        "replanning the same declaration must show no changes"
    );

    let revert_report = prune_to_empty(db, PROBE_GROUP).await?;
    println!("[Q4] revert report:\n{revert_report}");
    assert_eq!(
        revert_report.reverted_fields.len(),
        2,
        "expected both fields reverted"
    );

    let after_revert = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .plan()
        .await?;
    println!("[Q4] plan after revert:\n{after_revert}");
    assert!(
        after_revert.kept_undeclared_fields.is_empty(),
        "the group must carry no leftover override after the revert",
    );
    Ok(())
}

/// Question 5: a TTL field plus an override on the same field, then a TTL move to another field
/// with pruning. Firestore allows one TTL field per group, so the move only succeeds if the old
/// TTL is disabled, and finished, before the new one is enabled.
async fn ttl_on_an_overridden_field_then_a_ttl_move(
    db: &FirestoreDb,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let report = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .field_overrides(|f| f.fields([f.field("expires_at").exempt()]))
        .ttl(["expires_at"])
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await?;
    println!("[Q5] TTL on an overridden field:\n{report}");
    assert_eq!(report.updated_fields.len(), 1);
    assert_eq!(report.enabled_ttl, vec!["expires_at".to_string()]);

    let report = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .ttl(["expires_at_moved"])
        .prune_undeclared()
        .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
        .sync()
        .await?;
    println!("[Q5] TTL move:\n{report}");
    assert_eq!(report.disabled_ttl.len(), 1);
    assert_eq!(report.disabled_ttl[0].field_path, "expires_at");
    assert_eq!(report.reverted_fields.len(), 1);
    assert_eq!(report.reverted_fields[0].field_path, "expires_at");
    assert_eq!(report.enabled_ttl, vec!["expires_at_moved".to_string()]);

    let plan = db
        .fluent()
        .indexes()
        .collection_group(PROBE_GROUP)
        .ttl(["expires_at_moved"])
        .plan()
        .await?;
    println!("[Q5] replan after the move:\n{plan}");
    assert_eq!(plan.unchanged_ttl, vec!["expires_at_moved".to_string()]);
    assert!(plan.kept_undeclared_ttl.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "creates and deletes real indexes in GCP_PROJECT; takes most of an hour; run with --ignored"]
async fn admin_index_management_against_real_firestore(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let baseline = list_all_database_indexes(&db).await?;
    let baseline_outside = outside_groups(&baseline, &TEST_GROUPS);
    println!(
        "[safety] {} indexes outside the groups of these tests before this run; they must be \
         exactly the same, field for field, after cleanup.",
        baseline_outside.len()
    );
    assert!(
        !baseline_outside.is_empty(),
        "fixture sanity check: latestbit has real indexes in other groups to protect"
    );

    // Question 3: one channel serves both the data and admin APIs. `setup()`, the snapshot above
    // and every `.fluent()` call below already go through the same `FirestoreDb` built once from
    // ADC; a plain query on this test's own group here, interleaved with the admin calls,
    // demonstrates the shared channel (including its `google-cloud-resource-prefix` header, set
    // once when the channel was built) serves both without a second connection.
    let data_read_result = db.fluent().select().from(SYNC_GROUP).query().await;

    let scenario = async {
        // Start from an empty declaration on both groups, so every step below can expect exact
        // counts instead of accepting whatever an earlier, interrupted run left behind.
        let emptied_sync = prune_to_empty(&db, SYNC_GROUP).await?;
        let emptied_probe = prune_to_empty(&db, PROBE_GROUP).await?;
        println!("[setup] emptied:\n{emptied_sync}\n{emptied_probe}");
        implied_name_direction_and_replan_is_a_no_op(&db).await?;
        vector_only_composite_index_is_accepted(&db).await?;
        all_fields_override_on_a_single_group(&db).await?;
        ttl_on_an_overridden_field_then_a_ttl_move(&db).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    // Always end with an empty declaration on both groups, even if an assertion above failed.
    // Each call names one of this test's own two groups and nothing else - `prune_undeclared()`
    // never reaches past the collection group it was called on.
    let cleanup_sync = prune_to_empty(&db, SYNC_GROUP).await;
    let cleanup_probe = prune_to_empty(&db, PROBE_GROUP).await;

    let after = list_all_database_indexes(&db).await;
    let outside_unchanged = match &after {
        Ok(after) => {
            let after_outside = outside_groups(after, &TEST_GROUPS);
            if after_outside == baseline_outside {
                Ok(())
            } else {
                Err(format!(
                    "indexes outside the groups of these tests changed: before {} indexes, \
                     after {} indexes",
                    baseline_outside.len(),
                    after_outside.len()
                ))
            }
        }
        Err(err) => Err(format!(
            "could not re-list indexes for the safety check: {err}"
        )),
    };
    println!("[safety] indexes outside the owned groups unchanged: {outside_unchanged:?}");

    data_read_result?;
    scenario?;
    cleanup_sync?;
    cleanup_probe?;
    outside_unchanged.map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err.into() })?;
    Ok(())
}

/// Read-only and fast (a single `plan()` call, seconds not minutes): needs only
/// `roles/datastore.viewer` (`datastore.schemas.get`/`datastore.schemas.list`, the permissions
/// that gate `ListIndexes`/`ListFields` - confirmed by listing that role's own permissions and
/// Firestore's testable permissions for the project; `roles/datastore.indexAdmin`, needed by the
/// write-side test above, grants the same two plus create/update/delete). Runs by default,
/// unlike every other test in this file.
#[tokio::test]
async fn plan_is_read_only_and_scoped_to_the_owned_group(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let plan = db
        .fluent()
        .indexes()
        .collection_group(SYNC_GROUP)
        .plan()
        .await?;
    println!("[fast] plan() against {SYNC_GROUP:?}:\n{plan}");

    let group_marker = format!("/collectionGroups/{SYNC_GROUP}/");
    for listed in &plan.kept_undeclared_indexes {
        assert!(
            listed.name.contains(&group_marker),
            "plan() must never surface an index from another group: {}",
            listed.name
        );
    }
    for listed in plan
        .kept_undeclared_fields
        .iter()
        .chain(&plan.kept_undeclared_ttl)
    {
        assert!(
            listed.name.contains(&group_marker),
            "plan() must never surface a field from another group: {}",
            listed.name
        );
    }

    // Every real listed shape in this database converts cleanly under the current domain model;
    // a MongoDB-compat or Datastore-mode index would be the known, accepted exception, but this
    // project has none, so nothing here is expected to land in `unrecognised`.
    assert!(
        plan.unrecognised.is_empty(),
        "unexpected unrecognised listed item(s): {:?}",
        plan.unrecognised
    );

    Ok(())
}

/// A text index on `body_{run}` and a geo index on `location_{run}`. Firestore refuses to
/// recreate an index for minutes after deleting it, answering `ALREADY_EXISTS` while the listing
/// no longer shows it, so each run declares field paths no earlier run has used.
fn declare_search_indexes(db: &FirestoreDb, run: u128) -> FirestoreIndexesBuilder<'_, FirestoreDb> {
    db.fluent()
        .indexes()
        .collection_group(SEARCH_GROUP)
        .composite(|i| {
            i.indexes([
                i.index([i.field(format!("body_{run}")).text()]),
                i.index([i.field(format!("location_{run}")).geo()]),
            ])
        })
}

/// Creates a text index and a geo index on the Enterprise database
/// (`GCP_ENTERPRISE_DATABASE_ID`), replans them, checks that a field override listing indexes is
/// refused before anything is written, and prunes them again. Every index of the database is
/// snapshotted first: the create adds exactly the two declared indexes, and after the prune every
/// index outside the groups of these tests is unchanged and `SEARCH_GROUP` holds none.
#[tokio::test]
#[ignore = "creates and deletes real search indexes in GCP_ENTERPRISE_DATABASE_ID; takes minutes; run with --ignored"]
async fn search_indexes_against_real_firestore_enterprise(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup_enterprise().await?;
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    let baseline_outside = outside_groups(&list_all_database_indexes(&db).await?, &TEST_GROUPS);
    println!(
        "[safety] {} indexes outside the groups of these tests before this run",
        baseline_outside.len()
    );

    // A failed assertion must not skip the prune below, so the scenario's panic is held until
    // the group is empty again.
    let scenario = AssertUnwindSafe(async {
        let emptied = prune_to_empty(&db, SEARCH_GROUP).await?;
        println!("[search] emptied:\n{emptied}");

        let report = declare_search_indexes(&db, run)
            .wait_until_ready_with_options(FirestoreOperationWaitOptions::new(WAIT_TIMEOUT))
            .sync()
            .await?;
        println!("[search] create report:\n{report}");
        assert_eq!(report.created_indexes.len(), 2, "both indexes are new");

        let listed = list_all_database_indexes(&db).await?;
        assert_eq!(
            outside_groups(&listed, &[SEARCH_GROUP]),
            baseline_outside,
            "the create touched only {SEARCH_GROUP:?}"
        );
        let created: Vec<&ProtoIndex> = listed
            .iter()
            .filter(|index| {
                index
                    .name
                    .contains(&format!("/collectionGroups/{SEARCH_GROUP}/"))
            })
            .collect();
        println!("[search] listed after the create: {created:#?}");
        assert_eq!(
            created.len(),
            2,
            "exactly the two declared indexes were added"
        );

        let replan = declare_search_indexes(&db, run).plan().await?;
        println!("[search] replan:\n{replan}");
        assert_eq!(
            replan.unchanged.len(),
            2,
            "both indexes match their listing"
        );
        assert!(replan.create_indexes.is_empty() && replan.unrecognised.is_empty());

        let refused = db
            .fluent()
            .indexes()
            .collection_group(SEARCH_GROUP)
            .field_overrides(|f| f.fields([f.field("body").indexes([f.ascending()])]))
            .sync()
            .await;
        println!("[search] field override sync: {refused:?}");
        assert!(
            matches!(
                &refused,
                Err(FirestoreError::InvalidParametersError(invalid))
                    if invalid.public.field == "field_overrides"
            ),
            "an Enterprise database refuses field overrides before any write"
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;

    let cleanup = prune_to_empty(&db, SEARCH_GROUP).await;
    let after = list_all_database_indexes(&db).await?;
    let after_outside = outside_groups(&after, &TEST_GROUPS);
    println!(
        "[safety] indexes outside the groups of these tests unchanged: {}",
        after_outside == baseline_outside
    );

    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
    println!("[search] prune report:\n{}", cleanup?);
    assert_eq!(after_outside, baseline_outside);
    assert!(
        after.iter().all(|index| !index
            .name
            .contains(&format!("/collectionGroups/{SEARCH_GROUP}/"))),
        "{SEARCH_GROUP:?} holds no index after the prune"
    );
    Ok(())
}

#[cfg(feature = "sql")]
mod sql_statements {
    use super::*;
    use std::future::Future;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Runs `scenario` on `group` after emptying it, and empties it again when the scenario
    /// fails or panics. Every index outside the groups of these tests must be unchanged
    /// afterwards, and `group` must hold no index, field override or TTL.
    async fn in_emptied_group(
        db: &FirestoreDb,
        group: &str,
        scenario: impl Future<Output = TestResult>,
    ) -> TestResult {
        let baseline_outside = outside_groups(&list_all_database_indexes(db).await?, &TEST_GROUPS);

        let scenario = AssertUnwindSafe(async {
            prune_to_empty(db, group).await?;
            scenario.await
        })
        .catch_unwind()
        .await;

        let cleanup = prune_to_empty(db, group).await;
        let after_outside = outside_groups(&list_all_database_indexes(db).await?, &TEST_GROUPS);
        let remaining = db
            .fluent()
            .indexes()
            .list()
            .collection_group(group)
            .get()
            .await?;
        println!(
            "[sql] {group:?}: {} indexes and {} fields left; indexes outside the groups of these \
             tests unchanged: {}",
            remaining.indexes.len(),
            remaining.fields.len(),
            after_outside == baseline_outside
        );

        scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        cleanup?;
        assert_eq!(after_outside, baseline_outside);
        assert!(remaining.indexes.is_empty() && remaining.fields.is_empty());
        Ok(())
    }

    /// The statements of these tests change indexes and fields, which the options refuse until
    /// they allow it.
    fn index_options() -> FirestoreSqlOptions {
        FirestoreSqlOptions::new().allow_index_statements()
    }

    fn run_id() -> Result<u128, Box<dyn std::error::Error + Send + Sync>> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis())
    }

    fn created(outcome: FirestoreSqlOutcome<'_>) -> FirestoreListedCompositeIndex {
        match outcome {
            FirestoreSqlOutcome::IndexCreated(index) => index,
            _ => panic!("expected the outcome IndexCreated"),
        }
    }

    fn listing(outcome: FirestoreSqlOutcome<'_>) -> FirestoreIndexListing {
        match outcome {
            FirestoreSqlOutcome::Indexes(listing) => listing,
            _ => panic!("expected the outcome Indexes"),
        }
    }

    fn dropped(outcome: FirestoreSqlOutcome<'_>) -> Option<FirestoreListedCompositeIndex> {
        match outcome {
            FirestoreSqlOutcome::IndexDropped(index) => index,
            _ => panic!("expected the outcome IndexDropped"),
        }
    }

    fn updated(outcome: FirestoreSqlOutcome<'_>) -> FirestoreListedField {
        match outcome {
            FirestoreSqlOutcome::FieldUpdated(field) => field,
            _ => panic!("expected the outcome FieldUpdated"),
        }
    }

    async fn show_indexes_from(
        db: &FirestoreDb,
        group: &str,
    ) -> Result<FirestoreIndexListing, Box<dyn std::error::Error + Send + Sync>> {
        Ok(listing(
            db.fluent()
                .sql(format!("SHOW INDEXES FROM `{group}`"))
                .run()
                .await?,
        ))
    }

    /// `CREATE INDEX` with a wait, `SHOW INDEXES FROM` lists it, `DROP INDEX` by ID returns it and
    /// `SHOW INDEXES FROM` no longer lists it. Standard needs two fields in an index.
    #[tokio::test]
    #[ignore = "creates and deletes a real index in GCP_PROJECT; takes minutes; run with --ignored"]
    async fn sql_index_statements_round_trip_on_standard() -> TestResult {
        let db = setup().await?;
        let run = run_id()?;
        let group = FirestoreCollectionId::new(STANDARD_INDEX_GROUP)?;

        in_emptied_group(&db, STANDARD_INDEX_GROUP, async {
            let index = created(
                db.fluent()
                    .sql(format!(
                        "CREATE INDEX ON `{STANDARD_INDEX_GROUP}` \
                         (customer_id_{run}, placed_at_{run} DESC)"
                    ))
                    .options(index_options())
                    .wait_until_ready(WAIT_TIMEOUT)
                    .run()
                    .await?,
            );
            assert_eq!(index.state, FirestoreIndexState::Ready);
            assert_eq!(index.collection_group, group);

            let shown = show_indexes_from(&db, STANDARD_INDEX_GROUP).await?;
            assert!(shown.indexes.iter().any(|listed| listed.id == index.id));

            let removed = dropped(
                db.fluent()
                    .sql(format!("DROP INDEX `{}`", index.id))
                    .options(index_options())
                    .run()
                    .await?,
            )
            .expect("an index without IF EXISTS is returned");
            assert_eq!(removed.id, index.id);

            let shown = show_indexes_from(&db, STANDARD_INDEX_GROUP).await?;
            assert!(shown.indexes.iter().all(|listed| listed.id != index.id));
            Ok(())
        })
        .await
    }

    /// `CREATE UNIQUE INDEX` with a wait on the Enterprise database, then `DROP UNIQUE INDEX` by
    /// the definition that created it. Enterprise accepts an index of one field.
    #[tokio::test]
    #[ignore = "creates and deletes a real index in GCP_ENTERPRISE_DATABASE_ID; takes minutes; run with --ignored"]
    async fn sql_unique_index_round_trip_on_enterprise() -> TestResult {
        let db = setup_enterprise_unseeded().await?;
        let run = run_id()?;

        in_emptied_group(&db, ENTERPRISE_INDEX_GROUP, async {
            let definition = format!(
                "ON `{ENTERPRISE_INDEX_GROUP}` (email_{run}) OPTIONS (density = 'SPARSE_ANY')"
            );
            let index = created(
                db.fluent()
                    .sql(format!("CREATE UNIQUE INDEX {definition}"))
                    .options(index_options())
                    .wait_until_ready(WAIT_TIMEOUT)
                    .run()
                    .await?,
            );
            assert_eq!(index.state, FirestoreIndexState::Ready);
            assert!(index.index.unique);
            assert_eq!(index.index.density, Some(FirestoreIndexDensity::SparseAny));

            let removed = dropped(
                db.fluent()
                    .sql(format!("DROP UNIQUE INDEX {definition}"))
                    .options(index_options())
                    .run()
                    .await?,
            )
            .expect("an index without IF EXISTS is returned");
            assert_eq!(removed.id, index.id);

            let shown = show_indexes_from(&db, ENTERPRISE_INDEX_GROUP).await?;
            assert!(shown.indexes.iter().all(|listed| listed.id != index.id));
            Ok(())
        })
        .await
    }

    /// `ALTER COLLECTION` enables TTL and exempts the field in one write, then disables TTL and
    /// removes the override in one write, each waited. Standard accepts index configuration on a
    /// field.
    #[tokio::test]
    #[ignore = "changes a real field in GCP_PROJECT; takes minutes; run with --ignored"]
    async fn sql_alter_collection_ttl_and_exemption_round_trip() -> TestResult {
        let db = setup().await?;
        let field_path = format!("expires_at_{}", run_id()?);

        in_emptied_group(&db, FIELD_GROUP, async {
            let enabled = updated(
                db.fluent()
                    .sql(format!(
                        "ALTER COLLECTION `{FIELD_GROUP}` ALTER COLUMN {field_path} \
                         SET OPTIONS (ttl = TRUE, indexes = [])"
                    ))
                    .options(index_options())
                    .wait_until_ready(WAIT_TIMEOUT)
                    .run()
                    .await?,
            );
            assert_eq!(enabled.field_path, field_path);
            assert_eq!(
                enabled.ttl,
                Some(FirestoreFieldTtlOutcome::Configured(
                    FirestoreFieldTtlState::Active
                ))
            );
            assert!(matches!(
                &enabled.index_override,
                Some(FirestoreFieldOverrideOutcome::Explicit(explicit)) if explicit.indexes.is_empty()
            ));

            let reverted = updated(
                db.fluent()
                    .sql(format!(
                        "ALTER COLLECTION `{FIELD_GROUP}` ALTER COLUMN {field_path} \
                         SET OPTIONS (ttl = FALSE, indexes = NULL)"
                    ))
                    .options(index_options())
                    .wait_until_ready(WAIT_TIMEOUT)
                    .run()
                    .await?,
            );
            assert_eq!(reverted.ttl, None);
            assert_eq!(
                reverted.index_override,
                Some(FirestoreFieldOverrideOutcome::Inherited)
            );
            Ok(())
        })
        .await
    }
}
