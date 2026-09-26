//! Runs the declarative index management API against a real Firestore project
//! (`GCP_PROJECT`, local ADC). Uses dedicated collection groups so it never touches anything a
//! caller's own data depends on, and prunes both groups to an empty declaration at the end even
//! when an earlier assertion fails.
//!
//! `plan_is_read_only_and_scoped_to_the_owned_group` makes no writes and runs in seconds, so it
//! runs by default. Every other test here creates, updates or deletes real indexes, overrides or
//! TTL policy in `GCP_PROJECT` and can take 10-15 minutes (a composite or vector index build is
//! not instant even on an empty collection), so each carries `#[ignore]` and only runs on
//! `cargo test --test admin-indexes-tests --features admin -- --ignored --nocapture`.
//!
//! Safety net: `ListIndexes` scoped to one collection group's parent has been measured, against
//! this same project, to answer with composite indexes belonging to *every* group in the
//! database (see the fake-server tests in `src/db/admin/indexes.rs` that replay the exact
//! response captured). The library now filters and defends against that on its own, but the
//! write-side test does not take that on faith: it snapshots every index outside its own two
//! groups before touching anything, and asserts that snapshot is unchanged - not merely present,
//! but identical in fields, scope and state - after every write it makes, including the final
//! cleanup.

use firestore::*;
use gcloud_sdk::google::firestore::admin::v1::firestore_admin_client::FirestoreAdminClient;
use gcloud_sdk::google::firestore::admin::v1::{Index as ProtoIndex, ListIndexesRequest};
use std::collections::BTreeMap;
use std::time::Duration;

mod common;
use common::setup;

const SYNC_GROUP: &str = "firestore-rs-index-sync-test";
const PROBE_GROUP: &str = "firestore-rs-index-probe";
const OWNED_GROUPS: [&str; 2] = [SYNC_GROUP, PROBE_GROUP];

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

/// The subset of `indexes` outside this test's own two groups, keyed by resource name so a
/// before/after comparison catches a changed field list or state, not only a missing or added
/// entry.
fn outside_owned_groups(indexes: &[ProtoIndex]) -> BTreeMap<String, ProtoIndex> {
    indexes
        .iter()
        .filter(|index| {
            !OWNED_GROUPS
                .iter()
                .any(|group| index.name.contains(&format!("/collectionGroups/{group}/")))
        })
        .map(|index| (index.name.clone(), index.clone()))
        .collect()
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
/// does a second plan then show no changes?
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
    assert!(
        report.created_indexes.len() + report.unchanged.len() + report.pending.len() == 1,
        "expected exactly the one declared index, either freshly created or already there from \
         a previous run"
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
    assert!(
        plan.create_indexes.is_empty(),
        "replan must not want to create anything again"
    );
    assert_eq!(
        plan.unchanged.len(),
        1,
        "the created index must be reported unchanged"
    );
    Ok(())
}

/// Question 2: is a vector-only composite index accepted?
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
    assert!(
        report.created_indexes.len() == 1
            || report.unchanged.len() == 2
            || report.pending.len() == 1,
        "expected the vector-only index to be created (the [a, tags] index already existed)",
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
        after_revert.undeclared_fields.is_empty(),
        "the group must carry no leftover override after the revert",
    );
    Ok(())
}

#[tokio::test]
#[ignore = "creates and deletes real indexes in GCP_PROJECT; takes 10-15 minutes; run with --ignored"]
async fn admin_index_management_against_real_firestore(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = setup().await?;

    let baseline = list_all_database_indexes(&db).await?;
    let baseline_outside = outside_owned_groups(&baseline);
    println!(
        "[safety] {} indexes outside {SYNC_GROUP:?} and {PROBE_GROUP:?} before this run; they \
         must be exactly the same, field for field, after cleanup.",
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
        implied_name_direction_and_replan_is_a_no_op(&db).await?;
        vector_only_composite_index_is_accepted(&db).await?;
        all_fields_override_on_a_single_group(&db).await?;
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
            let after_outside = outside_owned_groups(after);
            if after_outside == baseline_outside {
                Ok(())
            } else {
                Err(format!(
                    "indexes outside {SYNC_GROUP:?}/{PROBE_GROUP:?} changed: before {} indexes, \
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
    for listed in &plan.undeclared_indexes {
        assert!(
            listed.name.contains(&group_marker),
            "plan() must never surface an index from another group: {}",
            listed.name
        );
    }
    for listed in plan.undeclared_fields.iter().chain(&plan.undeclared_ttl) {
        assert!(
            listed.name.contains(&group_marker),
            "plan() must never surface a field from another group: {}",
            listed.name
        );
    }

    // Every real listed shape in this database converts cleanly under the current domain model;
    // a MongoDB-compat, search or Datastore-mode index would be the known, accepted exception,
    // but this project has none, so nothing here is expected to land in `unrecognised`.
    assert!(
        plan.unrecognised.is_empty(),
        "unexpected unrecognised listed item(s): {:?}",
        plan.unrecognised
    );

    Ok(())
}
