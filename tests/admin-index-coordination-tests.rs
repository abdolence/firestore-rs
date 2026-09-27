//! Runs index sync coordination, the generation guard and the lease, against a real Firestore
//! project (`GCP_PROJECT`, local ADC).
//!
//! Every sync here declares nothing for a collection group that has no indexes and never prunes,
//! so it only lists and sends no admin write. The only data written is one coordination document,
//! in a collection of its own, which the test deletes at the end even when an assertion fails. It
//! runs in seconds, so it runs by default.

use firestore::*;

mod common;
use common::setup;

const GROUP: &str = "firestore-rs-index-coordination-probe";
const COORDINATION_COLLECTION: &str = "firestore-rs-index-coordination-test";

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn delete_coordination_document(db: &FirestoreDb) -> TestResult<()> {
    db.fluent()
        .delete()
        .from(COORDINATION_COLLECTION)
        .document_id(GROUP)
        .execute()
        .await?;
    Ok(())
}

async fn sync_holding_lease(db: &FirestoreDb, owner: &str) -> TestResult<FirestoreIndexSyncReport> {
    Ok(db
        .fluent()
        .indexes()
        .collection_group(GROUP)
        .coordination_collection(COORDINATION_COLLECTION)
        .lease(FirestoreIndexLeaseOptions::new().with_owner(FirestoreIndexLeaseOwner::new(owner)?))
        .sync()
        .await?)
}

async fn sync_at_generation(
    db: &FirestoreDb,
    generation: u64,
) -> TestResult<FirestoreIndexSyncReport> {
    Ok(db
        .fluent()
        .indexes()
        .collection_group(GROUP)
        .coordination_collection(COORDINATION_COLLECTION)
        .generation(generation)
        .sync()
        .await?)
}

/// Resolves once the coordination document shows a lease: another caller's sync has claimed the
/// group and is still running.
async fn lease_claimed(db: &FirestoreDb) -> TestResult<()> {
    loop {
        let document: Option<gcloud_sdk::google::firestore::v1::Document> = db
            .fluent()
            .select()
            .by_id_in(COORDINATION_COLLECTION)
            .one(GROUP)
            .await?;
        if document.is_some_and(|document| document.fields.contains_key("lease_owner")) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// How often a sync of [`sync_at_generation_holding_lease`] polls for anything it waits on:
/// longer than a claim that Firestore aborts and the transaction retries after its backoff, so
/// a claim that took this long shows a wait for the lease.
const LEASE_POLL: std::time::Duration = std::time::Duration::from_secs(3);

/// A sync at `generation` holding a lease that skips a held lease, and polls every
/// [`LEASE_POLL`] for anything it waits on.
async fn sync_at_generation_holding_lease(
    db: &FirestoreDb,
    generation: u64,
    owner: &str,
) -> TestResult<FirestoreIndexSyncReport> {
    Ok(db
        .fluent()
        .indexes()
        .collection_group(GROUP)
        .coordination_collection(COORDINATION_COLLECTION)
        .generation(generation)
        .lease(FirestoreIndexLeaseOptions::new().with_owner(FirestoreIndexLeaseOwner::new(owner)?))
        .wait_until_ready_with_options(
            FirestoreOperationWaitOptions::new(std::time::Duration::from_secs(60))
                .with_poll_interval(LEASE_POLL),
        )
        .sync()
        .await?)
}

fn applied_and_held(reports: &[FirestoreIndexSyncReport]) -> (usize, usize) {
    let applied = reports
        .iter()
        .filter(|report| report.skipped.is_none())
        .count();
    let held = reports
        .iter()
        .filter(|report| {
            matches!(
                report.skipped,
                Some(FirestoreIndexSyncSkipReason::LeaseHeld(_))
            )
        })
        .count();
    (applied, held)
}

async fn coordinate(db: &FirestoreDb) -> TestResult<()> {
    // Started together, one claim wins and the other's transaction is aborted and retried. A sync
    // of an empty group can finish before that retry, so the second may then claim the released
    // lease and apply after the first; what may never happen is that neither applies.
    let (first, second) = tokio::join!(
        sync_holding_lease(db, "live-replica-a"),
        sync_holding_lease(db, "live-replica-b")
    );
    let together = [first?, second?];
    for report in &together {
        println!("[started together] skipped: {:?}", report.skipped);
    }
    let (applied, held) = applied_and_held(&together);
    assert!(applied >= 1 && applied + held == 2, "{together:?}");

    // The second sync starts once the coordination document shows the first one's lease, so
    // the second claims while the first still runs: it skips, naming the holder. A sync of an
    // empty group can take under a quarter of a second, and the second claim is sometimes read
    // only after the first released; then both apply one after the other, which is correct but
    // shows no overlap, so the round is tried again, up to five times.
    let mut overlapped = false;
    for round in 1..=5 {
        let (first, second) = tokio::join!(sync_holding_lease(db, "live-replica-a"), async {
            tokio::time::timeout(std::time::Duration::from_secs(20), lease_claimed(db)).await??;
            sync_holding_lease(db, "live-replica-b").await
        });
        let reports = [first?, second?];
        println!(
            "[overlapping, round {round}] skipped: {:?}, {:?}",
            reports[0].skipped, reports[1].skipped
        );
        assert_eq!(
            reports[0].skipped, None,
            "the first sync claims a free lease"
        );
        match &reports[1].skipped {
            Some(FirestoreIndexSyncSkipReason::LeaseHeld(held)) => {
                assert_eq!(held.owner.as_str(), "live-replica-a");
                overlapped = true;
                break;
            }
            None => continue,
            other => panic!("unexpected skip: {other:?}"),
        }
    }
    assert!(
        overlapped,
        "a sync started while another held the lease never skipped"
    );

    let newer = sync_at_generation(db, 2).await?;
    println!("[generation 2] skipped: {:?}", newer.skipped);
    assert_eq!(newer.skipped, None);

    let older = sync_at_generation(db, 1).await?;
    println!("[generation 1] skipped: {:?}", older.skipped);
    match older.skipped {
        Some(FirestoreIndexSyncSkipReason::Superseded(superseded)) => {
            assert_eq!(superseded.stored.value(), 2);
            assert_eq!(superseded.ours.value(), 1);
        }
        other => panic!("generation 1 after 2 must be superseded, got {other:?}"),
    }

    // A newer generation that finds an older one holding the lease waits for it, although it is
    // told to skip a held lease. Waiting shows as a claim that took at least one poll interval;
    // a round whose newer claim came only after the older sync released is tried again, up to
    // five times, as above.
    let mut waited = false;
    for round in 1..=5u64 {
        let (older, newer) = tokio::join!(
            sync_at_generation_holding_lease(db, 2 * round + 1, "live-replica-a"),
            async {
                tokio::time::timeout(std::time::Duration::from_secs(20), lease_claimed(db))
                    .await??;
                sync_at_generation_holding_lease(db, 2 * round + 2, "live-replica-b").await
            }
        );
        let (older, newer) = (older?, newer?);
        println!(
            "[newer generation, round {round}] older skipped: {:?}; newer skipped: {:?}, claimed in {:?}",
            older.skipped, newer.skipped, newer.timings.coordination
        );
        assert_eq!(
            newer.skipped, None,
            "a newer generation never skips an older holder"
        );
        if newer
            .timings
            .coordination
            .is_some_and(|claimed| claimed >= LEASE_POLL)
        {
            waited = true;
            break;
        }
    }
    assert!(
        waited,
        "a newer generation never found an older one holding the lease"
    );
    Ok(())
}

#[tokio::test]
async fn coordination_against_real_firestore() -> TestResult<()> {
    let db = setup().await?;
    delete_coordination_document(&db).await?;

    let outcome = coordinate(&db).await;
    let cleanup = delete_coordination_document(&db).await;
    let remaining: Option<gcloud_sdk::google::firestore::v1::Document> = db
        .fluent()
        .select()
        .by_id_in(COORDINATION_COLLECTION)
        .one(GROUP)
        .await?;

    outcome?;
    cleanup?;
    assert!(remaining.is_none(), "the coordination document is deleted");
    Ok(())
}
