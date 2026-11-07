//! Pipeline writes against the Enterprise edition database `GCP_ENTERPRISE_DATABASE_ID`.
//!
//! Every write stays in the DML collections: [`guarded`] checks each pipeline write before it is
//! sent, and each test diffs snapshots of both collections taken around its writes.

use crate::common::setup_enterprise;
use firestore::errors::{BackoffError, FirestoreError};
use firestore::gcloud_sdk::google::firestore::v1::value::ValueType;
use firestore::*;
use futures::FutureExt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::info;

mod common;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DML: &str = "pipeline-dml";
const DML_ARCHIVE: &str = "pipeline-dml-archive";
const DML_COLLECTIONS: [&str; 2] = [DML, DML_ARCHIVE];

/// The tests write to the same collections and diff whole snapshots of them, so they take turns.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DML_LISTENER_TARGET: FirestoreListenerTarget = FirestoreListenerTarget::new(53);
/// How long a listener may take to report a change.
const LISTENER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize)]
struct DmlRow {
    #[serde(rename = "__name__")]
    name: FirestoreReference,
    suite: &'static str,
    title: &'static str,
    n: Option<i64>,
    key: Option<&'static str>,
}

impl DmlRow {
    fn new(db: &FirestoreDb, suite: &'static str, id: &str, title: &'static str) -> Self {
        Self {
            name: FirestoreReference(format!("{}/{DML}/{id}", db.get_documents_path())),
            suite,
            title,
            n: None,
            key: None,
        }
    }
}

/// The documents of the DML collections by path, such as `"pipeline-dml/doc-1"`, with their
/// fields.
type Snapshot = BTreeMap<String, serde_json::Value>;

/// Each state a listener saw a document in, in order: its fields, or `None` once deleted.
type Seen = Arc<Mutex<Vec<(String, Option<serde_json::Value>)>>>;

/// Returns `write` once it is clear that it changes documents of the DML collections alone: it
/// writes to one of them, or it writes the documents of its source, which is one of them or
/// names only their documents, and has no stage but filters that could rename them.
fn guarded<B>(db: &FirestoreDb, write: B) -> B
where
    B: Clone,
    FirestorePipelineWriteParams: From<B>,
{
    let params = FirestorePipelineWriteParams::from(write.clone());
    assert!(
        params
            .pipeline
            .stages
            .iter()
            .all(|stage| matches!(stage, FirestorePipelineStage::Where(_))),
        "a stage could rename the written documents: {params:?}"
    );
    let target = match &params.write {
        FirestorePipelineWrite::Insert(FirestoreInsertWrite { target })
        | FirestorePipelineWrite::Upsert(FirestoreUpsertWrite { target, .. }) => target.as_ref(),
        FirestorePipelineWrite::Update(_) | FirestorePipelineWrite::Delete => None,
    };
    let stays_in_dml = match target {
        Some(target) => DML_COLLECTIONS.contains(&target.collection_path.as_str()),
        None => source_in_dml(db, &params.pipeline.source),
    };
    assert!(
        stays_in_dml,
        "the write leaves the DML collections: {params:?}"
    );
    write
}

fn source_in_dml(db: &FirestoreDb, source: &FirestorePipelineSource) -> bool {
    let documents_root = format!("{}/", db.get_documents_path());
    match source {
        FirestorePipelineSource::Collection(source) => {
            source.parent.is_none() && DML_COLLECTIONS.contains(&source.collection_id.as_str())
        }
        FirestorePipelineSource::Documents(source) => {
            source.paths.iter().all(|path| path_in_dml(path))
        }
        FirestorePipelineSource::Literals(source) => source.rows.iter().all(|row| {
            let Some(ValueType::MapValue(row)) = &row.value.value_type else {
                return false;
            };
            matches!(
                row.fields.get("__name__").and_then(|name| name.value_type.as_ref()),
                Some(ValueType::ReferenceValue(name))
                    if name.strip_prefix(&documents_root).is_some_and(path_in_dml)
            )
        }),
        _ => false,
    }
}

/// Whether `path`, relative to the documents root, names a document of a DML collection.
fn path_in_dml(path: &str) -> bool {
    path.split_once('/').is_some_and(|(collection, id)| {
        DML_COLLECTIONS.contains(&collection) && !id.is_empty() && !id.contains('/')
    })
}

/// A document's fields, without the metadata the deserialiser adds.
fn fields(document: &FirestoreDocument) -> serde_json::Value {
    let mut fields: BTreeMap<String, serde_json::Value> =
        FirestoreDb::deserialize_doc_to(document).unwrap();
    fields.retain(|field, _| !field.starts_with("_firestore_"));
    serde_json::to_value(fields).unwrap()
}

async fn snapshot(db: &FirestoreDb) -> TestResult<Snapshot> {
    let documents_root = format!("{}/", db.get_documents_path());
    let mut snapshot = Snapshot::new();
    for collection in DML_COLLECTIONS {
        for document in db.fluent().select().from(collection).query().await? {
            let path = document
                .name
                .trim_start_matches(&documents_root)
                .to_string();
            snapshot.insert(path, fields(&document));
        }
    }
    Ok(snapshot)
}

/// One line per document that `after` adds, changes or removes against `before`.
fn diff(before: &Snapshot, after: &Snapshot) -> Vec<String> {
    let mut lines: Vec<String> = after
        .iter()
        .filter_map(|(path, fields)| match before.get(path) {
            None => Some(format!("+ {path} {fields}")),
            Some(earlier) if earlier != fields => Some(format!("~ {path} {fields}")),
            Some(_) => None,
        })
        .collect();
    lines.extend(
        before
            .keys()
            .filter(|path| !after.contains_key(*path))
            .map(|path| format!("- {path}")),
    );
    lines
}

/// Takes a snapshot, checks that it differs from `before` by `expected` alone, and returns it.
async fn expect_changes(
    db: &FirestoreDb,
    before: &Snapshot,
    expected: &[&str],
) -> TestResult<Snapshot> {
    let after = snapshot(db).await?;
    let changes = diff(before, &after);
    info!(?changes, "Snapshot diff.");
    assert_eq!(changes, expected);
    Ok(after)
}

/// Deletes the documents `ids` of a DML collection, which a failed run may have left behind.
async fn remove_documents(db: &FirestoreDb, collection: &str, ids: &[&str]) -> TestResult {
    assert!(
        DML_COLLECTIONS.contains(&collection),
        "{collection} is not a DML collection"
    );
    for id in ids {
        db.fluent()
            .delete()
            .from(collection)
            .document_id(*id)
            .execute()
            .await?;
    }
    Ok(())
}

async fn listen(
    db: &FirestoreDb,
) -> TestResult<(
    FirestoreListener<FirestoreDb, FirestoreMemListenStateStorage>,
    Seen,
)> {
    let mut listener = db
        .create_listener(FirestoreMemListenStateStorage::new())
        .await?;
    db.fluent()
        .select()
        .from(DML)
        .listen()
        .add_target(DML_LISTENER_TARGET, &mut listener)?;
    let documents_root = format!("{}/", db.get_documents_path());
    let seen = Seen::default();
    let recorder = seen.clone();
    listener
        .start(move |event| {
            let state = match event {
                FirestoreListenEvent::DocumentChange(change) => change
                    .document
                    .map(|document| (document.name.clone(), Some(fields(&document)))),
                FirestoreListenEvent::DocumentDelete(deleted) => Some((deleted.document, None)),
                _ => None,
            };
            recorder.lock().unwrap().extend(state.map(|(name, fields)| {
                (name.trim_start_matches(&documents_root).to_string(), fields)
            }));
            async { Ok(()) }
        })
        .await?;
    Ok((listener, seen))
}

/// Waits until the listener has seen every document that `after` changes against `before` in
/// its state in `after`.
async fn wait_until_listened(seen: &Seen, before: &Snapshot, after: &Snapshot) {
    let deadline = Instant::now() + LISTENER_TIMEOUT;
    let paths: Vec<&String> = before.keys().chain(after.keys()).collect();
    let has_seen = |path: &String| {
        let state = after.get(path).cloned();
        before.get(path) == state.as_ref()
            || seen
                .lock()
                .unwrap()
                .iter()
                .any(|(seen_path, seen_state)| seen_path == path && *seen_state == state)
    };
    while !paths.iter().all(|path| has_seen(path)) {
        assert!(
            Instant::now() < deadline,
            "the listener missed a change from {before:?} to {after:?}: {:?}",
            seen.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn pipeline_writes_reach_a_core_listener() -> TestResult {
    let _turn = TURN.lock().await;
    let db = setup_enterprise().await?;
    let suite = "listener";
    remove_documents(&db, DML, &["listened-1", "listened-2"]).await?;
    let before = snapshot(&db).await?;
    let (mut listener, seen) = listen(&db).await?;

    let inserted = guarded(
        &db,
        db.fluent().pipeline().insert().literals([
            DmlRow {
                n: Some(1),
                ..DmlRow::new(&db, suite, "listened-1", "one")
            },
            DmlRow {
                n: Some(2),
                ..DmlRow::new(&db, suite, "listened-2", "two")
            },
        ]),
    )
    .execute()
    .await?;
    let after_insert = expect_changes(
        &db,
        &before,
        &[
            r#"+ pipeline-dml/listened-1 {"n":1,"suite":"listener","title":"one"}"#,
            r#"+ pipeline-dml/listened-2 {"n":2,"suite":"listener","title":"two"}"#,
        ],
    )
    .await?;
    wait_until_listened(&seen, &before, &after_insert).await;

    let updated = guarded(
        &db,
        db.fluent()
            .pipeline()
            .update(|e| [e.constant("updated").alias("status")])
            .in_col(DML)
            .filter(|q| q.for_all([q.field("suite").eq(suite), q.field("title").eq("one")])),
    )
    .execute()
    .await?;
    let after_update = expect_changes(
        &db,
        &after_insert,
        &[r#"~ pipeline-dml/listened-1 {"n":1,"status":"updated","suite":"listener","title":"one"}"#],
    )
    .await?;
    wait_until_listened(&seen, &after_insert, &after_update).await;

    let upserted = guarded(
        &db,
        db.fluent()
            .pipeline()
            .upsert(|e| [e.constant("upsert").alias("source")])
            .literals([DmlRow::new(&db, suite, "listened-2", "two-replaced")]),
    )
    .atomic()
    .execute()
    .await?;
    let after_upsert = expect_changes(
        &db,
        &after_update,
        &[r#"~ pipeline-dml/listened-2 {"source":"upsert","suite":"listener","title":"two-replaced"}"#],
    )
    .await?;
    wait_until_listened(&seen, &after_update, &after_upsert).await;

    let deleted = guarded(
        &db,
        db.fluent()
            .pipeline()
            .delete()
            .from(DML)
            .filter(|q| q.field("suite").eq(suite)),
    )
    .execute()
    .await?;
    let after_delete = expect_changes(
        &db,
        &after_upsert,
        &["- pipeline-dml/listened-1", "- pipeline-dml/listened-2"],
    )
    .await?;
    wait_until_listened(&seen, &after_upsert, &after_delete).await;
    listener.shutdown().await?;

    assert_eq!(
        [inserted, updated, upserted, deleted].map(|result| result.documents_modified),
        [2, 1, 1, 2]
    );
    assert_eq!(after_delete, before);
    Ok(())
}

#[tokio::test]
async fn an_insert_into_a_collection_names_each_document_by_an_expression() -> TestResult {
    let _turn = TURN.lock().await;
    let db = setup_enterprise().await?;
    let suite = "into";
    remove_documents(&db, DML, &["into-1", "into-2"]).await?;
    remove_documents(&db, DML_ARCHIVE, &["archived-1", "archived-2"]).await?;
    let before = snapshot(&db).await?;
    guarded(
        &db,
        db.fluent().pipeline().insert().literals([
            DmlRow {
                key: Some("archived-1"),
                ..DmlRow::new(&db, suite, "into-1", "one")
            },
            DmlRow {
                key: Some("archived-2"),
                ..DmlRow::new(&db, suite, "into-2", "two")
            },
        ]),
    )
    .execute()
    .await?;
    let seeded = snapshot(&db).await?;

    let archived = guarded(
        &db,
        db.fluent()
            .pipeline()
            .insert()
            .into(DML_ARCHIVE)
            .document_id(|e| e.field("key"))
            .from(DML)
            .filter(|q| q.field("suite").eq(suite)),
    )
    .atomic()
    .execute()
    .await?;
    let after_archive = expect_changes(
        &db,
        &seeded,
        &[
            r#"+ pipeline-dml-archive/archived-1 {"key":"archived-1","suite":"into","title":"one"}"#,
            r#"+ pipeline-dml-archive/archived-2 {"key":"archived-2","suite":"into","title":"two"}"#,
        ],
    )
    .await?;
    for collection in DML_COLLECTIONS {
        guarded(
            &db,
            db.fluent()
                .pipeline()
                .delete()
                .from(collection)
                .filter(|q| q.field("suite").eq(suite)),
        )
        .execute()
        .await?;
    }

    assert_eq!(archived, FirestorePipelineWriteResult::new(2));
    assert_eq!(after_archive.len(), before.len() + 4);
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}

#[tokio::test]
async fn an_update_in_a_transaction_applies_when_it_commits() -> TestResult {
    let _turn = TURN.lock().await;
    let db = setup_enterprise().await?;
    let suite = "transaction";
    remove_documents(&db, DML, &["committed-1", "committed-2"]).await?;
    let before = snapshot(&db).await?;
    guarded(
        &db,
        db.fluent().pipeline().insert().literals([
            DmlRow::new(&db, suite, "committed-1", "one"),
            DmlRow::new(&db, suite, "committed-2", "two"),
        ]),
    )
    .execute()
    .await?;
    let seeded = snapshot(&db).await?;

    let in_run_transaction = db
        .run_transaction(|db, transaction| {
            async move {
                let updated = guarded(
                    &db,
                    db.fluent()
                        .pipeline()
                        .update(|e| [e.constant("committed").alias("status")])
                        .in_col(DML)
                        .filter(|q| q.field("suite").eq(suite)),
                )
                .execute_in_transaction(transaction)
                .await?;
                Ok::<_, BackoffError<FirestoreError>>(updated)
            }
            .boxed()
        })
        .await?;
    let after_run_transaction = expect_changes(
        &db,
        &seeded,
        &[
            r#"~ pipeline-dml/committed-1 {"status":"committed","suite":"transaction","title":"one"}"#,
            r#"~ pipeline-dml/committed-2 {"status":"committed","suite":"transaction","title":"two"}"#,
        ],
    )
    .await?;

    let mut transaction = db.begin_transaction().await?;
    let held = guarded(
        &db,
        db.fluent()
            .pipeline()
            .update(|e| [e.constant("held").alias("status")])
            .in_col(DML)
            .filter(|q| q.field("suite").eq(suite)),
    )
    .execute_in_transaction(&mut transaction)
    .await?;
    let before_commit = snapshot(&db).await?;
    let committed = transaction.commit().await?;
    info!(
        ?committed,
        "Commit of a transaction that held a two-document pipeline update."
    );
    expect_changes(
        &db,
        &after_run_transaction,
        &[
            r#"~ pipeline-dml/committed-1 {"status":"held","suite":"transaction","title":"one"}"#,
            r#"~ pipeline-dml/committed-2 {"status":"held","suite":"transaction","title":"two"}"#,
        ],
    )
    .await?;
    guarded(
        &db,
        db.fluent()
            .pipeline()
            .delete()
            .from(DML)
            .filter(|q| q.field("suite").eq(suite)),
    )
    .execute()
    .await?;

    assert_eq!(
        [in_run_transaction, held].map(|result| result.documents_modified),
        [2, 2]
    );
    assert_eq!(before_commit, after_run_transaction);
    assert!(committed.commit_time.is_some(), "{committed:?}");
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}
