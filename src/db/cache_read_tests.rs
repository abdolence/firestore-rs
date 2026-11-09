use super::*;
use crate::db::fake_firestore::{FakeFirestore, FakeResponse};
use crate::errors::FirestoreError;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{
    batch_get_documents_response, BatchGetDocumentsRequest, BatchGetDocumentsResponse, Document,
    ExecutePipelineResponse, GetDocumentRequest, ListDocumentsResponse, RunQueryResponse, Value,
};
use gcloud_sdk::prost::Message;
use std::time::Duration;

type MemoryCache = FirestoreCache<
    FirestoreMemoryCacheBackend,
    <FirestoreMemoryCacheKind as FirestoreCacheBackendKind>::DefaultStorage,
>;

const COLLECTION: &str = "items";
const CACHED_DOCUMENT: &str = "cached";
const FROM_CACHE: &str = "cache";
const FROM_FIRESTORE: &str = "firestore";

fn document(name: &str, source: &str) -> Document {
    Document {
        name: name.to_string(),
        fields: [(
            "source".to_string(),
            Value {
                value_type: Some(ValueType::StringValue(source.to_string())),
            },
        )]
        .into(),
        ..Default::default()
    }
}

fn source(document: &Document) -> String {
    match &document.fields["source"].value_type {
        Some(ValueType::StringValue(source)) => source.clone(),
        other => panic!("unexpected source field: {other:?}"),
    }
}

/// Answers every read with documents whose `source` field is [`FROM_FIRESTORE`].
fn answer(method: &str, body: &[u8]) -> (String, FakeResponse) {
    let documents_path = "projects/fake-firestore/databases/(default)/documents";
    let cached_name = format!("{documents_path}/{COLLECTION}/{CACHED_DOCUMENT}");
    if method.ends_with("/GetDocument") {
        let request = GetDocumentRequest::decode(body).unwrap();
        (
            "GetDocument".to_string(),
            FakeResponse::Message(document(&request.name, FROM_FIRESTORE).encode_to_vec()),
        )
    } else if method.ends_with("/BatchGetDocuments") {
        let request = BatchGetDocumentsRequest::decode(body).unwrap();
        let responses = request
            .documents
            .iter()
            .map(|name| {
                BatchGetDocumentsResponse {
                    result: Some(batch_get_documents_response::Result::Found(document(
                        name,
                        FROM_FIRESTORE,
                    ))),
                    ..Default::default()
                }
                .encode_to_vec()
            })
            .collect();
        (
            "BatchGetDocuments".to_string(),
            FakeResponse::Paced(responses, Duration::ZERO),
        )
    } else if method.ends_with("/RunQuery") {
        let response = RunQueryResponse {
            document: Some(document(&cached_name, FROM_FIRESTORE)),
            ..Default::default()
        };
        (
            "RunQuery".to_string(),
            FakeResponse::Paced(vec![response.encode_to_vec()], Duration::ZERO),
        )
    } else if method.ends_with("/ListDocuments") {
        let response = ListDocumentsResponse {
            documents: vec![document(&cached_name, FROM_FIRESTORE)],
            next_page_token: String::new(),
        };
        (
            "ListDocuments".to_string(),
            FakeResponse::Message(response.encode_to_vec()),
        )
    } else if method.ends_with("/ExecutePipeline") {
        let response = ExecutePipelineResponse {
            results: vec![document(&cached_name, FROM_FIRESTORE)],
            ..Default::default()
        };
        (
            "ExecutePipeline".to_string(),
            FakeResponse::Paced(vec![response.encode_to_vec()], Duration::ZERO),
        )
    } else {
        (method.to_string(), FakeResponse::empty())
    }
}

/// How many calls the server has answered, leaving out `Listen`. The cache's listener runs in the
/// background, and the server logs a `Listen` whenever the listener closes its request, at a time
/// no read controls.
fn answered_calls(server: &FakeFirestore) -> usize {
    server
        .calls()
        .iter()
        .filter(|call| !call.ends_with("/Listen"))
        .count()
}

/// A server and a memory cache over it whose preloaded collection holds one document, marked as
/// coming from the cache.
async fn cached_document_fixture() -> (FakeFirestore, MemoryCache) {
    let server = FakeFirestore::start(answer).await;
    let cache = FirestoreCache::memory(&server.db)
        .preloaded_collection(COLLECTION)
        .build()
        .await
        .unwrap();
    let name = format!(
        "{}/{COLLECTION}/{CACHED_DOCUMENT}",
        server.db.get_documents_path()
    );
    cache
        .backend()
        .update_doc_by_path(&document(&name, FROM_CACHE))
        .await
        .unwrap();
    (server, cache)
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Get,
    BatchGet,
    Query,
    List,
    Pipeline,
}

impl Read {
    const ALL: [Read; 5] = [
        Read::Get,
        Read::BatchGet,
        Read::Query,
        Read::List,
        Read::Pipeline,
    ];

    /// Where the first document read came from.
    async fn source(self, db: &FirestoreDb) -> FirestoreResult<String> {
        let document = match self {
            Self::Get => db.get_doc(COLLECTION, CACHED_DOCUMENT, None).await?,
            Self::BatchGet => db
                .batch_stream_get_docs_with_errors(COLLECTION, [CACHED_DOCUMENT], None)
                .await?
                .try_next()
                .await?
                .and_then(|(_, document)| document)
                .unwrap(),
            Self::Query => db
                .stream_query_doc_with_errors(FirestoreQueryParams::new(COLLECTION.into()))
                .await?
                .try_next()
                .await?
                .unwrap(),
            Self::List => db
                .stream_list_doc_with_errors(FirestoreListDocParams::new(COLLECTION.into()))
                .await?
                .try_next()
                .await?
                .unwrap(),
            Self::Pipeline => db
                .fluent()
                .pipeline()
                .select()
                .from(COLLECTION)
                .query()
                .await?
                .into_iter()
                .next()
                .unwrap(),
        };
        Ok(source(&document))
    }
}

fn selectors() -> [FirestoreConsistencySelector; 2] {
    [
        FirestoreConsistencySelector::Transaction(vec![1]),
        FirestoreConsistencySelector::ReadTime("2026-11-04T18:30:00Z".parse().unwrap()),
    ]
}

#[tokio::test]
async fn read_through_goes_to_firestore_when_the_session_has_a_consistency_selector() {
    let (server, cache) = cached_document_fixture().await;
    let through_cache = server.db.read_through_cache(&cache);

    for read in Read::ALL {
        let calls_before = answered_calls(&server);
        assert_eq!(read.source(&through_cache).await.unwrap(), FROM_CACHE);
        assert_eq!(
            answered_calls(&server),
            calls_before,
            "{read:?} without selector"
        );

        for selector in selectors() {
            let session = through_cache.clone_with_consistency_selector(selector);
            assert_eq!(
                read.source(&session).await.unwrap(),
                FROM_FIRESTORE,
                "{read:?}"
            );
        }
    }
}

#[tokio::test]
async fn cached_only_fails_when_the_session_has_a_consistency_selector() {
    let (server, cache) = cached_document_fixture().await;
    let cached_only = server.db.read_cached_only(&cache);

    for read in Read::ALL {
        assert_eq!(read.source(&cached_only).await.unwrap(), FROM_CACHE);

        for selector in selectors() {
            let calls_before = answered_calls(&server);
            let session = cached_only.clone_with_consistency_selector(selector);
            let FirestoreError::CacheError(error) = read.source(&session).await.unwrap_err() else {
                panic!("{read:?} did not fail with a cache error");
            };
            assert_eq!(
                error.public.code, "CacheConsistencySelectorUnsupported",
                "{read:?}"
            );
            assert_eq!(answered_calls(&server), calls_before, "{read:?}");
        }
    }
}

#[tokio::test]
async fn a_read_at_a_read_time_does_not_write_the_cache() {
    let (server, cache) = cached_document_fixture().await;
    let at_read_time = server
        .db
        .read_through_cache(&cache)
        .clone_with_consistency_selector(selectors()[1].clone());
    let name = format!("{}/{COLLECTION}/other", server.db.get_documents_path());

    at_read_time
        .get_doc(COLLECTION, "other", None)
        .await
        .unwrap();

    assert!(cache
        .backend()
        .get_doc_by_path(&name)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_batch_get_with_a_field_mask_does_not_write_the_cache() {
    let (server, cache) = cached_document_fixture().await;
    let through_cache = server.db.read_through_cache(&cache);
    let path = |id: &str| format!("{}/{COLLECTION}/{id}", server.db.get_documents_path());

    through_cache
        .batch_stream_get_docs_with_errors(COLLECTION, ["masked"], Some(vec!["source".into()]))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    through_cache
        .batch_stream_get_docs_with_errors(COLLECTION, ["whole"], None)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let backend = cache.backend();
    assert!(backend
        .get_doc_by_path(&path("masked"))
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .get_doc_by_path(&path("whole"))
        .await
        .unwrap()
        .is_some());
}

/// Reads a document the cache holds, in a way the cache cannot answer faithfully, and checks that
/// `read_through_cache` has Firestore answer it with a single call, and that `read_cached_only`
/// fails with `cached_only_code` without contacting Firestore.
async fn assert_read_bypasses_the_cache<Read, Pending>(read: Read, cached_only_code: &str)
where
    Read: Fn(FirestoreDb) -> Pending,
    Pending: std::future::Future<Output = FirestoreResult<Document>>,
{
    let (server, cache) = cached_document_fixture().await;

    let calls_before = answered_calls(&server);
    let document = read(server.db.read_through_cache(&cache)).await.unwrap();
    assert_eq!(source(&document), FROM_FIRESTORE);
    assert_eq!(answered_calls(&server), calls_before + 1);

    let calls_before = answered_calls(&server);
    let FirestoreError::CacheError(error) =
        read(server.db.read_cached_only(&cache)).await.unwrap_err()
    else {
        panic!("the cached-only read did not fail with a cache error");
    };
    assert_eq!(error.public.code, cached_only_code);
    assert_eq!(answered_calls(&server), calls_before);
}

async fn first_queried(db: FirestoreDb, params: FirestoreQueryParams) -> FirestoreResult<Document> {
    Ok(db
        .stream_query_doc_with_errors(params)
        .await?
        .try_next()
        .await?
        .unwrap())
}

#[tokio::test]
async fn a_find_nearest_query_is_not_answered_from_the_cache() {
    assert_read_bypasses_the_cache(
        |db| {
            first_queried(
                db,
                FirestoreQueryParams::new(COLLECTION.into()).with_find_nearest(
                    FirestoreFindNearestOptions::new(
                        "embedding".into(),
                        FirestoreVector(vec![1.0, 2.0]),
                        FirestoreFindNearestDistanceMeasure::Euclidean,
                        1,
                    ),
                ),
            )
        },
        "CacheIncompleteCollection",
    )
    .await;
}

#[tokio::test]
async fn a_query_with_a_field_projection_is_not_answered_from_the_cache() {
    assert_read_bypasses_the_cache(
        |db| {
            first_queried(
                db,
                FirestoreQueryParams::new(COLLECTION.into())
                    .with_return_only_fields(vec!["source".into()]),
            )
        },
        "CacheIncompleteCollection",
    )
    .await;
}

#[tokio::test]
async fn a_query_with_explain_options_is_not_answered_from_the_cache() {
    assert_read_bypasses_the_cache(
        |db| {
            first_queried(
                db,
                FirestoreQueryParams::new(COLLECTION.into())
                    .with_explain_options(FirestoreExplainOptions::new()),
            )
        },
        "CacheIncompleteCollection",
    )
    .await;
}

#[tokio::test]
async fn a_get_with_a_field_mask_is_not_answered_from_the_cache() {
    assert_read_bypasses_the_cache(
        |db| async move {
            db.get_doc(COLLECTION, CACHED_DOCUMENT, Some(vec!["source".into()]))
                .await
        },
        "CacheFieldMaskUnsupported",
    )
    .await;
}

#[tokio::test]
async fn a_batch_get_with_a_field_mask_is_not_answered_from_the_cache() {
    assert_read_bypasses_the_cache(
        |db| async move {
            Ok(db
                .batch_stream_get_docs_with_errors(
                    COLLECTION,
                    [CACHED_DOCUMENT],
                    Some(vec!["source".into()]),
                )
                .await?
                .try_next()
                .await?
                .and_then(|(_, document)| document)
                .unwrap())
        },
        "CacheFieldMaskUnsupported",
    )
    .await;
}

#[tokio::test]
async fn cached_only_reads_fail_once_the_cache_is_shut_down() {
    let (server, cache) = cached_document_fixture().await;
    let cached_only = server.db.read_cached_only(&cache);
    cache.shutdown().await.unwrap();

    for read in [Read::Query, Read::List] {
        let calls_before = answered_calls(&server);
        let FirestoreError::CacheError(error) = read.source(&cached_only).await.unwrap_err() else {
            panic!("{read:?} did not fail with a cache error");
        };
        assert_eq!(error.public.code, "CacheIncompleteCollection", "{read:?}");
        assert_eq!(answered_calls(&server), calls_before, "{read:?}");
    }
}

#[tokio::test]
async fn a_read_through_a_shut_down_cache_does_not_write_the_cache() {
    let (server, cache) = cached_document_fixture().await;
    let through_cache = server.db.read_through_cache(&cache);
    cache.shutdown().await.unwrap();
    let name = format!("{}/{COLLECTION}/other", server.db.get_documents_path());

    let document = through_cache
        .get_doc(COLLECTION, "other", None)
        .await
        .unwrap();

    assert_eq!(source(&document), FROM_FIRESTORE);
    assert!(cache
        .backend()
        .get_doc_by_path(&name)
        .await
        .unwrap()
        .is_none());
}
