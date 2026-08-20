use firestore::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    one_more_string: String,
    some_num: u64,
    created_at: FirestoreTimestamp,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    const TEST_COLLECTION_NAME: &str = "test-caching";

    // Populate the collection before the cache is created, so that the preload below has
    // something to read.
    if db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .one("test-0")
        .await?
        .is_none()
    {
        println!("Populating a test collection");
        let batch_writer = db.create_simple_batch_writer().await?;
        let mut current_batch = batch_writer.new_batch();

        for i in 0..500 {
            let my_struct = MyTestStructure {
                some_id: format!("test-{}", i),
                some_string: "Test".to_string(),
                one_more_string: "Test2".to_string(),
                some_num: i,
                created_at: FirestoreTimestamp::now(),
            };

            db.fluent()
                .update()
                .in_col(TEST_COLLECTION_NAME)
                .document_id(&my_struct.some_id)
                .object(&my_struct)
                .add_to_batch(&mut current_batch)?;
        }
        current_batch.write().await?;
    }

    // Create the cache. `preloaded_collection` downloads the whole collection, which is what
    // makes cached `list` and `select` possible: a partially populated collection could only
    // answer them with incomplete results, so the library refuses to do so.
    //
    // `data_dir` keeps the cache database and the listener resume tokens in the same directory,
    // so that they are always lost or kept together. Listener target IDs are assigned
    // automatically, and `build()` loads the cache and starts listening for changes.
    //
    // Because the data survives restarts, the collection is only downloaded on the first run.
    let cache_dir = std::env::temp_dir().join("firestore-caching-example");
    std::fs::create_dir_all(&cache_dir)?;

    let cache = FirestorePersistentCache::builder(&db)
        .name("example-persistent-cache")
        .data_dir(&cache_dir)
        .preloaded_collection(TEST_COLLECTION_NAME)
        .build()
        .await?;

    // Read through the cache: served from the cache when possible, from Firestore otherwise.
    let cached_db = db.read_through_cache(&cache);

    println!("Getting by id");
    let my_struct1: Option<MyTestStructure> = cached_db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one("test-1")
        .await?;

    println!("{:?}", my_struct1);

    println!("Getting batch by ids");
    let my_struct_stream: BoxStream<FirestoreResult<(String, Option<MyTestStructure>)>> = cached_db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .batch_with_errors(["test-1", "test-2"])
        .await?;

    let my_structs = my_struct_stream.try_collect::<Vec<_>>().await?;
    println!("{:?}", my_structs);

    // Listing and querying are served from the cache because the collection was preloaded.
    println!("Listing from cache");
    let all_items_stream = cached_db
        .fluent()
        .list()
        .from(TEST_COLLECTION_NAME)
        .obj::<MyTestStructure>()
        .stream_all_with_errors()
        .await?;

    let listed_items = all_items_stream.try_collect::<Vec<_>>().await?;
    println!("{:?}", listed_items.len());

    println!("Querying from cache");
    let queried_items_stream = cached_db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .filter(|q| {
            q.for_all(
                q.field(path!(MyTestStructure::some_num))
                    .greater_than_or_equal(250),
            )
        })
        .order_by([(
            path!(MyTestStructure::some_num),
            FirestoreQueryDirection::Ascending,
        )])
        .obj::<MyTestStructure>()
        .stream_query_with_errors()
        .await?;

    let queried_items = queried_items_stream.try_collect::<Vec<_>>().await?;
    println!("{:?}", queried_items.len());

    // Reading only from the cache never contacts Firestore. Because the collection is preloaded,
    // the listing below is complete; on a collection added with `.collection(..)` it would
    // return an error instead of a partial result.
    let cached_only_db = db.read_cached_only(&cache);

    println!("Getting by id from cache only");
    let my_struct2: Option<MyTestStructure> = cached_only_db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .obj()
        .one("test-1")
        .await?;

    println!("{:?}", my_struct2);

    cache.shutdown().await?;

    Ok(())
}
