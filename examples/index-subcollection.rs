use firestore::*;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_stream::StreamExt;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{name}: {e}"))
}

// Example structure to play with; used only for its field names via `path!()`.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Post {
    author_id: String,
    published: bool,
    created_at: FirestoreTimestamp,
    tags: Vec<String>,
}

// Dedicated IDs, so this example never touches a collection real data lives in. The
// subcollection's ID is also the collection group name a composite index is declared on.
const USERS_COLLECTION: FirestoreCollectionId =
    FirestoreCollectionId::from_static("firestore-rs-example-users");
const POSTS_COLLECTION: FirestoreCollectionId =
    FirestoreCollectionId::from_static("firestore-rs-example-posts");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create an instance
    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?).await?;

    // The first index serves one user's own posts, scoped to their subcollection. The second
    // is declared with `.all_descendants()`, so it serves the same shape of query across every
    // user's posts at once.
    let declared = db
        .fluent()
        .indexes()
        .collection_group(POSTS_COLLECTION)
        .composite(|i| {
            i.indexes([
                i.index([
                    i.field(path!(Post::published)).asc(),
                    i.field(path!(Post::created_at)).desc(),
                ]),
                i.index([
                    i.field(path!(Post::tags)).array_contains(),
                    i.field(path!(Post::created_at)).desc(),
                ])
                .all_descendants(),
            ])
        });

    println!("Syncing indexes...");
    let report = declared
        .wait_until_ready(Duration::from_secs(900))
        .sync()
        .await?;
    println!("{report}");

    let user_ids = ["alice", "bob"];
    for user_id in user_ids {
        let user_path = db.parent_path(&USERS_COLLECTION, user_id)?;

        // Clear out a previous run's posts before writing this run's.
        db.fluent()
            .delete()
            .from(&POSTS_COLLECTION)
            .parent(&user_path)
            .document_id("rust-post")
            .execute()
            .await?;
        db.fluent()
            .delete()
            .from(&POSTS_COLLECTION)
            .parent(&user_path)
            .document_id("draft-post")
            .execute()
            .await?;

        let published_post = Post {
            author_id: user_id.to_string(),
            published: true,
            created_at: FirestoreTimestamp::now(),
            tags: vec!["rust".to_string(), "firestore".to_string()],
        };
        db.fluent()
            .insert()
            .into(&POSTS_COLLECTION)
            .document_id("rust-post")
            .parent(&user_path)
            .object(&published_post)
            .execute::<()>()
            .await?;

        let draft_post = Post {
            author_id: user_id.to_string(),
            published: false,
            created_at: FirestoreTimestamp::now(),
            tags: vec!["draft".to_string()],
        };
        db.fluent()
            .insert()
            .into(&POSTS_COLLECTION)
            .document_id("draft-post")
            .parent(&user_path)
            .object(&draft_post)
            .execute::<()>()
            .await?;
    }

    let alice_path = db.parent_path(&USERS_COLLECTION, "alice")?;

    println!("Alice's own published posts, newest first:");
    let alice_published: BoxStream<Post> = db
        .fluent()
        .select()
        .from(&POSTS_COLLECTION)
        .parent(&alice_path)
        .filter(|q| q.for_all([q.field(path!(Post::published)).eq(true)]))
        .order(|o| o.fields([o.field(path!(Post::created_at)).desc()]))
        .obj()
        .stream_query()
        .await?;
    let as_vec: Vec<Post> = alice_published.collect().await;
    println!("{as_vec:?}");

    println!("Every user's posts tagged \"rust\", newest first:");
    let tagged_rust: BoxStream<Post> = db
        .fluent()
        .select()
        .from(&POSTS_COLLECTION)
        .all_descendants()
        .filter(|q| q.for_all([q.field(path!(Post::tags)).array_contains("rust")]))
        .order(|o| o.fields([o.field(path!(Post::created_at)).desc()]))
        .obj()
        .stream_query()
        .await?;
    let as_vec: Vec<Post> = tagged_rust.collect().await;
    println!("{as_vec:?}");

    Ok(())
}
