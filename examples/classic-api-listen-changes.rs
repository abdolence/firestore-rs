use chrono::prelude::*;
use firestore::*;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::listen_response;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    some_id: String,
    some_string: String,
    some_num: u64,

    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,
}

const TEST_COLLECTION_NAME: &str = "test-listen";

// The file where we store the cursor/token for the event when we read the last time
const RESUME_TOKEN_FILENAME: &str = "last-read-token.tmp";

// The ID of listener - must be different for different listeners in case you have many instances
const TEST_TARGET_ID: i32 = 42;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Logging with debug enabled
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("firestore=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = FirestoreDb::new(&config_env_var("PROJECT_ID")?)
        .await
        .unwrap();

    let my_struct = MyTestStructure {
        some_id: "test-1".to_string(),
        some_string: "test-str".to_string(),
        some_num: 42,
        created_at: Utc::now(),
    };

    db.fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .generate_document_id()
        .object(&my_struct)
        .execute()
        .await?;

    let shutdown_flag = Arc::new(AtomicBool::new(false));

    let (tx, rx): (UnboundedSender<i8>, UnboundedReceiver<i8>) =
        tokio::sync::mpsc::unbounded_channel();

    let shutdown_handle = tokio::spawn(listener(db, shutdown_flag.clone(), rx));

    // Wait any input until we shutdown
    println!(
        "Waiting any other changes. Try firebase console to change in {} now yourself",
        TEST_COLLECTION_NAME
    );
    std::io::stdin().read(&mut [1])?;

    shutdown_flag.store(true, Ordering::Relaxed);
    tx.send(1).ok();

    shutdown_handle
        .await
        .expect("The task being joined has panicked");

    Ok(())
}

async fn listener(
    db: FirestoreDb,
    shutdown_flag: Arc<AtomicBool>,
    mut shutdown_receiver: UnboundedReceiver<i8>,
) {
    while !shutdown_flag.load(Ordering::Relaxed) {
        let query_params =
            FirestoreQueryParams::new(TEST_COLLECTION_NAME.into()).with_order_by(vec![
                FirestoreQueryOrder::new(
                    path!(MyTestStructure::created_at),
                    FirestoreQueryDirection::Ascending,
                ),
            ]);

        let initial_token_value: Option<Vec<u8>> =
            std::fs::read_to_string(RESUME_TOKEN_FILENAME.clone())
                .map(|s| hex::decode(&s).expect("Unexpected resume token file format"))
                .ok();

        println!(
            "Start listening on {:?}/{:?}... Token: {:?}",
            query_params.parent, query_params.collection_id, initial_token_value
        );
        let mut listen_stream = db
            .listen_doc_changes(
                db.get_database_path().as_str(),
                &query_params,
                HashMap::new(),
                initial_token_value,
                TEST_TARGET_ID,
            )
            .await
            .unwrap();

        loop {
            tokio::select! {
                    _ = shutdown_receiver.recv() => {
                        println!("Exiting from listener...");
                        shutdown_receiver.close();
                        break;
                    }
                    tried = listen_stream.try_next() => {
                        if shutdown_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        else {
                            match tried {
                                Ok(Some(event)) => {
                                    println!("Received a listen response event to handle: {:?}", event);
                                    handle_listen_stream_event(event).await;
                                }
                                Ok(None) => break,
                                Err(err) => {
                                    println!("Listen error occurred {:?}. Restarting in 5 seconds...", err);
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    break;
                                }
                            }
                        }
                }
            }
        }
    }
}

async fn handle_listen_stream_event(event: gcloud_sdk::google::firestore::v1::ListenResponse) {
    match event.response_type {
        Some(listen_response::ResponseType::TargetChange(ref target_change))
            if !target_change.resume_token.is_empty() =>
        {
            std::fs::write(
                RESUME_TOKEN_FILENAME,
                hex::encode(&target_change.resume_token),
            )
            .expect("Unable to write file");
        }
        Some(listen_response::ResponseType::DocumentChange(ref doc_change)) => {
            println!("Doc changed: {:?}", doc_change);

            if let Some(doc) = &doc_change.document {
                let obj: MyTestStructure = FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                    .expect("Deserialized object");
                println!("As object: {:?}", obj);
            }
        }
        _ => {
            println!("Event was handled: {:?}", event);
        }
    }
}
