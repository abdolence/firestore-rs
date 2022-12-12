use async_trait::async_trait;
use chrono::prelude::*;
use firestore::*;
use rvstruct::ValueStruct;
use serde::{Deserialize, Serialize};
use std::io::Read;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|e| format!("{}: {}", name, e))
}

// Example structure to play with
#[derive(Debug, Clone, Deserialize, Serialize)]
struct MyTestStructure {
    #[serde(alias = "_firestore_id")]
    doc_id: Option<String>,
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
const TEST_TARGET_ID: FirestoreListenerTarget = FirestoreListenerTarget::new(42_i32);

#[derive(Clone)]
pub struct TempFileTokenStorage;

#[async_trait]
impl FirestoreResumeStateStorage for TempFileTokenStorage {
    async fn read_resume_state(
        &self,
        _target: &FirestoreListenerTarget,
    ) -> Result<Option<FirestoreListenerTargetResumeType>, Box<dyn std::error::Error + Send + Sync>>
    {
        let token = std::fs::read_to_string(RESUME_TOKEN_FILENAME.clone())
            .ok()
            .map(|str| {
                hex::decode(&str)
                    .map(FirestoreListenerToken::new)
                    .map(FirestoreListenerTargetResumeType::Token)
                    .map_err(|e| Box::new(e))
            })
            .transpose()?;

        Ok(token)
    }

    async fn update_resume_token(
        &self,
        _target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(std::fs::write(
            RESUME_TOKEN_FILENAME,
            hex::encode(token.value()),
        )?)
    }
}

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
        doc_id: None,
        some_id: "test-1".to_string(),
        some_string: "test-str".to_string(),
        some_num: 42,
        created_at: Utc::now(),
    };

    let new_doc: MyTestStructure = db
        .fluent()
        .insert()
        .into(TEST_COLLECTION_NAME)
        .generate_document_id()
        .object(&my_struct)
        .execute()
        .await?;

    let mut query_listener = db
        .fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .listen()
        .target(TEST_TARGET_ID, TempFileTokenStorage)
        .await?;

    query_listener
        .start(|event| async move {
            match event {
                FirestoreListenEvent::DocumentChange(ref doc_change) => {
                    println!("Doc changed: {:?}", doc_change);

                    if let Some(doc) = &doc_change.document {
                        let obj: MyTestStructure =
                            FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                                .expect("Deserialized object");
                        println!("As object: {:?}", obj);
                    }
                }
                _ => {
                    println!("Received a listen response event to handle: {:?}", event);
                }
            }

            Ok(())
        })
        .await?;
    // Wait any input until we shutdown
    println!(
        "Waiting any other changes. Try firebase console to change in {} now yourself",
        TEST_COLLECTION_NAME
    );
    std::io::stdin().read(&mut [1])?;

    query_listener.shutdown().await?;

    // Listener for documents
    println!(
        "Waiting any other changes for specified doc {:?}. Try firebase console to change in {} now yourself",
        new_doc.doc_id,
        TEST_COLLECTION_NAME
    );
    let mut docs_listener = db
        .fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .batch_listen([new_doc.doc_id.expect("Doc must be created before")])
        .target(TEST_TARGET_ID, TempFileTokenStorage)
        .await?;

    docs_listener
        .start(|event| async move {
            match event {
                FirestoreListenEvent::DocumentChange(ref doc_change) => {
                    println!("Doc changed: {:?}", doc_change);

                    if let Some(doc) = &doc_change.document {
                        let obj: MyTestStructure =
                            FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                                .expect("Deserialized object");
                        println!("As object: {:?}", obj);
                    }
                }
                _ => {
                    println!("Received a listen response event to handle: {:?}", event);
                }
            }

            Ok(())
        })
        .await?;

    // Wait any input until we shutdown
    println!(
        "Waiting any other changes. Try firebase console to change in {} now yourself",
        TEST_COLLECTION_NAME
    );
    std::io::stdin().read(&mut [1])?;

    docs_listener.shutdown().await?;

    Ok(())
}
