use chrono::prelude::*;
use firestore::*;
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
const RESUME_TOKEN_FILENAME: &str = "last-read-token";

// The IDs of targets - must be different for different listener targets/listeners in case you have many instances
const TEST_TARGET_ID_BY_QUERY: FirestoreListenerTarget = FirestoreListenerTarget::new(42_i32);
const TEST_TARGET_ID_BY_DOC_IDS: FirestoreListenerTarget = FirestoreListenerTarget::new(17_i32);

#[derive(Clone)]
pub struct TempFileTokenStorage;

#[async_trait]
impl FirestoreResumeStateStorage for TempFileTokenStorage {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Result<Option<FirestoreListenerTargetResumeType>, Box<dyn std::error::Error + Send + Sync>>
    {
        let target_state_file_name = format!("{}.{}.tmp", RESUME_TOKEN_FILENAME, target.value());
        let token = std::fs::read_to_string(target_state_file_name)
            .ok()
            .map(|str| {
                hex::decode(str)
                    .map(FirestoreListenerToken::new)
                    .map(FirestoreListenerTargetResumeType::Token)
                    .map_err(Box::new)
            })
            .transpose()?;

        Ok(token)
    }

    async fn update_resume_token(
        &self,
        target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target_state_file_name = format!("{}.{}.tmp", RESUME_TOKEN_FILENAME, target.value());

        Ok(std::fs::write(
            target_state_file_name,
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

    let mut listener = db.create_listener(TempFileTokenStorage).await?;

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

    db.fluent()
        .select()
        .from(TEST_COLLECTION_NAME)
        .listen()
        .add_target(TEST_TARGET_ID_BY_QUERY, &mut listener)?;

    db.fluent()
        .select()
        .by_id_in(TEST_COLLECTION_NAME)
        .batch_listen([new_doc.doc_id.clone().expect("Doc must be created before")])
        .add_target(TEST_TARGET_ID_BY_DOC_IDS, &mut listener)?;

    listener
        .start(|event| async move {
            match event {
                FirestoreListenEvent::DocumentChange(ref doc_change) => {
                    if let Some(doc) = &doc_change.document {
                        let obj: MyTestStructure =
                            FirestoreDb::deserialize_doc_to::<MyTestStructure>(doc)
                                .expect("Deserialized object");
                        println!("As object: {:?}", obj);
                    }
                }
                _ => {
                    println!("Received a listen response event to handle");
                }
            }

            Ok(())
        })
        .await?;
    // Wait any input until we shutdown
    println!(
        "Waiting any other changes. Try firebase console to change in {} now yourself. New doc created id: {:?}",
        TEST_COLLECTION_NAME,new_doc.doc_id
    );
    std::io::stdin().read(&mut [1])?;

    listener.shutdown().await?;

    Ok(())
}
