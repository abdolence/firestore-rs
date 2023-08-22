use crate::errors::AnyBoxedErrResult;
use crate::{FirestoreListenerTarget, FirestoreListenerTargetResumeType, FirestoreListenerToken};
use async_trait::async_trait;
use rvstruct::ValueStruct;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::*;

#[async_trait]
pub trait FirestoreResumeStateStorage {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> AnyBoxedErrResult<Option<FirestoreListenerTargetResumeType>>;

    async fn update_resume_token(
        &self,
        target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> AnyBoxedErrResult<()>;
}

#[derive(Clone, Debug)]
pub struct FirestoreTempFilesListenStateStorage {
    temp_dir: Option<std::path::PathBuf>,
}

impl FirestoreTempFilesListenStateStorage {
    pub fn new() -> Self {
        Self { temp_dir: None }
    }

    pub fn with_temp_dir<P: AsRef<std::path::Path>>(temp_dir: P) -> Self {
        debug!(
            "Using temp dir for listen state storage: {:?}",
            temp_dir.as_ref()
        );
        Self {
            temp_dir: Some(temp_dir.as_ref().to_path_buf()),
        }
    }

    fn get_file_path(&self, target: &FirestoreListenerTarget) -> std::path::PathBuf {
        let target_state_file_name = format!("{}.{}.tmp", TOKEN_FILENAME_PREFIX, target.value());
        match &self.temp_dir {
            Some(temp_dir) => temp_dir.join(target_state_file_name),
            None => std::path::PathBuf::from(target_state_file_name),
        }
    }
}

const TOKEN_FILENAME_PREFIX: &str = "firestore-listen-token";

#[async_trait]
impl FirestoreResumeStateStorage for FirestoreTempFilesListenStateStorage {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Result<Option<FirestoreListenerTargetResumeType>, Box<dyn std::error::Error + Send + Sync>>
    {
        let target_state_file_name = self.get_file_path(target);
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
        let target_state_file_name = self.get_file_path(target);

        Ok(std::fs::write(
            target_state_file_name,
            hex::encode(token.value()),
        )?)
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreMemListenStateStorage {
    tokens: Arc<RwLock<HashMap<FirestoreListenerTarget, FirestoreListenerToken>>>,
}

impl FirestoreMemListenStateStorage {
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_token(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Option<FirestoreListenerToken> {
        self.tokens.read().await.get(target).cloned()
    }
}

#[async_trait]
impl FirestoreResumeStateStorage for FirestoreMemListenStateStorage {
    async fn read_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Result<Option<FirestoreListenerTargetResumeType>, Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(self
            .get_token(target)
            .await
            .map(FirestoreListenerTargetResumeType::Token))
    }

    async fn update_resume_token(
        &self,
        target: &FirestoreListenerTarget,
        token: FirestoreListenerToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tokens.write().await.insert(target.clone(), token);
        Ok(())
    }
}
