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

    /// Forgets any stored resume state for a target, so that the next listen starts fresh.
    ///
    /// This is called when Firestore reports that it removed or reset a target, in which case the
    /// stored token is at best useless and at worst the cause, and when a target is removed from a
    /// listener so that its ID can be reused safely.
    ///
    /// The default implementation does nothing, which keeps existing implementations compiling.
    /// Implement it if your storage is durable: otherwise a stale token is read back on the next
    /// process start, and a resume token belonging to a different query is rejected by Firestore.
    async fn forget_resume_state(&self, target: &FirestoreListenerTarget) -> AnyBoxedErrResult<()> {
        let _ = target;
        Ok(())
    }
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
            directory = ?temp_dir.as_ref(),
            "Using temp dir for listen state storage.",
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

    async fn forget_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target_state_file_name = self.get_file_path(target);

        match std::fs::remove_file(&target_state_file_name) {
            Ok(()) => Ok(()),
            // Nothing stored for this target is the outcome we wanted anyway.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
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

    async fn forget_resume_state(
        &self,
        target: &FirestoreListenerTarget,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tokens.write().await.remove(target);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: u32) -> FirestoreListenerTarget {
        FirestoreListenerTarget::new(id)
    }

    fn token(bytes: &[u8]) -> FirestoreListenerToken {
        FirestoreListenerToken::new(bytes.to_vec())
    }

    fn stored_token(resume_state: Option<FirestoreListenerTargetResumeType>) -> Option<Vec<u8>> {
        match resume_state {
            Some(FirestoreListenerTargetResumeType::Token(token)) => Some(token.into_value()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn mem_storage_forgets_only_the_named_target() {
        let storage = FirestoreMemListenStateStorage::new();
        storage
            .update_resume_token(&target(1), token(b"one"))
            .await
            .unwrap();
        storage
            .update_resume_token(&target(2), token(b"two"))
            .await
            .unwrap();

        storage.forget_resume_state(&target(1)).await.unwrap();

        assert_eq!(
            stored_token(storage.read_resume_state(&target(1)).await.unwrap()),
            None
        );
        assert_eq!(
            stored_token(storage.read_resume_state(&target(2)).await.unwrap()),
            Some(b"two".to_vec())
        );
    }

    #[tokio::test]
    async fn mem_storage_forgetting_an_unknown_target_succeeds() {
        let storage = FirestoreMemListenStateStorage::new();
        storage.forget_resume_state(&target(42)).await.unwrap();
    }

    #[tokio::test]
    async fn temp_files_storage_forgets_only_the_named_target() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FirestoreTempFilesListenStateStorage::with_temp_dir(dir.path());

        storage
            .update_resume_token(&target(1), token(b"one"))
            .await
            .unwrap();
        storage
            .update_resume_token(&target(2), token(b"two"))
            .await
            .unwrap();

        storage.forget_resume_state(&target(1)).await.unwrap();

        assert_eq!(
            stored_token(storage.read_resume_state(&target(1)).await.unwrap()),
            None
        );
        assert_eq!(
            stored_token(storage.read_resume_state(&target(2)).await.unwrap()),
            Some(b"two".to_vec())
        );
    }

    #[tokio::test]
    async fn temp_files_storage_forgetting_an_unstored_target_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FirestoreTempFilesListenStateStorage::with_temp_dir(dir.path());

        storage.forget_resume_state(&target(42)).await.unwrap();
    }
}
