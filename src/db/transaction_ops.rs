use crate::db::safe_document_path;
use crate::{
    FirestoreDb, FirestoreError, FirestoreResult, FirestoreTransaction, FirestoreWritePrecondition,
};
use gcloud_sdk::google::firestore::v1::Write;
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct UpdateObjectOperation<'a, T, S>
where
    T: Serialize + Sync + Send,
    S: AsRef<str>,
{
    pub parent: String,
    pub collection_id: String,
    pub document_id: S,
    pub obj: &'a T,
    pub update_only: Option<Vec<String>>,
    pub precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, T, S> TryInto<Write> for UpdateObjectOperation<'a, T, S>
where
    T: Serialize + Sync + Send,
    S: AsRef<str>,
{
    type Error = FirestoreError;

    fn try_into(self) -> Result<Write, Self::Error> {
        Ok(Write {
            update_mask: self.update_only.map({
                |vf| gcloud_sdk::google::firestore::v1::DocumentMask {
                    field_paths: vf.iter().map(|f| f.to_string()).collect(),
                }
            }),
            update_transforms: vec![],
            current_document: self.precondition.map(|cond| cond.try_into()).transpose()?,
            operation: Some(gcloud_sdk::google::firestore::v1::write::Operation::Update(
                FirestoreDb::serialize_to_doc(
                    &safe_document_path(
                        &self.parent,
                        self.collection_id.as_str(),
                        self.document_id.as_ref(),
                    )?,
                    &self.obj,
                )?,
            )),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct DeleteOperation<S>
where
    S: AsRef<str>,
{
    pub parent: String,
    pub collection_id: String,
    pub document_id: S,
    pub precondition: Option<FirestoreWritePrecondition>,
}

impl<S> TryInto<Write> for DeleteOperation<S>
where
    S: AsRef<str>,
{
    type Error = FirestoreError;

    fn try_into(self) -> Result<Write, Self::Error> {
        Ok(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: self.precondition.map(|cond| cond.try_into()).transpose()?,
            operation: Some(gcloud_sdk::google::firestore::v1::write::Operation::Delete(
                safe_document_path(
                    &self.parent,
                    self.collection_id.as_str(),
                    self.document_id.as_ref(),
                )?,
            )),
        })
    }
}

impl<'a> FirestoreTransaction<'a> {
    pub fn update_object<T, S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.update_object_at(
            self.db.get_documents_path().as_str(),
            collection_id,
            document_id,
            obj,
            update_only,
            precondition,
        )
    }

    pub fn update_object_at<T, S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.add(UpdateObjectOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            obj,
            update_only,
            precondition,
        })
    }

    pub fn delete_by_id<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        let parent = self.db.get_documents_path().clone();
        self.delete_by_id_at(parent.as_str(), collection_id, document_id, precondition)
    }

    pub fn delete_by_id_at<S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.add(DeleteOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            precondition,
        })
    }
}
