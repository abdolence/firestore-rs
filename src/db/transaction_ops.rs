use crate::db::safe_document_path;
use crate::{FirestoreDb, FirestoreError, FirestoreResult, FirestoreTransaction};
use gcloud_sdk::google::firestore::v1::Write;
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Clone)]
struct UpdateObjectOperation<'a, T, S>
where
    T: Serialize + Sync + Send,
    S: AsRef<str>,
{
    parent: String,
    collection_id: String,
    document_id: S,
    obj: &'a T,
    update_only: Option<Vec<String>>,
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
            current_document: None,
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
struct DeleteOperation<S>
where
    S: AsRef<str>,
{
    parent: String,
    collection_id: String,
    document_id: S,
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
            current_document: None,
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
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.update_object_at(
            self.db.get_documents_path(),
            collection_id,
            document_id,
            obj,
            update_only,
        )
    }

    pub fn update_object_at<T, S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
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
        })
    }

    pub fn delete_by_id<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.delete_by_id_at(self.db.get_documents_path(), collection_id, document_id)
    }

    pub fn delete_by_id_at<S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.add(DeleteOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
        })
    }
}
