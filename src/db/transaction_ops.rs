use crate::db::safe_document_path;
use crate::{
    FirestoreDb, FirestoreError, FirestoreFieldTransform, FirestoreResult,
    FirestoreWritePrecondition,
};
use gcloud_sdk::google::firestore::v1::Write;
use serde::Serialize;

#[derive(Debug, PartialEq, Clone)]
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
    pub update_transforms: Vec<FirestoreFieldTransform>,
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
            update_transforms: self
                .update_transforms
                .into_iter()
                .map(|s| s.try_into())
                .collect::<FirestoreResult<
                Vec<gcloud_sdk::google::firestore::v1::document_transform::FieldTransform>,
            >>()?,
            current_document: self.precondition.map(|cond| cond.try_into()).transpose()?,
            operation: Some(gcloud_sdk::google::firestore::v1::write::Operation::Update(
                FirestoreDb::serialize_to_doc(
                    safe_document_path(
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

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TransformObjectOperation<S>
where
    S: AsRef<str>,
{
    pub parent: String,
    pub collection_id: String,
    pub document_id: S,
    pub precondition: Option<FirestoreWritePrecondition>,
    pub transforms: Vec<FirestoreFieldTransform>,
}

impl<S> TryInto<Write> for TransformObjectOperation<S>
where
    S: AsRef<str>,
{
    type Error = FirestoreError;

    fn try_into(self) -> Result<Write, Self::Error> {
        Ok(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: self.precondition.map(|cond| cond.try_into()).transpose()?,
            operation: Some(gcloud_sdk::google::firestore::v1::write::Operation::Transform(
                gcloud_sdk::google::firestore::v1::DocumentTransform {
                    document: safe_document_path(
                        &self.parent,
                        self.collection_id.as_str(),
                        self.document_id.as_ref(),
                    )?,
                    field_transforms: self.transforms
                        .into_iter()
                        .map(|s| s.try_into())
                        .collect::<FirestoreResult<Vec<gcloud_sdk::google::firestore::v1::document_transform::FieldTransform>>>()?
                }
            )),
        })
    }
}

/// Queues writes on a [`FirestoreTransaction`](crate::FirestoreTransaction) or on
/// [`FirestoreTransactionData`](crate::FirestoreTransactionData) - both stage writes the same
/// way, which is why this is a trait rather than inherent methods on either type. Nothing here
/// talks to Firestore: writes are only sent once the transaction is committed.
pub trait FirestoreTransactionOps {
    /// Queues a single write, converting `write` via its `TryInto<Write>` impl.
    ///
    /// This is the primitive every other method on this trait queues through; call it directly
    /// only when building a write Firestore's higher-level helpers do not cover.
    ///
    /// Returns an error if the conversion fails.
    fn add<I>(&mut self, write: I) -> FirestoreResult<&mut Self>
    where
        I: TryInto<gcloud_sdk::google::firestore::v1::Write, Error = FirestoreError>;

    /// Returns the documents path that `update_object`, `delete_by_id` and `transform` resolve
    /// against when no explicit parent is given.
    fn get_documents_path(&self) -> &String;

    /// Queues a create-or-replace of `obj` at `document_id` in `collection_id`, under this
    /// transaction's own documents path.
    ///
    /// A Firestore transaction cannot create a document with a server-generated ID, so this -
    /// with an explicit `document_id` - is how a transaction creates a document. `update_only`
    /// restricts the write to those field paths, leaving the rest of an existing document
    /// untouched; `precondition` fails the whole commit if the document's current state does not
    /// match it; `update_transforms` runs additional server-side transforms atomically with the
    /// write.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation, or
    /// if `obj` cannot be serialized.
    fn update_object<T, S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        update_transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        T: Serialize + Sync + Send,
        S: AsRef<str>,
    {
        self.update_object_at(
            &self.get_documents_path().clone(),
            collection_id,
            document_id,
            obj,
            update_only,
            precondition,
            update_transforms,
        )
    }

    /// Same as [`update_object`](Self::update_object), at an explicit `parent` path instead of
    /// this transaction's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation, or
    /// if `obj` cannot be serialized.
    fn update_object_at<T, S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        obj: &T,
        update_only: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        update_transforms: Vec<FirestoreFieldTransform>,
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
            update_transforms,
        })
    }

    /// Queues a delete of `document_id` in `collection_id`, under this transaction's own
    /// documents path. `precondition` fails the whole commit if the document's current state
    /// does not match it.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    fn delete_by_id<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.delete_by_id_at(
            &self.get_documents_path().clone(),
            collection_id,
            document_id,
            precondition,
        )
    }

    /// Same as [`delete_by_id`](Self::delete_by_id), at an explicit `parent` path instead of this
    /// transaction's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    fn delete_by_id_at<S>(
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

    /// Queues one or more atomic field transforms (for example `serverTimestamp()` or a numeric
    /// increment) on `document_id` in `collection_id`, under this transaction's own documents
    /// path, without reading or resending the rest of the document. `precondition` fails the
    /// whole commit if the document's current state does not match it.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    fn transform<S>(
        &mut self,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.transform_at(
            &self.get_documents_path().clone(),
            collection_id,
            document_id,
            precondition,
            transforms,
        )
    }

    /// Same as [`transform`](Self::transform), at an explicit `parent` path instead of this
    /// transaction's own documents path.
    ///
    /// Returns an error if `collection_id` or `document_id` fails Firestore's ID validation.
    fn transform_at<S>(
        &mut self,
        parent: &str,
        collection_id: &str,
        document_id: S,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> FirestoreResult<&mut Self>
    where
        S: AsRef<str>,
    {
        self.add(TransformObjectOperation {
            parent: parent.to_string(),
            collection_id: collection_id.to_string(),
            document_id,
            precondition,
            transforms,
        })
    }
}
