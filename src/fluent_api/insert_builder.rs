use crate::{FirestoreCreateSupport, FirestoreResult};
use gcloud_sdk::google::firestore::v1::Document;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct FirestoreInsertInitialBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
}

impl<'a, D> FirestoreInsertInitialBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    #[inline]
    pub fn into(self, collection_id: &str) -> FirestoreInsertDocIdBuilder<'a, D> {
        FirestoreInsertDocIdBuilder::new(self.db, collection_id.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreInsertDocIdBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
}

impl<'a, D> FirestoreInsertDocIdBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String) -> Self {
        Self { db, collection_id }
    }

    #[inline]
    pub fn document_id<S>(self, document_id: S) -> FirestoreInsertDocObjBuilder<'a, D>
    where
        S: AsRef<str> + Send,
    {
        FirestoreInsertDocObjBuilder::new(
            self.db,
            self.collection_id,
            Some(document_id.as_ref().to_string()),
        )
    }

    #[inline]
    pub fn generate_document_id(self) -> FirestoreInsertDocObjBuilder<'a, D> {
        FirestoreInsertDocObjBuilder::new(self.db, self.collection_id, None)
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreInsertDocObjBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: Option<String>,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreInsertDocObjBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, collection_id: String, document_id: Option<String>) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent: None,
            return_only_fields: None,
        }
    }

    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            parent: Some(parent.as_ref().to_string()),
            ..self
        }
    }

    #[inline]
    pub fn return_only_fields<I>(self, return_only_fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            return_only_fields: Some(
                return_only_fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    #[inline]
    pub fn document(self, document: Document) -> FirestoreInsertDocExecuteBuilder<'a, D> {
        FirestoreInsertDocExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.document_id,
            self.parent,
            document,
            self.return_only_fields,
        )
    }

    #[inline]
    pub fn object<T>(self, object: &'a T) -> FirestoreInsertObjExecuteBuilder<'a, D, T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreInsertObjExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.parent,
            self.document_id,
            object,
            self.return_only_fields,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreInsertDocExecuteBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    db: &'a D,
    collection_id: String,
    document_id: Option<String>,
    parent: Option<String>,
    document: Document,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreInsertDocExecuteBuilder<'a, D>
where
    D: FirestoreCreateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        document_id: Option<String>,
        parent: Option<String>,
        document: Document,
        return_only_fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            db,
            collection_id,
            document_id,
            parent,
            document,
            return_only_fields,
        }
    }

    pub async fn execute(self) -> FirestoreResult<Document> {
        if let Some(parent) = self.parent {
            self.db
                .create_doc_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.document,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .create_doc(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.document,
                    self.return_only_fields,
                )
                .await
        }
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreInsertObjExecuteBuilder<'a, D, T>
where
    D: FirestoreCreateSupport,
    T: Serialize + Sync + Send,
{
    db: &'a D,
    collection_id: String,
    parent: Option<String>,
    document_id: Option<String>,
    object: &'a T,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D, T> FirestoreInsertObjExecuteBuilder<'a, D, T>
where
    D: FirestoreCreateSupport,
    T: Serialize + Sync + Send,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        parent: Option<String>,
        document_id: Option<String>,
        object: &'a T,
        return_only_fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            db,
            collection_id,
            parent,
            document_id,
            object,
            return_only_fields,
        }
    }

    pub async fn execute<O>(self) -> FirestoreResult<O>
    where
        for<'de> O: Deserialize<'de>,
    {
        if let Some(parent) = self.parent {
            self.db
                .create_obj_at_return_fields(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .create_obj_return_fields(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.return_only_fields,
                )
                .await
        }
    }
}
