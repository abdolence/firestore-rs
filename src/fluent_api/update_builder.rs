use crate::document_transform_builder::FirestoreTransformBuilder;
use crate::{
    FirestoreBatch, FirestoreBatchWriter, FirestoreFieldTransform, FirestoreResult,
    FirestoreTransaction, FirestoreUpdateSupport, FirestoreWritePrecondition,
};
use gcloud_sdk::google::firestore::v1::Document;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct FirestoreUpdateInitialBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    db: &'a D,
    update_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreUpdateInitialBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            update_only_fields: None,
        }
    }

    #[inline]
    pub fn fields<I>(self, update_only_fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            update_only_fields: Some(
                update_only_fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    #[inline]
    pub fn in_col(self, collection_id: &str) -> FirestoreUpdateDocObjBuilder<'a, D> {
        FirestoreUpdateDocObjBuilder::new(
            self.db,
            collection_id.to_string(),
            self.update_only_fields,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreUpdateDocObjBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    db: &'a D,
    collection_id: String,
    update_only_fields: Option<Vec<String>>,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
    precondition: Option<FirestoreWritePrecondition>,
    transforms: Vec<FirestoreFieldTransform>,
}

impl<'a, D> FirestoreUpdateDocObjBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        update_only_fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            db,
            collection_id,
            update_only_fields,
            parent: None,
            return_only_fields: None,
            precondition: None,
            transforms: vec![],
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
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    #[inline]
    pub fn transforms<FN>(self, doc_transform: FN) -> Self
    where
        FN: Fn(FirestoreTransformBuilder) -> Vec<FirestoreFieldTransform>,
    {
        Self {
            transforms: doc_transform(FirestoreTransformBuilder::new()),
            ..self
        }
    }

    #[inline]
    pub fn document(self, document: Document) -> FirestoreUpdateDocExecuteBuilder<'a, D> {
        FirestoreUpdateDocExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.update_only_fields,
            document,
            self.return_only_fields,
            self.precondition,
        )
    }

    #[inline]
    pub fn document_id<S>(self, document_id: S) -> FirestoreUpdateObjInitExecuteBuilder<'a, D>
    where
        S: AsRef<str> + Send,
    {
        FirestoreUpdateObjInitExecuteBuilder::new(
            self.db,
            self.collection_id,
            self.update_only_fields,
            self.parent,
            document_id.as_ref().to_string(),
            self.return_only_fields,
            self.precondition,
            self.transforms,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreUpdateDocExecuteBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    db: &'a D,
    collection_id: String,
    update_only_fields: Option<Vec<String>>,
    document: Document,
    return_only_fields: Option<Vec<String>>,
    precondition: Option<FirestoreWritePrecondition>,
}

impl<'a, D> FirestoreUpdateDocExecuteBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        update_only_fields: Option<Vec<String>>,
        document: Document,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
    ) -> Self {
        Self {
            db,
            collection_id,
            update_only_fields,
            document,
            return_only_fields,
            precondition,
        }
    }

    pub async fn execute(self) -> FirestoreResult<Document> {
        self.db
            .update_doc(
                self.collection_id.as_str(),
                self.document,
                self.update_only_fields,
                self.return_only_fields,
                self.precondition,
            )
            .await
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreUpdateObjInitExecuteBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    db: &'a D,
    collection_id: String,
    update_only_fields: Option<Vec<String>>,
    parent: Option<String>,
    document_id: String,
    return_only_fields: Option<Vec<String>>,
    precondition: Option<FirestoreWritePrecondition>,
    transforms: Vec<FirestoreFieldTransform>,
}

impl<'a, D> FirestoreUpdateObjInitExecuteBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        update_only_fields: Option<Vec<String>>,
        parent: Option<String>,
        document_id: String,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> Self {
        Self {
            db,
            collection_id,
            update_only_fields,
            parent,
            document_id,
            return_only_fields,
            precondition,
            transforms,
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
    pub fn object<T>(self, object: &'a T) -> FirestoreUpdateObjExecuteBuilder<'a, D, T>
    where
        T: Serialize + Sync + Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreUpdateObjExecuteBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.update_only_fields,
            self.parent,
            self.document_id,
            object,
            self.return_only_fields,
            self.precondition,
            self.transforms,
        )
    }

    #[inline]
    pub fn transforms<FN>(self, doc_transform: FN) -> Self
    where
        FN: Fn(FirestoreTransformBuilder) -> Vec<FirestoreFieldTransform>,
    {
        Self {
            transforms: doc_transform(FirestoreTransformBuilder::new()),
            ..self
        }
    }

    #[inline]
    pub fn only_transform(self) -> FirestoreUpdateOnlyTransformBuilder<'a, D> {
        FirestoreUpdateOnlyTransformBuilder::new(
            self.db,
            self.collection_id.to_string(),
            self.parent,
            self.document_id,
            self.precondition,
            self.transforms,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreUpdateObjExecuteBuilder<'a, D, T>
where
    D: FirestoreUpdateSupport,
    T: Serialize + Sync + Send,
{
    db: &'a D,
    collection_id: String,
    update_only_fields: Option<Vec<String>>,
    parent: Option<String>,
    document_id: String,
    object: &'a T,
    return_only_fields: Option<Vec<String>>,
    precondition: Option<FirestoreWritePrecondition>,
    transforms: Vec<FirestoreFieldTransform>,
}

impl<'a, D, T> FirestoreUpdateObjExecuteBuilder<'a, D, T>
where
    D: FirestoreUpdateSupport,
    T: Serialize + Sync + Send,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        update_only_fields: Option<Vec<String>>,
        parent: Option<String>,
        document_id: String,
        object: &'a T,
        return_only_fields: Option<Vec<String>>,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> Self {
        Self {
            db,
            collection_id,
            update_only_fields,
            parent,
            document_id,
            object,
            return_only_fields,
            precondition,
            transforms,
        }
    }

    pub async fn execute<O>(self) -> FirestoreResult<O>
    where
        for<'de> O: Deserialize<'de>,
    {
        if let Some(parent) = self.parent {
            self.db
                .update_obj_at(
                    parent.as_str(),
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.update_only_fields,
                    self.return_only_fields,
                    self.precondition,
                )
                .await
        } else {
            self.db
                .update_obj(
                    self.collection_id.as_str(),
                    self.document_id,
                    self.object,
                    self.update_only_fields,
                    self.return_only_fields,
                    self.precondition,
                )
                .await
        }
    }

    #[inline]
    pub fn transforms<FN>(self, transforms_builder: FN) -> Self
    where
        FN: Fn(FirestoreTransformBuilder) -> Vec<FirestoreFieldTransform>,
    {
        Self {
            transforms: transforms_builder(FirestoreTransformBuilder::new()),
            ..self
        }
    }

    #[inline]
    pub fn add_to_transaction<'t>(
        self,
        transaction: &'a mut FirestoreTransaction<'t>,
    ) -> FirestoreResult<&'a mut FirestoreTransaction<'t>> {
        if let Some(parent) = self.parent {
            transaction.update_object_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.object,
                self.update_only_fields,
                self.precondition,
                self.transforms,
            )
        } else {
            transaction.update_object(
                self.collection_id.as_str(),
                self.document_id,
                self.object,
                self.update_only_fields,
                self.precondition,
                self.transforms,
            )
        }
    }

    #[inline]
    pub fn add_to_batch<'t, W>(
        self,
        batch: &'a mut FirestoreBatch<'t, W>,
    ) -> FirestoreResult<&'a mut FirestoreBatch<'t, W>>
    where
        W: FirestoreBatchWriter,
    {
        if let Some(parent) = self.parent {
            batch.update_object_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.object,
                self.update_only_fields,
                self.precondition,
                self.transforms,
            )
        } else {
            batch.update_object(
                self.collection_id.as_str(),
                self.document_id,
                self.object,
                self.update_only_fields,
                self.precondition,
                self.transforms,
            )
        }
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreUpdateOnlyTransformBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    _db: &'a D,
    collection_id: String,
    parent: Option<String>,
    document_id: String,
    precondition: Option<FirestoreWritePrecondition>,
    transforms: Vec<FirestoreFieldTransform>,
}

impl<'a, D> FirestoreUpdateOnlyTransformBuilder<'a, D>
where
    D: FirestoreUpdateSupport,
{
    #[inline]
    pub(crate) fn new(
        db: &'a D,
        collection_id: String,
        parent: Option<String>,
        document_id: String,
        precondition: Option<FirestoreWritePrecondition>,
        transforms: Vec<FirestoreFieldTransform>,
    ) -> Self {
        Self {
            _db: db,
            collection_id,
            parent,
            document_id,
            precondition,
            transforms,
        }
    }

    #[inline]
    pub fn add_to_transaction<'t>(
        self,
        transaction: &'a mut FirestoreTransaction<'t>,
    ) -> FirestoreResult<&'a mut FirestoreTransaction<'t>> {
        if let Some(parent) = self.parent {
            transaction.transform_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
                self.transforms,
            )
        } else {
            transaction.transform(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
                self.transforms,
            )
        }
    }

    #[inline]
    pub fn add_to_batch<'t, W>(
        self,
        batch: &'a mut FirestoreBatch<'t, W>,
    ) -> FirestoreResult<&'a mut FirestoreBatch<'t, W>>
    where
        W: FirestoreBatchWriter,
    {
        if let Some(parent) = self.parent {
            batch.transform_at(
                parent.as_str(),
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
                self.transforms,
            )
        } else {
            batch.transform(
                self.collection_id.as_str(),
                self.document_id,
                self.precondition,
                self.transforms,
            )
        }
    }
}
