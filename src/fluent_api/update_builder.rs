//! Builder for constructing Firestore update operations.
//!
//! This module provides a fluent API to specify the document to be updated,
//! the data to update (either a full object, specific fields, or field transformations),
//! and optional preconditions.

use crate::document_transform_builder::FirestoreTransformBuilder;
use crate::{
    FirestoreBatch, FirestoreBatchWriter, FirestoreFieldTransform, FirestoreResult,
    FirestoreTransaction, FirestoreTransactionOps, FirestoreUpdateSupport,
    FirestoreWritePrecondition,
};
use gcloud_sdk::google::firestore::v1::Document;
use serde::{Deserialize, Serialize};

/// The initial builder for a Firestore update operation.
///
/// Created by calling [`FirestoreExprBuilder::update()`](crate::FirestoreExprBuilder::update).
/// This builder allows specifying which fields to update. If no fields are specified,
/// the entire object provided later will be merged with the existing document.
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
    /// Creates a new `FirestoreUpdateInitialBuilder`.
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            update_only_fields: None,
        }
    }

    /// Specifies the exact set of fields to update.
    ///
    /// If this is set, only the fields listed here will be modified. Any other fields
    /// in the provided object/document will be ignored. If not set (the default),
    /// the update acts as a merge: fields in the provided object will overwrite
    /// existing fields, and new fields will be added. Fields not present in the
    /// provided object will remain untouched in the document.
    ///
    /// # Arguments
    /// * `update_only_fields`: An iterator of field paths (dot-separated for nested fields)
    ///   to be included in the update mask.
    ///
    /// # Returns
    /// The builder instance with the field mask set.
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

    /// Specifies the collection ID where the document to update resides.
    ///
    /// # Arguments
    /// * `collection_id`: The ID of the collection.
    ///
    /// # Returns
    /// A [`FirestoreUpdateDocObjBuilder`] to specify the document ID and data.
    #[inline]
    pub fn in_col(self, collection_id: &str) -> FirestoreUpdateDocObjBuilder<'a, D> {
        FirestoreUpdateDocObjBuilder::new(
            self.db,
            collection_id.to_string(),
            self.update_only_fields,
        )
    }
}

/// A builder for specifying the document ID and data for an update operation.
///
/// This stage allows setting the document data (as a raw `Document` or a serializable object),
/// preconditions, field transformations, and which fields to return after the update.
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
    /// Creates a new `FirestoreUpdateDocObjBuilder`.
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

    /// Specifies which fields of the updated document should be returned.
    ///
    /// If not set, the entire document is typically returned after the update.
    ///
    /// # Arguments
    /// * `return_only_fields`: An iterator of field paths to return.
    ///
    /// # Returns
    /// The builder instance with the projection mask for the return value set.
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

    /// Specifies a precondition for the update operation.
    ///
    /// The update will only be executed if the precondition is met.
    ///
    /// # Arguments
    /// * `precondition`: The [`FirestoreWritePrecondition`] to apply.
    ///
    /// # Returns
    /// The builder instance with the precondition set.
    #[inline]
    pub fn precondition(self, precondition: FirestoreWritePrecondition) -> Self {
        Self {
            precondition: Some(precondition),
            ..self
        }
    }

    /// Specifies server-side field transformations to apply as part of the update.
    ///
    /// The `doc_transform` argument is a closure that receives a [`FirestoreTransformBuilder`]
    /// and should return a `Vec<FirestoreFieldTransform>`.
    ///
    /// # Arguments
    /// * `doc_transform`: A closure to build the list of field transformations.
    ///
    /// # Returns
    /// The builder instance with the field transformations set.
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

    /// Specifies the document data to update using a raw [`Document`].
    ///
    /// The `document.name` field should contain the full path to the document.
    ///
    /// # Arguments
    /// * `document`: The Firestore `Document` containing the fields to update.
    ///
    /// # Returns
    /// A [`FirestoreUpdateDocExecuteBuilder`] to execute the operation.
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

    /// Specifies the ID of the document to update.
    ///
    /// This transitions the builder to expect a Rust object for the update data.
    ///
    /// # Arguments
    /// * `document_id`: The ID of the document to update.
    ///
    /// # Returns
    /// A [`FirestoreUpdateObjInitExecuteBuilder`] to specify the object and execute.
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

/// A builder for executing an update operation with raw [`Document`] data.
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
    /// Creates a new `FirestoreUpdateDocExecuteBuilder`.
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

    /// Executes the configured update operation using a raw `Document`.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the updated [`Document`].
    pub async fn execute(self) -> FirestoreResult<Document> {
        // Note: The `update_doc` method on `FirestoreUpdateSupport` expects the full document path
        // to be in `self.document.name`. The `collection_id` here is somewhat redundant if
        // `document.name` is correctly populated, but kept for consistency with other builders.
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

/// An intermediate builder stage for update operations using a Rust object.
/// This stage has the document ID and is ready to accept the object to update with.
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
    /// Creates a new `FirestoreUpdateObjInitExecuteBuilder`.
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

    /// Specifies the parent document path for updating a document in a sub-collection.
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

    /// Specifies the Rust object containing the data to update the document with.
    ///
    /// The object `T` must implement `serde::Serialize`.
    ///
    /// # Arguments
    /// * `object`: A reference to the Rust object.
    ///
    /// # Type Parameters
    /// * `T`: The type of the object.
    ///
    /// # Returns
    /// A [`FirestoreUpdateObjExecuteBuilder`] to execute the operation or add it to a batch/transaction.
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

    /// Specifies server-side field transformations to apply.
    /// This method is used when the update consists *only* of transformations,
    /// without merging an object's fields.
    ///
    /// # Arguments
    /// * `doc_transform`: A closure to build the list of field transformations.
    ///
    /// # Returns
    /// The builder instance with the field transformations set.
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

    /// Finalizes the builder for an update operation that *only* applies field transformations.
    ///
    /// This should be called if no `.object()` is provided, and the update relies solely
    /// on the transformations defined via `.transforms()`.
    ///
    /// # Returns
    /// A [`FirestoreUpdateOnlyTransformBuilder`] to add the transform-only operation to a batch or transaction.
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

/// A builder for executing an update operation with a serializable Rust object.
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
    /// Creates a new `FirestoreUpdateObjExecuteBuilder`.
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

    /// Executes the configured update operation, serializing the object and
    /// deserializing the result into type `O`.
    ///
    /// # Type Parameters
    /// * `O`: The type to deserialize the result into. Must implement `serde::Deserialize`.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the deserialized object `O` representing the updated document.
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
                    // Note: The current FirestoreUpdateSupport::update_obj_at doesn't take transforms.
                    // This might be an oversight or transforms are handled differently for object updates.
                    // If transforms are intended here, the trait method needs adjustment.
                    // For now, passing an empty vec or ignoring self.transforms if not supported by the trait.
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
                    // Similar note as above for transforms.
                )
                .await
        }
    }

    /// Adds server-side field transformations to the update operation.
    ///
    /// This can be combined with updating fields from an object. The transformations
    /// are applied *after* the object merge/update.
    ///
    /// # Arguments
    /// * `transforms_builder`: A closure to build the list of field transformations.
    ///
    /// # Returns
    /// The builder instance with added transformations.
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

    /// Adds this update operation (object merge and/or transforms) to a [`FirestoreTransaction`].
    ///
    /// # Arguments
    /// * `transaction`: A mutable reference to the transaction.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the transaction.
    #[inline]
    pub fn add_to_transaction<'t, TO>(self, transaction: &'t mut TO) -> FirestoreResult<&'t mut TO>
    where
        TO: FirestoreTransactionOps,
    {
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

    /// Adds this update operation (object merge and/or transforms) to a [`FirestoreBatch`].
    ///
    /// # Arguments
    /// * `batch`: A mutable reference to the batch writer.
    ///
    /// # Type Parameters
    /// * `W`: The type of the batch writer.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the batch.
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

/// A builder for an update operation that consists *only* of field transformations.
///
/// This is used when no object data is being merged, and the update is solely
/// defined by server-side atomic operations like increment, array manipulation, etc.
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
    /// Creates a new `FirestoreUpdateOnlyTransformBuilder`.
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

    /// Adds this transform-only update operation to a [`FirestoreTransaction`].
    ///
    /// # Arguments
    /// * `transaction`: A mutable reference to the transaction.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the transaction.
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

    /// Adds this transform-only update operation to a [`FirestoreBatch`].
    ///
    /// # Arguments
    /// * `batch`: A mutable reference to the batch writer.
    ///
    /// # Type Parameters
    /// * `W`: The type of the batch writer.
    ///
    /// # Returns
    /// A `FirestoreResult` containing the mutable reference to the batch.
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
