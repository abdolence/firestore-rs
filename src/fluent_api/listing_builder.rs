use crate::{
    FirestoreListCollectionIdsParams, FirestoreListCollectionIdsResult, FirestoreListDocParams,
    FirestoreListDocResult, FirestoreListingSupport, FirestoreQueryOrder, FirestoreResult,
};
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::Deserialize;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct FirestoreListingInitialBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreListingInitialBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            return_only_fields: None,
        }
    }

    #[inline]
    pub fn fields<I>(self, return_only_fields: I) -> Self
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
    pub fn from(self, collection: &str) -> FirestoreListingDocBuilder<'a, D> {
        let params: FirestoreListDocParams = FirestoreListDocParams::new(collection.to_string())
            .opt_return_only_fields(self.return_only_fields);
        FirestoreListingDocBuilder::new(self.db, params)
    }

    #[inline]
    pub fn collections(self) -> FirestoreListCollectionIdsBuilder<'a, D> {
        FirestoreListCollectionIdsBuilder::new(self.db)
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreListingDocBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    params: FirestoreListDocParams,
}

impl<'a, D> FirestoreListingDocBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreListDocParams) -> Self {
        Self { db, params }
    }

    #[inline]
    pub fn obj<T>(self) -> FirestoreListingObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreListingObjBuilder::new(self.db, self.params)
    }

    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            params: self.params.with_parent(parent.as_ref().to_string()),
            ..self
        }
    }

    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    #[inline]
    pub fn order_by<I>(self, fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<FirestoreQueryOrder>,
    {
        Self {
            params: self
                .params
                .with_order_by(fields.into_iter().map(|field| field.into()).collect()),
            ..self
        }
    }

    pub async fn get_page(self) -> FirestoreResult<FirestoreListDocResult> {
        self.db.list_doc(self.params).await
    }

    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_list_doc(self.params).await
    }

    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.db.stream_list_doc_with_errors(self.params).await
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreListingObjBuilder<'a, D, T>
where
    D: FirestoreListingSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreListDocParams,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreListingObjBuilder<'a, D, T>
where
    D: FirestoreListingSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    pub(crate) fn new(
        db: &'a D,
        params: FirestoreListDocParams,
    ) -> FirestoreListingObjBuilder<'a, D, T> {
        Self {
            db,
            params,
            _pd: PhantomData,
        }
    }

    pub async fn stream_all<'b>(self) -> FirestoreResult<BoxStream<'b, T>>
    where
        T: 'b,
    {
        self.db.stream_list_obj(self.params).await
    }

    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db.stream_list_obj_with_errors(self.params).await
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreListCollectionIdsBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    db: &'a D,
    params: FirestoreListCollectionIdsParams,
}

impl<'a, D> FirestoreListCollectionIdsBuilder<'a, D>
where
    D: FirestoreListingSupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            params: FirestoreListCollectionIdsParams::new(),
        }
    }

    #[inline]
    pub fn parent<S>(self, parent: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            params: self.params.with_parent(parent.as_ref().to_string()),
            ..self
        }
    }

    #[inline]
    pub fn page_size(self, value: usize) -> Self {
        Self {
            params: self.params.with_page_size(value),
            ..self
        }
    }

    pub async fn get_page(self) -> FirestoreResult<FirestoreListCollectionIdsResult> {
        self.db.list_collection_ids(self.params).await
    }

    pub async fn stream_all(self) -> FirestoreResult<BoxStream<'a, String>> {
        self.db.stream_list_collection_ids(self.params).await
    }

    pub async fn stream_all_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<String>>> {
        self.db
            .stream_list_collection_ids_with_errors(self.params)
            .await
    }
}
