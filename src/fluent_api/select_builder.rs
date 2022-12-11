use crate::errors::FirestoreError;
use crate::query_filter_builder::FirestoreQueryFilterBuilder;
use crate::{
    FirestoreGetByIdSupport, FirestoreListenSupport, FirestoreListener, FirestoreListenerParams,
    FirestoreListenerTarget, FirestorePartition, FirestorePartitionQueryParams,
    FirestoreQueryCollection, FirestoreQueryCursor, FirestoreQueryFilter, FirestoreQueryOrder,
    FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult, FirestoreTokenStorage,
};
use futures::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::Deserialize;
use std::collections::HashMap;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport + FirestoreGetByIdSupport + FirestoreListenSupport + Clone + 'static,
{
    db: &'a D,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport
        + FirestoreGetByIdSupport
        + FirestoreListenSupport
        + Clone
        + Send
        + Sync
        + 'static,
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
    pub fn from<C>(self, collection: C) -> FirestoreSelectDocBuilder<'a, D>
    where
        C: Into<FirestoreQueryCollection>,
    {
        let params: FirestoreQueryParams = FirestoreQueryParams::new(collection.into())
            .opt_return_only_fields(self.return_only_fields);
        FirestoreSelectDocBuilder::new(self.db, params)
    }

    #[inline]
    pub fn by_id_in(self, collection: &str) -> FirestoreSelectByIdBuilder<'a, D> {
        FirestoreSelectByIdBuilder::new(self.db, collection.to_string(), self.return_only_fields)
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport + FirestoreListenSupport + Clone + Send + Sync,
{
    db: &'a D,
    params: FirestoreQueryParams,
}

impl<'a, D> FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport + FirestoreListenSupport + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self { db, params }
    }

    #[inline]
    pub fn obj<T>(self) -> FirestoreSelectObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSelectObjBuilder::new(self.db, self.params)
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
    pub fn limit(self, value: u32) -> Self {
        Self {
            params: self.params.with_limit(value),
            ..self
        }
    }

    #[inline]
    pub fn offset(self, value: u32) -> Self {
        Self {
            params: self.params.with_offset(value),
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

    #[inline]
    pub fn start_at(self, cursor: FirestoreQueryCursor) -> Self {
        Self {
            params: self.params.with_start_at(cursor),
            ..self
        }
    }

    #[inline]
    pub fn end_at(self, cursor: FirestoreQueryCursor) -> Self {
        Self {
            params: self.params.with_end_at(cursor),
            ..self
        }
    }

    #[inline]
    pub fn all_descendants(self) -> Self {
        Self {
            params: self.params.with_all_descendants(true),
            ..self
        }
    }

    #[inline]
    pub fn filter<FN>(self, filter: FN) -> Self
    where
        FN: Fn(FirestoreQueryFilterBuilder) -> Option<FirestoreQueryFilter>,
    {
        let filter_builder = FirestoreQueryFilterBuilder::new();

        Self {
            params: self.params.opt_filter(filter(filter_builder)),
            ..self
        }
    }

    pub async fn query(self) -> FirestoreResult<Vec<Document>> {
        self.db.query_doc(self.params).await
    }

    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.db.stream_query_doc(self.params).await
    }

    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.db.stream_query_doc_with_errors(self.params).await
    }

    pub fn partition_query(self) -> FirestorePartitionQueryDocBuilder<'a, D> {
        FirestorePartitionQueryDocBuilder::new(self.db, self.params.with_all_descendants(true))
    }

    pub fn listen(self) -> FirestoreDocChangesListenerInitBuilder<'a, D> {
        FirestoreDocChangesListenerInitBuilder::new(self.db, self.params.with_all_descendants(true))
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreQueryParams,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreSelectObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    pub(crate) fn new(
        db: &'a D,
        params: FirestoreQueryParams,
    ) -> FirestoreSelectObjBuilder<'a, D, T> {
        Self {
            db,
            params,
            _pd: PhantomData::default(),
        }
    }

    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        self.db.query_obj(self.params).await
    }

    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>> {
        self.db.stream_query_obj(self.params).await
    }

    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.db.stream_query_obj_with_errors(self.params).await
    }

    pub fn partition_query(self) -> FirestorePartitionQueryObjBuilder<'a, D, T>
    where
        T: 'a,
    {
        FirestorePartitionQueryObjBuilder::new(self.db, self.params.with_all_descendants(true))
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectByIdBuilder<'a, D>
where
    D: FirestoreGetByIdSupport,
{
    db: &'a D,
    collection: String,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectByIdBuilder<'a, D>
where
    D: FirestoreGetByIdSupport,
{
    pub(crate) fn new(
        db: &'a D,
        collection: String,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreSelectByIdBuilder<'a, D> {
        Self {
            db,
            collection,
            parent: None,
            return_only_fields,
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
    pub fn obj<T>(self) -> FirestoreSelectObjByIdBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSelectObjByIdBuilder::new(
            self.db,
            self.collection,
            self.parent,
            self.return_only_fields,
        )
    }

    pub async fn one<S>(self, document_id: S) -> FirestoreResult<Option<Document>>
    where
        S: AsRef<str> + Send,
    {
        if let Some(parent) = self.parent {
            match self
                .db
                .get_doc_at::<S>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        } else {
            match self
                .db
                .get_doc::<S>(
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        }
    }

    pub async fn batch<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<Document>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_docs_at::<S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_docs::<S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }

    pub async fn batch_with_errors<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<Document>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_docs_at_with_errors::<S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_docs_with_errors::<S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectObjByIdBuilder<'a, D, T>
where
    D: FirestoreGetByIdSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    collection: String,
    parent: Option<String>,
    return_only_fields: Option<Vec<String>>,
    _pd: PhantomData<T>,
}

impl<'a, D, T> FirestoreSelectObjByIdBuilder<'a, D, T>
where
    D: FirestoreGetByIdSupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    pub(crate) fn new(
        db: &'a D,
        collection: String,
        parent: Option<String>,
        return_only_fields: Option<Vec<String>>,
    ) -> FirestoreSelectObjByIdBuilder<'a, D, T> {
        Self {
            db,
            collection,
            parent,
            return_only_fields,
            _pd: PhantomData::default(),
        }
    }

    pub async fn one<S>(self, document_id: S) -> FirestoreResult<Option<T>>
    where
        S: AsRef<str> + Send,
    {
        if let Some(parent) = self.parent {
            match self
                .db
                .get_obj_at_return_fields::<T, S>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        } else {
            match self
                .db
                .get_obj_return_fields::<T, S>(
                    self.collection.as_str(),
                    document_id,
                    self.return_only_fields,
                )
                .await
            {
                Ok(doc) => Ok(Some(doc)),
                Err(err) => match err {
                    FirestoreError::DataNotFoundError(_) => Ok(None),
                    _ => Err(err),
                },
            }
        }
    }

    pub async fn batch<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, (String, Option<T>)>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
        T: Send + 'a,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_objects_at::<T, S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_objects::<T, S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }

    pub async fn batch_with_errors<S, I>(
        self,
        document_ids: I,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(String, Option<T>)>>>
    where
        S: AsRef<str> + Send,
        I: IntoIterator<Item = S> + Send,
        T: Send + 'a,
    {
        if let Some(parent) = self.parent {
            self.db
                .batch_stream_get_objects_at_with_errors::<T, S, I>(
                    parent.as_str(),
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        } else {
            self.db
                .batch_stream_get_objects_with_errors::<T, S, I>(
                    self.collection.as_str(),
                    document_ids,
                    self.return_only_fields,
                )
                .await
        }
    }
}

#[derive(Clone, Debug)]
pub struct FirestorePartitionQueryDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    db: &'a D,
    params: FirestoreQueryParams,
    parallelism: usize,
    partition_count: u32,
    page_size: u32,
}

impl<'a, D> FirestorePartitionQueryDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self {
            db,
            params,
            parallelism: 2,
            partition_count: 10,
            page_size: 1000,
        }
    }

    #[inline]
    pub fn parallelism(self, max_threads: usize) -> Self {
        Self {
            parallelism: max_threads,
            ..self
        }
    }

    #[inline]
    pub fn partition_count(self, count: u32) -> Self {
        Self {
            partition_count: count,
            ..self
        }
    }

    #[inline]
    pub fn page_size(self, len: u32) -> Self {
        Self {
            page_size: len,
            ..self
        }
    }

    pub async fn stream_partitions_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, Document)>>> {
        self.db
            .stream_partition_query_doc_with_errors(
                self.parallelism,
                FirestorePartitionQueryParams::new(
                    self.params,
                    self.partition_count,
                    self.page_size,
                ),
            )
            .await
    }
}

#[derive(Clone, Debug)]
pub struct FirestorePartitionQueryObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    db: &'a D,
    params: FirestoreQueryParams,
    parallelism: usize,
    partition_count: u32,
    page_size: u32,
    _ph: PhantomData<T>,
}

impl<'a, D, T> FirestorePartitionQueryObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send + 'a,
    for<'de> T: Deserialize<'de>,
{
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self {
            db,
            params,
            parallelism: 2,
            partition_count: 10,
            page_size: 1000,
            _ph: PhantomData::default(),
        }
    }

    #[inline]
    pub fn parallelism(self, max_threads: usize) -> Self {
        Self {
            parallelism: max_threads,
            ..self
        }
    }

    #[inline]
    pub fn partition_count(self, count: u32) -> Self {
        Self {
            partition_count: count,
            ..self
        }
    }

    #[inline]
    pub fn page_size(self, len: u32) -> Self {
        Self {
            page_size: len,
            ..self
        }
    }

    pub async fn stream_partitions_with_errors(
        self,
    ) -> FirestoreResult<BoxStream<'a, FirestoreResult<(FirestorePartition, T)>>> {
        self.db
            .stream_partition_query_obj_with_errors(
                self.parallelism,
                FirestorePartitionQueryParams::new(
                    self.params,
                    self.partition_count,
                    self.page_size,
                ),
            )
            .await
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreDocChangesListenerInitBuilder<'a, D>
where
    D: FirestoreListenSupport + Clone,
{
    db: &'a D,
    listener_params: FirestoreListenerParams,
    query_params: FirestoreQueryParams,
    labels: HashMap<String, String>,
}

impl<'a, D> FirestoreDocChangesListenerInitBuilder<'a, D>
where
    D: FirestoreListenSupport + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(crate) fn new(db: &'a D, params: FirestoreQueryParams) -> Self {
        Self {
            db,
            listener_params: FirestoreListenerParams::new(),
            query_params: params,
            labels: HashMap::new(),
        }
    }

    #[inline]
    pub fn labels(self, labels: HashMap<String, String>) -> Self {
        Self { labels, ..self }
    }

    #[inline]
    pub fn retry_delay(self, delay: std::time::Duration) -> Self {
        Self {
            listener_params: self.listener_params.with_retry_delay(delay),
            ..self
        }
    }

    pub async fn target<S>(
        self,
        target: FirestoreListenerTarget,
        token_storage: S,
    ) -> FirestoreResult<FirestoreListener<D, S>>
    where
        S: FirestoreTokenStorage + Send + Sync + Clone + 'static,
    {
        FirestoreListener::new(
            self.db.clone(),
            target,
            token_storage,
            self.listener_params,
            self.query_params,
            self.labels,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::fluent_api::tests::*;
    use crate::fluent_api::FirestoreExprBuilder;
    use crate::{path, paths, FirestoreQueryCollection};

    #[test]
    fn select_query_builder_test_fields() {
        let select_only_fields = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .select()
            .fields(paths!(TestStructure::{some_id, one_more_string, some_num}))
            .return_only_fields;

        assert_eq!(
            select_only_fields,
            Some(vec![
                path!(TestStructure::some_id),
                path!(TestStructure::one_more_string),
                path!(TestStructure::some_num),
            ])
        )
    }

    #[test]
    fn select_query_builder_from_collection() {
        let select_only_fields = FirestoreExprBuilder::new(&mockdb::MockDatabase {})
            .select()
            .from("test");

        assert_eq!(
            select_only_fields.params.collection_id,
            FirestoreQueryCollection::Single("test".to_string())
        )
    }
}
