use crate::filter_builder::FirestoreQueryFilterBuilder;
use crate::{
    FirestoreQueryCollection, FirestoreQueryCursor, FirestoreQueryFilter, FirestoreQueryOrder,
    FirestoreQueryParams, FirestoreQuerySupport, FirestoreResult,
};
use futures_util::stream::BoxStream;
use gcloud_sdk::google::firestore::v1::Document;
use serde::Deserialize;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    db: &'a D,
    return_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport,
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
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    db: &'a D,
    params: FirestoreQueryParams,
}

impl<'a, D> FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
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
    pub fn start_at<I>(self, cursor: FirestoreQueryCursor) -> Self {
        Self {
            params: self.params.with_start_at(cursor),
            ..self
        }
    }

    #[inline]
    pub fn end_at<I>(self, cursor: FirestoreQueryCursor) -> Self {
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
