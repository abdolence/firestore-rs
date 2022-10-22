use crate::fluent_api::FirestoreExprBuilder;
use crate::{
    FirestoreQueryCollection, FirestoreQueryOrder, FirestoreQueryParams, FirestoreQuerySupport,
    FirestoreResult,
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
    builder: FirestoreExprBuilder<'a, D>,
    select_only_fields: Option<Vec<String>>,
}

impl<'a, D> FirestoreSelectInitialBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    pub(crate) fn new(builder: FirestoreExprBuilder<'a, D>) -> Self {
        Self {
            builder,
            select_only_fields: None,
        }
    }

    pub fn fields<I>(self, select_only_fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            select_only_fields: Some(
                select_only_fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
            ..self
        }
    }

    pub fn from<C>(self, collection: C) -> FirestoreSelectDocBuilder<'a, D>
    where
        C: Into<FirestoreQueryCollection>,
    {
        let params: FirestoreQueryParams = FirestoreQueryParams::new(collection.into())
            .opt_return_only_fields(self.select_only_fields);
        FirestoreSelectDocBuilder::new(self.builder, params)
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    builder: FirestoreExprBuilder<'a, D>,
    params: FirestoreQueryParams,
}

impl<'a, D> FirestoreSelectDocBuilder<'a, D>
where
    D: FirestoreQuerySupport,
{
    pub(crate) fn new(builder: FirestoreExprBuilder<'a, D>, params: FirestoreQueryParams) -> Self {
        Self { builder, params }
    }

    pub fn obj<T>(self) -> FirestoreSelectObjBuilder<'a, D, T>
    where
        T: Send,
        for<'de> T: Deserialize<'de>,
    {
        FirestoreSelectObjBuilder::new(self.builder, self.params)
    }

    pub fn parent(self, parent: String) -> Self {
        Self {
            params: self.params.with_parent(parent),
            ..self
        }
    }

    pub fn limit(self, value: u32) -> Self {
        Self {
            params: self.params.with_limit(value),
            ..self
        }
    }

    pub fn offset(self, value: u32) -> Self {
        Self {
            params: self.params.with_offset(value),
            ..self
        }
    }

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

    pub async fn query(self) -> FirestoreResult<Vec<Document>> {
        self.builder.db.query_doc(self.params).await
    }

    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, Document>> {
        self.builder.db.stream_query_doc(self.params).await
    }

    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<Document>>> {
        self.builder
            .db
            .stream_query_doc_with_errors(self.params)
            .await
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSelectObjBuilder<'a, D, T>
where
    D: FirestoreQuerySupport,
    T: Send,
    for<'de> T: Deserialize<'de>,
{
    builder: FirestoreExprBuilder<'a, D>,
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
        builder: FirestoreExprBuilder<'a, D>,
        params: FirestoreQueryParams,
    ) -> FirestoreSelectObjBuilder<'a, D, T> {
        Self {
            builder,
            params,
            _pd: PhantomData::default(),
        }
    }

    pub async fn query(self) -> FirestoreResult<Vec<T>> {
        self.builder.db.query_obj(self.params).await
    }

    pub async fn stream_query<'b>(self) -> FirestoreResult<BoxStream<'b, T>> {
        self.builder.db.stream_query_obj(self.params).await
    }

    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<T>>>
    where
        T: 'b,
    {
        self.builder
            .db
            .stream_query_obj_with_errors(self.params)
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
        let select_only_fields = FirestoreExprBuilder::new(&MockDatabase {})
            .select()
            .fields(paths!(TestStructure::{some_id, one_more_string, some_num}))
            .select_only_fields;

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
        let select_only_fields = FirestoreExprBuilder::new(&MockDatabase {})
            .select()
            .from("test");

        assert_eq!(
            select_only_fields.params.collection_id,
            FirestoreQueryCollection::Single("test".to_string())
        )
    }
}
