use crate::cache::cache_filter_engine::{value_order, FirestoreCacheFilterEngine};
use crate::*;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;

/// The pseudo field every query ends its ordering on, and that cursors may name.
const DOCUMENT_NAME_FIELD: &str = "__name__";

#[derive(Clone)]
pub struct FirestoreCacheQueryEngine {
    pub query: FirestoreQueryParams,
    order_by: Vec<FirestoreQueryOrder>,
}

impl FirestoreCacheQueryEngine {
    pub fn new(query: &FirestoreQueryParams) -> Self {
        Self {
            query: query.clone(),
            order_by: Self::effective_order_by(query),
        }
    }

    /// The order Firestore returns a query in: the explicit `order_by`, or else the fields of the
    /// filter's inequalities; then the document name, in the direction of the last field.
    fn effective_order_by(query: &FirestoreQueryParams) -> Vec<FirestoreQueryOrder> {
        let mut order_by = query.order_by.clone().unwrap_or_else(|| {
            query
                .filter
                .iter()
                .flat_map(FirestoreCacheFilterEngine::inequality_fields)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|field| {
                    FirestoreQueryOrder::new(field.to_string(), FirestoreQueryDirection::Ascending)
                })
                .collect()
        });
        if !order_by
            .iter()
            .any(|order| order.field_name == DOCUMENT_NAME_FIELD)
        {
            let direction = order_by
                .last()
                .map_or(FirestoreQueryDirection::Ascending, |order| {
                    order.direction.clone()
                });
            order_by.push(FirestoreQueryOrder::new(
                DOCUMENT_NAME_FIELD.to_string(),
                direction,
            ));
        }
        order_by
    }

    pub fn params_supported(&self) -> bool {
        self.query.all_descendants.iter().all(|x| !*x)
    }

    pub fn matches_doc(&self, doc: &FirestoreDocument) -> bool {
        if let Some(filter) = &self.query.filter {
            let filter_engine = FirestoreCacheFilterEngine::new(filter);
            filter_engine.matches_doc(doc)
        } else {
            true
        }
    }

    fn field_value<'d>(doc: &'d FirestoreDocument, field: &str) -> Option<Cow<'d, ValueType>> {
        if field == DOCUMENT_NAME_FIELD {
            Some(Cow::Owned(ValueType::ReferenceValue(doc.name.clone())))
        } else {
            firestore_doc_get_field_by_path(doc, field).map(Cow::Borrowed)
        }
    }

    fn compare_docs(&self, a: &FirestoreDocument, b: &FirestoreDocument) -> Ordering {
        Self::first_difference(self.order_by.iter().map(|order| {
            let ordering = Self::field_value(a, &order.field_name)
                .zip(Self::field_value(b, &order.field_name))
                .map_or(Ordering::Equal, |(a, b)| value_order(&a, &b));
            (order, ordering)
        }))
    }

    /// Where `doc` sits against a cursor, which is a position in the query's order given by one
    /// value per leading `order_by` field.
    fn compare_to_cursor(&self, doc: &FirestoreDocument, values: &[FirestoreValue]) -> Ordering {
        Self::first_difference(self.order_by.iter().zip(values).map(|(order, value)| {
            let ordering = Self::field_value(doc, &order.field_name)
                .zip(value.value.value_type.as_ref())
                .map_or(Ordering::Equal, |(field, cursor)| {
                    value_order(&field, cursor)
                });
            (order, ordering)
        }))
    }

    /// The outcome of the first order field whose values differ, read in that field's direction.
    fn first_difference<'o>(
        ascending_orderings: impl Iterator<Item = (&'o FirestoreQueryOrder, Ordering)>,
    ) -> Ordering {
        ascending_orderings
            .map(|(order, ordering)| order.directed(ordering))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal)
    }

    fn precedes_start(&self, doc: &FirestoreDocument) -> bool {
        match &self.query.start_at {
            None => false,
            Some(FirestoreQueryCursor::BeforeValue(values)) => {
                self.compare_to_cursor(doc, values).is_lt()
            }
            Some(FirestoreQueryCursor::AfterValue(values)) => {
                self.compare_to_cursor(doc, values).is_le()
            }
        }
    }

    fn precedes_end(&self, doc: &FirestoreDocument) -> bool {
        match &self.query.end_at {
            None => true,
            Some(FirestoreQueryCursor::BeforeValue(values)) => {
                self.compare_to_cursor(doc, values).is_lt()
            }
            Some(FirestoreQueryCursor::AfterValue(values)) => {
                self.compare_to_cursor(doc, values).is_le()
            }
        }
    }

    /// Orders the documents that matched the filter, then applies the cursors, the offset and the
    /// limit, in that order. Documents lacking a field of the ordering are not part of the result,
    /// as Firestore leaves them out of an ordered query.
    pub async fn process_query_stream<'b>(
        &self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let mut docs: Vec<FirestoreDocument> = input.try_collect().await?;
        docs.retain(|doc| {
            self.order_by
                .iter()
                .all(|order| Self::field_value(doc, &order.field_name).is_some())
        });
        docs.sort_by(|a, b| self.compare_docs(a, b));

        let limit = self.query.limit.map_or(usize::MAX, |limit| limit as usize);
        let offset = self.query.offset.map_or(0, |offset| offset as usize);
        let docs: Vec<FirestoreDocument> = docs
            .into_iter()
            .skip_while(|doc| self.precedes_start(doc))
            .take_while(|doc| self.precedes_end(doc))
            .skip(offset)
            .take(limit)
            .collect();
        Ok(futures::stream::iter(docs.into_iter().map(Ok)).boxed())
    }
}
