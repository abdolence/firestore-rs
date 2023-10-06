use crate::cache::cache_filter_engine::FirestoreCacheFilterEngine;
use crate::*;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::cmp::Ordering;

#[derive(Clone)]
pub struct FirestoreCacheQueryEngine {
    pub query: FirestoreQueryParams,
}

impl FirestoreCacheQueryEngine {
    pub fn new(query: &FirestoreQueryParams) -> Self {
        Self {
            query: query.clone(),
        }
    }

    pub fn params_supported(&self) -> bool {
        self.query.all_descendants.iter().all(|x| !*x)
            && self.query.order_by.is_none()
            && self.query.start_at.is_none()
            && self.query.end_at.is_none()
            && self.query.offset.is_none()
            && self.query.limit.is_none()
            && self.query.return_only_fields.is_none()
    }

    pub fn matches_doc(&self, doc: &FirestoreDocument) -> bool {
        if let Some(filter) = &self.query.filter {
            let filter_engine = FirestoreCacheFilterEngine::new(filter);
            filter_engine.matches_doc(doc)
        } else {
            true
        }
    }

    pub async fn sort_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        if let Some(order_by) = &self.query.order_by {
            let mut collected: Vec<FirestoreDocument> = input.try_collect().await?;

            collected.sort_by(|doc_a, doc_b| {
                let mut current_ordering = Ordering::Equal;
                for sort_field in order_by {
                    match (
                        firestore_doc_get_field_by_path(doc_a, &sort_field.field_name),
                        firestore_doc_get_field_by_path(doc_b, &sort_field.field_name),
                    ) {
                        (Some(field_a), Some(field_b)) => {
                            if cache::cache_filter_engine::compare_values(
                                cache::cache_filter_engine::CompareOp::Equal,
                                field_a,
                                field_b,
                            ) {
                                continue;
                            }

                            if cache::cache_filter_engine::compare_values(
                                cache::cache_filter_engine::CompareOp::LessThan,
                                field_a,
                                field_b,
                            ) {
                                current_ordering = match sort_field.direction {
                                    FirestoreQueryDirection::Ascending => Ordering::Less,
                                    FirestoreQueryDirection::Descending => Ordering::Greater,
                                }
                            } else {
                                current_ordering = match sort_field.direction {
                                    FirestoreQueryDirection::Ascending => Ordering::Greater,
                                    FirestoreQueryDirection::Descending => Ordering::Less,
                                }
                            }
                        }
                        (None, None) => current_ordering = Ordering::Equal,
                        (None, Some(_)) => current_ordering = Ordering::Equal,
                        (Some(_), None) => current_ordering = Ordering::Equal,
                    }
                }
                current_ordering
            });
            Ok(futures::stream::iter(collected.into_iter().map(Ok)).boxed())
        } else {
            Ok(input)
        }
    }
}
