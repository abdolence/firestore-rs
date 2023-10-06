use crate::cache::cache_filter_engine::FirestoreCacheFilterEngine;
use crate::*;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::{future, TryStreamExt};
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

    pub async fn limit_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        if let Some(limit) = self.query.limit {
            Ok(input
                .scan(0_u32, move |index, doc| {
                    if *index < limit {
                        *index += 1;
                        future::ready(Some(doc))
                    } else {
                        future::ready(None)
                    }
                })
                .boxed())
        } else {
            Ok(input)
        }
    }

    pub async fn offset_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        if let Some(offset) = self.query.offset {
            Ok(input.skip(offset as usize).boxed())
        } else {
            Ok(input)
        }
    }

    pub async fn start_at_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        if let Some(start_at) = &self.query.start_at {
            if let Some(order_by) = &self.query.order_by {
                let start_at = start_at.clone();
                let order_by = order_by.clone();
                Ok(input
                    .skip_while(move |doc_res| match doc_res {
                        Ok(doc) => match &start_at {
                            FirestoreQueryCursor::BeforeValue(values) => {
                                let result = values.iter().zip(&order_by).any(
                                    |(value, ordered_field)| {
                                        let order_by_comp = match ordered_field.direction {
                                            FirestoreQueryDirection::Ascending => cache::cache_filter_engine::CompareOp::LessThan,
                                            FirestoreQueryDirection::Descending => cache::cache_filter_engine::CompareOp::GreaterThan
                                        };
                                        match (
                                            firestore_doc_get_field_by_path(
                                                doc,
                                                &ordered_field.field_name,
                                            ),
                                            &value.value.value_type,
                                        ) {
                                            (Some(field_a), Some(field_b)) => {
                                                cache::cache_filter_engine::compare_values(
                                                    order_by_comp,
                                                    field_a,
                                                    field_b,
                                                )
                                            }
                                            (_, _) => false,
                                        }
                                    },
                                );
                                future::ready(result)
                            }
                            FirestoreQueryCursor::AfterValue(values) => {
                                let result = values.iter().zip(&order_by).any(
                                    |(value, ordered_field)| {
                                        let order_by_comp = match ordered_field.direction {
                                            FirestoreQueryDirection::Ascending => cache::cache_filter_engine::CompareOp::LessThanOrEqual,
                                            FirestoreQueryDirection::Descending => cache::cache_filter_engine::CompareOp::GreaterThanOrEqual
                                        };
                                        match (
                                            firestore_doc_get_field_by_path(
                                                doc,
                                                &ordered_field.field_name,
                                            ),
                                            &value.value.value_type,
                                        ) {
                                            (Some(field_a), Some(field_b)) => {
                                                cache::cache_filter_engine::compare_values(
                                                    order_by_comp,
                                                    field_a,
                                                    field_b,
                                                )
                                            }
                                            (_, _) => false,
                                        }
                                    },
                                );
                                future::ready(result)
                            }
                        },
                        Err(_) => future::ready(false),
                    })
                    .boxed())
            } else {
                Ok(input)
            }
        } else {
            Ok(input)
        }
    }

    pub async fn end_at_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        if let Some(end_at) = &self.query.end_at {
            if let Some(order_by) = &self.query.order_by {
                let end_at = end_at.clone();
                let order_by = order_by.clone();
                Ok(input
                    .take_while(move |doc_res| match doc_res {
                        Ok(doc) => match &end_at {
                            FirestoreQueryCursor::BeforeValue(values) => {
                                let result = values.iter().zip(&order_by).any(
                                    |(value, ordered_field)| {
                                        let order_by_comp = match ordered_field.direction {
                                            FirestoreQueryDirection::Ascending => cache::cache_filter_engine::CompareOp::LessThan,
                                            FirestoreQueryDirection::Descending => cache::cache_filter_engine::CompareOp::GreaterThan
                                        };
                                        match (
                                            firestore_doc_get_field_by_path(
                                                doc,
                                                &ordered_field.field_name,
                                            ),
                                            &value.value.value_type,
                                        ) {
                                            (Some(field_a), Some(field_b)) => {
                                                cache::cache_filter_engine::compare_values(
                                                    order_by_comp,
                                                    field_a,
                                                    field_b,
                                                )
                                            }
                                            (_, _) => false,
                                        }
                                    },
                                );
                                future::ready(result)
                            }
                            FirestoreQueryCursor::AfterValue(values) => {
                                let result = values.iter().zip(&order_by).any(
                                    |(value, ordered_field)| {
                                        let order_by_comp = match ordered_field.direction {
                                            FirestoreQueryDirection::Ascending => cache::cache_filter_engine::CompareOp::LessThanOrEqual,
                                            FirestoreQueryDirection::Descending => cache::cache_filter_engine::CompareOp::GreaterThanOrEqual
                                        };
                                        match (
                                            firestore_doc_get_field_by_path(
                                                doc,
                                                &ordered_field.field_name,
                                            ),
                                            &value.value.value_type,
                                        ) {
                                            (Some(field_a), Some(field_b)) => {
                                                cache::cache_filter_engine::compare_values(
                                                    order_by_comp,
                                                    field_a,
                                                    field_b,
                                                )
                                            }
                                            (_, _) => false,
                                        }
                                    },
                                );
                                future::ready(result)
                            }
                        },
                        Err(_) => future::ready(false),
                    })
                    .boxed())
            } else {
                Ok(input)
            }
        } else {
            Ok(input)
        }
    }

    pub async fn process_query_stream<'a, 'b>(
        &'a self,
        input: BoxStream<'b, FirestoreResult<FirestoreDocument>>,
    ) -> FirestoreResult<BoxStream<'b, FirestoreResult<FirestoreDocument>>> {
        let input = self.sort_stream(input).await?;
        let input = self.limit_stream(input).await?;
        let input = self.offset_stream(input).await?;
        let input = self.start_at_stream(input).await?;
        let input = self.end_at_stream(input).await?;
        Ok(input)
    }
}
