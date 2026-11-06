use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::{FirestoreExpr, FirestoreResult, FirestoreValue};
use gcloud_sdk::google::firestore::v1::Value;
use serde::Serialize;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan};
use std::collections::BTreeMap;

/// The values bound to the parameters of a statement: by name for `@name`, and in order for `?`.
///
/// A value is serialised into a constant of the pipeline, as a value compared in the fluent API
/// is, and never enters the SQL text. A statement uses one form or the other, and every
/// parameter it uses must be bound, and every bound one used.
#[derive(Debug, Clone, Default)]
pub struct FirestoreSqlParameters {
    named: BTreeMap<String, FirestoreExpr>,
    positional: Vec<FirestoreExpr>,
}

impl FirestoreSqlParameters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds `value` to the parameter `@name`. The name is given without the `@`, and matches
    /// case-sensitively.
    pub fn param<N, V>(mut self, name: N, value: V) -> Self
    where
        N: Into<String>,
        V: Serialize,
    {
        self.named
            .insert(name.into(), FirestoreExpr::constant(value));
        self
    }

    /// Binds `values` to the `?` parameters, in the order the `?`s appear in the text, replacing
    /// any bound before.
    pub fn params<I>(self, values: I) -> Self
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        Self {
            positional: values.into_iter().map(FirestoreExpr::constant).collect(),
            ..self
        }
    }
}

/// The parameters one statement uses, each with its bound value.
pub(super) struct FirestoreSqlBinding<'p> {
    named: &'p BTreeMap<String, FirestoreExpr>,
    /// The value of each `?`, by where it starts. The translator reads a statement in another
    /// order than the text, so a `?` finds its value by position rather than by count.
    positional: BTreeMap<Location, &'p FirestoreExpr>,
}

impl<'p> FirestoreSqlBinding<'p> {
    /// Matches the parameters `tokens` use with `parameters`, both ways.
    pub(super) fn new(
        tokens: &[TokenWithSpan],
        parameters: &'p FirestoreSqlParameters,
    ) -> FirestoreResult<Self> {
        let mut named_uses = BTreeMap::new();
        let mut positional_uses = Vec::new();
        for TokenWithSpan { token, span } in tokens {
            match token {
                Token::Placeholder(placeholder) if placeholder == "?" => {
                    positional_uses.push(span.start)
                }
                Token::Placeholder(placeholder) => {
                    return Err(FirestoreError::invalid_sql(
                        placeholder,
                        "a parameter is written @name or ?",
                        FirestoreSqlLocation::from_sqlparser(span.start),
                    ))
                }
                Token::Word(word) if word.quote_style.is_none() => {
                    if let Some(name) = word.value.strip_prefix('@') {
                        named_uses.entry(name).or_insert(span.start);
                    }
                }
                _ => {}
            }
        }

        if let (Some(_), Some(positional)) = (named_uses.values().next(), positional_uses.first()) {
            return Err(FirestoreError::invalid_sql(
                "?",
                "a statement uses @name parameters or ? parameters, not both",
                FirestoreSqlLocation::from_sqlparser(*positional),
            ));
        }
        for (name, start) in &named_uses {
            match parameters.named.get(*name) {
                Some(value) => Self::serialised(&format!("@{name}"), value, *start)?,
                None => {
                    return Err(FirestoreError::invalid_sql(
                        format!("@{name}"),
                        format!("no value is bound to @{name}"),
                        FirestoreSqlLocation::from_sqlparser(*start),
                    ))
                }
            }
        }
        if let Some(name) = parameters
            .named
            .keys()
            .find(|name| !named_uses.contains_key(name.as_str()))
        {
            return Err(FirestoreError::invalid_sql(
                format!("@{name}"),
                format!("a value is bound to @{name}, which the statement does not use"),
                None,
            ));
        }
        if positional_uses.len() != parameters.positional.len() {
            return Err(FirestoreError::invalid_sql(
                "?",
                format!(
                    "the statement has {} ? parameters and {} values are bound",
                    positional_uses.len(),
                    parameters.positional.len()
                ),
                positional_uses
                    .get(parameters.positional.len())
                    .and_then(|start| FirestoreSqlLocation::from_sqlparser(*start)),
            ));
        }
        for (start, value) in positional_uses.iter().zip(&parameters.positional) {
            Self::serialised("?", value, *start)?;
        }

        Ok(Self {
            named: &parameters.named,
            positional: positional_uses
                .into_iter()
                .zip(&parameters.positional)
                .collect(),
        })
    }

    /// The value bound to `@name`.
    pub(super) fn named(&self, name: &str) -> Option<&'p FirestoreExpr> {
        self.named.get(name)
    }

    /// The value bound to the `?` that starts at `start`.
    pub(super) fn positional(&self, start: Location) -> Option<&'p FirestoreExpr> {
        self.positional.get(&start).copied()
    }

    /// Fails for a value that did not serialise, which [`FirestoreExpr::constant`] keeps as a
    /// constant with no type.
    fn serialised(parameter: &str, value: &FirestoreExpr, start: Location) -> FirestoreResult<()> {
        match value {
            FirestoreExpr::Constant(FirestoreValue {
                value: Value { value_type: None },
            }) => Err(FirestoreError::invalid_sql(
                parameter,
                format!("the value bound to {parameter} could not be serialised"),
                FirestoreSqlLocation::from_sqlparser(start),
            )),
            _ => Ok(()),
        }
    }
}
