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

    /// Binds `value` to the parameter `@name`, given without the `@`. The name matches
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

#[cfg(feature = "codegen")]
impl FirestoreSqlParameters {
    // Each parameter bound to its placeholder: the variable `@name`, or `?1`, `?2`.. for the
    // `?`s in text order. The translator names its own variables `[A-Za-z0-9_]+`, so the
    // spellings cannot clash.
    pub(super) fn placeholders(&self) -> Self {
        Self {
            named: self
                .named
                .keys()
                .map(|name| (name.clone(), FirestoreExpr::Variable(format!("@{name}"))))
                .collect(),
            positional: (1..=self.positional.len())
                .map(|position| FirestoreExpr::Variable(format!("?{position}")))
                .collect(),
        }
    }

    // Each placeholder of `placeholders` with the value it stands for: by name, then in text
    // order.
    pub(super) fn samples(&self) -> impl Iterator<Item = (String, &FirestoreExpr)> {
        let named = self
            .named
            .iter()
            .map(|(name, value)| (format!("@{name}"), value));
        let positional = (1..)
            .zip(&self.positional)
            .map(|(position, value)| (format!("?{position}"), value));
        named.chain(positional)
    }
}

impl FirestoreExpr {
    // The parameter this expression stands for, when codegen translates with `placeholders` in
    // place of the values.
    pub(super) fn sql_placeholder(&self) -> Option<&str> {
        match self {
            Self::Variable(name) if name.starts_with(['@', '?']) => Some(name),
            _ => None,
        }
    }

    // Fails for a placeholder where the translation reads the bound value, since codegen would
    // otherwise show code that means something else. A no-op outside codegen.
    pub(super) fn require_value(
        &self,
        construct: &str,
        location: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<()> {
        let Some(placeholder) = self.sql_placeholder() else {
            return Ok(());
        };
        let parameter = if placeholder.starts_with('?') {
            "?"
        } else {
            placeholder
        };
        Err(FirestoreError::invalid_sql(
            parameter,
            format!(
                "{construct} reads the value of {parameter} when the statement is translated, so codegen cannot show it as an argument: write the value in the text"
            ),
            location,
        ))
    }
}

pub(super) struct FirestoreSqlBinding<'p> {
    named: &'p BTreeMap<String, FirestoreExpr>,
    // The value of each `?`, by where it starts. The translator reads a statement in another
    // order than the text, so a `?` finds its value by position rather than by count.
    positional: BTreeMap<Location, &'p FirestoreExpr>,
}

impl<'p> FirestoreSqlBinding<'p> {
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
                    return Err(Self::unsupported_placeholder(
                        placeholder,
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

    pub(super) fn unsupported_placeholder(
        placeholder: &str,
        location: Option<FirestoreSqlLocation>,
    ) -> FirestoreError {
        FirestoreError::invalid_sql(placeholder, "a parameter is written @name or ?", location)
    }

    pub(super) fn named(&self, name: &str) -> Option<&'p FirestoreExpr> {
        self.named.get(name)
    }

    pub(super) fn positional(&self, start: Location) -> Option<&'p FirestoreExpr> {
        self.positional.get(&start).copied()
    }

    // Fails for a value that did not serialise, which [`FirestoreExpr::constant`] keeps as a
    // constant with no type.
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
