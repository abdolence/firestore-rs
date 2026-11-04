use crate::{FirestoreError, FirestoreResult};
use std::fmt::{Display, Formatter};

/// The text of a text search: what the service's `document_matches` receives.
///
/// Build one with [`FirestoreTextQueryBuilder`](crate::pipeline_search_builder::FirestoreTextQueryBuilder)
/// from its terms, phrases and exclusions, which render with the quoting the service's parser
/// needs, or pass a string, which is sent as written. Read the text a query sends through
/// [`Display`].
///
/// Text queries match every indexed text field. The service has no OR, no field scoping and no
/// grouping, so this type offers none.
#[derive(Debug, Clone, PartialEq)]
pub struct FirestoreTextQuery(TextQuery);

#[derive(Debug, Clone, PartialEq)]
enum TextQuery {
    /// Clauses that must all match.
    Clauses(Vec<FirestoreTextQueryClause>),
    Raw(String),
}

/// A term, a phrase or the exclusion of one, which a text query requires to match or not to
/// match.
#[derive(Debug, Clone, PartialEq)]
pub struct FirestoreTextQueryClause {
    operand: TextOperand,
    excluded: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum TextOperand {
    /// One word, matched case- and accent-insensitively and stemmed.
    Term(String),
    /// Words in order, matched without stemming.
    Phrase(String),
}

/// Characters the service reads as quotes. A quote inside a phrase cannot be escaped, so it is
/// replaced by a space, which matches the same text because the tokeniser treats quotes as
/// punctuation.
const QUOTES: [char; 3] = ['"', '\u{201c}', '\u{201d}'];

impl FirestoreTextQueryClause {
    pub(crate) fn term(word: String) -> Self {
        Self {
            operand: TextOperand::Term(word),
            excluded: false,
        }
    }

    pub(crate) fn phrase(words: String) -> Self {
        Self {
            operand: TextOperand::Phrase(words),
            excluded: false,
        }
    }

    pub(crate) fn excluded(self) -> Self {
        Self {
            excluded: true,
            ..self
        }
    }

    fn is_blank(&self) -> bool {
        let (TextOperand::Term(text) | TextOperand::Phrase(text)) = &self.operand;
        text.chars()
            .all(|c| c.is_whitespace() || QUOTES.contains(&c))
    }
}

impl FirestoreTextQuery {
    pub(crate) fn from_clauses<I: IntoIterator<Item = FirestoreTextQueryClause>>(
        clauses: I,
    ) -> Self {
        Self(TextQuery::Clauses(clauses.into_iter().collect()))
    }

    /// A query without a clause, which a pipeline conversion rejects: what a `None` stands for.
    pub(crate) fn without_clauses() -> Self {
        Self::from_clauses([])
    }

    /// Fails with
    /// [`InvalidParametersError`](crate::errors::FirestoreError::InvalidParametersError) for a
    /// query of clauses that the service would accept and match nothing with: one without a
    /// clause that must match, and one with a clause that has no text.
    pub(crate) fn ensure_matchable(&self) -> FirestoreResult<()> {
        let TextQuery::Clauses(clauses) = &self.0 else {
            return Ok(());
        };
        if clauses.iter().any(FirestoreTextQueryClause::is_blank) {
            return Err(FirestoreError::invalid_parameters(
                "text_query",
                "a term or phrase must have text",
            ));
        }
        if clauses.iter().all(|clause| clause.excluded) {
            return Err(FirestoreError::invalid_parameters(
                "text_query",
                "a query needs a term or phrase that is not excluded",
            ));
        }
        Ok(())
    }
}

impl From<&str> for FirestoreTextQuery {
    fn from(text: &str) -> Self {
        Self(TextQuery::Raw(text.to_string()))
    }
}

impl From<String> for FirestoreTextQuery {
    fn from(text: String) -> Self {
        Self(TextQuery::Raw(text))
    }
}

impl From<FirestoreTextQueryClause> for FirestoreTextQuery {
    fn from(clause: FirestoreTextQueryClause) -> Self {
        Self(TextQuery::Clauses(vec![clause]))
    }
}

impl Display for FirestoreTextQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            TextQuery::Raw(text) => f.write_str(text),
            TextQuery::Clauses(clauses) => {
                for (index, clause) in clauses.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" ")?;
                    }
                    clause.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}

impl Display for FirestoreTextQueryClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.excluded {
            f.write_str("-")?;
        }
        match &self.operand {
            TextOperand::Term(word) if !needs_quotes(word) => f.write_str(word),
            TextOperand::Term(text) | TextOperand::Phrase(text) => {
                write!(f, "\"{}\"", text.replace(QUOTES, " "))
            }
        }
    }
}

/// Whether the service would read `word` as more than one plain term: as several words, as an
/// operator, as an exclusion or as a wildcard. A quoted word is matched as a one-word phrase,
/// without stemming.
fn needs_quotes(word: &str) -> bool {
    word.starts_with('-')
        || word.eq_ignore_ascii_case("and")
        || word.eq_ignore_ascii_case("or")
        || word
            .chars()
            .any(|c| c.is_whitespace() || QUOTES.contains(&c) || matches!(c, '*' | '?' | '^' | ':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(word: &str) -> FirestoreTextQueryClause {
        FirestoreTextQueryClause::term(word.to_string())
    }

    fn phrase(words: &str) -> FirestoreTextQueryClause {
        FirestoreTextQueryClause::phrase(words.to_string())
    }

    #[test]
    fn a_plain_word_is_sent_bare() {
        for word in [
            "waffles",
            "WAFFLES",
            "cr\u{ea}pes",
            "gluten-free",
            "waffles~",
        ] {
            assert_eq!(term(word).to_string(), word);
        }
    }

    #[test]
    fn a_term_the_parser_would_split_or_read_as_syntax_is_quoted() {
        let quoted = [
            ("maple syrup", "\"maple syrup\""),
            ("menu:waffles", "\"menu:waffles\""),
            ("waff*", "\"waff*\""),
            ("w?ffles", "\"w?ffles\""),
            ("waffles^2", "\"waffles^2\""),
            ("-bacon", "\"-bacon\""),
            ("say\"hi", "\"say hi\""),
            ("\u{201c}best\u{201d}", "\" best \""),
        ];
        for (word, expected) in quoted {
            assert_eq!(term(word).to_string(), expected, "{word}");
        }
    }

    #[test]
    fn an_operator_word_is_quoted_in_any_case() {
        for word in ["and", "AND", "And", "or", "OR", "oR"] {
            assert_eq!(term(word).to_string(), format!("\"{word}\""));
        }
    }

    #[test]
    fn a_phrase_is_quoted_and_loses_its_quote_characters() {
        assert_eq!(phrase("maple syrup").to_string(), "\"maple syrup\"");
        assert_eq!(phrase("waffle").to_string(), "\"waffle\"");
        assert_eq!(
            phrase("the \"best\" \u{201c}waffles\u{201d}").to_string(),
            "\"the  best   waffles \""
        );
    }

    #[test]
    fn an_exclusion_prefixes_the_rendered_operand() {
        assert_eq!(term("bacon").excluded().to_string(), "-bacon");
        assert_eq!(phrase("New York").excluded().to_string(), "-\"New York\"");
        assert_eq!(term("-bacon").excluded().to_string(), "-\"-bacon\"");
        assert_eq!(term("bacon").excluded().excluded().to_string(), "-bacon");
    }

    #[test]
    fn clauses_are_joined_by_one_space() {
        let query = FirestoreTextQuery::from_clauses([
            term("waffles"),
            phrase("maple syrup"),
            term("bacon").excluded(),
        ]);
        assert_eq!(query.to_string(), "waffles \"maple syrup\" -bacon");
    }

    #[test]
    fn a_raw_string_is_sent_unchanged() {
        let text = "waffles OR (crepes -\"bacon\"";
        assert_eq!(FirestoreTextQuery::from(text).to_string(), text);
        assert_eq!(FirestoreTextQuery::from(text.to_string()).to_string(), text);
    }

    #[test]
    fn a_query_that_can_match_nothing_is_rejected() {
        let rejected: [FirestoreTextQuery; 5] = [
            FirestoreTextQuery::without_clauses(),
            term("bacon").excluded().into(),
            FirestoreTextQuery::from_clauses([term("bacon").excluded(), phrase("eggs").excluded()]),
            FirestoreTextQuery::from_clauses([term("waffles"), term(" ")]),
            phrase("\"\u{201d}").into(),
        ];
        for query in rejected {
            let result = query.ensure_matchable();
            assert!(
                matches!(result, Err(FirestoreError::InvalidParametersError(_))),
                "{query:?}: {result:?}"
            );
        }
    }

    #[test]
    fn a_raw_string_is_never_rejected() {
        for text in ["", "-bacon", "   "] {
            assert!(FirestoreTextQuery::from(text).ensure_matchable().is_ok());
        }
    }

    /// Splits `text` the way the service's parser separates clauses: at whitespace outside a
    /// quoted run. Fails on a quote left open.
    fn split_clauses(text: &str) -> Vec<String> {
        let mut clauses = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        for c in text.chars() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                    current.push(c);
                }
                c if c.is_whitespace() && !in_quotes => {
                    if !current.is_empty() {
                        clauses.push(std::mem::take(&mut current));
                    }
                }
                c => current.push(c),
            }
        }
        assert!(!in_quotes, "unbalanced quote in {text:?}");
        if !current.is_empty() {
            clauses.push(current);
        }
        clauses
    }

    /// Every string of up to four characters from an alphabet of the characters the renderer
    /// treats specially.
    fn strings() -> Vec<String> {
        let alphabet = ['x', ' ', '"', '\u{201c}', '\u{201d}', '-', ':', '\\'];
        let mut of_length = vec![String::new()];
        let mut all = of_length.clone();
        for _ in 0..4 {
            of_length = of_length
                .iter()
                .flat_map(|prefix| alphabet.map(|c| format!("{prefix}{c}")))
                .collect();
            all.extend(of_length.iter().cloned());
        }
        all
    }

    #[test]
    fn rendered_clauses_split_back_into_the_same_clauses() {
        let clauses: Vec<FirestoreTextQueryClause> = strings()
            .into_iter()
            .flat_map(|text| {
                [
                    term(&text),
                    phrase(&text),
                    term(&text).excluded(),
                    phrase(&text).excluded(),
                ]
            })
            .filter(|clause| !clause.is_blank())
            .collect();
        assert!(clauses.len() > 10_000);

        for window in clauses.windows(3) {
            let rendered = window.iter().map(ToString::to_string).collect::<Vec<_>>();
            let query = FirestoreTextQuery::from_clauses(window.iter().cloned());
            assert_eq!(split_clauses(&query.to_string()), rendered, "{window:?}");
            assert!(
                !query.to_string().contains(['\u{201c}', '\u{201d}']),
                "{window:?}"
            );
        }
    }
}
