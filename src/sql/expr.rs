use super::scope::FirestoreSqlScope;
use super::translator::{at, expr_start, is_parameter, FirestoreSqlTranslator};
use crate::errors::{FirestoreError, FirestoreSqlLocation};
use crate::pipeline_expr_builder::FirestorePipelineExprBuilder;
use crate::{
    FirestoreBoolExpr, FirestoreExpr, FirestoreReference, FirestoreResult, FirestoreValue,
};
use gcloud_sdk::google::firestore::v1::{value::ValueType, ArrayValue, Value};
use sqlparser::ast::{
    AccessExpr, Array, BinaryOperator, CaseWhen, CeilFloorKind, DateTimeField, Expr, Ident,
    Subscript, TrimWhereField, UnaryOperator, Value as SqlValue, ValueWithSpan,
};
use sqlparser::tokenizer::Span;
use std::borrow::Cow;

/// The field every document holds its own path in.
pub(super) const SQL_DOCUMENT_NAME_FIELD: &str = "__name__";

impl FirestoreSqlTranslator<'_> {
    pub(super) fn expr(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        let mark = self.read_mark();
        let translated = self.nested(expr_start(expr), |translator| {
            translator.translate_expr(scope, expr)
        })?;
        Ok(self.grouped(translated, mark))
    }

    // Every arm is a call whose result is the function's: the translation recurses once per
    // level of nesting, and a debug build gives each temporary of an arm its own stack slot.
    fn translate_expr(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        match expr {
            Expr::Identifier(ident) => self.identifier(scope, ident),
            Expr::CompoundIdentifier(idents) => self.column(scope, idents),
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.field_access(scope, root, access_chain)
            }
            Expr::Value(value) => self.value(value),
            Expr::Nested(inner) => self.expr(scope, inner),
            Expr::BinaryOp { left, op, right } => self.binary(scope, expr, left, op, right),
            Expr::UnaryOp { op, expr: operand } => self.unary(scope, expr, op, operand),
            Expr::IsNull(operand) => self.predicate(scope, operand, FirestoreExpr::is_null),
            Expr::IsNotNull(operand) => self.predicate(scope, operand, FirestoreExpr::is_not_null),
            Expr::IsTrue(operand) => self.predicate(scope, operand, |value| value.equal(true)),
            Expr::IsNotTrue(operand) => {
                self.predicate(scope, operand, |value| value.not_equal(true))
            }
            Expr::IsFalse(operand) => self.predicate(scope, operand, |value| value.equal(false)),
            Expr::IsNotFalse(operand) => {
                self.predicate(scope, operand, |value| value.not_equal(false))
            }
            // Firestore's `equal` is true for two nulls, which is what `IS NOT DISTINCT FROM`
            // means in SQL.
            Expr::IsDistinctFrom(left, right) => {
                self.comparison(scope, left, right, FirestoreExpr::not_equal)
            }
            Expr::IsNotDistinctFrom(left, right) => {
                self.comparison(scope, left, right, FirestoreExpr::equal)
            }
            Expr::InList {
                expr: operand,
                list,
                negated,
            } => self.in_list(scope, operand, list, *negated),
            Expr::Between {
                expr: operand,
                negated,
                low,
                high,
            } => self.between(scope, operand, low, high, *negated),
            Expr::Like {
                negated,
                any: false,
                expr: operand,
                pattern,
                escape_char: None,
            } => self.like(scope, operand, pattern, *negated, false),
            Expr::ILike {
                negated,
                any: false,
                expr: operand,
                pattern,
                escape_char: None,
            } => self.like(scope, operand, pattern, *negated, true),
            Expr::Like {
                escape_char: Some(escape_char),
                ..
            }
            | Expr::ILike {
                escape_char: Some(escape_char),
                ..
            } => Err(FirestoreError::unsupported_sql(
                "ESCAPE",
                expr_start(escape_char),
            )),
            Expr::Like { .. } | Expr::ILike { .. } => Err(FirestoreError::unsupported_sql(
                "LIKE ANY",
                expr_start(expr),
            )),
            Expr::Function(function) => self.function(scope, function),
            Expr::Case {
                case_token: _,
                end_token: _,
                operand,
                conditions,
                else_result,
            } => self.case(
                scope,
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
            ),
            Expr::Substring {
                expr: operand,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => self.substring(
                scope,
                operand,
                substring_from.as_deref(),
                substring_for.as_deref(),
            ),
            Expr::Trim {
                trim_where,
                trim_what,
                expr: operand,
                trim_characters,
            } => match (trim_what.as_deref(), trim_characters.as_deref()) {
                (None, None) => self.trim(scope, operand, trim_where.as_ref(), None),
                (Some(characters), None) | (None, Some([characters])) => {
                    self.trim(scope, operand, trim_where.as_ref(), Some(characters))
                }
                _ => Err(FirestoreError::unsupported_sql(
                    "this TRIM",
                    expr_start(expr),
                )),
            },
            Expr::Ceil {
                expr: operand,
                field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            } => self.value_of(scope, operand, FirestoreExpr::ceil),
            Expr::Floor {
                expr: operand,
                field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
            } => self.value_of(scope, operand, FirestoreExpr::floor),
            Expr::Array(Array { elem, named: _ }) => self.array_literal(scope, elem),
            Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => Err(
                FirestoreError::unsupported_sql("a subquery", expr_start(expr)),
            ),
            Expr::InUnnest { .. } => {
                Err(FirestoreError::unsupported_sql("UNNEST", expr_start(expr)))
            }
            Expr::Cast { .. } => Err(FirestoreError::unsupported_sql("CAST", expr_start(expr))),
            Expr::Ceil { .. } | Expr::Floor { .. } => Err(FirestoreError::unsupported_sql(
                "CEIL or FLOOR with a unit or scale",
                expr_start(expr),
            )),
            Expr::TypedString(_) => Err(FirestoreError::unsupported_sql(
                "a typed literal",
                expr_start(expr),
            )),
            Expr::Interval(_) => Err(FirestoreError::unsupported_sql(
                "INTERVAL",
                expr_start(expr),
            )),
            Expr::Struct { .. } => Err(FirestoreError::unsupported_sql("STRUCT", expr_start(expr))),
            _ => Err(FirestoreError::unsupported_sql(
                "this expression",
                expr_start(expr),
            )),
        }
    }

    /// `operand` with `compute` applied to its translation.
    fn value_of(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        compute: impl FnOnce(FirestoreExpr) -> FirestoreExpr,
    ) -> FirestoreResult<FirestoreExpr> {
        Ok(compute(self.expr(scope, operand)?))
    }

    /// A test of `operand`, such as `IS NULL`.
    fn predicate(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        test: impl FnOnce(FirestoreExpr) -> FirestoreBoolExpr,
    ) -> FirestoreResult<FirestoreExpr> {
        Ok(test(self.expr(scope, operand)?).into())
    }

    /// A comparison of two values.
    fn comparison(
        &mut self,
        scope: &FirestoreSqlScope,
        left: &Expr,
        right: &Expr,
        compare: impl FnOnce(FirestoreExpr, FirestoreExpr) -> FirestoreBoolExpr,
    ) -> FirestoreResult<FirestoreExpr> {
        let (left, right) = self.compared(scope, left, right)?;
        Ok(compare(left, right).into())
    }

    /// An arithmetic operation on two values.
    fn arithmetic(
        &mut self,
        scope: &FirestoreSqlScope,
        left: &Expr,
        right: &Expr,
        combine: impl FnOnce(FirestoreExpr, FirestoreExpr) -> FirestoreExpr,
    ) -> FirestoreResult<FirestoreExpr> {
        let left = self.expr(scope, left)?;
        let right = self.expr(scope, right)?;
        Ok(combine(left, right))
    }

    fn array_literal(
        &mut self,
        scope: &FirestoreSqlScope,
        elements: &[Expr],
    ) -> FirestoreResult<FirestoreExpr> {
        let elements = elements
            .iter()
            .map(|element| self.expr(scope, element))
            .collect::<FirestoreResult<_>>()?;
        Ok(Self::array(elements))
    }

    /// A column, or the value bound to the parameter `@name`. In an aggregate query, a name the
    /// select list gives is the item it names.
    fn identifier(
        &mut self,
        scope: &FirestoreSqlScope,
        ident: &Ident,
    ) -> FirestoreResult<FirestoreExpr> {
        if is_parameter(ident) {
            return self.parameter(ident);
        }
        match self
            .aggregation
            .as_ref()
            .and_then(|aggregation| aggregation.output(&ident.value))
        {
            Some(output) => Ok(output.clone()),
            None => self.column(scope, std::slice::from_ref(ident)),
        }
    }

    /// The value bound to the parameter `ident` names.
    pub(super) fn parameter(&self, ident: &Ident) -> FirestoreResult<FirestoreExpr> {
        let name = ident.value.strip_prefix('@').unwrap_or(&ident.value);
        self.binding.named(name).cloned().ok_or_else(|| {
            FirestoreError::invalid_sql(
                &ident.value,
                format!("no value is bound to {}", ident.value),
                at(ident.span),
            )
        })
    }

    /// A literal, or the value bound to a `?`.
    pub(super) fn value(&self, value: &ValueWithSpan) -> FirestoreResult<FirestoreExpr> {
        let ValueWithSpan { value, span } = value;
        match value {
            SqlValue::Number(number, false) => number_constant(number, *span),
            SqlValue::SingleQuotedString(text)
            | SqlValue::DoubleQuotedString(text)
            | SqlValue::TripleSingleQuotedString(text)
            | SqlValue::TripleDoubleQuotedString(text)
            | SqlValue::SingleQuotedRawStringLiteral(text)
            | SqlValue::DoubleQuotedRawStringLiteral(text)
            | SqlValue::TripleSingleQuotedRawStringLiteral(text)
            | SqlValue::TripleDoubleQuotedRawStringLiteral(text) => {
                Ok(FirestoreExpr::constant(text))
            }
            SqlValue::Boolean(value) => Ok(FirestoreExpr::constant(*value)),
            SqlValue::Null => Ok(FirestoreExpr::constant(None::<bool>)),
            SqlValue::Placeholder(placeholder) => {
                self.binding.positional(span.start).cloned().ok_or_else(|| {
                    FirestoreError::invalid_sql(
                        placeholder,
                        "no value is bound to this parameter",
                        at(*span),
                    )
                })
            }
            _ => Err(FirestoreError::unsupported_sql("this literal", at(*span))),
        }
    }

    fn binary(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        match op {
            BinaryOperator::And => self.connective(scope, expr, op, |conditions| {
                FirestorePipelineExprBuilder.and(conditions)
            }),
            BinaryOperator::Or => self.connective(scope, expr, op, |conditions| {
                FirestorePipelineExprBuilder.or(conditions)
            }),
            BinaryOperator::StringConcat => self.concatenation(scope, expr, op),
            BinaryOperator::Eq => self.comparison(scope, left, right, FirestoreExpr::equal),
            BinaryOperator::NotEq => self.comparison(scope, left, right, FirestoreExpr::not_equal),
            BinaryOperator::Lt => self.comparison(scope, left, right, FirestoreExpr::less_than),
            BinaryOperator::LtEq => {
                self.comparison(scope, left, right, FirestoreExpr::less_than_or_equal)
            }
            BinaryOperator::Gt => self.comparison(scope, left, right, FirestoreExpr::greater_than),
            BinaryOperator::GtEq => {
                self.comparison(scope, left, right, FirestoreExpr::greater_than_or_equal)
            }
            BinaryOperator::Plus => self.arithmetic(scope, left, right, FirestoreExpr::add),
            BinaryOperator::Minus => self.arithmetic(scope, left, right, FirestoreExpr::subtract),
            BinaryOperator::Multiply => {
                self.arithmetic(scope, left, right, FirestoreExpr::multiply)
            }
            BinaryOperator::Divide => self.arithmetic(scope, left, right, FirestoreExpr::divide),
            BinaryOperator::Modulo => self.arithmetic(scope, left, right, FirestoreExpr::modulo),
            other => Err(FirestoreError::unsupported_sql(
                &format!("the operator {other}"),
                expr_start(expr),
            )),
        }
    }

    /// A chain of `AND` or `OR`, which becomes one `and` or `or` of every condition.
    fn connective(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        op: &BinaryOperator,
        connect: impl FnOnce(Vec<FirestoreBoolExpr>) -> FirestoreBoolExpr,
    ) -> FirestoreResult<FirestoreExpr> {
        let conditions = self.conditions(scope, &chain(expr, op))?;
        Ok(connect(conditions).into())
    }

    /// `operands` as one condition: itself when there is one, and their `and` otherwise.
    pub(super) fn conjunction(
        &mut self,
        scope: &FirestoreSqlScope,
        operands: &[&Expr],
    ) -> FirestoreResult<FirestoreExpr> {
        match operands {
            [only] => self.expr(scope, only),
            _ => {
                let conditions = self.conditions(scope, operands)?;
                Ok(FirestorePipelineExprBuilder.and(conditions).into())
            }
        }
    }

    fn conditions(
        &mut self,
        scope: &FirestoreSqlScope,
        operands: &[&Expr],
    ) -> FirestoreResult<Vec<FirestoreBoolExpr>> {
        operands
            .iter()
            .map(|operand| Ok(FirestoreBoolExpr::unchecked(self.expr(scope, operand)?)))
            .collect()
    }

    /// A chain of `||`, which becomes one `concat` of every operand.
    fn concatenation(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        op: &BinaryOperator,
    ) -> FirestoreResult<FirestoreExpr> {
        let operands = chain(expr, op)
            .into_iter()
            .map(|operand| self.expr(scope, operand))
            .collect::<FirestoreResult<Vec<_>>>()?;
        Ok(FirestoreExpr::raw("concat", operands))
    }

    fn unary(
        &mut self,
        scope: &FirestoreSqlScope,
        expr: &Expr,
        op: &UnaryOperator,
        operand: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        match (op, operand) {
            (UnaryOperator::Not, _) => Ok(FirestorePipelineExprBuilder
                .not(FirestoreBoolExpr::unchecked(self.expr(scope, operand)?))
                .into()),
            (UnaryOperator::Plus, _) => self.expr(scope, operand),
            (
                UnaryOperator::Minus,
                Expr::Value(ValueWithSpan {
                    value: SqlValue::Number(number, false),
                    span,
                }),
            ) => number_constant(&format!("-{number}"), *span),
            (UnaryOperator::Minus, _) => {
                Ok(FirestoreExpr::constant(0).subtract(self.expr(scope, operand)?))
            }
            (other, _) => Err(FirestoreError::unsupported_sql(
                &format!("the operator {other}"),
                expr_start(expr),
            )),
        }
    }

    /// Both sides of a comparison. A string compared with `__name__` names a document, and
    /// becomes the reference that `__name__` holds.
    fn compared(
        &mut self,
        scope: &FirestoreSqlScope,
        left: &Expr,
        right: &Expr,
    ) -> FirestoreResult<(FirestoreExpr, FirestoreExpr)> {
        let left_value = self.expr(scope, left)?;
        let right_value = self.expr(scope, right)?;
        if Self::is_document_name(&left_value) {
            let right_value = self.document_reference(scope, right_value, expr_start(right))?;
            Ok((left_value, right_value))
        } else if Self::is_document_name(&right_value) {
            let left_value = self.document_reference(scope, left_value, expr_start(left))?;
            Ok((left_value, right_value))
        } else {
            Ok((left_value, right_value))
        }
    }

    fn in_list(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        list: &[Expr],
        negated: bool,
    ) -> FirestoreResult<FirestoreExpr> {
        let operand = self.expr(scope, operand)?;
        let names_documents = Self::is_document_name(&operand);
        let mut elements = Vec::with_capacity(list.len());
        for element in list {
            let value = self.expr(scope, element)?;
            elements.push(if names_documents {
                self.document_reference(scope, value, expr_start(element))?
            } else {
                value
            });
        }
        let search_space = Self::array(elements);
        Ok(if negated {
            operand.not_equal_any(search_space)
        } else {
            operand.equal_any(search_space)
        }
        .into())
    }

    fn between(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
    ) -> FirestoreResult<FirestoreExpr> {
        let (operand, low) = self.compared(scope, operand, low)?;
        let mut high_value = self.expr(scope, high)?;
        if Self::is_document_name(&operand) {
            high_value = self.document_reference(scope, high_value, expr_start(high))?;
        }
        let values = FirestorePipelineExprBuilder;
        Ok(if negated {
            values.or([
                operand.clone().less_than(low),
                operand.greater_than(high_value),
            ])
        } else {
            values.and([
                operand.clone().greater_than_or_equal(low),
                operand.less_than_or_equal(high_value),
            ])
        }
        .into())
    }

    /// `LIKE`, or `ILIKE` as a `LIKE` of both sides in lower case.
    fn like(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        pattern: &Expr,
        negated: bool,
        case_insensitive: bool,
    ) -> FirestoreResult<FirestoreExpr> {
        let operand = self.expr(scope, operand)?;
        let pattern = self.expr(scope, pattern)?;
        let matched = if case_insensitive {
            operand.to_lower().like(pattern.to_lower())
        } else {
            operand.like(pattern)
        };
        Ok(if negated {
            FirestorePipelineExprBuilder.not(matched)
        } else {
            matched
        }
        .into())
    }

    fn case(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: Option<&Expr>,
        conditions: &[CaseWhen],
        else_result: Option<&Expr>,
    ) -> FirestoreResult<FirestoreExpr> {
        let operand = operand
            .map(|operand| self.expr(scope, operand))
            .transpose()?;
        let mut cases = Vec::with_capacity(conditions.len());
        for CaseWhen { condition, result } in conditions {
            let condition = self.expr(scope, condition)?;
            let condition = match &operand {
                Some(operand) => operand.clone().equal(condition),
                None => FirestoreBoolExpr::unchecked(condition),
            };
            cases.push((condition, self.expr(scope, result)?));
        }
        // Without ELSE, SQL's CASE is null when no condition holds, where `switch_on` fails.
        let default = match else_result {
            Some(else_result) => self.expr(scope, else_result)?,
            None => FirestoreExpr::constant(None::<bool>),
        };
        Ok(FirestorePipelineExprBuilder.switch_on_with_default(cases, default))
    }

    fn substring(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        from: Option<&Expr>,
        length: Option<&Expr>,
    ) -> FirestoreResult<FirestoreExpr> {
        let operand = self.expr(scope, operand)?;
        let position = match from {
            Some(from) => self.zero_based(scope, from)?,
            None => FirestoreExpr::constant(0),
        };
        Ok(match length {
            Some(length) => operand.substring_with_length(position, self.expr(scope, length)?),
            None => operand.substring(position),
        })
    }

    /// A position SQL counts from 1, counted from 0 as pipelines count it.
    pub(super) fn zero_based(
        &mut self,
        scope: &FirestoreSqlScope,
        position: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        let position = self.expr(scope, position)?;
        Ok(match &position {
            FirestoreExpr::Constant(FirestoreValue {
                value:
                    Value {
                        value_type: Some(ValueType::IntegerValue(counted_from_one)),
                    },
            }) => match counted_from_one.checked_sub(1) {
                Some(counted_from_zero) => FirestoreExpr::constant(counted_from_zero),
                None => position.subtract(1),
            },
            _ => position.subtract(1),
        })
    }

    fn trim(
        &mut self,
        scope: &FirestoreSqlScope,
        operand: &Expr,
        side: Option<&TrimWhereField>,
        characters: Option<&Expr>,
    ) -> FirestoreResult<FirestoreExpr> {
        let operand = self.expr(scope, operand)?;
        let characters = characters
            .map(|characters| self.expr(scope, characters))
            .transpose()?;
        Ok(match (side, characters) {
            (None | Some(TrimWhereField::Both), None) => operand.trim(),
            (None | Some(TrimWhereField::Both), Some(characters)) => operand.trim_with(characters),
            (Some(TrimWhereField::Leading), None) => operand.ltrim(),
            (Some(TrimWhereField::Leading), Some(characters)) => operand.ltrim_with(characters),
            (Some(TrimWhereField::Trailing), None) => operand.rtrim(),
            (Some(TrimWhereField::Trailing), Some(characters)) => operand.rtrim_with(characters),
        })
    }

    /// `x.field`, `x['key']` and `x[OFFSET(n)]` after a column or another value.
    fn field_access(
        &mut self,
        scope: &FirestoreSqlScope,
        root: &Expr,
        access_chain: &[AccessExpr],
    ) -> FirestoreResult<FirestoreExpr> {
        let mut value = self.expr(scope, root)?;
        for access in access_chain {
            value = match access {
                AccessExpr::Dot(Expr::Identifier(ident)) => match value {
                    FirestoreExpr::Field(path) => {
                        let nested = scope.column(std::slice::from_ref(ident))?;
                        FirestoreExpr::Field(format!("{path}.{nested}"))
                    }
                    other => other.map_get(ident.value.clone()),
                },
                AccessExpr::Subscript(Subscript::Index { index }) => {
                    self.subscript(scope, value, index)?
                }
                AccessExpr::Dot(other) => {
                    return Err(FirestoreError::unsupported_sql(
                        "this field access",
                        expr_start(other),
                    ))
                }
                AccessExpr::Subscript(Subscript::Slice { .. }) => {
                    return Err(FirestoreError::unsupported_sql(
                        "an array slice",
                        expr_start(root),
                    ))
                }
            };
        }
        Ok(value)
    }

    /// `value[index]`: an element of an array by its position, or the value of a map by a
    /// string key.
    fn subscript(
        &mut self,
        scope: &FirestoreSqlScope,
        value: FirestoreExpr,
        index: &Expr,
    ) -> FirestoreResult<FirestoreExpr> {
        if let Expr::Function(function) = index {
            if let Some(position) = self.array_position(scope, function)? {
                return Ok(value.array_get(position));
            }
        }
        Ok(match self.expr(scope, index)? {
            key @ FirestoreExpr::Constant(FirestoreValue {
                value:
                    Value {
                        value_type: Some(ValueType::StringValue(_)),
                    },
            }) => value.map_get(key),
            position => value.array_get(position),
        })
    }

    /// The constant reference to the document a string names, when `operand` is a string: a
    /// path relative to the documents root, a full path, or a bare ID in the source's
    /// collection. Any other operand is kept.
    pub(super) fn document_reference(
        &self,
        scope: &FirestoreSqlScope,
        operand: FirestoreExpr,
        start: Option<FirestoreSqlLocation>,
    ) -> FirestoreResult<FirestoreExpr> {
        let FirestoreExpr::Constant(FirestoreValue {
            value:
                Value {
                    value_type: Some(ValueType::StringValue(name)),
                },
        }) = &operand
        else {
            return Ok(operand);
        };
        let path = match (name.contains('/'), &scope.relation().collection_path) {
            (true, _) => Cow::Borrowed(name.as_str()),
            (false, Some(collection_path)) => Cow::Owned(format!("{collection_path}/{name}")),
            (false, None) => {
                return Err(FirestoreError::invalid_sql(
                    SQL_DOCUMENT_NAME_FIELD,
                    format!(
                        "`{name}` is a document ID, which needs a single collection as the source; give the document's path"
                    ),
                    start,
                ))
            }
        };
        let path = self.document_path(&path, start)?;
        Ok(FirestoreExpr::constant(FirestoreReference(format!(
            "{}/{path}",
            self.documents_path
        ))))
    }

    /// Whether `expr` is the field that holds each document's path.
    pub(super) fn is_document_name(expr: &FirestoreExpr) -> bool {
        matches!(expr, FirestoreExpr::Field(path) if path == SQL_DOCUMENT_NAME_FIELD)
    }

    /// An array of `elements`: a constant when they all are, so that `IN` reads like the fluent
    /// `is_in` with a list.
    fn array(elements: Vec<FirestoreExpr>) -> FirestoreExpr {
        let constants = elements
            .iter()
            .map(|element| match element {
                FirestoreExpr::Constant(constant) => Some(constant.value.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        match constants {
            Some(values) => FirestoreExpr::Constant(FirestoreValue::from(Value {
                value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
            })),
            None => FirestorePipelineExprBuilder.array(elements),
        }
    }
}

/// The operands of a chain of `op`, such as `a AND b AND c`, in order. sqlparser nests the chain
/// to the left, one level per operator, so it is read in a loop rather than recursively.
pub(super) fn chain<'e>(expr: &'e Expr, op: &BinaryOperator) -> Vec<&'e Expr> {
    let mut operands = Vec::new();
    let mut rest = expr;
    while let Expr::BinaryOp {
        left,
        op: rest_op,
        right,
    } = rest
    {
        if rest_op != op {
            break;
        }
        operands.push(right.as_ref());
        rest = left;
    }
    operands.push(rest);
    operands.reverse();
    operands
}

/// A number literal: an integer unless it has a fraction or an exponent.
fn number_constant(number: &str, span: Span) -> FirestoreResult<FirestoreExpr> {
    let out_of_range = || {
        FirestoreError::invalid_sql(
            number,
            format!("{number} is out of the range of its type"),
            at(span),
        )
    };
    if number.contains(['.', 'e', 'E']) {
        number
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(FirestoreExpr::constant)
            .ok_or_else(out_of_range)
    } else {
        number
            .parse::<i64>()
            .map(FirestoreExpr::constant)
            .map_err(|_| out_of_range())
    }
}
