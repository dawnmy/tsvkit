use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use regex::Regex;

use crate::aggregate::{AggregateKind, evaluate_row_aggregate, try_parse_aggregate_kind};
use crate::common::{
    ColumnSelector, parse_selector_list, parse_single_selector, resolve_selectors,
};

#[derive(Debug, Clone)]
pub enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare(ValueExpr, CompareOp, ValueExpr),
    Value(ValueExpr),
}

#[derive(Debug, Clone)]
pub enum ValueExpr {
    Column(ColumnSelector),
    Columns(Vec<ColumnSelector>),
    String(String),
    Number(f64),
    Unary(UnaryOp, Box<ValueExpr>),
    Binary(BinaryOp, Box<ValueExpr>, Box<ValueExpr>),
    Function(FunctionName, Box<ValueExpr>),
    Aggregate(AggregateSpecExpr),
}

#[derive(Debug, Clone)]
pub struct AggregateSpecExpr {
    pub kind: AggregateKind,
    pub selectors: Vec<ColumnSelector>,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Neg,
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Pow,
}

#[derive(Debug, Clone, Copy)]
pub enum FunctionName {
    Abs,
    Sqrt,
    Exp,
    Exp2,
    Ln,
    Log10,
    Log2,
}

impl FunctionName {
    pub fn from_ident(ident: &str) -> Result<Self> {
        match ident.to_ascii_lowercase().as_str() {
            "abs" => Ok(FunctionName::Abs),
            "sqrt" => Ok(FunctionName::Sqrt),
            "exp" => Ok(FunctionName::Exp),
            "exp2" => Ok(FunctionName::Exp2),
            "ln" => Ok(FunctionName::Ln),
            "log" | "log10" => Ok(FunctionName::Log10),
            "log2" => Ok(FunctionName::Log2),
            other => bail!(
                "unsupported function '{}': try abs, sqrt, exp, exp2, ln, log, log10, log2",
                other
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    RegexMatch,
    RegexNotMatch,
}

#[derive(Debug, Clone)]
enum Token {
    Column(ColumnSelector),
    Columns(Vec<ColumnSelector>),
    String(String),
    Number(f64),
    Ident(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Compare(CompareOp),
    Plus,
    Minus,
    Star,
    Slash,
    Caret,
}

pub fn parse_expression(input: &str) -> Result<Expr> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    while let Some(token) = lexer.next_token()? {
        tokens.push(token);
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;
    if parser.has_more() {
        bail!("unexpected trailing tokens in expression");
    }
    Ok(expr)
}

pub fn parse_value_expression(input: &str) -> Result<ValueExpr> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    while let Some(token) = lexer.next_token()? {
        tokens.push(token);
    }
    let mut parser = Parser::new(tokens);
    let value = parser.parse_arith()?;
    if parser.has_more() {
        bail!("unexpected trailing tokens in expression");
    }
    Ok(value)
}

pub fn bind_expression(expr: Expr, headers: &[String], no_header: bool) -> Result<BoundExpr> {
    match expr {
        Expr::Or(lhs, rhs) => Ok(BoundExpr::Or(
            Box::new(bind_expression(*lhs, headers, no_header)?),
            Box::new(bind_expression(*rhs, headers, no_header)?),
        )),
        Expr::And(lhs, rhs) => Ok(BoundExpr::And(
            Box::new(bind_expression(*lhs, headers, no_header)?),
            Box::new(bind_expression(*rhs, headers, no_header)?),
        )),
        Expr::Not(inner) => Ok(BoundExpr::Not(Box::new(bind_expression(
            *inner, headers, no_header,
        )?))),
        Expr::Compare(lhs, op, rhs) => match op {
            CompareOp::RegexMatch | CompareOp::RegexNotMatch => {
                let value = bind_value(lhs, headers, no_header)?;
                let pattern = bind_regex_pattern(rhs, headers, no_header)?;
                Ok(BoundExpr::RegexMatch {
                    value,
                    pattern,
                    invert: matches!(op, CompareOp::RegexNotMatch),
                })
            }
            _ => Ok(BoundExpr::Compare(
                bind_value(lhs, headers, no_header)?,
                op,
                bind_value(rhs, headers, no_header)?,
            )),
        },
        Expr::Value(val) => Ok(BoundExpr::Value(bind_value(val, headers, no_header)?)),
    }
}

pub fn bind_value_expression(
    value: ValueExpr,
    headers: &[String],
    no_header: bool,
) -> Result<BoundValue> {
    bind_value(value, headers, no_header)
}

pub trait RowAccessor {
    fn get(&self, idx: usize) -> Option<&str>;
}

impl RowAccessor for csv::StringRecord {
    fn get(&self, idx: usize) -> Option<&str> {
        csv::StringRecord::get(self, idx)
    }
}

impl RowAccessor for [String] {
    fn get(&self, idx: usize) -> Option<&str> {
        self.get(idx).map(|s| s.as_str())
    }
}

impl RowAccessor for Vec<String> {
    fn get(&self, idx: usize) -> Option<&str> {
        self.as_slice().get(idx).map(|s| s.as_str())
    }
}

#[derive(Debug, Clone)]
pub enum BoundValue {
    Column(usize),
    Columns(Vec<usize>),
    String(String),
    Number(f64),
    Unary(UnaryOp, Box<BoundValue>),
    Binary(BinaryOp, Box<BoundValue>, Box<BoundValue>),
    Function(FunctionName, Box<BoundValue>),
    Aggregate(BoundAggregate),
}

#[derive(Debug, Clone)]
pub struct BoundAggregate {
    pub kind: AggregateKind,
    pub columns: Vec<usize>,
}

#[derive(Debug)]
pub enum BoundExpr {
    Or(Box<BoundExpr>, Box<BoundExpr>),
    And(Box<BoundExpr>, Box<BoundExpr>),
    Not(Box<BoundExpr>),
    Compare(BoundValue, CompareOp, BoundValue),
    RegexMatch {
        value: BoundValue,
        pattern: RegexPattern,
        invert: bool,
    },
    Value(BoundValue),
}

#[derive(Debug)]
pub(crate) enum RegexPattern {
    Static(Arc<Regex>),
    Dynamic(Box<BoundValue>),
}

pub struct EvalValue<'a> {
    pub text: Cow<'a, str>,
    pub numeric: Option<f64>,
}

pub fn evaluate<R>(expr: &BoundExpr, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    match expr {
        BoundExpr::Or(lhs, rhs) => evaluate(lhs, row) || evaluate(rhs, row),
        BoundExpr::And(lhs, rhs) => evaluate(lhs, row) && evaluate(rhs, row),
        BoundExpr::Not(inner) => !evaluate(inner, row),
        BoundExpr::Compare(lhs, op, rhs) => evaluate_compare(lhs, *op, rhs, row),
        BoundExpr::RegexMatch {
            value,
            pattern,
            invert,
        } => evaluate_regex(value, pattern, *invert, row),
        BoundExpr::Value(value) => evaluate_truthy(value, row),
    }
}

pub fn eval_value<'a, R>(value: &'a BoundValue, row: &'a R) -> EvalValue<'a>
where
    R: RowAccessor + ?Sized,
{
    match value {
        BoundValue::Column(idx) => {
            let text = row.get(*idx).unwrap_or("");
            EvalValue {
                text: Cow::Borrowed(text),
                numeric: parse_float(text),
            }
        }
        BoundValue::Columns(indices) => {
            if indices.is_empty() {
                return empty_eval();
            }
            let mut combined = String::new();
            let mut numeric = None;
            for (pos, idx) in indices.iter().enumerate() {
                let text = row.get(*idx).unwrap_or("");
                if pos > 0 {
                    combined.push('\t');
                }
                combined.push_str(text);
                if numeric.is_none() {
                    numeric = parse_float(text);
                }
            }
            EvalValue {
                text: Cow::Owned(combined),
                numeric,
            }
        }
        BoundValue::String(text) => EvalValue {
            text: Cow::Borrowed(text.as_str()),
            numeric: parse_float(text),
        },
        BoundValue::Number(number) => numeric_eval(*number),
        BoundValue::Unary(op, inner) => {
            let inner_eval = eval_value(inner, row);
            if let Some(val) = inner_eval.numeric {
                numeric_eval(match op {
                    UnaryOp::Neg => -val,
                })
            } else {
                empty_eval()
            }
        }
        BoundValue::Binary(op, left, right) => {
            let left_eval = eval_value(left, row);
            let right_eval = eval_value(right, row);
            match (left_eval.numeric, right_eval.numeric) {
                (Some(a), Some(b)) => match op {
                    BinaryOp::Add => numeric_eval(a + b),
                    BinaryOp::Sub => numeric_eval(a - b),
                    BinaryOp::Mul => numeric_eval(a * b),
                    BinaryOp::Div => {
                        if b.abs() < f64::EPSILON {
                            empty_eval()
                        } else {
                            numeric_eval(a / b)
                        }
                    }
                    BinaryOp::Pow => {
                        let value = a.powf(b);
                        if value.is_finite() {
                            numeric_eval(value)
                        } else {
                            empty_eval()
                        }
                    }
                },
                _ => empty_eval(),
            }
        }
        BoundValue::Function(func, inner) => {
            let inner_eval = eval_value(inner, row);
            if let Some(val) = inner_eval.numeric {
                let result = match func {
                    FunctionName::Abs => Some(val.abs()),
                    FunctionName::Sqrt => {
                        let value = val.sqrt();
                        value.is_finite().then_some(value)
                    }
                    FunctionName::Exp => {
                        let value = val.exp();
                        value.is_finite().then_some(value)
                    }
                    FunctionName::Exp2 => {
                        let value = val.exp2();
                        value.is_finite().then_some(value)
                    }
                    FunctionName::Ln => (val > 0.0).then(|| val.ln()),
                    FunctionName::Log10 => (val > 0.0).then(|| val.log10()),
                    FunctionName::Log2 => (val > 0.0).then(|| val.log2()),
                };
                result.map(numeric_eval).unwrap_or_else(empty_eval)
            } else {
                empty_eval()
            }
        }
        BoundValue::Aggregate(spec) => {
            let values = spec
                .columns
                .iter()
                .map(|&idx| row.get(idx).unwrap_or(""))
                .collect::<Vec<_>>();
            let result = evaluate_row_aggregate(&spec.kind, &values);
            EvalValue {
                text: Cow::Owned(result.text),
                numeric: result.numeric,
            }
        }
    }
}

pub fn evaluate_truthy<R>(value: &BoundValue, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    match value {
        BoundValue::Columns(indices) => indices.iter().any(|idx| {
            row.get(*idx)
                .map(|text| !text.trim().is_empty())
                .unwrap_or(false)
        }),
        _ => {
            let eval = eval_value(value, row);
            if let Some(number) = eval.numeric {
                number != 0.0
            } else {
                !eval.text.trim().is_empty()
            }
        }
    }
}

fn evaluate_compare<R>(lhs: &BoundValue, op: CompareOp, rhs: &BoundValue, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    let left = eval_value(lhs, row);
    let right = eval_value(rhs, row);

    match op {
        CompareOp::Eq => {
            if let (Some(a), Some(b)) = (left.numeric, right.numeric) {
                a == b
            } else {
                left.text == right.text
            }
        }
        CompareOp::Ne => {
            if let (Some(a), Some(b)) = (left.numeric, right.numeric) {
                a != b
            } else {
                left.text != right.text
            }
        }
        CompareOp::Gt => compare_numeric(&left, &right, |a, b| a > b),
        CompareOp::Ge => compare_numeric(&left, &right, |a, b| a >= b),
        CompareOp::Lt => compare_numeric(&left, &right, |a, b| a < b),
        CompareOp::Le => compare_numeric(&left, &right, |a, b| a <= b),
        CompareOp::RegexMatch | CompareOp::RegexNotMatch => false,
    }
}

fn evaluate_regex<R>(value: &BoundValue, pattern: &RegexPattern, invert: bool, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    let is_match = match pattern {
        RegexPattern::Static(regex) => match value {
            BoundValue::Column(idx) => regex.is_match(row.get(*idx).unwrap_or("")),
            BoundValue::Columns(indices) => indices
                .iter()
                .any(|idx| regex.is_match(row.get(*idx).unwrap_or(""))),
            _ => {
                let hay = eval_value(value, row);
                regex.is_match(hay.text.as_ref())
            }
        },
        RegexPattern::Dynamic(bound) => {
            let pat_eval = eval_value(bound, row);
            let pattern_text = pat_eval.text.as_ref();
            if let Ok(regex) = Regex::new(pattern_text) {
                match value {
                    BoundValue::Column(idx) => regex.is_match(row.get(*idx).unwrap_or("")),
                    BoundValue::Columns(indices) => indices
                        .iter()
                        .any(|idx| regex.is_match(row.get(*idx).unwrap_or(""))),
                    _ => {
                        let hay = eval_value(value, row);
                        regex.is_match(hay.text.as_ref())
                    }
                }
            } else {
                false
            }
        }
    };
    if invert { !is_match } else { is_match }
}

fn bind_value(value: ValueExpr, headers: &[String], no_header: bool) -> Result<BoundValue> {
    match value {
        ValueExpr::Column(selector) => {
            let indices = resolve_selectors(headers, &[selector], no_header)?;
            if indices.is_empty() {
                bail!("column selector resolved to no columns");
            }
            if indices.len() == 1 {
                Ok(BoundValue::Column(indices[0]))
            } else {
                Ok(BoundValue::Columns(indices))
            }
        }
        ValueExpr::Columns(selectors) => {
            let mut indices = Vec::new();
            for selector in selectors {
                let mut resolved = resolve_selectors(headers, &[selector], no_header)?;
                indices.append(&mut resolved);
            }
            if indices.is_empty() {
                bail!("column selector resolved to no columns");
            }
            if indices.len() == 1 {
                Ok(BoundValue::Column(indices[0]))
            } else {
                Ok(BoundValue::Columns(indices))
            }
        }
        ValueExpr::Aggregate(spec) => {
            let mut indices = Vec::new();
            for selector in spec.selectors {
                let mut resolved = resolve_selectors(headers, &[selector], no_header)?;
                indices.append(&mut resolved);
            }
            if indices.is_empty() {
                bail!("aggregate selector resolved to no columns");
            }
            Ok(BoundValue::Aggregate(BoundAggregate {
                kind: spec.kind,
                columns: indices,
            }))
        }
        ValueExpr::String(text) => Ok(BoundValue::String(text)),
        ValueExpr::Number(number) => Ok(BoundValue::Number(number)),
        ValueExpr::Unary(op, inner) => {
            let bound_inner = bind_value(*inner, headers, no_header)?;
            Ok(BoundValue::Unary(op, Box::new(bound_inner)))
        }
        ValueExpr::Binary(op, left, right) => {
            let bound_left = bind_value(*left, headers, no_header)?;
            let bound_right = bind_value(*right, headers, no_header)?;
            Ok(BoundValue::Binary(
                op,
                Box::new(bound_left),
                Box::new(bound_right),
            ))
        }
        ValueExpr::Function(func, inner) => {
            let bound_inner = bind_value(*inner, headers, no_header)?;
            Ok(BoundValue::Function(func, Box::new(bound_inner)))
        }
    }
}

fn bind_regex_pattern(
    value: ValueExpr,
    headers: &[String],
    no_header: bool,
) -> Result<RegexPattern> {
    match value {
        ValueExpr::String(pattern) => {
            let regex = Regex::new(&pattern)
                .with_context(|| format!("invalid regex pattern '{}'", pattern))?;
            Ok(RegexPattern::Static(Arc::new(regex)))
        }
        other => {
            let bound = bind_value(other, headers, no_header)?;
            Ok(RegexPattern::Dynamic(Box::new(bound)))
        }
    }
}

fn compare_numeric<F>(left: &EvalValue<'_>, right: &EvalValue<'_>, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (left.numeric, right.numeric) {
        (Some(a), Some(b)) => cmp(a, b),
        _ => false,
    }
}

fn numeric_eval(value: f64) -> EvalValue<'static> {
    if value.is_finite() {
        EvalValue {
            text: Cow::Owned(format_number(value)),
            numeric: Some(value),
        }
    } else {
        empty_eval()
    }
}

fn empty_eval<'a>() -> EvalValue<'a> {
    EvalValue {
        text: Cow::Owned(String::new()),
        numeric: None,
    }
}

fn parse_float(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.6}", value)
    }
}

struct Lexer<'a> {
    chars: &'a [u8],
    pos: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_matches_any_in_range() {
        let mut lexer = Lexer::new("$a:$c ~ \"RNA\"");
        match lexer.next_token().unwrap().unwrap() {
            Token::Columns(ref cols) => {
                assert_eq!(cols.len(), 1);
            }
            other => panic!("expected column token, got {:?}", other),
        }
        let expr = parse_expression("$a:$c ~ \"RNA\"").unwrap();
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec!["foo".to_string(), "bar".to_string(), "sRNA".to_string()];
        assert!(evaluate(&bound, &row));
        let row = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        assert!(!evaluate(&bound, &row));
    }

    #[test]
    fn regex_matches_all_columns_when_omitted() {
        let expr = parse_expression("~ \"RNA\"").unwrap();
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec!["foo".to_string(), "pre-RNA".to_string(), "baz".to_string()];
        assert!(evaluate(&bound, &row));
    }

    #[test]
    fn regex_matches_mixed_selector_list() {
        let expr = parse_expression("$a,$c: ~ \"RNA\"").unwrap();
        let headers = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "mRNA".to_string(),
        ];
        assert!(evaluate(&bound, &row));
    }

    #[test]
    fn numeric_equality_uses_numeric_comparison() {
        let expr = parse_expression("$1 == 0").unwrap();
        let headers = vec!["value".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = csv::StringRecord::from(vec!["0.0"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn string_equality_remains_textual_when_not_numeric() {
        let expr = parse_expression("$1 == 0").unwrap();
        let headers = vec!["value".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = csv::StringRecord::from(vec!["00a"]);
        assert!(!evaluate(&bound, &record));
    }

    #[test]
    fn subtraction_without_spaces_between_columns() {
        let expr = parse_expression("($dna_ug-$rna_ug)>10").unwrap();
        let headers = vec!["dna_ug".to_string(), "rna_ug".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec!["25.0".to_string(), "12.0".to_string()];
        assert!(evaluate(&bound, &row));
    }

    #[test]
    fn subtraction_with_space_after_minus() {
        let expr = parse_expression("($dna_ug- $rna_ug)>10").unwrap();
        let headers = vec!["dna_ug".to_string(), "rna_ug".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec!["22.0".to_string(), "5.0".to_string()];
        assert!(evaluate(&bound, &row));
    }

    #[test]
    fn exponentiation_operator_supported() {
        let expr = parse_value_expression("$dna_ug^2").unwrap();
        let headers = vec!["dna_ug".to_string()];
        let bound = bind_value_expression(expr, &headers, false).unwrap();
        let row = vec!["3".to_string()];
        let eval = eval_value(&bound, &row);
        assert_eq!(eval.numeric, Some(9.0));
    }

    #[test]
    fn exponentiation_is_right_associative() {
        let expr = parse_value_expression("2^3^2").unwrap();
        let bound = bind_value_expression(expr, &[], false).unwrap();
        let row: Vec<String> = Vec::new();
        let eval = eval_value(&bound, &row);
        assert_eq!(eval.numeric, Some(512.0));
    }

    #[test]
    fn braces_allow_literal_column_names() {
        let expr = parse_expression("${dna-}").unwrap();
        let headers = vec!["dna-".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let row = vec!["value".to_string()];
        assert!(evaluate(&bound, &row));
    }

    #[test]
    fn value_expression_supports_common_escape_sequences() {
        let expr = parse_value_expression("\"line\\nfeed\\tend\"").unwrap();
        match expr {
            ValueExpr::String(text) => assert_eq!(text, "line\nfeed\tend"),
            other => panic!("expected string literal, got {:?}", other),
        }
    }

    #[test]
    fn value_expression_preserves_unknown_escape_sequences() {
        let expr = parse_value_expression("\"\\\\.\\\\|\\\\(\\\\)\"").unwrap();
        match expr {
            ValueExpr::String(text) => assert_eq!(text, "\\.\\|\\(\\)"),
            other => panic!("expected string literal, got {:?}", other),
        }
    }
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Lexer {
            chars: input.as_bytes(),
            pos: 0,
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        self.skip_whitespace();
        if self.pos >= self.chars.len() {
            return Ok(None);
        }
        let ch = self.chars[self.pos];
        match ch {
            b'$' => self.lex_column(),
            b'"' => self.lex_string(),
            b'&' => {
                self.pos += 1;
                if self.match_char(b'&') {
                    self.pos += 1;
                }
                Ok(Some(Token::And))
            }
            b'|' => {
                self.pos += 1;
                if self.match_char(b'|') {
                    self.pos += 1;
                }
                Ok(Some(Token::Or))
            }
            b'!' => {
                if self.peek_char(1) == Some(b'=') {
                    self.pos += 2;
                    Ok(Some(Token::Compare(CompareOp::Ne)))
                } else if self.peek_char(1) == Some(b'~') {
                    self.pos += 2;
                    Ok(Some(Token::Compare(CompareOp::RegexNotMatch)))
                } else {
                    self.pos += 1;
                    Ok(Some(Token::Not))
                }
            }
            b'~' => {
                self.pos += 1;
                Ok(Some(Token::Compare(CompareOp::RegexMatch)))
            }
            b'=' => {
                if self.peek_char(1) == Some(b'=') {
                    self.pos += 2;
                    Ok(Some(Token::Compare(CompareOp::Eq)))
                } else {
                    bail!("unexpected '='; use '==' for equality");
                }
            }
            b'>' => {
                self.pos += 1;
                if self.match_char(b'=') {
                    self.pos += 1;
                    Ok(Some(Token::Compare(CompareOp::Ge)))
                } else {
                    Ok(Some(Token::Compare(CompareOp::Gt)))
                }
            }
            b'<' => {
                self.pos += 1;
                if self.match_char(b'=') {
                    self.pos += 1;
                    Ok(Some(Token::Compare(CompareOp::Le)))
                } else {
                    Ok(Some(Token::Compare(CompareOp::Lt)))
                }
            }
            b'(' => {
                self.pos += 1;
                Ok(Some(Token::LParen))
            }
            b')' => {
                self.pos += 1;
                Ok(Some(Token::RParen))
            }
            b'+' => {
                self.pos += 1;
                Ok(Some(Token::Plus))
            }
            b'-' => {
                self.pos += 1;
                Ok(Some(Token::Minus))
            }
            b'*' => {
                self.pos += 1;
                Ok(Some(Token::Star))
            }
            b'/' => {
                self.pos += 1;
                Ok(Some(Token::Slash))
            }
            b'^' => {
                self.pos += 1;
                Ok(Some(Token::Caret))
            }
            c if c.is_ascii_digit() || c == b'.' => self.lex_number(),
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let ident = self.lex_identifier();
                Ok(Some(Token::Ident(ident)))
            }
            _ => bail!("unexpected character '{}' in expression", ch as char),
        }
    }

    fn lex_column(&mut self) -> Result<Option<Token>> {
        self.pos += 1;
        if self.match_char(b'{') {
            self.pos += 1;
            let mut value = String::new();
            let mut escaped = false;
            while self.pos < self.chars.len() {
                let c = self.chars[self.pos];
                self.pos += 1;
                if escaped {
                    value.push(c as char);
                    escaped = false;
                    continue;
                }
                match c {
                    b'\\' => escaped = true,
                    b'}' => {
                        return Ok(Some(Token::Column(ColumnSelector::Name(value))));
                    }
                    other => value.push(other as char),
                }
            }
            bail!("unterminated '{{' in column selector");
        }
        let start = self.pos;
        let mut is_numeric = true;
        let mut has_range_syntax = false;
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            if c == b'$' {
                self.pos += 1;
                continue;
            }
            if c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b',' | b':') {
                if c == b',' || c == b':' {
                    has_range_syntax = true;
                    is_numeric = false;
                } else if !c.is_ascii_digit() {
                    is_numeric = false;
                }
                if c == b',' || c == b':' {
                    self.pos += 1;
                    continue;
                }
                if !c.is_ascii_digit() {
                    is_numeric = false;
                }
                self.pos += 1;
                continue;
            }
            if c == b'-' {
                if is_numeric && self.pos > start {
                    break;
                }
                let mut should_break = false;
                if self.pos > start {
                    if let Some(next) = self.peek_non_whitespace(1) {
                        should_break = matches!(
                            next,
                            b'$' | b'(' | b')' | b'+' | b'-' | b'*' | b'/' | b'^' | b'"'
                        ) || next.is_ascii_digit()
                            || next == b'.';
                    }
                }
                if should_break {
                    break;
                }
                is_numeric = false;
                self.pos += 1;
                continue;
            }
            break;
        }
        if start == self.pos {
            bail!("column reference requires a name or index after '$'");
        }
        let raw = std::str::from_utf8(&self.chars[start..self.pos]).unwrap();
        let cleaned = raw.replace('$', "");
        let text = cleaned.trim();
        if has_range_syntax || text.contains(',') {
            let selectors = parse_selector_list(text)
                .with_context(|| format!("invalid column selector '{}'", text))?;
            if selectors.is_empty() {
                bail!("column selector list must not be empty");
            }
            return Ok(Some(Token::Columns(selectors)));
        }
        let selector = parse_single_selector(text)
            .with_context(|| format!("invalid column selector '{}'", text))?;
        Ok(Some(Token::Column(selector)))
    }

    fn lex_string(&mut self) -> Result<Option<Token>> {
        self.pos += 1; // skip opening quote
        let mut value = String::new();
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            match c {
                b'\\' => {
                    self.pos += 1;
                    if self.pos >= self.chars.len() {
                        bail!("unterminated escape sequence in string literal");
                    }
                    let escaped = self.chars[self.pos];
                    match escaped {
                        b'"' => value.push('"'),
                        b'\\' => value.push('\\'),
                        b'n' => value.push('\n'),
                        b'r' => value.push('\r'),
                        b't' => value.push('\t'),
                        other => {
                            value.push('\\');
                            value.push(other as char);
                        }
                    }
                }
                b'"' => {
                    self.pos += 1;
                    return Ok(Some(Token::String(value)));
                }
                _ => value.push(c as char),
            }
            self.pos += 1;
        }
        bail!("unterminated string literal");
    }

    fn lex_number(&mut self) -> Result<Option<Token>> {
        let start = self.pos;
        let mut seen_digit = false;
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            if c.is_ascii_digit() {
                seen_digit = true;
                self.pos += 1;
            } else if c == b'.' {
                self.pos += 1;
            } else if c == b'e' || c == b'E' {
                self.pos += 1;
                if self.pos < self.chars.len()
                    && (self.chars[self.pos] == b'+' || self.chars[self.pos] == b'-')
                {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
        if !seen_digit {
            bail!("invalid numeric literal");
        }
        let slice = &self.chars[start..self.pos];
        let text = std::str::from_utf8(slice).unwrap();
        let number: f64 = text
            .parse()
            .with_context(|| format!("failed to parse number '{}'", text))?;
        Ok(Some(Token::Number(number)))
    }

    fn lex_identifier(&mut self) -> String {
        let start = self.pos;
        self.pos += 1;
        while self.pos < self.chars.len()
            && (self.chars[self.pos].is_ascii_alphanumeric() || self.chars[self.pos] == b'_')
        {
            self.pos += 1;
        }
        std::str::from_utf8(&self.chars[start..self.pos])
            .unwrap()
            .to_string()
    }

    fn match_char(&self, expected: u8) -> bool {
        self.pos < self.chars.len() && self.chars[self.pos] == expected
    }

    fn peek_char(&self, offset: usize) -> Option<u8> {
        self.chars.get(self.pos + offset).copied()
    }

    fn peek_non_whitespace(&self, offset: usize) -> Option<u8> {
        let mut idx = self.pos + offset;
        while idx < self.chars.len() {
            let c = self.chars[idx];
            if !c.is_ascii_whitespace() {
                return Some(c);
            }
            idx += 1;
        }
        None
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut expr = self.parse_and()?;
        while self.match_token(TokenKind::Or) {
            self.pos += 1;
            let rhs = self.parse_and()?;
            expr = Expr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut expr = self.parse_not()?;
        while self.match_token(TokenKind::And) {
            self.pos += 1;
            let rhs = self.parse_not()?;
            expr = Expr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.match_token(TokenKind::Not) {
            self.pos += 1;
            let inner = self.parse_not()?;
            Ok(Expr::Not(Box::new(inner)))
        } else if matches!(
            self.peek_token(),
            Some(Token::Compare(
                CompareOp::RegexMatch | CompareOp::RegexNotMatch
            ))
        ) {
            self.parse_regex_without_left()
        } else if self.match_token(TokenKind::LParen) {
            let saved_pos = self.pos;
            self.pos += 1;
            let expr = self.parse_expr()?;
            if !self.consume_token(TokenKind::RParen) {
                bail!("missing closing ')' in expression");
            }
            if matches!(expr, Expr::Value(_)) {
                if let Some(next) = self.peek_token() {
                    if matches!(
                        next,
                        Token::Compare(_)
                            | Token::Plus
                            | Token::Minus
                            | Token::Star
                            | Token::Slash
                            | Token::Caret
                    ) {
                        self.pos = saved_pos;
                        return self.parse_comparison();
                    }
                }
            }
            Ok(expr)
        } else {
            self.parse_comparison()
        }
    }

    fn parse_regex_without_left(&mut self) -> Result<Expr> {
        let op = self
            .consume_compare()
            .expect("regex operator should be present");
        let right = self.parse_arith()?;
        let left = ValueExpr::Column(ColumnSelector::Range(None, None));
        Ok(Expr::Compare(left, op, right))
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let first = self.parse_arith()?;
        let mut comparisons: Vec<(ValueExpr, CompareOp, ValueExpr)> = Vec::new();
        let mut current_left = first.clone();
        while let Some(op) = self.consume_compare() {
            let right = self.parse_arith()?;
            comparisons.push((current_left, op, right.clone()));
            current_left = right;
        }
        if comparisons.is_empty() {
            Ok(Expr::Value(first))
        } else {
            let mut iter = comparisons.into_iter();
            let (lhs, op, rhs) = iter.next().expect("at least one comparison");
            let mut expr = Expr::Compare(lhs.clone(), op, rhs.clone());
            let mut prev_right = rhs;
            for (lhs, op, rhs) in iter {
                let left_side = if matches!(
                    lhs,
                    ValueExpr::Column(_)
                        | ValueExpr::Columns(_)
                        | ValueExpr::String(_)
                        | ValueExpr::Number(_)
                ) {
                    lhs
                } else {
                    prev_right.clone()
                };
                expr = Expr::And(
                    Box::new(expr),
                    Box::new(Expr::Compare(left_side.clone(), op, rhs.clone())),
                );
                prev_right = rhs;
            }
            Ok(expr)
        }
    }

    fn parse_arith(&mut self) -> Result<ValueExpr> {
        let mut expr = self.parse_term()?;
        loop {
            if self.match_token(TokenKind::Plus) {
                self.pos += 1;
                let rhs = self.parse_term()?;
                expr = ValueExpr::Binary(BinaryOp::Add, Box::new(expr), Box::new(rhs));
            } else if self.match_token(TokenKind::Minus) {
                self.pos += 1;
                let rhs = self.parse_term()?;
                expr = ValueExpr::Binary(BinaryOp::Sub, Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_term(&mut self) -> Result<ValueExpr> {
        let mut expr = self.parse_power()?;
        loop {
            if self.match_token(TokenKind::Star) {
                self.pos += 1;
                let rhs = self.parse_power()?;
                expr = ValueExpr::Binary(BinaryOp::Mul, Box::new(expr), Box::new(rhs));
            } else if self.match_token(TokenKind::Slash) {
                self.pos += 1;
                let rhs = self.parse_power()?;
                expr = ValueExpr::Binary(BinaryOp::Div, Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_power(&mut self) -> Result<ValueExpr> {
        let mut expr = self.parse_factor()?;
        if self.match_token(TokenKind::Caret) {
            self.pos += 1;
            let rhs = self.parse_power()?;
            expr = ValueExpr::Binary(BinaryOp::Pow, Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_factor(&mut self) -> Result<ValueExpr> {
        if self.match_token(TokenKind::Minus) {
            self.pos += 1;
            let inner = self.parse_factor()?;
            return Ok(ValueExpr::Unary(UnaryOp::Neg, Box::new(inner)));
        }
        if self.match_token(TokenKind::Plus) {
            self.pos += 1;
            return self.parse_factor();
        }
        match self.peek_token() {
            Some(Token::Column(_)) => {
                if let Some(Token::Column(selector)) = self.advance().cloned() {
                    Ok(ValueExpr::Column(selector))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Columns(_)) => {
                if let Some(Token::Columns(selectors)) = self.advance().cloned() {
                    Ok(ValueExpr::Columns(selectors))
                } else {
                    unreachable!()
                }
            }
            Some(Token::String(_)) => {
                if let Some(Token::String(value)) = self.advance().cloned() {
                    Ok(ValueExpr::String(value))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Number(_)) => {
                if let Some(Token::Number(value)) = self.advance().cloned() {
                    Ok(ValueExpr::Number(value))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Ident(_)) => {
                if let Some(Token::Ident(name)) = self.advance().cloned() {
                    if !self.consume_token(TokenKind::LParen) {
                        bail!(
                            "identifier '{}' is not a function; prefix column names with '$' (e.g. '${}')",
                            name,
                            name
                        );
                    }
                    let argument = self.parse_arith()?;
                    if !self.consume_token(TokenKind::RParen) {
                        bail!("missing ')' after function call");
                    }
                    if let Some(kind) = try_parse_aggregate_kind(&name)? {
                        let selectors = match argument {
                            ValueExpr::Column(selector) => vec![selector],
                            ValueExpr::Columns(list) => list,
                            other => {
                                bail!(
                                    "function '{}' expects column selectors, got {:?}",
                                    name,
                                    other
                                )
                            }
                        };
                        return Ok(ValueExpr::Aggregate(AggregateSpecExpr { kind, selectors }));
                    }
                    let func = FunctionName::from_ident(&name)?;
                    Ok(ValueExpr::Function(func, Box::new(argument)))
                } else {
                    unreachable!()
                }
            }
            Some(Token::LParen) => {
                self.pos += 1;
                let expr = self.parse_arith()?;
                if !self.consume_token(TokenKind::RParen) {
                    bail!("missing closing ')' in expression");
                }
                Ok(expr)
            }
            _ => bail!("unexpected token in expression"),
        }
    }

    fn consume_compare(&mut self) -> Option<CompareOp> {
        if let Some(Token::Compare(op)) = self.peek_token().cloned() {
            self.pos += 1;
            Some(op)
        } else {
            None
        }
    }

    fn match_token(&self, kind: TokenKind) -> bool {
        match (kind, self.peek_token()) {
            (TokenKind::And, Some(Token::And)) => true,
            (TokenKind::Or, Some(Token::Or)) => true,
            (TokenKind::Not, Some(Token::Not)) => true,
            (TokenKind::LParen, Some(Token::LParen)) => true,
            (TokenKind::RParen, Some(Token::RParen)) => true,
            (TokenKind::Plus, Some(Token::Plus)) => true,
            (TokenKind::Minus, Some(Token::Minus)) => true,
            (TokenKind::Star, Some(Token::Star)) => true,
            (TokenKind::Slash, Some(Token::Slash)) => true,
            (TokenKind::Caret, Some(Token::Caret)) => true,
            _ => false,
        }
    }

    fn consume_token(&mut self, kind: TokenKind) -> bool {
        if self.match_token(kind) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek_token(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn has_more(&self) -> bool {
        self.pos < self.tokens.len()
    }
}

#[derive(Clone, Copy)]
enum TokenKind {
    And,
    Or,
    Not,
    LParen,
    RParen,
    Plus,
    Minus,
    Star,
    Slash,
    Caret,
}
