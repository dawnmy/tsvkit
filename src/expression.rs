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
    CaseWhen(Vec<(Expr, ValueExpr)>, Option<Box<ValueExpr>>),
    Switch {
        target: Box<ValueExpr>,
        branches: Vec<(Vec<ValueExpr>, ValueExpr)>,
        default: Option<Box<ValueExpr>>,
    },
    RegexCall(Box<ValueExpr>, Box<ValueExpr>),
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
    Len,
    IsNa,
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
            "len" => Ok(FunctionName::Len),
            "is_na" => Ok(FunctionName::IsNa),
            other => bail!(
                "unsupported function '{}': try abs, sqrt, exp, exp2, ln, log, log10, log2, len, is_na",
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
    LBracket,
    RBracket,
    Comma,
    Arrow,
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
    CaseWhen {
        branches: Vec<(BoundExpr, BoundValue)>,
        default: Option<Box<BoundValue>>,
    },
    Switch {
        target: Box<BoundValue>,
        branches: Vec<(Vec<BoundValue>, BoundValue)>,
        default: Option<Box<BoundValue>>,
    },
    RegexCall {
        value: Box<BoundValue>,
        pattern: RegexPattern,
    },
}

#[derive(Debug, Clone)]
pub struct BoundAggregate {
    pub kind: AggregateKind,
    pub columns: Vec<usize>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub(crate) enum RegexPattern {
    Static(Arc<Regex>),
    Dynamic(Box<BoundValue>),
}

pub struct EvalValue<'a> {
    pub text: Cow<'a, str>,
    pub numeric: Option<f64>,
}

pub struct EvalContext<'a, R>
where
    R: RowAccessor + ?Sized,
{
    row: &'a R,
    regex_captures: Option<Vec<String>>,
}

impl<'a, R> EvalContext<'a, R>
where
    R: RowAccessor + ?Sized,
{
    pub fn new(row: &'a R) -> Self {
        EvalContext {
            row,
            regex_captures: None,
        }
    }

    fn row(&self) -> &'a R {
        self.row
    }

    fn clear_captures(&mut self) {
        self.regex_captures = None;
    }

    fn set_captures(&mut self, captures: Vec<String>) {
        self.regex_captures = Some(captures);
    }

    fn take_captures(&mut self) -> Option<Vec<String>> {
        self.regex_captures.take()
    }

    fn restore_captures(&mut self, captures: Option<Vec<String>>) {
        self.regex_captures = captures;
    }
}

pub fn evaluate<R>(expr: &BoundExpr, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    let mut ctx = EvalContext::new(row);
    evaluate_with_context(expr, &mut ctx)
}

fn evaluate_with_context<'a, R>(expr: &'a BoundExpr, ctx: &mut EvalContext<'a, R>) -> bool
where
    R: RowAccessor + ?Sized,
{
    match expr {
        BoundExpr::Or(lhs, rhs) => {
            evaluate_with_context(lhs, ctx) || evaluate_with_context(rhs, ctx)
        }
        BoundExpr::And(lhs, rhs) => {
            evaluate_with_context(lhs, ctx) && evaluate_with_context(rhs, ctx)
        }
        BoundExpr::Not(inner) => !evaluate_with_context(inner, ctx),
        BoundExpr::Compare(lhs, op, rhs) => evaluate_compare(lhs, *op, rhs, ctx),
        BoundExpr::RegexMatch {
            value,
            pattern,
            invert,
        } => evaluate_regex(value, pattern, *invert, ctx),
        BoundExpr::Value(value) => evaluate_truthy_with_context(value, ctx),
    }
}

pub fn eval_value<'a, R>(value: &'a BoundValue, row: &'a R) -> EvalValue<'a>
where
    R: RowAccessor + ?Sized,
{
    let mut ctx = EvalContext::new(row);
    eval_value_with_context(value, &mut ctx)
}

pub fn eval_value_with_context<'a, R>(
    value: &'a BoundValue,
    ctx: &mut EvalContext<'a, R>,
) -> EvalValue<'a>
where
    R: RowAccessor + ?Sized,
{
    match value {
        BoundValue::Column(idx) => {
            if let Some(captures) = ctx.regex_captures.as_ref() {
                let capture_idx = idx + 1;
                if capture_idx < captures.len() {
                    let capture = captures[capture_idx].clone();
                    let numeric = parse_float(&capture);
                    return EvalValue {
                        text: Cow::Owned(capture),
                        numeric,
                    };
                }
            }
            let text = ctx.row().get(*idx).unwrap_or("");
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
                let text = ctx.row().get(*idx).unwrap_or("");
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
            let inner_eval = eval_value_with_context(inner, ctx);
            if let Some(val) = inner_eval.numeric {
                numeric_eval(match op {
                    UnaryOp::Neg => -val,
                })
            } else {
                empty_eval()
            }
        }
        BoundValue::Binary(op, left, right) => {
            let left_eval = eval_value_with_context(left, ctx);
            let right_eval = eval_value_with_context(right, ctx);
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
            let inner_eval = eval_value_with_context(inner, ctx);
            match func {
                FunctionName::Abs
                | FunctionName::Sqrt
                | FunctionName::Exp
                | FunctionName::Exp2
                | FunctionName::Ln
                | FunctionName::Log10
                | FunctionName::Log2 => {
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
                            _ => None,
                        };
                        result.map(numeric_eval).unwrap_or_else(empty_eval)
                    } else {
                        empty_eval()
                    }
                }
                FunctionName::Len => {
                    let len = inner_eval.text.chars().count() as f64;
                    numeric_eval(len)
                }
                FunctionName::IsNa => {
                    let text = inner_eval.text.as_ref().trim();
                    let is_na = text.is_empty()
                        || text.eq_ignore_ascii_case("na")
                        || text.eq_ignore_ascii_case("nan");
                    bool_eval(is_na)
                }
            }
        }
        BoundValue::Aggregate(spec) => {
            let values = spec
                .columns
                .iter()
                .map(|&idx| ctx.row().get(idx).unwrap_or(""))
                .collect::<Vec<_>>();
            let result = evaluate_row_aggregate(&spec.kind, &values);
            EvalValue {
                text: Cow::Owned(result.text),
                numeric: result.numeric,
            }
        }
        BoundValue::CaseWhen { branches, default } => {
            for (cond, result) in branches {
                ctx.clear_captures();
                if evaluate_with_context(cond, ctx) {
                    return eval_value_with_context(result, ctx);
                }
            }
            ctx.clear_captures();
            if let Some(default) = default {
                eval_value_with_context(default, ctx)
            } else {
                empty_eval()
            }
        }
        BoundValue::Switch {
            target,
            branches,
            default,
        } => {
            let target_eval = eval_value_with_context(target, ctx);
            let target_numeric = target_eval.numeric;
            let target_text = target_eval.text.into_owned();
            ctx.clear_captures();
            for (values, result) in branches {
                for value in values {
                    let saved = ctx.take_captures();
                    let candidate = eval_value_with_context(value, ctx);
                    ctx.restore_captures(saved);
                    let is_match = match (target_numeric, candidate.numeric) {
                        (Some(a), Some(b)) => a == b,
                        _ => target_text == candidate.text.as_ref(),
                    };
                    if is_match {
                        ctx.clear_captures();
                        return eval_value_with_context(result, ctx);
                    }
                }
            }
            ctx.clear_captures();
            if let Some(default) = default {
                eval_value_with_context(default, ctx)
            } else {
                empty_eval()
            }
        }
        BoundValue::RegexCall { value, pattern } => {
            let hay = eval_value_with_context(value, ctx);
            let hay_text = hay.text.into_owned();
            let captures = match pattern {
                RegexPattern::Static(regex) => regex.captures(&hay_text),
                RegexPattern::Dynamic(bound) => {
                    let pat_eval = eval_value_with_context(bound, ctx);
                    Regex::new(pat_eval.text.as_ref())
                        .ok()
                        .and_then(|regex| regex.captures(&hay_text))
                }
            };
            if let Some(captures) = captures {
                let mut values = Vec::with_capacity(captures.len());
                for idx in 0..captures.len() {
                    let text = captures.get(idx).map(|m| m.as_str()).unwrap_or("");
                    values.push(text.to_string());
                }
                ctx.set_captures(values);
                bool_eval(true)
            } else {
                ctx.clear_captures();
                bool_eval(false)
            }
        }
    }
}

pub fn evaluate_truthy<R>(value: &BoundValue, row: &R) -> bool
where
    R: RowAccessor + ?Sized,
{
    let mut ctx = EvalContext::new(row);
    evaluate_truthy_with_context(value, &mut ctx)
}

pub fn evaluate_truthy_with_context<'a, R>(
    value: &'a BoundValue,
    ctx: &mut EvalContext<'a, R>,
) -> bool
where
    R: RowAccessor + ?Sized,
{
    match value {
        BoundValue::Columns(indices) => indices.iter().any(|idx| {
            ctx.row()
                .get(*idx)
                .map(|text| !text.trim().is_empty())
                .unwrap_or(false)
        }),
        _ => {
            let eval = eval_value_with_context(value, ctx);
            if let Some(number) = eval.numeric {
                number != 0.0
            } else {
                !eval.text.trim().is_empty()
            }
        }
    }
}

fn evaluate_compare<'a, R>(
    lhs: &'a BoundValue,
    op: CompareOp,
    rhs: &'a BoundValue,
    ctx: &mut EvalContext<'a, R>,
) -> bool
where
    R: RowAccessor + ?Sized,
{
    let left = eval_value_with_context(lhs, ctx);
    let right = eval_value_with_context(rhs, ctx);

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

fn evaluate_regex<'a, R>(
    value: &'a BoundValue,
    pattern: &'a RegexPattern,
    invert: bool,
    ctx: &mut EvalContext<'a, R>,
) -> bool
where
    R: RowAccessor + ?Sized,
{
    let is_match = match pattern {
        RegexPattern::Static(regex) => match value {
            BoundValue::Column(idx) => regex.is_match(ctx.row().get(*idx).unwrap_or("")),
            BoundValue::Columns(indices) => indices
                .iter()
                .any(|idx| regex.is_match(ctx.row().get(*idx).unwrap_or(""))),
            _ => {
                let hay = eval_value_with_context(value, ctx);
                regex.is_match(hay.text.as_ref())
            }
        },
        RegexPattern::Dynamic(bound) => {
            let pat_eval = eval_value_with_context(bound, ctx);
            let pattern_text = pat_eval.text.as_ref();
            if let Ok(regex) = Regex::new(pattern_text) {
                match value {
                    BoundValue::Column(idx) => regex.is_match(ctx.row().get(*idx).unwrap_or("")),
                    BoundValue::Columns(indices) => indices
                        .iter()
                        .any(|idx| regex.is_match(ctx.row().get(*idx).unwrap_or(""))),
                    _ => {
                        let hay = eval_value_with_context(value, ctx);
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
        ValueExpr::CaseWhen(branches, default) => {
            let mut bound_branches = Vec::with_capacity(branches.len());
            for (cond, result) in branches {
                let bound_cond = bind_expression(cond, headers, no_header)?;
                let bound_result = bind_value(result, headers, no_header)?;
                bound_branches.push((bound_cond, bound_result));
            }
            let bound_default = match default {
                Some(expr) => Some(Box::new(bind_value(*expr, headers, no_header)?)),
                None => None,
            };
            Ok(BoundValue::CaseWhen {
                branches: bound_branches,
                default: bound_default,
            })
        }
        ValueExpr::Switch {
            target,
            branches,
            default,
        } => {
            let bound_target = bind_value(*target, headers, no_header)?;
            let mut bound_branches = Vec::with_capacity(branches.len());
            for (values, result) in branches {
                let mut bound_values = Vec::with_capacity(values.len());
                for value in values {
                    bound_values.push(bind_value(value, headers, no_header)?);
                }
                let bound_result = bind_value(result, headers, no_header)?;
                bound_branches.push((bound_values, bound_result));
            }
            let bound_default = match default {
                Some(expr) => Some(Box::new(bind_value(*expr, headers, no_header)?)),
                None => None,
            };
            Ok(BoundValue::Switch {
                target: Box::new(bound_target),
                branches: bound_branches,
                default: bound_default,
            })
        }
        ValueExpr::RegexCall(value, pattern) => {
            let bound_value = bind_value(*value, headers, no_header)?;
            let bound_pattern = bind_regex_pattern(*pattern, headers, no_header)?;
            Ok(BoundValue::RegexCall {
                value: Box::new(bound_value),
                pattern: bound_pattern,
            })
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

fn bool_eval(value: bool) -> EvalValue<'static> {
    if value {
        numeric_eval(1.0)
    } else {
        numeric_eval(0.0)
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

    #[test]
    fn case_when_selects_first_matching_branch() {
        let value_expr =
            parse_value_expression("case_when($1 > 5 -> \"high\", _ -> \"low\")").unwrap();
        let headers = vec!["score".to_string()];
        let bound = bind_value_expression(value_expr, &headers, false).unwrap();

        let row_high = vec!["6".to_string()];
        assert_eq!(eval_value(&bound, &row_high).text.as_ref(), "high");

        let row_low = vec!["2".to_string()];
        assert_eq!(eval_value(&bound, &row_low).text.as_ref(), "low");
    }

    #[test]
    fn regex_call_populates_capture_groups() {
        let value_expr =
            parse_value_expression("case_when(re($1, \"^ERR(\\\\d+)$\") -> $1, _ -> \"nomatch\")")
                .unwrap();
        let headers = vec!["sample".to_string()];
        let bound = bind_value_expression(value_expr, &headers, false).unwrap();

        let matched = vec!["ERR123".to_string()];
        assert_eq!(eval_value(&bound, &matched).text.as_ref(), "123");

        let unmatched = vec!["SRR55".to_string()];
        assert_eq!(eval_value(&bound, &unmatched).text.as_ref(), "nomatch");
    }

    #[test]
    fn switch_maps_values_to_labels() {
        let value_expr = parse_value_expression(
            "switch($1, [\"DE\",\"FR\"] -> \"EU\", [\"US\",\"CA\"] -> \"NA\", _ -> \"Other\")",
        )
        .unwrap();
        let headers = vec!["country".to_string()];
        let bound = bind_value_expression(value_expr, &headers, false).unwrap();

        let row_eu = vec!["DE".to_string()];
        assert_eq!(eval_value(&bound, &row_eu).text.as_ref(), "EU");

        let row_other = vec!["JP".to_string()];
        assert_eq!(eval_value(&bound, &row_other).text.as_ref(), "Other");
    }

    #[test]
    fn len_function_counts_characters() {
        let value_expr = parse_value_expression("len($1)").unwrap();
        let headers = vec!["text".to_string()];
        let bound = bind_value_expression(value_expr, &headers, false).unwrap();
        let row = vec!["hello".to_string()];
        let result = eval_value(&bound, &row);
        assert_eq!(result.numeric, Some(5.0));
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
            b'[' => {
                self.pos += 1;
                Ok(Some(Token::LBracket))
            }
            b']' => {
                self.pos += 1;
                Ok(Some(Token::RBracket))
            }
            b',' => {
                self.pos += 1;
                Ok(Some(Token::Comma))
            }
            b'+' => {
                self.pos += 1;
                Ok(Some(Token::Plus))
            }
            b'-' => {
                if self.peek_char(1) == Some(b'>') {
                    self.pos += 2;
                    Ok(Some(Token::Arrow))
                } else {
                    self.pos += 1;
                    Ok(Some(Token::Minus))
                }
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
                if c == b',' {
                    let mut idx = self.pos + 1;
                    while idx < self.chars.len() && self.chars[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    let next = self.chars.get(idx).copied();
                    let is_selector_start = next.map_or(false, |next_char| {
                        if next_char == b'$' || next_char == b'{' {
                            return true;
                        }
                        if next_char == b'_' {
                            let mut lookahead = idx + 1;
                            while lookahead < self.chars.len()
                                && self.chars[lookahead].is_ascii_whitespace()
                            {
                                lookahead += 1;
                            }
                            if lookahead < self.chars.len() && self.chars[lookahead] == b'-' {
                                return false;
                            }
                        }
                        next_char.is_ascii_alphanumeric() || next_char == b'_' || next_char == b'.'
                    });
                    if is_selector_start {
                        has_range_syntax = true;
                        is_numeric = false;
                        self.pos += 1;
                        continue;
                    } else {
                        break;
                    }
                } else if c == b':' {
                    has_range_syntax = true;
                    is_numeric = false;
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
                    let lower = name.to_ascii_lowercase();
                    if lower == "case_when" {
                        let expr = self.parse_case_when_function()?;
                        return Ok(expr);
                    }
                    if lower == "switch" {
                        let expr = self.parse_switch_function()?;
                        return Ok(expr);
                    }
                    if lower == "re" {
                        let mut args = self.parse_function_arguments()?;
                        if args.len() != 2 {
                            bail!("re() expects two arguments: value, pattern");
                        }
                        let pattern = args.pop().unwrap();
                        let value = args.pop().unwrap();
                        return Ok(ValueExpr::RegexCall(Box::new(value), Box::new(pattern)));
                    }
                    if let Some(kind) = try_parse_aggregate_kind(&name)? {
                        let argument = self.parse_arith()?;
                        if !self.consume_token(TokenKind::RParen) {
                            bail!("missing ')' after function call");
                        }
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
                    let mut args = self.parse_function_arguments()?;
                    if args.len() != 1 {
                        bail!("function '{}' expects exactly one argument", name);
                    }
                    let argument = args.pop().unwrap();
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

    fn parse_function_arguments(&mut self) -> Result<Vec<ValueExpr>> {
        let mut args = Vec::new();
        if self.consume_token(TokenKind::RParen) {
            return Ok(args);
        }
        loop {
            let expr = self.parse_arith()?;
            args.push(expr);
            if self.consume_token(TokenKind::Comma) {
                continue;
            } else if self.consume_token(TokenKind::RParen) {
                break;
            } else {
                bail!("expected ',' or ')' after function argument");
            }
        }
        Ok(args)
    }

    fn parse_case_when_function(&mut self) -> Result<ValueExpr> {
        let mut branches = Vec::new();
        let mut default = None;
        if self.consume_token(TokenKind::RParen) {
            bail!("case_when requires at least one branch");
        }
        loop {
            if let Some(Token::Ident(name)) = self.peek_token().cloned() {
                if name == "_" {
                    self.pos += 1;
                    if !self.consume_token(TokenKind::Arrow) {
                        bail!("case_when default branch must use '->'");
                    }
                    let result = self.parse_case_result_value()?;
                    if default.is_some() {
                        bail!("case_when default branch specified more than once");
                    }
                    default = Some(Box::new(result));
                    if !self.consume_token(TokenKind::RParen) {
                        bail!("case_when default branch must be last");
                    }
                    break;
                }
            }
            let condition = self.parse_case_condition()?;
            if !self.consume_token(TokenKind::Arrow) {
                bail!("case_when branches must use '->'");
            }
            let result = self.parse_case_result_value()?;
            branches.push((condition, result));
            if self.consume_token(TokenKind::Comma) {
                continue;
            } else if self.consume_token(TokenKind::RParen) {
                break;
            } else {
                bail!("expected ',' or ')' after case_when branch");
            }
        }
        if branches.is_empty() && default.is_none() {
            bail!("case_when requires at least one branch");
        }
        Ok(ValueExpr::CaseWhen(branches, default))
    }

    fn parse_case_condition(&mut self) -> Result<Expr> {
        let start = self.pos;
        let mut depth = 0;
        let mut idx = self.pos;
        while idx < self.tokens.len() {
            match self.tokens[idx] {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => {
                    if depth == 0 {
                        break;
                    } else {
                        depth -= 1;
                    }
                }
                Token::Arrow if depth == 0 => {
                    let slice = self.tokens[start..idx].to_vec();
                    let mut parser = Parser::new(slice);
                    let expr = parser.parse_expr()?;
                    if parser.has_more() {
                        bail!("unexpected token in case_when condition");
                    }
                    self.pos = idx;
                    return Ok(expr);
                }
                _ => {}
            }
            idx += 1;
        }
        bail!("case_when branches must use '->'")
    }

    fn parse_case_result_value(&mut self) -> Result<ValueExpr> {
        let start = self.pos;
        let mut depth = 0;
        let mut idx = self.pos;
        while idx < self.tokens.len() {
            match self.tokens[idx] {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen => {
                    if depth == 0 {
                        break;
                    } else {
                        depth -= 1;
                    }
                }
                Token::Comma if depth == 0 => {
                    break;
                }
                _ => {}
            }
            idx += 1;
        }
        if idx == start {
            bail!("case_when result must not be empty");
        }
        let slice = self.tokens[start..idx].to_vec();
        let mut parser = Parser::new(slice);
        let value = parser.parse_arith()?;
        if parser.has_more() {
            bail!("unexpected token in case_when result");
        }
        self.pos = idx;
        Ok(value)
    }

    fn parse_switch_function(&mut self) -> Result<ValueExpr> {
        let target = self.parse_arith()?;
        if !self.consume_token(TokenKind::Comma) {
            bail!("switch() requires a comma after the target expression");
        }
        let mut branches = Vec::new();
        let mut default = None;
        if self.consume_token(TokenKind::RParen) {
            bail!("switch requires at least one branch");
        }
        loop {
            if let Some(Token::Ident(name)) = self.peek_token().cloned() {
                if name == "_" {
                    self.pos += 1;
                    if !self.consume_token(TokenKind::Arrow) {
                        bail!("switch default branch must use '->'");
                    }
                    let result = self.parse_arith()?;
                    if default.is_some() {
                        bail!("switch default branch specified more than once");
                    }
                    default = Some(Box::new(result));
                    if !self.consume_token(TokenKind::RParen) {
                        bail!("switch default branch must be last");
                    }
                    break;
                }
            }
            let values = self.parse_switch_values()?;
            if !self.consume_token(TokenKind::Arrow) {
                bail!("switch branches must use '->'");
            }
            let result = self.parse_arith()?;
            branches.push((values, result));
            if self.consume_token(TokenKind::Comma) {
                continue;
            } else if self.consume_token(TokenKind::RParen) {
                break;
            } else {
                bail!("expected ',' or ')' after switch branch");
            }
        }
        if branches.is_empty() && default.is_none() {
            bail!("switch requires at least one branch");
        }
        Ok(ValueExpr::Switch {
            target: Box::new(target),
            branches,
            default,
        })
    }

    fn parse_switch_values(&mut self) -> Result<Vec<ValueExpr>> {
        if self.consume_token(TokenKind::LBracket) {
            let mut values = Vec::new();
            if self.consume_token(TokenKind::RBracket) {
                bail!("switch value list must not be empty");
            }
            loop {
                let value = self.parse_arith()?;
                values.push(value);
                if self.consume_token(TokenKind::Comma) {
                    continue;
                } else if self.consume_token(TokenKind::RBracket) {
                    break;
                } else {
                    bail!("expected ',' or ']' in switch value list");
                }
            }
            Ok(values)
        } else {
            let value = self.parse_arith()?;
            Ok(vec![value])
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
            (TokenKind::LBracket, Some(Token::LBracket)) => true,
            (TokenKind::RBracket, Some(Token::RBracket)) => true,
            (TokenKind::Comma, Some(Token::Comma)) => true,
            (TokenKind::Arrow, Some(Token::Arrow)) => true,
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
    LBracket,
    RBracket,
    Comma,
    Arrow,
    Plus,
    Minus,
    Star,
    Slash,
    Caret,
}
