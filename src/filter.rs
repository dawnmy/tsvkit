use std::borrow::Cow;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Args;
use regex::Regex;

use crate::common::{ColumnSelector, default_headers, reader_for_path, resolve_selectors};

#[derive(Args, Debug)]
#[command(
    about = "Filter TSV rows using boolean expressions",
    long_about = r#"Filter rows using expressions with column references ($name or $index), comparisons, logical operators, arithmetic, regex (~ and !~), and numeric functions (abs, sqrt, exp, ln, log, log10, log2). Strings require double quotes. Defaults to header-aware mode; add -H for headerless input.

Examples:
  tsvkit filter -e '$sample2>=5 & $sample3!=9' examples/profiles.tsv
  tsvkit filter -e '$kingdom ~ "^Bact"' examples/abundance.tsv
  tsvkit filter -e 'log2($coverage) > 10' reads.tsv"#
)]
pub struct FilterArgs {
    /// Input TSV file (use '-' for stdin; compressed files supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Filter expression (e.g. `$purity>=0.9 & log2($dna_ug)>4`); supports `$col`/`$1` selectors, comparisons, arithmetic, regex (~ / !~), and functions (abs, sqrt, exp, ln, log, log10, log2)
    #[arg(short = 'e', long = "expr", value_name = "EXPR", required = true)]
    pub expr: String,

    /// Treat input as headerless (columns become 1-based indices only)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,
}

#[derive(Debug, Clone)]
enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare(ValueExpr, CompareOp, ValueExpr),
    Value(ValueExpr),
}

#[derive(Debug, Clone)]
enum ValueExpr {
    Column(ColumnSelector),
    String(String),
    Number(f64),
    Unary(UnaryOp, Box<ValueExpr>),
    Binary(BinaryOp, Box<ValueExpr>, Box<ValueExpr>),
    Function(FunctionName, Box<ValueExpr>),
}

#[derive(Debug, Clone, Copy)]
enum UnaryOp {
    Neg,
}

#[derive(Debug, Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy)]
enum FunctionName {
    Abs,
    Sqrt,
    Exp,
    Ln,
    Log10,
    Log2,
}

impl FunctionName {
    fn from_ident(ident: &str) -> Result<Self> {
        match ident.to_ascii_lowercase().as_str() {
            "abs" => Ok(FunctionName::Abs),
            "sqrt" => Ok(FunctionName::Sqrt),
            "exp" => Ok(FunctionName::Exp),
            "ln" => Ok(FunctionName::Ln),
            "log" | "log10" => Ok(FunctionName::Log10),
            "log2" => Ok(FunctionName::Log2),
            other => bail!(
                "unsupported function '{}': try abs, sqrt, exp, ln, log, log10, log2",
                other
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CompareOp {
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
}

pub fn run(args: FilterArgs) -> Result<()> {
    let expr_ast = parse_expression(&args.expr)?;
    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first_record = match records.next() {
            Some(rec) => rec.with_context(|| format!("failed reading from {:?}", args.file))?,
            None => return Ok(()),
        };
        let headers = default_headers(first_record.len());
        let bound = bind_expression(expr_ast, &headers, true)?;

        if evaluate(&bound, &first_record) {
            emit_record(&first_record, &mut writer)?;
        }
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if evaluate(&bound, &record) {
                emit_record(&record, &mut writer)?;
            }
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let bound = bind_expression(expr_ast, &headers, false)?;

        if !headers.is_empty() {
            writeln!(writer, "{}", headers.join("\t"))?;
        }
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if evaluate(&bound, &record) {
                emit_record(&record, &mut writer)?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn emit_record(
    record: &csv::StringRecord,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    let line = record.iter().collect::<Vec<_>>().join("\t");
    writeln!(writer, "{}", line)?;
    Ok(())
}

fn parse_expression(input: &str) -> Result<Expr> {
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

struct Lexer<'a> {
    chars: &'a [u8],
    pos: usize,
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
                if self
                    .peek_char(1)
                    .map_or(false, |c| c.is_ascii_digit() || c == b'.')
                {
                    self.lex_number(true)
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
            c if c.is_ascii_digit() || c == b'.' => self.lex_number(false),
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let ident = self.lex_identifier();
                Ok(Some(Token::Ident(ident)))
            }
            _ => bail!("unexpected character '{}' in expression", ch as char),
        }
    }

    fn lex_column(&mut self) -> Result<Option<Token>> {
        self.pos += 1;
        let start = self.pos;
        let mut is_numeric = true;
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            if c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.') {
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
                is_numeric = false;
                self.pos += 1;
                continue;
            }
            break;
        }
        if start == self.pos {
            bail!("column reference requires a name or index after '$'");
        }
        let text = std::str::from_utf8(&self.chars[start..self.pos]).unwrap();
        if let Ok(idx) = text.parse::<usize>() {
            if idx == 0 {
                bail!("column indices use 1-based positions");
            }
            Ok(Some(Token::Column(ColumnSelector::Index(idx - 1))))
        } else {
            Ok(Some(Token::Column(ColumnSelector::Name(text.to_string()))))
        }
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
                    value.push(self.chars[self.pos] as char);
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

    fn lex_number(&mut self, negative: bool) -> Result<Option<Token>> {
        let start = self.pos;
        if negative {
            self.pos += 1; // skip '-'
        }
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

    fn skip_whitespace(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bind_expression, evaluate, parse_expression};
    use csv::StringRecord;

    #[test]
    fn parses_numeric_columns_with_minus_operator() {
        assert!(parse_expression("$3-$2").is_ok());
    }

    #[test]
    fn parses_named_columns_with_dash() {
        assert!(parse_expression("$foo-bar").is_ok());
    }

    #[test]
    fn regex_match_operator_evaluates() {
        let expr = parse_expression("$1 ~ \"^foo\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["foobar"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn regex_not_match_operator_evaluates() {
        let expr = parse_expression("$1 !~ \"bar\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["foo"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn log2_function_supported() {
        let expr = parse_expression("log2($1) > 3").unwrap();
        let headers = vec!["value".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["10"]);
        assert!(evaluate(&bound, &record));
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
        } else if self.match_token(TokenKind::LParen) {
            self.pos += 1;
            let expr = self.parse_expr()?;
            if !self.consume_token(TokenKind::RParen) {
                bail!("missing closing ')' in expression");
            }
            Ok(expr)
        } else {
            self.parse_comparison()
        }
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
                    ValueExpr::Column(_) | ValueExpr::String(_) | ValueExpr::Number(_)
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
        let mut expr = self.parse_factor()?;
        loop {
            if self.match_token(TokenKind::Star) {
                self.pos += 1;
                let rhs = self.parse_factor()?;
                expr = ValueExpr::Binary(BinaryOp::Mul, Box::new(expr), Box::new(rhs));
            } else if self.match_token(TokenKind::Slash) {
                self.pos += 1;
                let rhs = self.parse_factor()?;
                expr = ValueExpr::Binary(BinaryOp::Div, Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
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
                        bail!("function '{}' must be followed by parentheses", name);
                    }
                    let argument = self.parse_arith()?;
                    if !self.consume_token(TokenKind::RParen) {
                        bail!("missing ')' after function call");
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
}

#[derive(Debug, Clone)]
enum BoundValue {
    Column(usize),
    String(String),
    Number(f64),
    Unary(UnaryOp, Box<BoundValue>),
    Binary(BinaryOp, Box<BoundValue>, Box<BoundValue>),
    Function(FunctionName, Box<BoundValue>),
}

#[derive(Debug)]
enum BoundExpr {
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
enum RegexPattern {
    Static(Arc<Regex>),
    Dynamic(Box<BoundValue>),
}

fn bind_expression(expr: Expr, headers: &[String], no_header: bool) -> Result<BoundExpr> {
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

fn bind_value(value: ValueExpr, headers: &[String], no_header: bool) -> Result<BoundValue> {
    match value {
        ValueExpr::Column(selector) => {
            let indices = resolve_selectors(headers, &[selector], no_header)?;
            let index = *indices.first().expect("single column");
            Ok(BoundValue::Column(index))
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

fn evaluate(expr: &BoundExpr, record: &csv::StringRecord) -> bool {
    match expr {
        BoundExpr::Or(lhs, rhs) => evaluate(lhs, record) || evaluate(rhs, record),
        BoundExpr::And(lhs, rhs) => evaluate(lhs, record) && evaluate(rhs, record),
        BoundExpr::Not(inner) => !evaluate(inner, record),
        BoundExpr::Compare(lhs, op, rhs) => evaluate_compare(lhs, *op, rhs, record),
        BoundExpr::RegexMatch {
            value,
            pattern,
            invert,
        } => evaluate_regex(value, pattern, *invert, record),
        BoundExpr::Value(value) => evaluate_truthy(value, record),
    }
}

fn evaluate_compare(
    lhs: &BoundValue,
    op: CompareOp,
    rhs: &BoundValue,
    record: &csv::StringRecord,
) -> bool {
    let left = eval_value(lhs, record);
    let right = eval_value(rhs, record);

    match op {
        CompareOp::Eq => left.text == right.text,
        CompareOp::Ne => left.text != right.text,
        CompareOp::Gt => compare_numeric(&left, &right, |a, b| a > b),
        CompareOp::Ge => compare_numeric(&left, &right, |a, b| a >= b),
        CompareOp::Lt => compare_numeric(&left, &right, |a, b| a < b),
        CompareOp::Le => compare_numeric(&left, &right, |a, b| a <= b),
        CompareOp::RegexMatch | CompareOp::RegexNotMatch => false,
    }
}

fn evaluate_regex(
    value: &BoundValue,
    pattern: &RegexPattern,
    invert: bool,
    record: &csv::StringRecord,
) -> bool {
    let hay = eval_value(value, record);
    let haystack = hay.text.as_ref();
    let is_match = match pattern {
        RegexPattern::Static(regex) => regex.is_match(haystack),
        RegexPattern::Dynamic(bound) => {
            let pat_eval = eval_value(bound, record);
            let pattern_text = pat_eval.text.as_ref();
            Regex::new(pattern_text)
                .ok()
                .map(|re| re.is_match(haystack))
                .unwrap_or(false)
        }
    };
    if invert { !is_match } else { is_match }
}

fn evaluate_truthy(value: &BoundValue, record: &csv::StringRecord) -> bool {
    let eval = eval_value(value, record);
    if let Some(number) = eval.numeric {
        number != 0.0
    } else {
        !eval.text.trim().is_empty()
    }
}

struct EvalValue<'a> {
    text: Cow<'a, str>,
    numeric: Option<f64>,
}

fn eval_value<'a>(value: &'a BoundValue, record: &'a csv::StringRecord) -> EvalValue<'a> {
    match value {
        BoundValue::Column(idx) => {
            let text = record.get(*idx).unwrap_or("");
            EvalValue {
                text: Cow::Borrowed(text),
                numeric: parse_float(text),
            }
        }
        BoundValue::String(text) => EvalValue {
            text: Cow::Borrowed(text.as_str()),
            numeric: parse_float(text),
        },
        BoundValue::Number(number) => numeric_eval(*number),
        BoundValue::Unary(op, inner) => {
            let inner_eval = eval_value(inner, record);
            if let Some(val) = inner_eval.numeric {
                numeric_eval(match op {
                    UnaryOp::Neg => -val,
                })
            } else {
                empty_eval()
            }
        }
        BoundValue::Binary(op, left, right) => {
            let left_eval = eval_value(left, record);
            let right_eval = eval_value(right, record);
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
                },
                _ => empty_eval(),
            }
        }
        BoundValue::Function(func, inner) => {
            let inner_eval = eval_value(inner, record);
            if let Some(val) = inner_eval.numeric {
                let result = match func {
                    FunctionName::Abs => Some(val.abs()),
                    FunctionName::Sqrt => {
                        let value = val.sqrt();
                        if value.is_finite() { Some(value) } else { None }
                    }
                    FunctionName::Exp => {
                        let value = val.exp();
                        if value.is_finite() { Some(value) } else { None }
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

fn compare_numeric<F>(left: &EvalValue<'_>, right: &EvalValue<'_>, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (left.numeric, right.numeric) {
        (Some(a), Some(b)) => cmp(a, b),
        _ => false,
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.6}", value)
    }
}
