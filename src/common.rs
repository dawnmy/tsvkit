use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use csv::ReaderBuilder;
use flate2::read::MultiGzDecoder;
use xz2::read::XzDecoder;

#[derive(Debug, Clone)]
pub enum ColumnSelector {
    Index(usize),
    FromEnd(usize),
    Name(String),
    Range(Option<Box<ColumnSelector>>, Option<Box<ColumnSelector>>),
}

pub fn parse_selector_list(spec: &str) -> Result<Vec<ColumnSelector>> {
    if spec.trim().is_empty() {
        bail!("column specification must not be empty");
    }

    tokenize_selector_spec(spec)?
        .into_iter()
        .map(parse_selector_token)
        .collect()
}

pub fn parse_single_selector(token: &str) -> Result<ColumnSelector> {
    let mut selectors = parse_selector_list(token)?;
    if selectors.len() != 1 {
        bail!("column specification must reference exactly one column");
    }
    match selectors.remove(0) {
        ColumnSelector::Range(_, _) => {
            bail!("column range not allowed in this context")
        }
        selector => Ok(selector),
    }
}

pub fn resolve_single_selector(
    headers: &[String],
    selector: ColumnSelector,
    no_header: bool,
) -> Result<usize> {
    let selectors = vec![selector];
    let indices = resolve_selectors(headers, &selectors, no_header)?;
    Ok(indices[0])
}

pub fn parse_multi_selector_spec(
    spec: &str,
    file_count: usize,
) -> Result<Vec<Vec<ColumnSelector>>> {
    let groups: Vec<&str> = spec
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if groups.is_empty() {
        bail!("column specification must not be empty");
    }

    if groups.len() == 1 && file_count > 1 {
        let parsed = parse_selector_list(groups[0])?;
        return Ok(vec![parsed; file_count]);
    }

    if groups.len() != file_count {
        bail!(
            "expected column specification for each file, got {}",
            groups.len()
        );
    }

    groups.into_iter().map(parse_selector_list).collect()
}

pub fn resolve_selectors(
    headers: &[String],
    selectors: &[ColumnSelector],
    no_header: bool,
) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(selectors.len());
    for selector in selectors {
        match selector {
            ColumnSelector::Index(_) | ColumnSelector::FromEnd(_) | ColumnSelector::Name(_) => {
                let index = resolve_selector_index(headers, selector, no_header)?;
                indices.push(index);
            }
            ColumnSelector::Range(start, end) => {
                if headers.is_empty() {
                    bail!("column range cannot be resolved without any columns");
                }
                let start_idx = match start {
                    Some(sel) => resolve_selector_index(headers, sel, no_header)?,
                    None => 0,
                };
                let end_idx = match end {
                    Some(sel) => resolve_selector_index(headers, sel, no_header)?,
                    None => headers.len().checked_sub(1).ok_or_else(|| {
                        anyhow!("column range end cannot be determined in empty header")
                    })?,
                };
                if start_idx > end_idx {
                    bail!("column range start comes after end");
                }
                for idx in start_idx..=end_idx {
                    indices.push(idx);
                }
            }
        }
    }
    Ok(indices)
}

#[derive(Debug, Clone)]
pub struct InputOptions {
    pub comment: Option<u8>,
    pub ignore_empty: bool,
    pub ignore_illegal: bool,
}

impl InputOptions {
    pub fn from_flags(
        comment_char: &str,
        ignore_empty: bool,
        ignore_illegal: bool,
    ) -> Result<Self> {
        let comment = if comment_char.is_empty() {
            None
        } else {
            let mut chars = comment_char.chars();
            let first = chars
                .next()
                .with_context(|| "comment character must not be empty")?;
            if chars.next().is_some() {
                bail!("comment character must be a single UTF-8 scalar value");
            }
            Some(first as u8)
        };
        Ok(InputOptions {
            comment,
            ignore_empty,
            ignore_illegal,
        })
    }
}

pub fn reader_for_path(
    path: &Path,
    no_header: bool,
    options: &InputOptions,
) -> Result<csv::Reader<Box<dyn io::Read>>> {
    let reader = open_path_reader(path)?;

    Ok(ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(!no_header)
        .comment(options.comment)
        .flexible(true)
        .from_reader(reader))
}

pub fn open_path_reader(path: &Path) -> Result<Box<dyn io::Read>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdin().lock()));
    }

    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    let reader: Box<dyn io::Read> = match ext.as_str() {
        "gz" => Box::new(MultiGzDecoder::new(file)),
        "xz" => Box::new(XzDecoder::new(file)),
        _ => Box::new(BufReader::new(file)),
    };

    Ok(reader)
}

pub fn record_is_empty(record: &csv::StringRecord) -> bool {
    record.iter().all(|field| field.trim().is_empty())
}

pub fn should_skip_record(
    record: &csv::StringRecord,
    options: &InputOptions,
    expected_width: Option<usize>,
) -> bool {
    if options.ignore_empty && record_is_empty(record) {
        return true;
    }
    if options.ignore_illegal {
        if let Some(width) = expected_width {
            if record.len() != width {
                return true;
            }
        }
    }
    false
}

pub fn default_headers(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("col{}", i)).collect()
}

fn parse_selector_token(token: SelectorToken) -> Result<ColumnSelector> {
    if token.text.is_empty() {
        return Err(anyhow!("empty column selector"));
    }

    if let Some((start, end, extra)) = split_range_token(&token.text) {
        if extra {
            bail!(
                "invalid column range '{}': too many ':' characters",
                token.text
            );
        }
        let start_trim = start.trim();
        let end_trim = end.trim();
        let start_selector = if start_trim.is_empty() {
            None
        } else {
            Some(Box::new(parse_simple_selector(start_trim)?))
        };
        let end_selector = if end_trim.is_empty() {
            None
        } else {
            Some(Box::new(parse_simple_selector(end_trim)?))
        };
        return Ok(ColumnSelector::Range(start_selector, end_selector));
    }

    parse_simple_selector(&token.text)
}

fn parse_simple_selector(token: &str) -> Result<ColumnSelector> {
    if token.is_empty() {
        return Err(anyhow!("empty column selector"));
    }
    if let Some(literal) = parse_backtick_literal(token)? {
        return Ok(ColumnSelector::Name(literal));
    }
    if let Some(literal) = parse_brace_literal(token)? {
        return Ok(ColumnSelector::Name(literal));
    }
    if let Some(stripped) = token.strip_prefix('-') {
        if stripped.is_empty() {
            bail!("column selector '-' must include an index");
        }
        let offset: usize = stripped
            .parse()
            .with_context(|| format!("invalid trailing index '{}'", token))?;
        if offset == 0 {
            bail!("column selector '-0' is not allowed");
        }
        return Ok(ColumnSelector::FromEnd(offset));
    }
    if let Ok(idx) = token.parse::<usize>() {
        if idx == 0 {
            bail!("column indices use 1-based positions");
        }
        return Ok(ColumnSelector::Index(idx - 1));
    }
    Ok(ColumnSelector::Name(token.to_string()))
}

fn parse_backtick_literal(token: &str) -> Result<Option<String>> {
    let trimmed = token.trim();
    if !trimmed.starts_with('`') {
        return Ok(None);
    }
    let bytes = trimmed.as_bytes();
    let mut value = String::new();
    let mut idx = 1;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'\\' {
            idx += 1;
            if idx >= bytes.len() {
                bail!("unterminated escape sequence in backtick-quoted column selector");
            }
            value.push(bytes[idx] as char);
            idx += 1;
            continue;
        }
        if b == b'`' {
            idx += 1;
            if idx != bytes.len() {
                bail!("unexpected characters after closing backtick in column selector");
            }
            return Ok(Some(value));
        }
        value.push(b as char);
        idx += 1;
    }
    bail!("unterminated backtick-quoted column selector");
}

fn parse_brace_literal(token: &str) -> Result<Option<String>> {
    let trimmed = token.trim();
    if !trimmed.starts_with('{') {
        return Ok(None);
    }
    let bytes = trimmed.as_bytes();
    let mut value = String::new();
    let mut idx = 1;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'\\' {
            idx += 1;
            if idx >= bytes.len() {
                bail!("unterminated escape sequence in '{{' column selector");
            }
            value.push(bytes[idx] as char);
            idx += 1;
            continue;
        }
        if b == b'}' {
            idx += 1;
            if idx != bytes.len() {
                bail!("unexpected characters after closing '}}' in column selector");
            }
            return Ok(Some(value));
        }
        value.push(b as char);
        idx += 1;
    }
    bail!("unterminated '{{' in column selector");
}

fn split_range_token(token: &str) -> Option<(&str, &str, bool)> {
    let mut in_backtick = false;
    let mut in_braces = false;
    let mut escaped = false;
    let mut colon_index = None;
    let mut extra = false;
    let bytes = token.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if in_backtick {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'`' {
                in_backtick = false;
            }
            idx += 1;
            continue;
        }
        if in_braces {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'}' {
                in_braces = false;
            }
            idx += 1;
            continue;
        }
        match b {
            b'`' => {
                in_backtick = true;
            }
            b'{' => {
                in_braces = true;
            }
            b':' => {
                if colon_index.is_none() {
                    colon_index = Some(idx);
                } else {
                    extra = true;
                }
            }
            _ => {}
        }
        idx += 1;
    }
    let colon = colon_index?;
    let start = &token[..colon];
    let end = &token[colon + 1..];
    Some((start, end, extra))
}

#[derive(Debug)]
struct SelectorToken {
    text: String,
}

fn tokenize_selector_spec(spec: &str) -> Result<Vec<SelectorToken>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = spec.chars().peekable();
    let mut in_backtick = false;
    let mut in_braces = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if in_backtick {
            current.push(ch);
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
            } else if ch == '`' {
                in_backtick = false;
            }
            continue;
        }
        if in_braces {
            current.push(ch);
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
            } else if ch == '}' {
                in_braces = false;
            }
            continue;
        }
        match ch {
            ',' => {
                let trimmed = current.trim();
                if trimmed.is_empty() {
                    bail!("column specification must not be empty");
                }
                tokens.push(SelectorToken {
                    text: trimmed.to_string(),
                });
                current.clear();
            }
            '`' => {
                current.push(ch);
                in_backtick = true;
                escaped = false;
            }
            '{' => {
                current.push(ch);
                in_braces = true;
                escaped = false;
            }
            c if c.is_whitespace() && current.is_empty() => {
                continue;
            }
            other => current.push(other),
        }
    }

    if in_backtick {
        bail!("unterminated backtick-quoted column selector");
    }
    if in_braces {
        bail!("unterminated '{{' in column selector");
    }
    if !current.is_empty() {
        let trimmed = current.trim();
        if trimmed.is_empty() {
            bail!("column specification must not be empty");
        }
        tokens.push(SelectorToken {
            text: trimmed.to_string(),
        });
    }

    Ok(tokens)
}

fn resolve_selector_index(
    headers: &[String],
    selector: &ColumnSelector,
    no_header: bool,
) -> Result<usize> {
    match selector {
        ColumnSelector::Index(idx) => {
            let index = *idx;
            if index >= headers.len() {
                bail!(
                    "column index {} out of range ({} columns)",
                    index + 1,
                    headers.len()
                );
            }
            Ok(index)
        }
        ColumnSelector::FromEnd(offset) => {
            let offset = *offset;
            if offset == 0 {
                bail!("column selector '-0' is not allowed");
            }
            if offset > headers.len() {
                bail!(
                    "column selector '-{}' out of range ({} columns)",
                    offset,
                    headers.len()
                );
            }
            Ok(headers.len() - offset)
        }
        ColumnSelector::Name(name) => {
            if no_header {
                bail!("column names cannot be used when input lacks a header row");
            }
            let index = headers
                .iter()
                .position(|h| h == name)
                .with_context(|| format!("column '{}' not found", name))?;
            Ok(index)
        }
        ColumnSelector::Range(_, _) => {
            bail!("unexpected nested column range")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnSelector, parse_selector_list, parse_single_selector, resolve_selectors};

    #[test]
    fn resolves_name_range() {
        let headers = vec![
            "Purity".to_string(),
            "sample".to_string(),
            "FN".to_string(),
            "F1".to_string(),
        ];
        let selectors = parse_selector_list("Purity,sample:FN,F1").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 1, 2, 3]);
    }

    #[test]
    fn resolves_index_range() {
        let headers = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        let selectors = parse_selector_list("1:3").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn resolves_open_range_start() {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let selectors = parse_selector_list(":b").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn resolves_open_range_end() {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let selectors = parse_selector_list("b:").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn resolves_full_range() {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let selectors = parse_selector_list(":").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn single_selector_rejects_range() {
        assert!(parse_single_selector("1:2").is_err());
    }

    #[test]
    fn name_range_disallowed_without_headers() {
        let headers = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        let selectors = parse_selector_list("col1:col3").unwrap();
        assert!(resolve_selectors(&headers, &selectors, true).is_err());
    }

    #[test]
    fn parses_backtick_literals() {
        let selectors = parse_selector_list("`a:b`,`c,d`,plain").unwrap();
        assert!(matches!(selectors[0], ColumnSelector::Name(ref name) if name == "a:b"));
        assert!(matches!(selectors[1], ColumnSelector::Name(ref name) if name == "c,d"));
        assert!(matches!(selectors[2], ColumnSelector::Name(ref name) if name == "plain"));
    }

    #[test]
    fn quoted_tokens_ignore_range_syntax() {
        let selectors = parse_selector_list("`2:4`").unwrap();
        assert!(matches!(selectors[0], ColumnSelector::Name(ref name) if name == "2:4"));
    }

    #[test]
    fn quoted_names_allow_ranges() {
        let selectors = parse_selector_list("`dna_ug`:`rna_ug`").unwrap();
        assert!(matches!(
            selectors[0],
            ColumnSelector::Range(Some(ref start), Some(ref end))
                if matches!(**start, ColumnSelector::Name(ref name) if name == "dna_ug")
                    && matches!(**end, ColumnSelector::Name(ref name) if name == "rna_ug")
        ));
    }

    #[test]
    fn quoted_names_allow_open_range() {
        let selectors = parse_selector_list("`dna_ug`:").unwrap();
        assert!(matches!(
            selectors[0],
            ColumnSelector::Range(Some(ref start), None)
                if matches!(**start, ColumnSelector::Name(ref name) if name == "dna_ug")
        ));
    }

    #[test]
    fn parses_brace_literals() {
        let selectors = parse_selector_list("{a:b},plain").unwrap();
        assert!(matches!(selectors[0], ColumnSelector::Name(ref name) if name == "a:b"));
        assert!(matches!(selectors[1], ColumnSelector::Name(ref name) if name == "plain"));
    }

    #[test]
    fn parses_negative_indices() {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let selectors = parse_selector_list("-1,-2").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![2, 1]);
    }

    #[test]
    fn rejects_unterminated_backtick() {
        let err = parse_selector_list("`foo").unwrap_err();
        assert!(err.to_string().contains("unterminated backtick"));
    }
}
