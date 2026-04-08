use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use csv::ReaderBuilder;
use flate2::read::MultiGzDecoder;
use regex::Regex;
use xz2::read::XzDecoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialColumn {
    FilePath,
    FileBase,
}

impl SpecialColumn {
    pub fn default_header(self) -> &'static str {
        match self {
            SpecialColumn::FilePath => "__file__",
            SpecialColumn::FileBase => "__base__",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnSelector {
    Index(usize),
    FromEnd(usize),
    Name(String),
    Regex(String),
    Template(String),
    Range(Option<Box<ColumnSelector>>, Option<Box<ColumnSelector>>),
    Special(SpecialColumn),
}

pub fn parse_selector_list(spec: &str) -> Result<Vec<ColumnSelector>> {
    parse_selector_list_impl(spec, false)
}

pub fn parse_selector_list_with_templates(spec: &str) -> Result<Vec<ColumnSelector>> {
    parse_selector_list_impl(spec, true)
}

fn parse_selector_list_impl(spec: &str, braces_as_templates: bool) -> Result<Vec<ColumnSelector>> {
    if spec.trim().is_empty() {
        bail!("column specification must not be empty");
    }

    tokenize_selector_spec(spec)?
        .into_iter()
        .map(|token| parse_selector_token(token, braces_as_templates))
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
    resolve_selectors_with_options(headers, selectors, no_header, false)
}

pub fn resolve_selectors_allow_duplicates(
    headers: &[String],
    selectors: &[ColumnSelector],
    no_header: bool,
) -> Result<Vec<usize>> {
    resolve_selectors_with_options(headers, selectors, no_header, true)
}

fn resolve_selectors_with_options(
    headers: &[String],
    selectors: &[ColumnSelector],
    no_header: bool,
    allow_duplicates: bool,
) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for selector in selectors {
        match selector {
            ColumnSelector::Special(special) => {
                bail!(
                    "special column '{}' cannot be resolved as a positional index",
                    special.default_header()
                );
            }
            ColumnSelector::Index(_)
            | ColumnSelector::FromEnd(_)
            | ColumnSelector::Name(_)
            | ColumnSelector::Regex(_) => {
                let mut resolved =
                    resolve_selector_indices(headers, selector, no_header, allow_duplicates)?;
                indices.append(&mut resolved);
            }
            ColumnSelector::Template(template) => {
                bail!(
                    "template selector '{{{}}}' cannot be resolved as a positional index",
                    template
                );
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

pub fn inconsistent_width_error(
    source: &str,
    row_number: usize,
    expected: usize,
    actual: usize,
) -> anyhow::Error {
    anyhow!(
        "rows in {} have inconsistent column counts at row {} (expected {}, got {})",
        source,
        row_number,
        expected,
        actual,
    )
}

pub fn default_headers(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("col{}", i)).collect()
}

#[derive(Debug, Clone)]
pub struct FileTemplateContext {
    pub path: String,
    pub base: String,
    pub dir: String,
}

impl FileTemplateContext {
    pub fn from_path(path: &Path) -> Self {
        if path == Path::new("-") {
            return FileTemplateContext {
                path: "-".to_string(),
                base: "-".to_string(),
                dir: ".".to_string(),
            };
        }
        let full = path.to_string_lossy().into_owned();
        let base = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| full.clone());
        let dir = path
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string());
        FileTemplateContext {
            path: full,
            base,
            dir,
        }
    }
}

pub fn render_file_template(template: &str, ctx: &FileTemplateContext) -> Result<String> {
    let mut out = String::new();
    let chars = template.chars().collect::<Vec<_>>();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '{' {
            let mut j = i + 1;
            while j < chars.len() && chars[j] != '}' {
                j += 1;
            }
            if j >= chars.len() {
                bail!("unterminated '{{' in template '{}'", template);
            }
            let token = chars[i + 1..j].iter().collect::<String>();
            out.push_str(&expand_file_token(token.trim(), ctx)?);
            i = j + 1;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    Ok(out)
}

fn expand_file_token(token: &str, ctx: &FileTemplateContext) -> Result<String> {
    let (core, case_mode) = if let Some((left, right)) = token.split_once('!') {
        (left.trim(), Some(right.trim().to_ascii_lowercase()))
    } else {
        (token, None)
    };

    let (core, suffix) = if let Some((left, right)) = core.split_once('^') {
        (left.trim(), Some(right.to_string()))
    } else {
        (core, None)
    };

    let (core, prefix) = if let Some((left, right)) = core.split_once('#') {
        (left.trim(), Some(right.to_string()))
    } else {
        (core, None)
    };

    let mut value = match core {
        "file" | "__file__" => ctx.path.clone(),
        "base" | "__base__" => ctx.base.clone(),
        "dir" | "__dir__" => ctx.dir.clone(),
        _ => {
            let source;
            let mut actions = String::new();
            for ch in core.chars() {
                if ch.is_ascii_alphabetic() {
                    actions.push(ch);
                    continue;
                }
            }
            if core.starts_with("__base__") {
                source = "base";
                actions = core["__base__".len()..].to_string();
            } else if core.starts_with("__dir__") {
                source = "dir";
                actions = core["__dir__".len()..].to_string();
            } else if core.starts_with("__file__") {
                source = "file";
                actions = core["__file__".len()..].to_string();
            } else if core.starts_with("base") {
                source = "base";
                actions = core["base".len()..].to_string();
            } else if core.starts_with("dir") {
                source = "dir";
                actions = core["dir".len()..].to_string();
            } else if core.starts_with("file") {
                source = "file";
                actions = core["file".len()..].to_string();
            } else {
                bail!("unsupported template token '{}'", token);
            }
            apply_template_actions(source, &actions, ctx)?
        }
    };

    if let Some(sfx) = suffix {
        if value.ends_with(&sfx) {
            let trimmed = value.len().saturating_sub(sfx.len());
            value.truncate(trimmed);
        }
    }

    if let Some(pfx) = prefix {
        if value.starts_with(&pfx) {
            value = value[pfx.len()..].to_string();
        }
    }

    if let Some(mode) = case_mode {
        value = match mode.as_str() {
            "upper" | "u" => value.to_uppercase(),
            "lower" | "l" => value.to_lowercase(),
            "cap" | "capitalize" | "c" => capitalize_first(&value),
            _ => bail!("unsupported case mode '{}'", mode),
        };
    }

    Ok(value)
}

fn apply_template_actions(
    source: &str,
    actions: &str,
    ctx: &FileTemplateContext,
) -> Result<String> {
    let mut value = match source {
        "file" => ctx.path.clone(),
        "base" => ctx.base.clone(),
        "dir" => ctx.dir.clone(),
        _ => bail!("unsupported template source '{}'", source),
    };

    for ch in actions.chars() {
        match ch {
            '%' => value = basename(&value),
            '/' => value = basedir(&value),
            ':' => value = strip_all_extensions(&value),
            '.' => value = strip_last_extension(&value),
            _ if ch.is_whitespace() => {}
            _ => bail!("unsupported template modifier '{}' in '{}'", ch, actions),
        }
    }
    Ok(value)
}

fn basename(input: &str) -> String {
    Path::new(input)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| input.to_string())
}

fn basedir(input: &str) -> String {
    PathBuf::from(input)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| ".".to_string())
}

fn strip_last_extension(input: &str) -> String {
    let path = Path::new(input);
    if let Some(stem) = path.file_stem() {
        if let Some(parent) = path.parent() {
            if parent.as_os_str().is_empty() {
                return stem.to_string_lossy().into_owned();
            }
            return format!("{}/{}", parent.to_string_lossy(), stem.to_string_lossy());
        }
        return stem.to_string_lossy().into_owned();
    }
    input.to_string()
}

fn strip_all_extensions(input: &str) -> String {
    let path = Path::new(input);
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| input.to_string());
    let mut core = name.clone();
    while let Some((left, _)) = core.rsplit_once('.') {
        if left.is_empty() {
            break;
        }
        core = left.to_string();
    }
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return core;
        }
        return format!("{}/{}", parent.to_string_lossy(), core);
    }
    core
}

fn capitalize_first(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out = first.to_uppercase().to_string();
            out.push_str(chars.as_str());
            out
        }
    }
}

fn parse_selector_token(token: SelectorToken, braces_as_templates: bool) -> Result<ColumnSelector> {
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
            Some(Box::new(parse_simple_selector(
                start_trim,
                braces_as_templates,
            )?))
        };
        let end_selector = if end_trim.is_empty() {
            None
        } else {
            Some(Box::new(parse_simple_selector(
                end_trim,
                braces_as_templates,
            )?))
        };
        return Ok(ColumnSelector::Range(start_selector, end_selector));
    }

    parse_simple_selector(&token.text, braces_as_templates)
}

fn parse_simple_selector(token: &str, braces_as_templates: bool) -> Result<ColumnSelector> {
    if token.is_empty() {
        return Err(anyhow!("empty column selector"));
    }
    if let Some(regex) = parse_regex_literal(token)? {
        return Ok(ColumnSelector::Regex(regex));
    }
    if let Some(literal) = parse_backtick_literal(token)? {
        return Ok(ColumnSelector::Name(literal));
    }
    if let Some(literal) = parse_brace_literal(token)? {
        if braces_as_templates {
            return Ok(ColumnSelector::Template(literal));
        }
        return Ok(ColumnSelector::Name(literal));
    }
    match token {
        "__file__" => return Ok(ColumnSelector::Special(SpecialColumn::FilePath)),
        "__base__" => return Ok(ColumnSelector::Special(SpecialColumn::FileBase)),
        _ => {}
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

fn parse_regex_literal(token: &str) -> Result<Option<String>> {
    let trimmed = token.trim();
    if !trimmed.starts_with('~') {
        return Ok(None);
    }
    let remainder = trimmed[1..].trim_start();
    let mut chars = remainder.chars();
    match chars.next() {
        Some('"') => {
            let mut value = String::new();
            let mut escaped = false;
            while let Some(ch) = chars.next() {
                if escaped {
                    value.push(ch);
                    escaped = false;
                    continue;
                }
                match ch {
                    '\\' => {
                        escaped = true;
                    }
                    '"' => {
                        if !chars.as_str().is_empty() {
                            bail!("unexpected trailing characters after regex selector literal");
                        }
                        return Ok(Some(value));
                    }
                    other => value.push(other),
                }
            }
            bail!("unterminated regex selector literal");
        }
        Some(other) => bail!(
            "regex column selector must use double quotes (e.g. ~\"pattern\"), got '{}'",
            other
        ),
        None => bail!("regex column selector requires a quoted pattern"),
    }
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

fn resolve_selector_indices(
    headers: &[String],
    selector: &ColumnSelector,
    no_header: bool,
    allow_duplicates: bool,
) -> Result<Vec<usize>> {
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
            Ok(vec![index])
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
            Ok(vec![headers.len() - offset])
        }
        ColumnSelector::Name(name) => {
            if no_header {
                bail!("column names cannot be used when input lacks a header row");
            }
            if allow_duplicates {
                let mut matches = Vec::new();
                for (idx, header) in headers.iter().enumerate() {
                    if header == name {
                        matches.push(idx);
                    }
                }
                if matches.is_empty() {
                    bail!("column '{}' not found", name);
                }
                Ok(matches)
            } else {
                let index = headers
                    .iter()
                    .position(|h| h == name)
                    .with_context(|| format!("column '{}' not found", name))?;
                Ok(vec![index])
            }
        }
        ColumnSelector::Regex(pattern) => {
            if no_header {
                bail!("regex column selectors require headers");
            }
            let regex = Regex::new(pattern)
                .with_context(|| format!("invalid regex pattern '{}'", pattern))?;
            let mut seen = HashSet::new();
            let mut matches = Vec::new();
            for (idx, header) in headers.iter().enumerate() {
                if regex.is_match(header) {
                    if allow_duplicates || seen.insert(header.clone()) {
                        matches.push(idx);
                    }
                }
            }
            if matches.is_empty() {
                bail!("regex pattern '{}' did not match any columns", pattern);
            }
            Ok(matches)
        }
        ColumnSelector::Template(template) => bail!(
            "template selector '{{{}}}' cannot be resolved as a positional index",
            template
        ),
        ColumnSelector::Range(_, _) => unreachable!("range selectors handled separately"),
        ColumnSelector::Special(special) => bail!(
            "special column '{}' not supported without column injection",
            special.default_header()
        ),
    }
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
        ColumnSelector::Regex(_) => {
            bail!("regex column selectors cannot be used in range endpoints")
        }
        ColumnSelector::Template(template) => {
            bail!(
                "template selector '{{{}}}' cannot be used in range endpoints",
                template
            )
        }
        ColumnSelector::Special(special) => bail!(
            "special column '{}' not supported without column injection",
            special.default_header()
        ),
        ColumnSelector::Range(_, _) => {
            bail!("unexpected nested column range")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        ColumnSelector, FileTemplateContext, SpecialColumn, parse_selector_list,
        parse_selector_list_with_templates, parse_single_selector, render_file_template,
        resolve_selectors, resolve_selectors_allow_duplicates,
    };

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
    fn parses_brace_templates_when_enabled() {
        let selectors = parse_selector_list_with_templates("{base:},plain").unwrap();
        assert!(matches!(selectors[0], ColumnSelector::Template(ref name) if name == "base:"));
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
    fn resolves_negative_open_range() {
        let headers = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let selectors = parse_selector_list("-2:").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![2, 3]);
    }

    #[test]
    fn renders_file_template_variants() {
        let ctx = FileTemplateContext::from_path(Path::new("/tmp/a.b.c.tsv"));
        assert_eq!(render_file_template("{base}", &ctx).unwrap(), "a.b.c.tsv");
        assert_eq!(render_file_template("{base.}", &ctx).unwrap(), "a.b.c");
        assert_eq!(render_file_template("{base:}", &ctx).unwrap(), "a");
        assert_eq!(render_file_template("{file%:}", &ctx).unwrap(), "a");
        let ctx2 = FileTemplateContext::from_path(Path::new("/tmp/sample_A.tsv"));
        assert_eq!(render_file_template("{base:#sample_}", &ctx2).unwrap(), "A");
        assert_eq!(
            render_file_template("{file^.tsv}", &ctx).unwrap(),
            "/tmp/a.b.c"
        );
        assert_eq!(render_file_template("{__file__%:}", &ctx).unwrap(), "a");
        assert_eq!(render_file_template("{__base__.}", &ctx).unwrap(), "a.b.c");
    }

    #[test]
    fn rejects_unterminated_backtick() {
        let err = parse_selector_list("`foo").unwrap_err();
        assert!(err.to_string().contains("unterminated backtick"));
    }

    #[test]
    fn distinguishes_injected_and_literal_file_columns() {
        let selectors = parse_selector_list("__file__,{__file__},`__base__`").unwrap();
        assert!(matches!(
            selectors[0],
            ColumnSelector::Special(SpecialColumn::FilePath)
        ));
        assert!(matches!(selectors[1], ColumnSelector::Name(ref name) if name == "__file__"));
        assert!(matches!(selectors[2], ColumnSelector::Name(ref name) if name == "__base__"));
    }

    #[test]
    fn regex_selector_matches_columns() {
        let headers = vec![
            "sample_a".to_string(),
            "other".to_string(),
            "sample_b".to_string(),
        ];
        let selectors = parse_selector_list("~\"^sample_\"").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn allow_duplicates_includes_repeated_headers() {
        let headers = vec![
            "value".to_string(),
            "value".to_string(),
            "other".to_string(),
        ];
        let selectors = parse_selector_list("value").unwrap();
        let indices = resolve_selectors(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0]);
        let indices = resolve_selectors_allow_duplicates(&headers, &selectors, false).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }
}
