use std::fs::File;
use std::io;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use csv::ReaderBuilder;

#[derive(Debug, Clone)]
pub enum ColumnSelector {
    Index(usize),
    Name(String),
    Range(Box<ColumnSelector>, Box<ColumnSelector>),
}

pub fn parse_selector_list(spec: &str) -> Result<Vec<ColumnSelector>> {
    if spec.trim().is_empty() {
        bail!("column specification must not be empty");
    }

    spec.split(',')
        .map(|token| parse_selector_token(token.trim()))
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
            ColumnSelector::Index(_) | ColumnSelector::Name(_) => {
                let index = resolve_selector_index(headers, selector, no_header)?;
                indices.push(index);
            }
            ColumnSelector::Range(start, end) => {
                let start_idx = resolve_selector_index(headers, start, no_header)?;
                let end_idx = resolve_selector_index(headers, end, no_header)?;
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

pub fn reader_for_path(path: &Path, no_header: bool) -> Result<csv::Reader<Box<dyn io::Read>>> {
    let reader: Box<dyn io::Read> = if path == Path::new("-") {
        Box::new(io::stdin().lock())
    } else {
        Box::new(File::open(path).with_context(|| format!("failed to open {}", path.display()))?)
    };

    Ok(ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(!no_header)
        .flexible(true)
        .from_reader(reader))
}

pub fn default_headers(len: usize) -> Vec<String> {
    (1..=len).map(|i| format!("col{}", i)).collect()
}

fn parse_selector_token(token: &str) -> Result<ColumnSelector> {
    if token.is_empty() {
        return Err(anyhow!("empty column selector"));
    }

    if let Some((start, end, extra)) = split_range_token(token) {
        if extra {
            bail!("invalid column range '{}': too many ':' characters", token);
        }
        let start_selector = parse_simple_selector(start.trim())?;
        let end_selector = parse_simple_selector(end.trim())?;
        return Ok(ColumnSelector::Range(
            Box::new(start_selector),
            Box::new(end_selector),
        ));
    }

    parse_simple_selector(token)
}

fn parse_simple_selector(token: &str) -> Result<ColumnSelector> {
    if token.is_empty() {
        return Err(anyhow!("empty column selector"));
    }
    if let Ok(idx) = token.parse::<usize>() {
        if idx == 0 {
            bail!("column indices use 1-based positions");
        }
        Ok(ColumnSelector::Index(idx - 1))
    } else {
        Ok(ColumnSelector::Name(token.to_string()))
    }
}

fn split_range_token(token: &str) -> Option<(&str, &str, bool)> {
    let mut parts = token.split(':');
    let start = parts.next()?;
    let end = parts.next()?;
    let extra = parts.next().is_some();
    Some((start, end, extra))
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
    use super::{parse_selector_list, parse_single_selector, resolve_selectors};

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
    fn single_selector_rejects_range() {
        assert!(parse_single_selector("1:2").is_err());
    }

    #[test]
    fn name_range_disallowed_without_headers() {
        let headers = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        let selectors = parse_selector_list("col1:col3").unwrap();
        assert!(resolve_selectors(&headers, &selectors, true).is_err());
    }
}
