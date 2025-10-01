use std::fs::File;
use std::io;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use csv::ReaderBuilder;

#[derive(Debug, Clone)]
pub enum ColumnSelector {
    Index(usize),
    Name(String),
}

pub fn parse_selector_list(spec: &str) -> Result<Vec<ColumnSelector>> {
    if spec.trim().is_empty() {
        bail!("column specification must not be empty");
    }

    spec.split(',')
        .map(|token| {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(anyhow!("empty column selector"));
            }
            if let Ok(idx) = trimmed.parse::<usize>() {
                if idx == 0 {
                    bail!("column indices use 1-based positions");
                }
                Ok(ColumnSelector::Index(idx - 1))
            } else {
                Ok(ColumnSelector::Name(trimmed.to_string()))
            }
        })
        .collect()
}

pub fn parse_single_selector(token: &str) -> Result<ColumnSelector> {
    let mut selectors = parse_selector_list(token)?;
    if selectors.len() != 1 {
        bail!("column specification must reference exactly one column");
    }
    Ok(selectors.remove(0))
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
        let index = match selector {
            ColumnSelector::Index(idx) => *idx,
            ColumnSelector::Name(name) => {
                if no_header {
                    bail!("column names cannot be used when input lacks a header row");
                }
                headers
                    .iter()
                    .position(|h| h == name)
                    .with_context(|| format!("column '{}' not found", name))?
            }
        };
        if index >= headers.len() {
            bail!(
                "column index {} out of range ({} columns)",
                index + 1,
                headers.len()
            );
        }
        indices.push(index);
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
