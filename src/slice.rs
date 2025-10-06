use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{InputOptions, reader_for_path, should_skip_record};

#[derive(Args, Debug)]
#[command(
    about = "Extract specific TSV rows by position",
    long_about = r#"Slice rows by 1-based position. Provide a comma-separated list of indices and inclusive ranges with -r/--rows (e.g. 1,10:20,200). When headers are present they are always emitted once at the top.

Examples:
  tsvkit slice -r 1,10:20 examples/profiles.tsv
  tsvkit slice -H -r 5:10 raw.tsv"#
)]
pub struct SliceArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Row specification (comma-separated 1-based indices and inclusive ranges, e.g. `1,10:20,200`)
    #[arg(short = 'r', long = "rows", value_name = "SPEC", required = true)]
    pub rows: String,

    /// Treat input as headerless (emits only selected rows)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Lines starting with this comment character are skipped (set to an uncommon symbol if your header begins with '#')
    #[arg(
        short = 'C',
        long = "comment-char",
        value_name = "CHAR",
        default_value = "#"
    )]
    pub comment_char: String,

    /// Ignore rows where every field is empty/whitespace
    #[arg(short = 'E', long = "ignore-empty-row")]
    pub ignore_empty_row: bool,

    /// Ignore rows whose column count differs from the header/first row
    #[arg(short = 'I', long = "ignore-illegal-row")]
    pub ignore_illegal_row: bool,
}

pub fn run(args: SliceArgs) -> Result<()> {
    let selection = parse_row_spec(&args.rows)?;
    if selection.is_empty() {
        bail!("row specification must select at least one row");
    }

    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if !selection.requires_from_end() {
        let ranges = selection.resolve_absolute()?;
        if ranges.is_empty() {
            bail!("row specification must select at least one row");
        }
        if args.no_header {
            let mut expected_width: Option<usize> = None;
            let mut data_row_idx = 0usize;
            for record in reader.records() {
                let record =
                    record.with_context(|| format!("failed reading from {:?}", args.file))?;
                if should_skip_record(&record, &input_opts, expected_width) {
                    continue;
                }
                if expected_width.is_none() {
                    expected_width = Some(record.len());
                }
                data_row_idx += 1;
                if row_selected(data_row_idx, &ranges) {
                    writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        } else {
            let headers = reader
                .headers()
                .with_context(|| format!("failed reading header from {:?}", args.file))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            if !headers.is_empty() {
                writer.write_all(headers.join("\t").as_bytes())?;
                writer.write_all(b"\n")?;
            }
            let mut data_row_idx = 0usize;
            for record in reader.records() {
                let record =
                    record.with_context(|| format!("failed reading from {:?}", args.file))?;
                if should_skip_record(&record, &input_opts, Some(headers.len())) {
                    continue;
                }
                data_row_idx += 1;
                if row_selected(data_row_idx, &ranges) {
                    writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    } else {
        if args.no_header {
            let mut expected_width: Option<usize> = None;
            let mut data_rows = Vec::new();
            for record in reader.records() {
                let record =
                    record.with_context(|| format!("failed reading from {:?}", args.file))?;
                if should_skip_record(&record, &input_opts, expected_width) {
                    continue;
                }
                if expected_width.is_none() {
                    expected_width = Some(record.len());
                }
                data_rows.push(record);
            }
            if data_rows.is_empty() {
                writer.flush()?;
                return Ok(());
            }
            let ranges = selection.resolve_with_total(data_rows.len())?;
            if ranges.is_empty() {
                bail!("row specification must select at least one row");
            }
            for (idx, record) in data_rows.iter().enumerate() {
                if row_selected(idx + 1, &ranges) {
                    writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        } else {
            let headers = reader
                .headers()
                .with_context(|| format!("failed reading header from {:?}", args.file))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            if !headers.is_empty() {
                writer.write_all(headers.join("\t").as_bytes())?;
                writer.write_all(b"\n")?;
            }
            let mut data_rows = Vec::new();
            for record in reader.records() {
                let record =
                    record.with_context(|| format!("failed reading from {:?}", args.file))?;
                if should_skip_record(&record, &input_opts, Some(headers.len())) {
                    continue;
                }
                data_rows.push(record);
            }
            if data_rows.is_empty() {
                writer.flush()?;
                return Ok(());
            }
            let ranges = selection.resolve_with_total(data_rows.len())?;
            if ranges.is_empty() {
                bail!("row specification must select at least one row");
            }
            for (idx, record) in data_rows.iter().enumerate() {
                if row_selected(idx + 1, &ranges) {
                    writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn parse_row_spec(spec: &str) -> Result<RowSelection> {
    let mut specs = Vec::new();
    for token in spec.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((start, end)) = trimmed.split_once(':') {
            let start_ep = parse_optional_row_endpoint(start)?;
            let end_ep = parse_optional_row_endpoint(end)?;
            if let (Some(RowEndpoint::Absolute(s)), Some(RowEndpoint::Absolute(e))) =
                (start_ep, end_ep)
            {
                if s > e {
                    bail!("row range start {} is greater than end {}", s, e);
                }
            }
            specs.push(RowRangeSpec {
                start: start_ep,
                end: end_ep,
            });
        } else {
            let endpoint = parse_row_endpoint(trimmed)?;
            specs.push(RowRangeSpec {
                start: Some(endpoint),
                end: Some(endpoint),
            });
        }
    }
    Ok(RowSelection { specs })
}

fn convert_row_specs(selection: &RowSelection, total_rows: Option<usize>) -> Result<Vec<RowRange>> {
    let mut ranges = Vec::with_capacity(selection.specs.len());
    for spec in &selection.specs {
        let start = match spec.start {
            Some(RowEndpoint::Absolute(idx)) => Some(idx),
            Some(RowEndpoint::FromEnd(offset)) => {
                let total = total_rows
                    .with_context(|| "row selector requires total row count to resolve")?;
                Some(resolve_from_end(offset, total)?)
            }
            None => None,
        };
        let end = match spec.end {
            Some(RowEndpoint::Absolute(idx)) => Some(idx),
            Some(RowEndpoint::FromEnd(offset)) => {
                let total = total_rows
                    .with_context(|| "row selector requires total row count to resolve")?;
                Some(resolve_from_end(offset, total)?)
            }
            None => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            if s > e {
                bail!("row range start {} is greater than end {}", s, e);
            }
        }
        ranges.push(RowRange { start, end });
    }
    ranges.sort_by_key(|r| r.start.unwrap_or(1));
    merge_ranges(ranges)
}

fn merge_ranges(ranges: Vec<RowRange>) -> Result<Vec<RowRange>> {
    if ranges.is_empty() {
        return Ok(ranges);
    }
    let mut merged = Vec::new();
    let mut iter = ranges.into_iter();
    let mut current = iter.next().unwrap();
    for range in iter {
        if ranges_touch_or_overlap(&current, &range) {
            current = merge_pair(current, range);
        } else {
            merged.push(current);
            current = range;
        }
    }
    merged.push(current);
    Ok(merged)
}

fn row_selected(row_idx: usize, ranges: &[RowRange]) -> bool {
    for range in ranges {
        let start = range.start.unwrap_or(1);
        if row_idx < start {
            return false;
        }
        match range.end {
            Some(end) => {
                if row_idx <= end {
                    return true;
                }
            }
            None => return true,
        }
    }
    false
}

fn parse_row_endpoint(text: &str) -> Result<RowEndpoint> {
    if let Some(stripped) = text.strip_prefix('-') {
        if stripped.is_empty() {
            bail!("row selector '-' must include an index");
        }
        let value: usize = stripped
            .parse()
            .with_context(|| format!("invalid row index '{}'", text))?;
        if value == 0 {
            bail!("row selector '-0' is not allowed");
        }
        Ok(RowEndpoint::FromEnd(value))
    } else {
        let value: usize = text
            .parse()
            .with_context(|| format!("invalid row index '{}'", text))?;
        if value == 0 {
            bail!("row indices are 1-based");
        }
        Ok(RowEndpoint::Absolute(value))
    }
}

fn parse_optional_row_endpoint(text: &str) -> Result<Option<RowEndpoint>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    parse_row_endpoint(trimmed).map(Some)
}

fn resolve_from_end(offset: usize, total_rows: usize) -> Result<usize> {
    if offset == 0 {
        bail!("row selector '-0' is not allowed");
    }
    if offset > total_rows {
        bail!(
            "row selector '-{}' exceeds total row count {}",
            offset,
            total_rows
        );
    }
    Ok(total_rows - offset + 1)
}

fn ranges_touch_or_overlap(a: &RowRange, b: &RowRange) -> bool {
    match a.end {
        None => true,
        Some(end) => {
            let next_start = b.start.unwrap_or(1);
            match end.checked_add(1) {
                Some(limit) => next_start <= limit,
                None => true,
            }
        }
    }
}

fn merge_pair(mut a: RowRange, b: RowRange) -> RowRange {
    if a.start.is_none() || b.start.is_none() {
        a.start = None;
    }
    match (a.end, b.end) {
        (None, _) | (_, None) => a.end = None,
        (Some(left), Some(right)) => {
            if right > left {
                a.end = Some(right);
            }
        }
    }
    a
}

#[derive(Clone, Debug)]
struct RowSelection {
    specs: Vec<RowRangeSpec>,
}

impl RowSelection {
    fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    fn requires_from_end(&self) -> bool {
        self.specs.iter().any(|spec| {
            matches!(spec.start, Some(RowEndpoint::FromEnd(_)))
                || matches!(spec.end, Some(RowEndpoint::FromEnd(_)))
        })
    }

    fn resolve_absolute(&self) -> Result<Vec<RowRange>> {
        convert_row_specs(self, None)
    }

    fn resolve_with_total(&self, total_rows: usize) -> Result<Vec<RowRange>> {
        convert_row_specs(self, Some(total_rows))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RowRangeSpec {
    start: Option<RowEndpoint>,
    end: Option<RowEndpoint>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RowEndpoint {
    Absolute(usize),
    FromEnd(usize),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RowRange {
    start: Option<usize>,
    end: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_rows() {
        let selection = parse_row_spec("1,5,10").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange {
                    start: Some(1),
                    end: Some(1)
                },
                RowRange {
                    start: Some(5),
                    end: Some(5)
                },
                RowRange {
                    start: Some(10),
                    end: Some(10)
                }
            ]
        );
    }

    #[test]
    fn parse_ranges() {
        let selection = parse_row_spec("1:3,10:12").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange {
                    start: Some(1),
                    end: Some(3)
                },
                RowRange {
                    start: Some(10),
                    end: Some(12)
                }
            ]
        );
    }

    #[test]
    fn merge_overlapping_ranges() {
        let selection = parse_row_spec("1:3,2:5,10").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange {
                    start: Some(1),
                    end: Some(5)
                },
                RowRange {
                    start: Some(10),
                    end: Some(10)
                }
            ]
        );
    }

    #[test]
    fn parse_open_start_range() {
        let selection = parse_row_spec(":5").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![RowRange {
                start: None,
                end: Some(5)
            }]
        );
    }

    #[test]
    fn parse_open_end_range() {
        let selection = parse_row_spec("10:").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![RowRange {
                start: Some(10),
                end: None
            }]
        );
    }

    #[test]
    fn parse_full_range() {
        let selection = parse_row_spec(":").unwrap();
        let ranges = selection.resolve_absolute().unwrap();
        assert_eq!(
            ranges,
            vec![RowRange {
                start: None,
                end: None
            }]
        );
    }

    #[test]
    fn reject_zero_index() {
        assert!(parse_row_spec("0").is_err());
    }

    #[test]
    fn parse_from_end_range() {
        let selection = parse_row_spec("-3:").unwrap();
        assert!(selection.requires_from_end());
        let ranges = selection.resolve_with_total(5).unwrap();
        assert_eq!(
            ranges,
            vec![RowRange {
                start: Some(3),
                end: None
            }]
        );
    }

    #[test]
    fn from_end_range_errors_when_too_large() {
        let selection = parse_row_spec(":-2").unwrap();
        assert!(selection.resolve_with_total(1).is_err());
    }
}
