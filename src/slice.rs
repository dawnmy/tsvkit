use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::reader_for_path;

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
}

pub fn run(args: SliceArgs) -> Result<()> {
    let ranges = parse_row_spec(&args.rows)?;
    if ranges.is_empty() {
        bail!("row specification must select at least one row");
    }

    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    let mut data_row_idx = 0usize;

    if args.no_header {
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
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
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            data_row_idx += 1;
            if row_selected(data_row_idx, &ranges) {
                writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn parse_row_spec(spec: &str) -> Result<Vec<RowRange>> {
    let mut ranges = Vec::new();
    for token in spec.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((start, end)) = trimmed.split_once(':') {
            let start_idx = parse_positive_index(start)?;
            let end_idx = parse_positive_index(end)?;
            if start_idx > end_idx {
                bail!(
                    "row range start {} is greater than end {}",
                    start_idx,
                    end_idx
                );
            }
            ranges.push(RowRange {
                start: start_idx,
                end: end_idx,
            });
        } else {
            let idx = parse_positive_index(trimmed)?;
            ranges.push(RowRange {
                start: idx,
                end: idx,
            });
        }
    }
    ranges.sort_by_key(|r| r.start);
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
        if range.start <= current.end + 1 {
            current.end = current.end.max(range.end);
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
        if row_idx < range.start {
            return false;
        }
        if row_idx <= range.end {
            return true;
        }
    }
    false
}

fn parse_positive_index(text: &str) -> Result<usize> {
    let value: usize = text
        .parse()
        .with_context(|| format!("invalid row index '{}'", text))?;
    if value == 0 {
        bail!("row indices are 1-based");
    }
    Ok(value)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RowRange {
    start: usize,
    end: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_rows() {
        let ranges = parse_row_spec("1,5,10").unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange { start: 1, end: 1 },
                RowRange { start: 5, end: 5 },
                RowRange { start: 10, end: 10 }
            ]
        );
    }

    #[test]
    fn parse_ranges() {
        let ranges = parse_row_spec("1:3,10:12").unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange { start: 1, end: 3 },
                RowRange { start: 10, end: 12 }
            ]
        );
    }

    #[test]
    fn merge_overlapping_ranges() {
        let ranges = parse_row_spec("1:3,2:5,10").unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange { start: 1, end: 5 },
                RowRange { start: 10, end: 10 }
            ]
        );
    }

    #[test]
    fn reject_zero_index() {
        assert!(parse_row_spec("0").is_err());
    }
}
