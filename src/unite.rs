use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    InputOptions, default_headers, parse_selector_list, reader_for_path, resolve_selectors,
    should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Combine multiple columns into one column",
    long_about = "Combine values from selected columns into one output column separated by text. By default selected source columns are removed; use --keep to keep them.",
    after_help = "Examples:
  tsvkit unite -f subject_id,timepoint --name subject_time --sep '_' data.tsv
  tsvkit unite -H -f '1:3' --name key --sep '|' data.tsv"
)]
pub struct UniteArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Source column selectors to combine (supports mixed names/indices/ranges/regex)
    #[arg(
        short = 'f',
        long = "fields",
        value_name = "SELECTORS",
        required = true
    )]
    pub fields: String,

    /// Name of the output combined column (default: united)
    #[arg(long = "name", value_name = "NAME", default_value = "united")]
    pub name: String,

    /// Separator inserted between source values
    #[arg(long = "sep", value_name = "SEP", default_value = "_")]
    pub sep: String,

    /// Keep source columns instead of replacing them
    #[arg(long = "keep")]
    pub keep: bool,

    /// Skip empty source values when combining
    #[arg(long = "na-rm")]
    pub na_rm: bool,

    /// Treat input as headerless
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    #[arg(
        short = 'C',
        long = "comment-char",
        value_name = "CHAR",
        default_value = "#"
    )]
    pub comment_char: String,
    #[arg(short = 'E', long = "ignore-empty-row")]
    pub ignore_empty_row: bool,
    #[arg(short = 'I', long = "ignore-illegal-row")]
    pub ignore_illegal_row: bool,
}

pub fn run(args: UniteArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first_record = loop {
            match records.next() {
                Some(rec) => {
                    let record =
                        rec.with_context(|| format!("failed reading from {:?}", args.file))?;
                    if should_skip_record(&record, &input_opts, None) {
                        continue;
                    }
                    break record;
                }
                None => return Ok(()),
            }
        };
        let expected_width = first_record.len();
        let headers = default_headers(expected_width);
        let selected = parse_selector_list(&args.fields)?;
        let indices = resolve_selectors(&headers, &selected, true)?;
        if indices.is_empty() {
            bail!("--fields resolved to no columns");
        }
        let output_header = build_header(&headers, &indices, &args.name, args.keep);
        writeln!(writer, "{}", output_header.join("\t"))?;
        let row = unite_row(
            first_record.iter().map(|s| s.to_string()).collect(),
            &indices,
            &args.name,
            &args.sep,
            args.keep,
            args.na_rm,
        );
        writeln!(writer, "{}", row.join("\t"))?;
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let row = unite_row(
                record.iter().map(|s| s.to_string()).collect(),
                &indices,
                &args.name,
                &args.sep,
                args.keep,
                args.na_rm,
            );
            writeln!(writer, "{}", row.join("\t"))?;
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let expected_width = headers.len();
        let selected = parse_selector_list(&args.fields)?;
        let indices = resolve_selectors(&headers, &selected, false)?;
        if indices.is_empty() {
            bail!("--fields resolved to no columns");
        }
        let output_header = build_header(&headers, &indices, &args.name, args.keep);
        writeln!(writer, "{}", output_header.join("\t"))?;
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let row = unite_row(
                record.iter().map(|s| s.to_string()).collect(),
                &indices,
                &args.name,
                &args.sep,
                args.keep,
                args.na_rm,
            );
            writeln!(writer, "{}", row.join("\t"))?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn build_header(headers: &[String], indices: &[usize], name: &str, keep: bool) -> Vec<String> {
    let selected: HashSet<usize> = indices.iter().copied().collect();
    let first_selected = indices.iter().copied().min().unwrap_or(0);
    let mut out = Vec::new();
    for (idx, header) in headers.iter().enumerate() {
        if idx == first_selected {
            out.push(name.to_string());
        }
        if selected.contains(&idx) {
            if keep {
                out.push(header.clone());
            }
        } else {
            out.push(header.clone());
        }
    }
    out
}

fn unite_row(
    row: Vec<String>,
    indices: &[usize],
    _name: &str,
    sep: &str,
    keep: bool,
    na_rm: bool,
) -> Vec<String> {
    let selected: HashSet<usize> = indices.iter().copied().collect();
    let first_selected = indices.iter().copied().min().unwrap_or(0);
    let mut pieces = Vec::new();
    for &idx in indices {
        let text = row.get(idx).cloned().unwrap_or_default();
        if na_rm && text.is_empty() {
            continue;
        }
        pieces.push(text);
    }
    let combined = pieces.join(sep);
    let mut out = Vec::new();
    for (idx, value) in row.into_iter().enumerate() {
        if idx == first_selected {
            out.push(combined.clone());
        }
        if selected.contains(&idx) {
            if keep {
                out.push(value);
            }
        } else {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unite_row_replaces_selected_columns() {
        let row = vec!["s1".to_string(), "baseline".to_string(), "case".to_string()];
        let out = unite_row(row, &[0, 1], "id", "_", false, false);
        assert_eq!(out, vec!["s1_baseline", "case"]);
    }

    #[test]
    fn unite_row_keep_and_na_rm() {
        let row = vec!["A".to_string(), "".to_string(), "Z".to_string()];
        let out = unite_row(row, &[0, 1], "id", "-", true, true);
        assert_eq!(out, vec!["A", "A", "", "Z"]);
    }
}
