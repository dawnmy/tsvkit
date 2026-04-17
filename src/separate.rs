use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use regex::Regex;

use crate::common::{
    InputOptions, default_headers, parse_selector_list, reader_for_path, resolve_selectors,
    should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Split one column into multiple columns",
    long_about = "Split a character column into multiple output columns by separator text (or regex with --regex-sep). By default, the source column is replaced by the new columns. Use --keep to retain the original column too.",
    after_help = "Examples:
  tsvkit separate -f sample_id --into subject,visit --sep '-' data.tsv
  tsvkit separate -f 1 --into id,run,lane --sep '_' -H data.tsv
  tsvkit separate -f barcode --into part1,part2 --regex-sep '\\\\|' data.tsv"
)]
pub struct SeparateArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Source column selector (must resolve to exactly one column)
    #[arg(short = 'f', long = "field", value_name = "SELECTOR", required = true)]
    pub field: String,

    /// Output column names (comma-separated). If omitted, defaults to source_1, source_2, ...
    #[arg(long = "into", value_name = "NAMES")]
    pub into: Option<String>,

    /// Separator string (literal by default, regex if --regex-sep is set)
    #[arg(long = "sep", value_name = "SEP", default_value = "_")]
    pub sep: String,

    /// Interpret --sep as a regex pattern
    #[arg(long = "regex-sep")]
    pub regex_sep: bool,

    /// Keep source column instead of replacing it
    #[arg(long = "keep")]
    pub keep: bool,

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

pub fn run(args: SeparateArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    let splitter = if args.regex_sep {
        Splitter::Regex(
            Regex::new(&args.sep)
                .with_context(|| format!("invalid --sep regex pattern '{}'", args.sep))?,
        )
    } else {
        Splitter::Literal(args.sep.clone())
    };

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
        let selected = parse_selector_list(&args.field)?;
        let idx = resolve_single_column(&headers, &selected, true)?;
        let into_names = resolve_into_names(args.into.as_deref(), headers[idx].as_str())?;
        let output_header = build_output_header(&headers, idx, &into_names, args.keep);
        writeln!(writer, "{}", output_header.join("\t"))?;

        let row = transform_row(
            first_record.iter().map(|s| s.to_string()).collect(),
            idx,
            &into_names,
            &splitter,
            args.keep,
        );
        writeln!(writer, "{}", row.join("\t"))?;

        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let row = transform_row(
                record.iter().map(|s| s.to_string()).collect(),
                idx,
                &into_names,
                &splitter,
                args.keep,
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
        let selected = parse_selector_list(&args.field)?;
        let idx = resolve_single_column(&headers, &selected, false)?;
        let into_names = resolve_into_names(args.into.as_deref(), headers[idx].as_str())?;
        let output_header = build_output_header(&headers, idx, &into_names, args.keep);
        writeln!(writer, "{}", output_header.join("\t"))?;

        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let row = transform_row(
                record.iter().map(|s| s.to_string()).collect(),
                idx,
                &into_names,
                &splitter,
                args.keep,
            );
            writeln!(writer, "{}", row.join("\t"))?;
        }
    }

    writer.flush()?;
    Ok(())
}

enum Splitter {
    Literal(String),
    Regex(Regex),
}

fn resolve_single_column(
    headers: &[String],
    selectors: &[crate::common::ColumnSelector],
    no_header: bool,
) -> Result<usize> {
    let indices = resolve_selectors(headers, selectors, no_header)?;
    if indices.len() != 1 {
        bail!(
            "--field must resolve to exactly one column, got {}",
            indices.len()
        );
    }
    Ok(indices[0])
}

fn resolve_into_names(spec: Option<&str>, source_name: &str) -> Result<Vec<String>> {
    match spec {
        Some(text) if !text.trim().is_empty() => text
            .split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
            .pipe(|names| {
                if names.is_empty() {
                    bail!("--into must provide at least one output name")
                } else {
                    Ok(names)
                }
            }),
        _ => Ok(vec![
            format!("{}_1", source_name),
            format!("{}_2", source_name),
        ]),
    }
}

fn build_output_header(
    headers: &[String],
    source_idx: usize,
    into_names: &[String],
    keep_source: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for (idx, name) in headers.iter().enumerate() {
        if idx == source_idx {
            if keep_source {
                out.push(name.clone());
            }
            out.extend(into_names.iter().cloned());
        } else {
            out.push(name.clone());
        }
    }
    out
}

fn transform_row(
    row: Vec<String>,
    source_idx: usize,
    into_names: &[String],
    splitter: &Splitter,
    keep_source: bool,
) -> Vec<String> {
    let source = row.get(source_idx).cloned().unwrap_or_default();
    let split = split_value(&source, into_names.len(), splitter);
    let mut out = Vec::new();
    for (idx, value) in row.into_iter().enumerate() {
        if idx == source_idx {
            if keep_source {
                out.push(value);
            }
            out.extend(split.iter().cloned());
        } else {
            out.push(value);
        }
    }
    out
}

fn split_value(value: &str, parts: usize, splitter: &Splitter) -> Vec<String> {
    let mut out = Vec::new();
    if parts == 0 {
        return out;
    }
    match splitter {
        Splitter::Literal(sep) => {
            for token in value.splitn(parts, sep) {
                out.push(token.to_string());
            }
        }
        Splitter::Regex(re) => {
            for token in re.splitn(value, parts) {
                out.push(token.to_string());
            }
        }
    }
    while out.len() < parts {
        out.push(String::new());
    }
    out
}

trait Pipe: Sized {
    fn pipe<F, T>(self, f: F) -> T
    where
        F: FnOnce(Self) -> T,
    {
        f(self)
    }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_value_pads_missing_parts() {
        let parts = split_value("A", 3, &Splitter::Literal("_".to_string()));
        assert_eq!(parts, vec!["A", "", ""]);
    }

    #[test]
    fn split_value_limits_to_requested_parts() {
        let parts = split_value("A_B_C_D", 3, &Splitter::Literal("_".to_string()));
        assert_eq!(parts, vec!["A", "B", "C_D"]);
    }
}
