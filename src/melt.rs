use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use indexmap::IndexSet;

use crate::common::{
    InputOptions, default_headers, inconsistent_width_error, parse_selector_list, reader_for_path,
    resolve_selectors, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Melt wide TSV tables into long form",
    long_about = "Convert wide TSV tables into a tidy long format. Use -i/--id to keep identifier columns, optionally -v/--value-cols to target specific value columns, and rename the generated columns with --variable/--value. Defaults to header-aware mode; add -H for headerless files.\n\nExample:\n  tsvkit melt -i id examples/profiles.tsv"
)]
pub struct MeltArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Identifier columns to keep in the long output (optional; names/indices/ranges)
    #[arg(short = 'i', long = "id", value_name = "COLS")]
    pub id_cols: Option<String>,

    /// Columns to melt (comma-separated names/indices/ranges). Defaults to all non-id columns.
    #[arg(short = 'v', long = "value-cols", value_name = "COLS")]
    pub value_cols: Option<String>,

    /// Column name for melted variable labels (default `variable`)
    #[arg(long = "variable", value_name = "NAME", default_value = "variable")]
    pub variable_name: String,

    /// Column name for melted values (default `value`)
    #[arg(long = "value", value_name = "NAME", default_value = "value")]
    pub value_name: String,

    /// Treat input as headerless (columns referenced by indices only)
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

    /// Fill value to use when a source cell is empty or missing
    #[arg(long = "fill", value_name = "TEXT")]
    pub fill: Option<String>,
}

pub fn run(args: MeltArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let source_name = format!("\"{}\"", args.file.display());
    let mut writer = BufWriter::new(io::stdout().lock());
    let fill_value = args.fill.clone().unwrap_or_else(String::new);

    if args.no_header {
        let mut rows = Vec::new();
        let mut expected_width: Option<usize> = None;
        let mut row_number = 0usize;
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            row_number += 1;
            if let Some(width) = expected_width {
                if record.len() != width {
                    if input_opts.ignore_illegal {
                        continue;
                    } else {
                        return Err(inconsistent_width_error(
                            &source_name,
                            row_number,
                            width,
                            record.len(),
                        ));
                    }
                }
            }
            if should_skip_record(&record, &input_opts, expected_width) {
                continue;
            }
            if expected_width.is_none() {
                expected_width = Some(record.len());
            }
            rows.push(record);
        }
        let headers = default_headers(expected_width.unwrap_or(0));
        emit_melt(&headers, rows, &args, &fill_value, &mut writer)?;
        writer.flush()?;
        return Ok(());
    }

    let headers = reader
        .headers()
        .with_context(|| format!("failed reading header from {:?}", args.file))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut row_number = 0usize;
    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
        row_number += 1;
        if record.len() != headers.len() {
            if input_opts.ignore_illegal {
                continue;
            } else {
                return Err(inconsistent_width_error(
                    &source_name,
                    row_number + 1,
                    headers.len(),
                    record.len(),
                ));
            }
        }
        if should_skip_record(&record, &input_opts, Some(headers.len())) {
            continue;
        }
        rows.push(record);
    }

    emit_melt(&headers, rows, &args, &fill_value, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn emit_melt(
    headers: &[String],
    rows: Vec<csv::StringRecord>,
    args: &MeltArgs,
    fill_value: &str,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    if rows.is_empty() {
        if !args.no_header {
            let mut header = Vec::new();
            if let Some(id_cols) = &args.id_cols {
                let selectors = parse_selector_list(id_cols)?;
                let indices = resolve_selectors(headers, &selectors, args.no_header)?;
                header.extend(indices.into_iter().map(|idx| headers[idx].clone()));
            }
            header.push(args.variable_name.clone());
            header.push(args.value_name.clone());
            writeln!(writer, "{}", header.join("\t"))?;
        }
        return Ok(());
    }

    let id_indices = if let Some(spec) = &args.id_cols {
        let selectors = parse_selector_list(spec)?;
        resolve_selectors(headers, &selectors, args.no_header)?
    } else {
        Vec::new()
    };

    let mut id_set: IndexSet<usize> = IndexSet::new();
    for idx in &id_indices {
        id_set.insert(*idx);
    }

    let value_indices = if let Some(spec) = &args.value_cols {
        let selectors = parse_selector_list(spec)?;
        resolve_selectors(headers, &selectors, args.no_header)?
    } else {
        headers
            .iter()
            .enumerate()
            .filter_map(|(idx, _)| (!id_set.contains(&idx)).then_some(idx))
            .collect::<Vec<_>>()
    };

    if value_indices.is_empty() {
        bail!("no value columns selected for melt");
    }

    let mut value_set: IndexSet<usize> = IndexSet::new();
    for idx in &value_indices {
        if !value_set.insert(*idx) {
            bail!("duplicate column index {} in value selection", idx + 1);
        }
        if id_set.contains(idx) {
            bail!("column {} appears in both id and value selections", idx + 1);
        }
    }

    let mut header = Vec::new();
    if !args.no_header {
        for &idx in &id_indices {
            header.push(headers[idx].clone());
        }
        header.push(args.variable_name.clone());
        header.push(args.value_name.clone());
        writeln!(writer, "{}", header.join("\t"))?;
    }

    let default_headers = if args.no_header {
        default_headers(headers.len())
    } else {
        headers.to_vec()
    };

    for record in rows {
        let mut id_values = Vec::with_capacity(id_indices.len());
        for &idx in &id_indices {
            let raw = record.get(idx).unwrap_or("");
            id_values.push(if raw.is_empty() {
                fill_value.to_string()
            } else {
                raw.to_string()
            });
        }
        for &value_idx in &value_indices {
            let mut row = id_values.clone();
            let variable = default_headers.get(value_idx).cloned().unwrap_or_default();
            row.push(variable);
            let raw = record.get(value_idx).unwrap_or("");
            row.push(if raw.is_empty() {
                fill_value.to_string()
            } else {
                raw.to_string()
            });
            writeln!(writer, "{}", row.join("\t"))?;
        }
    }

    Ok(())
}
