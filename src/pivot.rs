use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use indexmap::{IndexMap, IndexSet};

use crate::common::{
    InputOptions, default_headers, inconsistent_width_error, parse_selector_list,
    parse_single_selector, reader_for_path, resolve_selectors, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Pivot long-form TSV data into wide tables",
    long_about = "Convert long (tidy) TSV data into a wide table by promoting row values to columns. Specify which columns remain as identifiers (-i/--index), which column provides the new headers (-c/--column), and which supplies cell values (-v/--value). Use --fill for missing combinations.\n\nExample:\n  tsvkit melt -i id examples/profiles.tsv | tsvkit pivot -i id -c variable -v value"
)]
pub struct PivotArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Identifier columns to keep (comma-separated names/indices/ranges)
    #[arg(short = 'i', long = "index", value_name = "COLS", required = true)]
    pub index_cols: String,

    /// Column whose values become new column names in the output (single selector). Alias: -f.
    #[arg(
        short = 'c',
        long = "column",
        short_alias = 'f',
        value_name = "COL",
        required = true
    )]
    pub column: String,

    /// Column whose values populate the pivoted cells (single selector)
    #[arg(short = 'v', long = "value", value_name = "COL", required = true)]
    pub value: String,

    /// Fill value to use for missing combinations (defaults to empty string)
    #[arg(long = "fill", value_name = "TEXT")]
    pub fill: Option<String>,

    /// Treat the input as headerless (columns referenced by indices only)
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

pub fn run(args: PivotArgs) -> Result<()> {
    let index_selectors = parse_selector_list(&args.index_cols)?;
    let column_selector = parse_single_selector(&args.column)?;
    let value_selector = parse_single_selector(&args.value)?;

    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;

    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let source_name = format!("\"{}\"", args.file.display());
    let mut writer = BufWriter::new(io::stdout().lock());

    let headers = if args.no_header {
        let mut all_rows = Vec::new();
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
            all_rows.push(record);
        }
        let headers = default_headers(expected_width.unwrap_or(0));
        emit_pivot(
            &headers,
            all_rows,
            &index_selectors,
            &column_selector,
            &value_selector,
            &args,
            &mut writer,
        )?;
        writer.flush()?;
        return Ok(());
    } else {
        reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    };

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

    emit_pivot(
        &headers,
        rows,
        &index_selectors,
        &column_selector,
        &value_selector,
        &args,
        &mut writer,
    )?;

    writer.flush()?;
    Ok(())
}

fn emit_pivot(
    headers: &[String],
    rows: Vec<csv::StringRecord>,
    index_selectors: &[crate::common::ColumnSelector],
    column_selector: &crate::common::ColumnSelector,
    value_selector: &crate::common::ColumnSelector,
    args: &PivotArgs,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    let index_indices = resolve_selectors(headers, index_selectors, args.no_header)?;
    let column_index = resolve_single(headers, column_selector, args.no_header)?;
    let value_index = resolve_single(headers, value_selector, args.no_header)?;

    if rows.is_empty() {
        if !args.no_header {
            let index_headers = index_indices
                .iter()
                .map(|&idx| headers.get(idx).cloned().unwrap_or_default())
                .collect::<Vec<_>>();
            let column_header = headers.get(column_index).cloned().unwrap_or_default();
            let value_header = headers.get(value_index).cloned().unwrap_or_default();
            let mut header_fields = index_headers;
            header_fields.push(column_header);
            header_fields.push(value_header);
            if !header_fields.is_empty() {
                writeln!(writer, "{}", header_fields.join("\t"))?;
            }
        }
        return Ok(());
    }

    let mut index_headers = Vec::new();
    if !args.no_header {
        for &idx in &index_indices {
            index_headers.push(headers.get(idx).cloned().unwrap_or_default());
        }
    }

    let mut output = IndexMap::<Vec<String>, IndexMap<String, String>>::new();
    let mut pivot_columns: IndexSet<String> = IndexSet::new();

    for record in &rows {
        let key = index_indices
            .iter()
            .map(|&idx| record.get(idx).unwrap_or("").to_string())
            .collect::<Vec<_>>();
        let column_name = record.get(column_index).unwrap_or("").to_string();
        let value_text = record.get(value_index).unwrap_or("").to_string();

        pivot_columns.insert(column_name.clone());
        let entry = output.entry(key.clone()).or_insert_with(IndexMap::new);
        if entry.contains_key(&column_name) {
            bail!(
                "duplicate rows for pivot combination {:?} / '{}'",
                key,
                column_name
            );
        }
        entry.insert(column_name, value_text);
    }

    if !args.no_header {
        let mut header_fields = index_headers;
        header_fields.extend(pivot_columns.iter().cloned());
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("\t"))?;
        }
    }

    let fill_value = args.fill.as_deref().unwrap_or("");

    for (key, values) in output {
        let mut row = Vec::with_capacity(key.len() + pivot_columns.len());
        row.extend(key);
        for column in &pivot_columns {
            if let Some(value) = values.get(column) {
                row.push(value.clone());
            } else {
                row.push(fill_value.to_string());
            }
        }
        writeln!(writer, "{}", row.join("\t"))?;
    }

    Ok(())
}

fn resolve_single(
    headers: &[String],
    selector: &crate::common::ColumnSelector,
    no_header: bool,
) -> Result<usize> {
    crate::common::resolve_single_selector(headers, selector.clone(), no_header)
}
