use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use indexmap::IndexSet;

use crate::common::{default_headers, parse_selector_list, reader_for_path, resolve_selectors};

#[derive(Args, Debug)]
pub struct MeltArgs {
    /// Input TSV file (use '-' for stdin)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Identifier columns to keep (comma-separated)
    #[arg(short = 'i', long = "id", value_name = "COLS")]
    pub id_cols: Option<String>,

    /// Columns to unpivot (comma-separated). Defaults to all non-id columns.
    #[arg(short = 'v', long = "value-cols", value_name = "COLS")]
    pub value_cols: Option<String>,

    /// Name for the variable column in the output
    #[arg(long = "variable", value_name = "NAME", default_value = "variable")]
    pub variable_name: String,

    /// Name for the value column in the output
    #[arg(long = "value", value_name = "NAME", default_value = "value")]
    pub value_name: String,

    /// Treat input as having no header row
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,
}

pub fn run(args: MeltArgs) -> Result<()> {
    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first = match records.next() {
            Some(rec) => rec.with_context(|| format!("failed reading from {:?}", args.file))?,
            None => {
                writer.flush()?;
                return Ok(());
            }
        };
        let mut rows = Vec::new();
        rows.push(first.clone());
        for record in records {
            rows.push(record.with_context(|| format!("failed reading from {:?}", args.file))?);
        }
        let headers = default_headers(first.len());
        emit_melt(&headers, rows, &args, &mut writer)?;
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
    for record in reader.records() {
        rows.push(record.with_context(|| format!("failed reading from {:?}", args.file))?);
    }

    emit_melt(&headers, rows, &args, &mut writer)?;
    writer.flush()?;
    Ok(())
}

fn emit_melt(
    headers: &[String],
    rows: Vec<csv::StringRecord>,
    args: &MeltArgs,
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
            id_values.push(record.get(idx).unwrap_or("").to_string());
        }
        for &value_idx in &value_indices {
            let mut row = id_values.clone();
            let variable = default_headers.get(value_idx).cloned().unwrap_or_default();
            row.push(variable);
            row.push(record.get(value_idx).unwrap_or("").to_string());
            writeln!(writer, "{}", row.join("\t"))?;
        }
    }

    Ok(())
}
