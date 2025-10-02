use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{default_headers, parse_selector_list, reader_for_path, resolve_selectors};

#[derive(Args, Debug)]
#[command(
    about = "Select and reorder TSV columns",
    long_about = "Pick columns by name or 1-based index. Combine comma-separated selectors with ranges (colA:colD or 2:6) and single fields in one spec. Defaults to header-aware mode; add -H for headerless input.\n\nExamples:\n  tsvkit cut -f id,sample3,sample1 examples/profiles.tsv\n  tsvkit cut -f 'Purity,sample:FN,F1' examples/profiles.tsv\n  tsvkit cut -H -f 3,1 data.tsv"
)]
pub struct CutArgs {
    /// Input TSV file (use '-' for stdin; supports gz/xz)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Fields to select, using names, 1-based indices, ranges (`colA:colD`, `2:5`), or mixes. Comma-separated list.
    #[arg(short = 'f', long = "fields", value_name = "COLS", required = true)]
    pub fields: String,

    /// Treat the input as headerless (columns referenced by 1-based indices)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,
}

pub fn run(args: CutArgs) -> Result<()> {
    let selectors = parse_selector_list(&args.fields)?;
    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first_record = match records.next() {
            Some(record) => {
                record.with_context(|| format!("failed reading from {:?}", args.file))?
            }
            None => return Ok(()),
        };
        let headers = default_headers(first_record.len());
        let indices = resolve_selectors(&headers, &selectors, true)?;
        emit_record(&first_record, &indices, &mut writer)?;
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            emit_record(&record, &indices, &mut writer)?;
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let indices = resolve_selectors(&headers, &selectors, false)?;

        let header_fields: Vec<&str> = indices
            .iter()
            .map(|&idx| headers.get(idx).map(|s| s.as_str()).unwrap_or(""))
            .collect();
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("\t"))?;
        }

        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            emit_record(&record, &indices, &mut writer)?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn emit_record(
    record: &csv::StringRecord,
    indices: &[usize],
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    let mut fields = Vec::with_capacity(indices.len());
    for &idx in indices {
        fields.push(record.get(idx).unwrap_or(""));
    }
    if !fields.is_empty() {
        writeln!(writer, "{}", fields.join("\t"))?;
    } else {
        writer.write_all(b"\n")?;
    }
    Ok(())
}
