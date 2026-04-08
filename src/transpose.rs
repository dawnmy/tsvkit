use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{InputOptions, inconsistent_width_error, reader_for_path, should_skip_record};

#[derive(Args, Debug)]
#[command(
    about = "Transpose rows and columns",
    long_about = "Swap table axes so rows become columns and columns become rows. This is handy when samples are rows but downstream tooling expects samples as columns (or vice versa). By default, headers are included as the first row before transposition.",
    after_help = "Examples:
  tsvkit transpose examples/profiles.tsv
  tsvkit transpose -H matrix_no_header.tsv
  tsvkit cut -f 'sample1:sample3' examples/profiles.tsv | tsvkit transpose

Behavior notes:
  Header mode (default): header participates in transpose as row 1.
  No-header mode (-H): all rows are treated as data only.
  Ragged rows are rejected unless -I/--ignore-illegal-row is enabled."
)]
pub struct TransposeArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Treat input as headerless
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Lines starting with this comment character are skipped
    #[arg(short = 'C', long = "comment-char", default_value = "#")]
    pub comment_char: String,

    /// Ignore rows where every field is empty/whitespace
    #[arg(short = 'E', long = "ignore-empty-row")]
    pub ignore_empty_row: bool,

    /// Ignore rows whose column count differs from the header/first row
    #[arg(short = 'I', long = "ignore-illegal-row")]
    pub ignore_illegal_row: bool,
}

pub fn run(args: TransposeArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let source_name = format!("\"{}\"", args.file.display());

    let mut matrix = Vec::<Vec<String>>::new();
    if !args.no_header {
        let header = reader
            .headers()
            .with_context(|| format!("failed reading header from {}", source_name))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        matrix.push(header);
    }

    let mut expected_width = matrix.first().map(|r| r.len());
    let mut row_number = 0usize;
    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {}", source_name))?;
        row_number += 1;
        if should_skip_record(&record, &input_opts, expected_width) {
            continue;
        }
        if let Some(width) = expected_width {
            if record.len() != width {
                if input_opts.ignore_illegal {
                    continue;
                }
                return Err(inconsistent_width_error(
                    &source_name,
                    row_number,
                    width,
                    record.len(),
                ));
            }
        } else {
            expected_width = Some(record.len());
        }
        matrix.push(record.iter().map(|s| s.to_string()).collect());
    }

    let cols = matrix.iter().map(|r| r.len()).max().unwrap_or(0);
    let rows = matrix.len();
    let mut writer = BufWriter::new(io::stdout().lock());
    for c in 0..cols {
        let mut out = Vec::with_capacity(rows);
        for r in 0..rows {
            out.push(matrix[r].get(c).cloned().unwrap_or_default());
        }
        writeln!(writer, "{}", out.join("\t"))?;
    }
    writer.flush()?;
    Ok(())
}
