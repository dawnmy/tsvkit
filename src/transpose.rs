use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{InputOptions, inconsistent_width_error, reader_for_path, should_skip_record};

#[derive(Args, Debug)]
#[command(about = "Transpose rows and columns")]
pub struct TransposeArgs {
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    #[arg(short = 'C', long = "comment-char", default_value = "#")]
    pub comment_char: String,

    #[arg(short = 'E', long = "ignore-empty-row")]
    pub ignore_empty_row: bool,

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
