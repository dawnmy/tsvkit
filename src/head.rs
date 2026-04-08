use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{InputOptions, inconsistent_width_error, should_skip_record};
use crate::pretty::render_table;

#[derive(Args, Debug)]
#[command(about = "Preview first rows in pretty format")]
pub struct HeadArgs {
    #[arg(value_name = "FILES", num_args = 1..)]
    pub files: Vec<PathBuf>,

    #[arg(short = 'n', long = "lines", default_value_t = 10)]
    pub lines: usize,

    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    #[arg(short = 'C', long = "comment-char", default_value = "#")]
    pub comment_char: String,

    #[arg(short = 'E', long = "ignore-empty-row")]
    pub ignore_empty_row: bool,

    #[arg(short = 'I', long = "ignore-illegal-row")]
    pub ignore_illegal_row: bool,
}

pub fn run(args: HeadArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;

    for (idx, file) in args.files.iter().enumerate() {
        if idx > 0 {
            println!();
        }
        println!("# {}", file.display());

        let mut reader = crate::common::reader_for_path(file, args.no_header, &input_opts)?;
        let source_name = format!("\"{}\"", file.display());
        let header = if args.no_header {
            None
        } else {
            Some(
                reader
                    .headers()
                    .with_context(|| format!("failed reading header from {}", source_name))?
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )
        };
        let mut rows = Vec::new();
        let mut reference_width = header.as_ref().map(|h| h.len());
        let header_rows = if header.is_some() { 1 } else { 0 };
        let mut row_number = 0usize;

        for record in reader.records() {
            if rows.len() >= args.lines {
                break;
            }
            let record = record.with_context(|| format!("failed reading from {}", source_name))?;
            row_number += 1;
            if let Some(width) = reference_width {
                if record.len() != width {
                    if input_opts.ignore_illegal {
                        continue;
                    }
                    return Err(inconsistent_width_error(
                        &source_name,
                        row_number + header_rows,
                        width,
                        record.len(),
                    ));
                }
            }
            if should_skip_record(&record, &input_opts, reference_width) {
                continue;
            }
            if reference_width.is_none() {
                reference_width = Some(record.len());
            }
            rows.push(record.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        }

        render_table(header, rows)?;
        io::stdout().flush()?;
    }

    Ok(())
}
