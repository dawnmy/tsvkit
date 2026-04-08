use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{InputOptions, inconsistent_width_error, should_skip_record};

#[derive(Args, Debug)]
#[command(about = "Print first rows from TSV input")]
pub struct HeadArgs {
    #[arg(value_name = "FILES", default_value = "-", num_args = 0..)]
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

    let mut writer = BufWriter::new(io::stdout().lock());
    let show_file_banner = args.files.len() > 1;

    for (idx, file) in args.files.iter().enumerate() {
        if idx > 0 {
            writeln!(writer)?;
        }
        if show_file_banner {
            writeln!(writer, "# {}", file.display())?;
        }

        let mut reader = crate::common::reader_for_path(file, args.no_header, &input_opts)?;
        let source_name = format!("\"{}\"", file.display());
        let mut reference_width = None;
        let mut header_rows = 0usize;
        if !args.no_header {
            let header = reader
                .headers()
                .with_context(|| format!("failed reading header from {}", source_name))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            reference_width = Some(header.len());
            header_rows = 1;
            writeln!(writer, "{}", header.join("\t"))?;
        }
        let mut row_number = 0usize;
        let mut emitted_rows = 0usize;

        for record in reader.records() {
            if emitted_rows >= args.lines {
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
            writeln!(
                writer,
                "{}",
                record.iter().collect::<Vec<_>>().join("\t")
            )?;
            emitted_rows += 1;
        }
    }

    writer.flush()?;
    Ok(())
}
