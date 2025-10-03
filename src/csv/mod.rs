use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Args};

use crate::common::{InputOptions, open_path_reader, should_skip_record};

#[derive(Args, Debug)]
#[command(
    about = "Tools for working with comma-separated values",
    long_about = "Convert comma-separated data to TSV while preserving headers and allowing custom delimiters."
)]
pub struct CsvArgs {
    /// Input CSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", required = true)]
    pub input: PathBuf,

    /// CSV delimiter character (default ',')
    #[arg(long = "delim", value_name = "CHAR", default_value = ",")]
    pub delim: String,

    /// Treat the input CSV as headerless
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

    /// Substitute string for blank fields
    #[arg(long = "na", value_name = "STR")]
    pub na: Option<String>,

    /// Allow stray quotes inside fields (treats embedded quotes literally)
    #[arg(long = "lazy-quotes", action = ArgAction::SetTrue)]
    pub lazy_quotes: bool,
}

pub fn run(args: CsvArgs) -> Result<()> {
    let delimiter = parse_delimiter(&args.delim)?;
    let reader_source = open_path_reader(&args.input)?;

    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(!args.no_header)
        .flexible(true)
        .comment(input_opts.comment)
        .double_quote(!args.lazy_quotes)
        .from_reader(reader_source);

    let mut writer = BufWriter::new(io::stdout().lock());

    let mut expected_width: Option<usize> = None;

    if !args.no_header {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.input))?;
        if !headers.is_empty() {
            writer.write_all(headers.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
            writer.write_all(b"\n")?;
        }
        expected_width = Some(headers.len());
    }

    let na_value = args.na.as_deref();

    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", args.input))?;
        if should_skip_record(&record, &input_opts, expected_width) {
            continue;
        }
        if expected_width.is_none() {
            expected_width = Some(record.len());
        }
        if let Some(na) = na_value {
            let mut fields = Vec::with_capacity(record.len());
            for field in record.iter() {
                if field.is_empty() {
                    fields.push(na);
                } else {
                    fields.push(field);
                }
            }
            writer.write_all(fields.join("\t").as_bytes())?;
        } else {
            writer.write_all(record.iter().collect::<Vec<_>>().join("\t").as_bytes())?;
        }
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    Ok(())
}

fn parse_delimiter(spec: &str) -> Result<u8> {
    if spec.is_empty() {
        bail!("delimiter must not be empty");
    }
    if spec.len() != 1 {
        bail!("delimiter must be a single UTF-8 scalar value");
    }
    Ok(spec.as_bytes()[0])
}
