use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::aggregate::parse_float;
use crate::common::{InputOptions, reader_for_path, should_skip_record};

#[derive(Args, Debug)]
#[command(
    about = "Render TSV data in a pretty table",
    long_about = "Format TSV rows into an aligned, boxed table for quick inspection. Reads from files or stdin, keeps headers by default, and supports width control.\n\nExample:\n  tsvkit pretty examples/profiles.tsv"
)]
pub struct PrettyArgs {
    /// Input TSV file (use '-' for stdin)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Treat input as having no header row
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

    /// Round floating-point values to N decimal places (enables scientific notation for tiny magnitudes)
    #[arg(short = 'r', long = "round", value_name = "DIGITS")]
    pub round: Option<u32>,
}

pub fn run(args: PrettyArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;

    let header = if args.no_header {
        None
    } else {
        Some(
            reader
                .headers()
                .with_context(|| format!("failed reading header from {:?}", args.file))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
    };

    let mut rows = Vec::new();
    let mut reference_width = header.as_ref().map(|h| h.len());
    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
        if let Some(width) = reference_width {
            if record.len() != width {
                if input_opts.ignore_illegal {
                    continue;
                } else {
                    bail!("rows in {:?} have inconsistent column counts", args.file);
                }
            }
        }
        if should_skip_record(&record, &input_opts, reference_width) {
            continue;
        }
        if reference_width.is_none() {
            reference_width = Some(record.len());
        }
        let rounded = record
            .iter()
            .map(|s| maybe_round_value(s, args.round))
            .collect::<Vec<_>>();
        rows.push(rounded);
    }

    render_table(header, rows)
}

fn maybe_round_value(value: &str, digits: Option<u32>) -> String {
    let Some(places) = digits else {
        return value.to_string();
    };
    let Some(number) = parse_float(value) else {
        return value.to_string();
    };
    if !number.is_finite() {
        return value.to_string();
    }
    let exponent = (places as i32).max(4);
    let threshold = 10f64.powi(-exponent);
    if number != 0.0 && number.abs() < threshold {
        format_with_scientific(number, places)
    } else {
        format_with_precision(number, places)
    }
}

fn format_with_precision(number: f64, places: u32) -> String {
    let precision = places as usize;
    let formatted = format!("{number:.precision$}");
    trim_trailing_zeros(formatted)
}

fn format_with_scientific(number: f64, places: u32) -> String {
    let precision = places as usize;
    let formatted = format!("{number:.precision$e}");
    trim_trailing_zeros(formatted)
}

fn trim_trailing_zeros(value: String) -> String {
    if let Some(idx) = value.find(['e', 'E']) {
        let (mantissa, exponent) = value.split_at(idx);
        let cleaned = trim_decimal_suffix(mantissa.to_string());
        return cleaned + exponent;
    }
    trim_decimal_suffix(value)
}

fn trim_decimal_suffix(mut value: String) -> String {
    if value.contains('.') {
        while value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        if value.is_empty() {
            return "0".to_string();
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::maybe_round_value;

    #[test]
    fn rounds_numeric_values_to_requested_precision() {
        let result = maybe_round_value("3.14159", Some(2));
        assert_eq!(result, "3.14");
    }

    #[test]
    fn formats_very_small_numbers_in_scientific_notation() {
        let result = maybe_round_value("0.0000001234", Some(3));
        assert_eq!(result, "1.234e-7");
    }

    #[test]
    fn leaves_text_values_unchanged() {
        let value = maybe_round_value("NA", Some(2));
        assert_eq!(value, "NA");
    }
}

pub fn render_table(header: Option<Vec<String>>, rows: Vec<Vec<String>>) -> Result<()> {
    let mut writer = BufWriter::new(io::stdout().lock());

    let column_count = compute_column_count(&header, &rows);
    if column_count == 0 {
        return Ok(());
    }

    let widths = compute_widths(column_count, header.as_ref(), &rows);

    print_separator(&mut writer, &widths)?;
    if let Some(ref header_row) = header {
        print_row(&mut writer, header_row, &widths)?;
        print_separator(&mut writer, &widths)?;
    }

    for row in &rows {
        print_row(&mut writer, row, &widths)?;
    }
    print_separator(&mut writer, &widths)?;

    writer.flush()?;
    Ok(())
}

fn compute_column_count(header: &Option<Vec<String>>, rows: &[Vec<String>]) -> usize {
    let mut count = header.as_ref().map(|h| h.len()).unwrap_or(0);
    for row in rows {
        if row.len() > count {
            count = row.len();
        }
    }
    count
}

fn compute_widths(count: usize, header: Option<&Vec<String>>, rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths = vec![0usize; count];
    if let Some(header_row) = header {
        for (idx, value) in header_row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }
    widths
}

fn print_separator(writer: &mut BufWriter<io::StdoutLock<'_>>, widths: &[usize]) -> Result<()> {
    write!(writer, "+")?;
    for width in widths {
        let segment = "-".repeat(width + 2);
        write!(writer, "{}+", segment)?;
    }
    writeln!(writer)?;
    Ok(())
}

fn print_row(
    writer: &mut BufWriter<io::StdoutLock<'_>>,
    row: &[String],
    widths: &[usize],
) -> Result<()> {
    write!(writer, "|")?;
    for (idx, width) in widths.iter().enumerate() {
        let value = row.get(idx).map(|s| s.as_str()).unwrap_or("");
        write!(writer, " {:<width$} |", value, width = width)?;
    }
    writeln!(writer)?;
    Ok(())
}
