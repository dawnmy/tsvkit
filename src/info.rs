use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Args;

use crate::common::{InputOptions, reader_for_path, should_skip_record};

#[derive(Args, Debug)]
#[command(
    about = "Inspect TSV dimensions, column types, and value previews",
    long_about = r#"Report the table shape and column details for a TSV file (or stdin). The output starts with #shape(rows, cols) followed by a TSV summary listing each column's index, optional name, inferred type (num/date/str), and the first N observed values (default 3). Respects shared options like -H/--no-header, -C/--comment-char, -E/--ignore-empty-row, and -I/--ignore-illegal-row."#
)]
pub struct InfoArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Number of sample values to include in the preview column
    #[arg(short = 'n', long = "preview", value_name = "N", default_value_t = 3)]
    pub preview: usize,

    /// Treat input as headerless (suppress column names in the summary)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Lines starting with this comment character are skipped
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

pub fn run(args: InfoArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    let preview_limit = args.preview;
    let mut column_names: Vec<String> = Vec::new();
    let mut summaries: Vec<ColumnSummary> = Vec::new();
    let mut row_count = 0usize;
    let mut expected_width: Option<usize> = None;

    if !args.no_header {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        expected_width = if headers.is_empty() {
            None
        } else {
            Some(headers.len())
        };
        column_names = headers;
        summaries = (0..column_names.len())
            .map(|_| ColumnSummary::new(preview_limit))
            .collect();
    }

    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
        if should_skip_record(&record, &input_opts, expected_width) {
            continue;
        }

        if record.len() > column_names.len() {
            extend_columns(
                record.len(),
                &mut column_names,
                &mut summaries,
                preview_limit,
            );
        }

        if expected_width.is_none() {
            expected_width = Some(record.len());
        }

        absorb_record(&record, &mut summaries);
        row_count += 1;
    }

    let column_count = summaries.len();
    writeln!(writer, "#shape({}, {})", row_count, column_count)?;

    let mut header_fields = vec!["index".to_string()];
    if !args.no_header {
        header_fields.push("name".to_string());
    }
    header_fields.push("type".to_string());
    header_fields.push(format!("first{}", preview_limit));
    writeln!(writer, "{}", header_fields.join("\t"))?;

    for (idx, summary) in summaries.iter().enumerate() {
        let mut fields = vec![(idx + 1).to_string()];
        if !args.no_header {
            let name = column_names.get(idx).map(|s| s.as_str()).unwrap_or("");
            fields.push(name.to_string());
        }
        fields.push(summary.type_label().to_string());
        fields.push(summary.preview_string());
        writeln!(writer, "{}", fields.join("\t"))?;
    }

    writer.flush()?;
    Ok(())
}

fn extend_columns(
    target_len: usize,
    column_names: &mut Vec<String>,
    summaries: &mut Vec<ColumnSummary>,
    preview_limit: usize,
) {
    while column_names.len() < target_len {
        let idx = column_names.len();
        column_names.push(format!("col{}", idx + 1));
        summaries.push(ColumnSummary::new(preview_limit));
    }
}

fn absorb_record(record: &csv::StringRecord, summaries: &mut [ColumnSummary]) {
    for (idx, summary) in summaries.iter_mut().enumerate() {
        let value = record.get(idx).unwrap_or("");
        summary.update(value);
    }
}

struct ColumnSummary {
    preview_limit: usize,
    previews: Vec<String>,
    kind: ColumnTypeState,
}

impl ColumnSummary {
    fn new(preview_limit: usize) -> Self {
        ColumnSummary {
            preview_limit,
            previews: Vec::new(),
            kind: ColumnTypeState::Unknown,
        }
    }

    fn update(&mut self, value: &str) {
        if self.previews.len() < self.preview_limit {
            self.previews.push(value.to_string());
        }
        self.kind.observe(value);
    }

    fn type_label(&self) -> &'static str {
        self.kind.label()
    }

    fn preview_string(&self) -> String {
        if self.previews.is_empty() {
            "[]".to_string()
        } else {
            format!("[{}]", self.previews.join(", "))
        }
    }
}

#[derive(Clone, Copy)]
enum ColumnTypeState {
    Unknown,
    Numeric,
    Date,
    Text,
}

enum ValueKind {
    Numeric,
    Date,
    Text,
}

impl ColumnTypeState {
    fn observe(&mut self, raw: &str) {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return;
        }
        let value_kind = if is_numeric(trimmed) {
            ValueKind::Numeric
        } else if is_date(trimmed) {
            ValueKind::Date
        } else {
            ValueKind::Text
        };

        match (*self, value_kind) {
            (ColumnTypeState::Unknown, ValueKind::Numeric) => *self = ColumnTypeState::Numeric,
            (ColumnTypeState::Unknown, ValueKind::Date) => *self = ColumnTypeState::Date,
            (ColumnTypeState::Unknown, ValueKind::Text) => *self = ColumnTypeState::Text,
            (ColumnTypeState::Numeric, ValueKind::Numeric) => {}
            (ColumnTypeState::Date, ValueKind::Date) => {}
            (ColumnTypeState::Text, _) => {}
            _ => *self = ColumnTypeState::Text,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            ColumnTypeState::Unknown => "str",
            ColumnTypeState::Numeric => "num",
            ColumnTypeState::Date => "date",
            ColumnTypeState::Text => "str",
        }
    }
}

fn is_numeric(value: &str) -> bool {
    value.parse::<f64>().is_ok()
}

fn is_date(value: &str) -> bool {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}
