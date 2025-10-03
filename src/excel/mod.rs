use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use calamine::{self, Data, ExcelDateTime, Range, Reader, Xlsx, open_workbook};
use chrono::{Duration, NaiveTime};
use clap::{ArgAction, ArgGroup, Args, ValueEnum};

use crate::common::open_path_reader;

const DEFAULT_PREVIEW_ROWS: usize = 10;
const DEFAULT_MAX_ROWS_PER_SHEET: usize = 1_048_576;

type CellValue = Data;

#[derive(Args, Debug)]
#[command(
    about = "Interact with Excel workbooks",
    long_about = "Inspect, preview, export, or build Excel workbooks (xlsx).",
    group = ArgGroup::new("mode")
        .args(["sheets", "preview", "dump", "load"])
        .required(true),
    group = ArgGroup::new("value_mode")
        .args(["values", "formulas"])
        .multiple(false)
)]
pub struct ExcelArgs {
    /// List sheet metadata for a workbook
    #[arg(long = "sheets", value_name = "FILE", conflicts_with_all = ["preview", "dump", "load"])]
    pub sheets: Option<PathBuf>,

    /// Preview sheet rows from a workbook
    #[arg(long = "preview", value_name = "FILE", conflicts_with_all = ["sheets", "dump", "load"])]
    pub preview: Option<PathBuf>,

    /// Dump sheet data as TSV to stdout
    #[arg(long = "dump", value_name = "FILE", conflicts_with_all = ["sheets", "preview", "load"])]
    pub dump: Option<PathBuf>,

    /// TSV inputs to load into a new workbook (repeatable)
    #[arg(long = "load", value_name = "TSV", action = ArgAction::Append, conflicts_with_all = ["sheets", "preview", "dump"])]
    pub load: Vec<PathBuf>,

    /// Restrict operations to specific sheets (names or 1-based indices)
    #[arg(short = 's', long = "sheet", value_name = "NAME|INDEX", action = ArgAction::Append)]
    pub sheet: Vec<String>,

    /// Number of rows to preview (default 10)
    #[arg(short = 'n', long = "head", value_name = "N", requires = "preview")]
    pub head: Option<usize>,

    /// Column selection (names, 1-based indices, or Excel letters)
    #[arg(short = 'f', long = "fields", value_name = "SPEC")]
    pub fields: Option<String>,

    /// Row selection (1-based indices and ranges like 1,5:10,100:)
    #[arg(short = 'r', long = "rows", value_name = "ROWS", requires = "dump")]
    pub rows: Option<String>,

    /// Value to use for blanks when exporting or interpreting TSV NA values when loading
    #[arg(long = "na", value_name = "STR")]
    pub na: Option<String>,

    /// Escape tab characters as \t in dump output
    #[arg(long = "escape-tabs", action = ArgAction::SetTrue)]
    pub escape_tabs: bool,

    /// Escape newlines as \n in dump output
    #[arg(long = "escape-newlines", action = ArgAction::SetTrue)]
    pub escape_newlines: bool,

    /// Show evaluated values (default)
    #[arg(long = "values", action = ArgAction::SetTrue)]
    pub values: bool,

    /// Show raw formulas instead of evaluated values
    #[arg(long = "formulas", action = ArgAction::SetTrue)]
    pub formulas: bool,

    /// Control how Excel dates are rendered or written
    #[arg(long = "dates", value_name = "MODE")]
    pub dates: Option<DateMode>,

    /// Output workbook when loading TSV data
    #[arg(short = 'o', long = "out", value_name = "FILE", requires = "load")]
    pub output: Option<PathBuf>,

    /// Treat TSV inputs as headerless when loading
    #[arg(short = 'H', long = "no-header", action = ArgAction::SetTrue)]
    pub no_header: bool,

    /// Type handling when loading TSV data
    #[arg(long = "types", value_name = "MODE", default_value_t = LoadTypeMode::Infer)]
    pub types: LoadTypeMode,

    /// Maximum rows per sheet when loading TSV data (defaults to Excel limit)
    #[arg(long = "max-rows-per-sheet", value_name = "N", requires = "load")]
    pub max_rows_per_sheet: Option<usize>,

    /// Additional positional arguments captured for load mode (supporting `--load file1 file2`)
    #[arg(value_name = "EXTRA", num_args = 0.., trailing_var_arg = true)]
    pub extra: Vec<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum DateMode {
    Raw,
    Excel,
    Iso,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum LoadTypeMode {
    Infer,
    String,
}

impl fmt::Display for LoadTypeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadTypeMode::Infer => write!(f, "infer"),
            LoadTypeMode::String => write!(f, "string"),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ValueMode {
    Values,
    Formulas,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SheetRef {
    index: usize,
    name: String,
}

#[derive(Debug, Clone)]
struct ColumnToken {
    start: String,
    end: Option<String>,
}

#[derive(Debug, Clone)]
struct RowFilter {
    ranges: Vec<(Option<usize>, Option<usize>)>,
}

#[derive(Debug, Clone)]
struct LoadInputSpec {
    path: PathBuf,
    sheet: String,
}

pub fn run(args: ExcelArgs, raw_args: &[OsString]) -> Result<()> {
    if let Some(path) = &args.sheets {
        return list_sheets(path);
    }
    if let Some(path) = &args.preview {
        return preview_workbook(path, &args);
    }
    if let Some(path) = &args.dump {
        return dump_sheet(path, &args);
    }
    run_load(&args, raw_args)
}

fn list_sheets(path: &Path) -> Result<()> {
    let mut workbook: Xlsx<_> = open_workbook(path)
        .with_context(|| format!("failed to open workbook {}", path.display()))?;
    let mut output = BufWriter::new(io::stdout().lock());

    let sheet_names = workbook.sheet_names();
    for (idx, name) in sheet_names.iter().enumerate() {
        let range = workbook
            .worksheet_range(name)
            .map_err(|e| anyhow::anyhow!("failed loading sheet '{}': {}", name, e))?;
        let (rows, cols) = range.get_size();
        let types = infer_column_types(&range);
        writeln!(
            output,
            "{}	{}	rows={}	cols={}	types=[{}]",
            idx + 1,
            name,
            rows,
            cols,
            types.join(",")
        )?;
    }

    output.flush()?;
    Ok(())
}

fn write_headers(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    sheet_name: &str,
    headers: Option<&Vec<String>>,
) -> Result<usize> {
    let mut row_index = 0usize;
    if let Some(headers) = headers {
        if !headers.is_empty() {
            for (col, header) in headers.iter().enumerate() {
                worksheet.write_string(0, col as u16, header).map_err(|e| {
                    anyhow::anyhow!(
                        "failed writing header {} in sheet '{}': {}",
                        display_column(col),
                        sheet_name,
                        e
                    )
                })?;
            }
            row_index = 1;
        }
    }
    Ok(row_index)
}

fn preview_workbook(path: &Path, args: &ExcelArgs) -> Result<()> {
    let mut workbook: Xlsx<_> = open_workbook(path)
        .with_context(|| format!("failed to open workbook {}", path.display()))?;
    let sheets = resolve_sheet_selection(&mut workbook, &args.sheet)?;
    let head = args.head.unwrap_or(DEFAULT_PREVIEW_ROWS);
    let value_mode = if args.formulas {
        ValueMode::Formulas
    } else {
        ValueMode::Values
    };
    let date_mode = args.dates.unwrap_or(DateMode::Iso);
    let mut output = BufWriter::new(io::stdout().lock());

    for (pos, sheet) in sheets.iter().enumerate() {
        writeln!(output, "#{} {}", sheet.index + 1, sheet.name)?;
        let (values, formulas) = load_sheet_ranges(&mut workbook, &sheet.name, value_mode)?;
        emit_preview(&mut output, &values, formulas.as_ref(), head, date_mode)?;
        if pos + 1 < sheets.len() {
            writeln!(output)?;
        }
    }

    output.flush()?;
    Ok(())
}

fn dump_sheet(path: &Path, args: &ExcelArgs) -> Result<()> {
    let mut workbook: Xlsx<_> = open_workbook(path)
        .with_context(|| format!("failed to open workbook {}", path.display()))?;
    let sheet = resolve_single_sheet(&mut workbook, &args.sheet)?;
    let value_mode = if args.formulas {
        ValueMode::Formulas
    } else {
        ValueMode::Values
    };
    let date_mode = args.dates.unwrap_or(DateMode::Iso);

    let column_tokens = args
        .fields
        .as_deref()
        .map(parse_column_tokens)
        .transpose()?;
    let row_filter = args.rows.as_deref().map(parse_row_filter).transpose()?;

    let (values, formulas) = load_sheet_ranges(&mut workbook, &sheet.name, value_mode)?;
    let headers = extract_headers(&values);
    let column_indices = column_tokens
        .map(|tokens| resolve_columns(&tokens, &headers, &values))
        .transpose()?;

    let na_value = args.na.clone().unwrap_or_default();
    let mut output = BufWriter::new(io::stdout().lock());
    emit_table(
        &mut output,
        &values,
        formulas.as_ref(),
        column_indices.as_ref(),
        row_filter.as_ref(),
        &na_value,
        date_mode,
        args.escape_tabs,
        args.escape_newlines,
    )?;
    output.flush()?;
    Ok(())
}

fn run_load(args: &ExcelArgs, raw_args: &[OsString]) -> Result<()> {
    if args.load.is_empty() && args.extra.is_empty() {
        bail!("--load requires at least one TSV input");
    }
    let inputs = parse_load_inputs(args, raw_args)?;
    if inputs.is_empty() {
        bail!("provide at least one TSV input after --load");
    }
    let output_path = args
        .output
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("--out is required when using --load"))?;
    perform_load(args, &inputs, output_path)
}

fn resolve_sheet_selection(
    workbook: &mut Xlsx<BufReader<std::fs::File>>,
    selectors: &[String],
) -> Result<Vec<SheetRef>> {
    let names = workbook.sheet_names();
    if names.is_empty() {
        bail!("workbook contains no sheets");
    }
    if selectors.is_empty() {
        return Ok(names
            .into_iter()
            .enumerate()
            .map(|(index, name)| SheetRef { index, name })
            .collect());
    }
    selectors
        .iter()
        .map(|sel| resolve_sheet_selector(&names, sel))
        .collect()
}

fn resolve_single_sheet(
    workbook: &mut Xlsx<BufReader<std::fs::File>>,
    selectors: &[String],
) -> Result<SheetRef> {
    let names = workbook.sheet_names();
    if names.is_empty() {
        bail!("workbook contains no sheets");
    }
    if selectors.len() > 1 {
        bail!("specify at most one sheet for this operation");
    }
    if let Some(selector) = selectors.first() {
        resolve_sheet_selector(&names, selector)
    } else {
        Ok(SheetRef {
            index: 0,
            name: names[0].clone(),
        })
    }
}

fn resolve_sheet_selector(names: &[String], selector: &str) -> Result<SheetRef> {
    if let Ok(idx) = selector.parse::<usize>() {
        if idx == 0 {
            bail!("sheet indices are 1-based");
        }
        let index = idx - 1;
        let name = names
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("sheet index {} out of range", idx))?;
        return Ok(SheetRef { index, name });
    }
    if let Some(index) = names.iter().position(|name| name == selector) {
        return Ok(SheetRef {
            index,
            name: selector.to_string(),
        });
    }
    bail!("sheet '{}' not found", selector)
}

fn parse_column_tokens(spec: &str) -> Result<Vec<ColumnToken>> {
    if spec.trim().is_empty() {
        bail!("column specification must not be empty");
    }
    let mut tokens = Vec::new();
    for raw in spec.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once(':') {
            tokens.push(ColumnToken {
                start: start.trim().to_string(),
                end: Some(end.trim().to_string()),
            });
        } else {
            tokens.push(ColumnToken {
                start: token.to_string(),
                end: None,
            });
        }
    }
    if tokens.is_empty() {
        bail!("column specification must select at least one column");
    }
    Ok(tokens)
}

fn parse_row_filter(spec: &str) -> Result<RowFilter> {
    if spec.trim().is_empty() {
        bail!("row specification must not be empty");
    }
    let mut ranges = Vec::new();
    for raw in spec.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once(':') {
            let start_idx = if start.trim().is_empty() {
                None
            } else {
                Some(parse_positive_index(start.trim())?)
            };
            let end_idx = if end.trim().is_empty() {
                None
            } else {
                Some(parse_positive_index(end.trim())?)
            };
            if let (Some(s), Some(e)) = (start_idx, end_idx) {
                if s > e {
                    bail!("row range start {} exceeds end {}", s, e);
                }
            }
            ranges.push((start_idx, end_idx));
        } else {
            let value = parse_positive_index(token)?;
            ranges.push((Some(value), Some(value)));
        }
    }
    if ranges.is_empty() {
        bail!("row specification must not be empty");
    }
    Ok(RowFilter { ranges })
}

fn parse_positive_index(token: &str) -> Result<usize> {
    let value: usize = token
        .parse()
        .with_context(|| format!("invalid 1-based index '{}'", token))?;
    if value == 0 {
        bail!("indices are 1-based");
    }
    Ok(value)
}

fn resolve_columns(
    tokens: &[ColumnToken],
    headers: &[String],
    values: &Range<CellValue>,
) -> Result<Vec<usize>> {
    let mut indices = BTreeSet::new();
    let sheet_width = max_sheet_width(values);
    for token in tokens {
        let start = resolve_single_column(&token.start, headers, sheet_width)?;
        if let Some(end_token) = &token.end {
            let end = resolve_single_column(end_token, headers, sheet_width)?;
            if start > end {
                bail!(
                    "column range start {} exceeds end {}",
                    display_column(start),
                    display_column(end)
                );
            }
            for idx in start..=end {
                indices.insert(idx);
            }
        } else {
            indices.insert(start);
        }
    }
    Ok(indices.into_iter().collect())
}

fn resolve_single_column(token: &str, headers: &[String], sheet_width: usize) -> Result<usize> {
    if let Ok(idx) = token.parse::<usize>() {
        if idx == 0 {
            bail!("column indices are 1-based");
        }
        return Ok(idx - 1);
    }
    if let Some(index) = headers.iter().position(|h| h == token) {
        return Ok(index);
    }
    if let Some(idx) = parse_excel_column_letters(token) {
        if idx >= sheet_width {
            bail!(
                "column {} out of range (sheet width {})",
                display_column(idx),
                sheet_width
            );
        }
        return Ok(idx);
    }
    bail!("column '{}' not found", token)
}

fn parse_excel_column_letters(token: &str) -> Option<usize> {
    if token.is_empty() || !token.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    let mut value = 0usize;
    for ch in token.chars() {
        let letter = ch.to_ascii_uppercase();
        let byte = letter as u8;
        if !(b'A'..=b'Z').contains(&byte) {
            return None;
        }
        value = value * 26 + ((byte - b'A') as usize + 1);
    }
    Some(value - 1)
}

fn display_column(idx: usize) -> String {
    let mut n = idx + 1;
    let mut letters = Vec::new();
    while n > 0 {
        n -= 1;
        letters.push((b'A' + (n % 26) as u8) as char);
        n /= 26;
    }
    letters.iter().rev().collect()
}
fn load_sheet_ranges(
    workbook: &mut Xlsx<BufReader<std::fs::File>>,
    sheet: &str,
    mode: ValueMode,
) -> Result<(Range<CellValue>, Option<Range<String>>)> {
    let values = workbook
        .worksheet_range(sheet)
        .map_err(|e| anyhow::anyhow!("failed loading sheet '{}': {}", sheet, e))?;
    let formulas = if mode == ValueMode::Formulas {
        Some(
            workbook
                .worksheet_formula(sheet)
                .map_err(|e| anyhow::anyhow!("failed reading formulas from '{}': {}", sheet, e))?,
        )
    } else {
        None
    };
    Ok((values, formulas))
}

fn emit_preview(
    writer: &mut BufWriter<io::StdoutLock>,
    values: &Range<CellValue>,
    formulas: Option<&Range<String>>,
    head: usize,
    date_mode: DateMode,
) -> Result<()> {
    let (height, width) = values.get_size();
    let start = values.start().unwrap_or((0, 0));
    let mut emitted = 0usize;
    let limit = height.min(head + 1); // header + head rows

    for row_idx in 0..limit {
        let absolute_row = start.0 as usize + row_idx;
        let mut row_cells = Vec::with_capacity(width);
        for col_idx in 0..width {
            let absolute_col = start.1 as usize + col_idx;
            let cell = values.get((row_idx, col_idx));
            let formula =
                formulas.and_then(|r| r.get_value((absolute_row as u32, absolute_col as u32)));
            row_cells.push(render_cell(cell, formula, date_mode));
        }
        writer.write_all(row_cells.join("\t").as_bytes())?;
        writer.write_all(b"\n")?;
        emitted += 1;
    }

    if emitted == 0 {
        writer.write_all(b"<empty sheet>\n")?;
    }

    Ok(())
}

fn emit_table(
    writer: &mut BufWriter<io::StdoutLock>,
    values: &Range<CellValue>,
    formulas: Option<&Range<String>>,
    columns: Option<&Vec<usize>>,
    row_filter: Option<&RowFilter>,
    na_value: &str,
    date_mode: DateMode,
    escape_tabs: bool,
    escape_newlines: bool,
) -> Result<()> {
    let (height, width) = values.get_size();
    let start = values.start().unwrap_or((0, 0));
    let selected_columns: Vec<usize> = if let Some(cols) = columns {
        cols.clone()
    } else {
        (0..width).collect()
    };

    let total_rows = height;
    for row_idx in 0..height {
        let absolute_row = start.0 as usize + row_idx;
        let logical_row = absolute_row + 1; // 1-based
        if let Some(filter) = row_filter {
            if !row_filter_contains(filter, logical_row, total_rows) {
                continue;
            }
        }

        let mut row_cells = Vec::with_capacity(selected_columns.len());
        for &col_idx in &selected_columns {
            let absolute_col = start.1 as usize + col_idx;
            let cell = values.get((row_idx, col_idx));
            let formula =
                formulas.and_then(|r| r.get_value((absolute_row as u32, absolute_col as u32)));
            let rendered = render_cell(cell, formula, date_mode);
            let text = if rendered.is_empty() {
                na_value
            } else {
                &rendered
            };
            row_cells.push(escape_text(text, escape_tabs, escape_newlines));
        }
        writer.write_all(row_cells.join("\t").as_bytes())?;
        writer.write_all(b"\n")?;
    }

    Ok(())
}

fn row_filter_contains(filter: &RowFilter, row: usize, total_rows: usize) -> bool {
    for (start, end) in &filter.ranges {
        let start_idx = start.unwrap_or(1);
        let end_idx = end.unwrap_or(total_rows);
        if row >= start_idx && row <= end_idx {
            return true;
        }
    }
    false
}
fn render_cell(value: Option<&CellValue>, formula: Option<&String>, date_mode: DateMode) -> String {
    if let Some(formula) = formula {
        return format!("={}", formula);
    }
    match value {
        None | Some(CellValue::Empty) => String::new(),
        Some(CellValue::String(s)) => s.clone(),
        Some(CellValue::Float(f)) => format_float(*f),
        Some(CellValue::Int(i)) => i.to_string(),
        Some(CellValue::Bool(b)) => b.to_string(),
        Some(CellValue::DateTime(dt)) => render_datetime(dt, date_mode),
        Some(CellValue::DateTimeIso(s)) => s.clone(),
        Some(CellValue::DurationIso(s)) => s.clone(),
        Some(CellValue::Error(err)) => format!("#ERROR:{:?}", err),
    }
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{}", value)
    }
}

fn render_datetime(dt: &ExcelDateTime, mode: DateMode) -> String {
    if dt.is_duration() {
        return render_duration(dt, mode);
    }
    match mode {
        DateMode::Excel => format_float(dt.as_f64()),
        DateMode::Raw => format!("DateTime({})", dt.as_f64()),
        DateMode::Iso => {
            if let Some(datetime) = dt.as_datetime() {
                if datetime.time() == NaiveTime::MIN {
                    datetime.date().to_string()
                } else {
                    datetime.to_string()
                }
            } else {
                format_float(dt.as_f64())
            }
        }
    }
}

fn render_duration(dt: &ExcelDateTime, mode: DateMode) -> String {
    match mode {
        DateMode::Excel => format_float(dt.as_f64()),
        DateMode::Raw => format!("Duration({})", dt.as_f64()),
        DateMode::Iso => {
            if let Some(duration) = dt.as_duration() {
                format_duration(duration)
            } else {
                format_float(dt.as_f64())
            }
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let total_millis = duration.num_milliseconds();
    let sign = if total_millis < 0 { "-" } else { "" };
    let mut millis = total_millis.abs();
    let hours = millis / 3_600_000;
    millis %= 3_600_000;
    let minutes = millis / 60_000;
    millis %= 60_000;
    let seconds = millis / 1_000;
    let remainder = millis % 1_000;
    if remainder > 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{remainder:03}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn escape_text(value: &str, escape_tabs: bool, escape_newlines: bool) -> String {
    let mut result = value.to_string();
    if escape_tabs {
        result = result.replace('\t', "\\t");
    }
    if escape_newlines {
        result = result.replace('\r', "\\r").replace('\n', "\\n");
    }
    result
}
fn infer_column_types(range: &Range<CellValue>) -> Vec<String> {
    let (_, width) = range.get_size();
    if width == 0 {
        return Vec::new();
    }
    let mut kinds = vec![ColumnKind::Unknown; width];
    for row in range.rows() {
        for (idx, cell) in row.iter().enumerate() {
            let cell_kind = classify_cell(cell);
            if cell_kind == CellType::Empty {
                continue;
            }
            kinds[idx].update(cell_kind);
        }
    }
    kinds.into_iter().map(|k| k.label()).collect()
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CellType {
    Empty,
    Int,
    Float,
    Bool,
    Date,
    Duration,
    String,
    Error,
}

fn classify_cell(cell: &CellValue) -> CellType {
    match cell {
        CellValue::Empty => CellType::Empty,
        CellValue::Int(_) => CellType::Int,
        CellValue::Float(_) => CellType::Float,
        CellValue::Bool(_) => CellType::Bool,
        CellValue::DateTime(dt) => {
            if dt.is_duration() {
                CellType::Duration
            } else {
                CellType::Date
            }
        }
        CellValue::DateTimeIso(_) => CellType::Date,
        CellValue::DurationIso(_) => CellType::Duration,
        CellValue::String(_) => CellType::String,
        CellValue::Error(_) => CellType::Error,
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ColumnKind {
    Unknown,
    Int,
    Float,
    Bool,
    Date,
    Duration,
    String,
    Error,
    Mixed,
}

impl ColumnKind {
    fn update(&mut self, cell: CellType) {
        use ColumnKind::*;
        match cell {
            CellType::Empty => {}
            CellType::Int => match self {
                Unknown => *self = Int,
                Int | Float => {}
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::Float => match self {
                Unknown | Int | Float => *self = Float,
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::Bool => match self {
                Unknown | Bool => *self = Bool,
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::Date => match self {
                Unknown | Date => *self = Date,
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::Duration => match self {
                Unknown | Duration => *self = Duration,
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::String => match self {
                Unknown | String => *self = String,
                Mixed => {}
                _ => *self = Mixed,
            },
            CellType::Error => match self {
                Unknown | Error => *self = Error,
                Mixed => {}
                _ => *self = Mixed,
            },
        }
    }

    fn label(self) -> String {
        match self {
            ColumnKind::Unknown => "empty".to_string(),
            ColumnKind::Int => "int".to_string(),
            ColumnKind::Float => "float".to_string(),
            ColumnKind::Bool => "bool".to_string(),
            ColumnKind::Date => "date".to_string(),
            ColumnKind::Duration => "duration".to_string(),
            ColumnKind::String => "string".to_string(),
            ColumnKind::Error => "error".to_string(),
            ColumnKind::Mixed => "mixed".to_string(),
        }
    }
}

fn extract_headers(range: &Range<CellValue>) -> Vec<String> {
    if range.height() == 0 {
        return Vec::new();
    }
    range
        .rows()
        .next()
        .map(|row| {
            row.iter()
                .map(|cell| render_cell(Some(cell), None, DateMode::Iso))
                .collect()
        })
        .unwrap_or_default()
}

fn max_sheet_width(range: &Range<CellValue>) -> usize {
    range.start().map(|(_, col)| col as usize).unwrap_or(0) + range.width()
}
fn parse_load_inputs(args: &ExcelArgs, raw_args: &[OsString]) -> Result<Vec<LoadInputSpec>> {
    let mut loads: Vec<(PathBuf, Option<String>)> = Vec::new();
    let mut tokens = raw_excel_tokens(raw_args)?.into_iter().peekable();

    while let Some(token) = tokens.next() {
        if let Some(value) = token.strip_prefix("--load=") {
            loads.push((PathBuf::from(value), None));
            continue;
        }
        if let Some(value) = token.strip_prefix("--sheet=") {
            assign_sheet_name(&mut loads, value.to_string())?;
            continue;
        }

        match token.as_str() {
            "--load" => {
                let path = tokens
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--load expects a TSV path"))?;
                loads.push((PathBuf::from(path), None));
            }
            "-s" | "--sheet" => {
                let name = tokens
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("-s/--sheet expects a sheet name"))?;
                assign_sheet_name(&mut loads, name)?;
            }
            "-o"
            | "--out"
            | "--dates"
            | "--types"
            | "--na"
            | "--na-load"
            | "--max-rows-per-sheet"
            | "--head" => {
                if let Some(peek) = tokens.peek() {
                    if !peek.starts_with('-') || *peek == "-" {
                        tokens.next();
                    }
                }
            }
            other if other.starts_with('-') => {
                // other flags handled elsewhere; ignore
            }
            other => {
                loads.push((PathBuf::from(other), None));
            }
        }
    }

    if loads.is_empty() {
        loads.extend(args.load.iter().cloned().map(|p| (p, None)));
        loads.extend(args.extra.iter().cloned().map(|p| (p, None)));
    }

    if loads.is_empty() {
        return Ok(Vec::new());
    }

    let specs = loads
        .into_iter()
        .enumerate()
        .map(|(idx, (path, name))| LoadInputSpec {
            path,
            sheet: name.unwrap_or_else(|| (idx + 1).to_string()),
        })
        .collect();

    Ok(specs)
}

fn assign_sheet_name(loads: &mut Vec<(PathBuf, Option<String>)>, name: String) -> Result<()> {
    if let Some((_, slot)) = loads.iter_mut().rev().find(|(_, sheet)| sheet.is_none()) {
        *slot = Some(name);
        Ok(())
    } else {
        bail!("-s/--sheet must follow a --load <TSV> argument");
    }
}

fn raw_excel_tokens(raw_args: &[OsString]) -> Result<Vec<String>> {
    if raw_args.is_empty() {
        return Ok(Vec::new());
    }
    let mut iter = raw_args.iter();
    iter.next(); // executable
    while let Some(arg) = iter.next() {
        if arg == "excel" {
            break;
        }
    }
    Ok(iter.map(|s| s.to_string_lossy().to_string()).collect())
}
fn perform_load(args: &ExcelArgs, inputs: &[LoadInputSpec], output: &Path) -> Result<()> {
    use rust_xlsxwriter::Workbook;

    if inputs.is_empty() {
        bail!("provide at least one TSV input to load");
    }

    let mut workbook = Workbook::new();
    let na_token = args.na.clone().unwrap_or_default();
    let max_rows = args
        .max_rows_per_sheet
        .unwrap_or(DEFAULT_MAX_ROWS_PER_SHEET);
    if max_rows == 0 {
        bail!("--max-rows-per-sheet must be greater than zero");
    }

    for spec in inputs {
        process_input(&mut workbook, spec, args, max_rows, &na_token)?;
    }

    workbook
        .save(output)
        .with_context(|| format!("failed writing workbook to {}", output.display()))?;
    Ok(())
}

fn process_input(
    workbook: &mut rust_xlsxwriter::Workbook,
    spec: &LoadInputSpec,
    args: &ExcelArgs,
    max_rows: usize,
    na_token: &str,
) -> Result<()> {
    let reader_handle = open_path_reader(&spec.path)
        .with_context(|| format!("failed to open TSV input {}", spec.path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(!args.no_header)
        .flexible(true)
        .from_reader(reader_handle);

    let headers = if args.no_header {
        args.fields.as_deref().map(parse_header_names).transpose()?
    } else {
        let record = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", spec.path))?;
        Some(record.iter().map(|s| s.to_string()).collect())
    };

    let date_mode = args.dates.unwrap_or(DateMode::Excel);
    let mut part = 1usize;
    let mut sheet_name = spec.sheet.clone();
    let mut worksheet = workbook.add_worksheet();
    worksheet
        .set_name(&sheet_name)
        .map_err(|e| anyhow::anyhow!("failed renaming worksheet to '{}': {}", sheet_name, e))?;
    let mut row_index = write_headers(worksheet, &sheet_name, headers.as_ref())?;

    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", spec.path))?;
        if row_index >= max_rows {
            part += 1;
            sheet_name = format!("{} ({})", spec.sheet, part);
            worksheet = workbook.add_worksheet();
            worksheet.set_name(&sheet_name).map_err(|e| {
                anyhow::anyhow!("failed renaming worksheet to '{}': {}", sheet_name, e)
            })?;
            row_index = write_headers(worksheet, &sheet_name, headers.as_ref())?;
        }

        write_record(
            worksheet,
            &sheet_name,
            row_index,
            &record,
            args,
            date_mode,
            na_token,
        )?;
        row_index += 1;
    }

    Ok(())
}

fn write_record(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    sheet_name: &str,
    row_index: usize,
    record: &csv::StringRecord,
    args: &ExcelArgs,
    date_mode: DateMode,
    na_token: &str,
) -> Result<()> {
    use rust_xlsxwriter::{ExcelDateTime, Format};

    for (col_idx, raw) in record.iter().enumerate() {
        let value = raw.trim_end_matches('\r');
        if value.is_empty() {
            continue;
        }
        if !na_token.is_empty() && value == na_token {
            continue;
        }

        let row = row_index as u32;
        let col = col_idx as u16;

        if args.types == LoadTypeMode::String {
            worksheet
                .write_string(row, col, value)
                .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
            continue;
        }

        if date_mode == DateMode::Excel {
            if let Some(kind) = detect_date_kind(value) {
                if let Ok(dt) = ExcelDateTime::parse_from_str(value) {
                    let format = match kind {
                        DateKind::Date => Format::new().set_num_format("yyyy-mm-dd"),
                        DateKind::DateTime => Format::new().set_num_format("yyyy-mm-dd hh:mm:ss"),
                        DateKind::Time => Format::new().set_num_format("hh:mm:ss"),
                    };
                    worksheet
                        .write_datetime_with_format(row, col, &dt, &format)
                        .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
                    continue;
                }
            }
        }

        if let Some(boolean) = parse_bool(value) {
            worksheet
                .write_boolean(row, col, boolean)
                .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
            continue;
        }

        if let Some(int_value) = parse_int(value) {
            worksheet
                .write_number(row, col, int_value as f64)
                .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
            continue;
        }

        if let Some(float_value) = parse_float(value) {
            worksheet
                .write_number(row, col, float_value)
                .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
            continue;
        }

        worksheet
            .write_string(row, col, value)
            .map_err(|e| cell_error(sheet_name, row_index, col_idx, e))?;
    }

    Ok(())
}

fn cell_error(
    sheet: &str,
    row: usize,
    col: usize,
    err: rust_xlsxwriter::XlsxError,
) -> anyhow::Error {
    anyhow::anyhow!(
        "failed writing cell {}{} on '{}': {}",
        display_column(col),
        row + 1,
        sheet,
        err
    )
}

#[derive(Copy, Clone, Debug)]
enum DateKind {
    Date,
    DateTime,
    Time,
}

fn detect_date_kind(value: &str) -> Option<DateKind> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() == 10
        && trimmed.chars().nth(4) == Some('-')
        && trimmed.chars().nth(7) == Some('-')
    {
        if trimmed.chars().all(|c| c.is_ascii_digit() || c == '-') {
            return Some(DateKind::Date);
        }
    }
    if trimmed.contains('T') || trimmed.contains(' ') {
        // split on space or T
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() == 2 && looks_like_date(parts[0]) && looks_like_time(parts[1]) {
            return Some(DateKind::DateTime);
        }
        if trimmed.contains('T') {
            let mut pieces = trimmed.split('T');
            if let (Some(date), Some(time)) = (pieces.next(), pieces.next()) {
                if looks_like_date(date) && looks_like_time(time) {
                    return Some(DateKind::DateTime);
                }
            }
        }
    }
    if looks_like_time(trimmed) {
        return Some(DateKind::Time);
    }
    None
}

fn looks_like_date(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }
    value.chars().enumerate().all(|(idx, ch)| match idx {
        4 | 7 => ch == '-',
        _ => ch.is_ascii_digit(),
    })
}

fn looks_like_time(value: &str) -> bool {
    let mut parts = value.split(':');
    let hour = parts.next();
    let minute = parts.next();
    let second = parts.next();
    if let (Some(h), Some(m)) = (hour, minute) {
        if !h.chars().all(|c| c.is_ascii_digit()) || !m.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        if let Some(sec) = second {
            let sec = sec.trim_end_matches(|c| c == 'Z' || c == 'z');
            if sec.is_empty() {
                return true;
            }
            return sec.chars().all(|c| c.is_ascii_digit() || c == '.');
        }
        return true;
    }
    false
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_int(value: &str) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    let digits = value.trim_start_matches(['+', '-']);
    if digits.len() > 1
        && digits.starts_with('0')
        && !digits.starts_with("0.")
        && !digits.starts_with("0,")
    {
        return None;
    }
    value.parse::<i64>().ok()
}

fn parse_float(value: &str) -> Option<f64> {
    if !(value.contains('.') || value.contains('e') || value.contains('E')) {
        return None;
    }
    value.parse::<f64>().ok()
}

fn parse_header_names(spec: &str) -> Result<Vec<String>> {
    if spec.contains("..") {
        let parts: Vec<&str> = spec.split("..").collect();
        if parts.len() != 2 {
            bail!("invalid header range '{}'", spec);
        }
        let (start_prefix, start_num) = split_prefix_number(parts[0])?;
        let (end_prefix, end_num) = split_prefix_number(parts[1])?;
        if start_prefix != end_prefix {
            bail!("header range '{}' must share the same prefix", spec);
        }
        if start_num > end_num {
            bail!("header range start {} exceeds end {}", start_num, end_num);
        }
        return Ok((start_num..=end_num)
            .map(|n| format!("{}{}", start_prefix, n))
            .collect());
    }
    Ok(spec
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn split_prefix_number(token: &str) -> Result<(String, usize)> {
    let mut prefix = String::new();
    let mut digits = String::new();
    for ch in token.chars() {
        if digits.is_empty() && !ch.is_ascii_digit() {
            prefix.push(ch);
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            bail!("invalid header token '{}': unexpected character", token);
        }
    }
    if digits.is_empty() {
        bail!("header token '{}' lacks numeric suffix", token);
    }
    let number: usize = digits
        .parse()
        .with_context(|| format!("invalid numeric suffix in '{}'", token))?;
    Ok((prefix, number))
}
