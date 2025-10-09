use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    ColumnSelector, InputOptions, SpecialColumn, default_headers, parse_selector_list,
    reader_for_path, resolve_selectors, should_skip_record,
};

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

    /// Rename the injected file column when using `__file__` or `__base__`
    #[arg(long = "file-col", visible_alias = "fc", value_name = "NAME")]
    pub file_col: Option<String>,

    /// Treat the input as headerless (columns referenced by 1-based indices)
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
}

pub fn run(args: CutArgs) -> Result<()> {
    let selectors = parse_selector_list(&args.fields)?;
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    let file_info = FileInfo::from_path(&args.file);
    let file_column_config = FileColumnConfig::new(args.file_col.as_deref());

    if args.no_header {
        let mut records = reader.records();
        let first_record = loop {
            match records.next() {
                Some(record) => {
                    let record =
                        record.with_context(|| format!("failed reading from {:?}", args.file))?;
                    if should_skip_record(&record, &input_opts, None) {
                        continue;
                    }
                    break record;
                }
                None => return Ok(()),
            }
        };
        let expected_width = first_record.len();
        let headers = default_headers(expected_width);
        let columns = build_cut_columns(&headers, &selectors, true)?;
        emit_record(&first_record, &columns, &file_info, &mut writer)?;
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            emit_record(&record, &columns, &file_info, &mut writer)?;
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let columns = build_cut_columns(&headers, &selectors, false)?;
        let expected_width = headers.len();

        let header_fields: Vec<String> = columns
            .iter()
            .map(|column| match column {
                CutColumn::Index(idx) => headers
                    .get(*idx)
                    .map(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                CutColumn::Injected(special) => file_column_config.header_for(*special),
            })
            .collect();
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("\t"))?;
        }

        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            emit_record(&record, &columns, &file_info, &mut writer)?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn emit_record(
    record: &csv::StringRecord,
    columns: &[CutColumn],
    file_info: &FileInfo,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        match column {
            CutColumn::Index(idx) => fields.push(record.get(*idx).unwrap_or("")),
            CutColumn::Injected(special) => fields.push(file_info.value_for(*special)),
        }
    }
    if !fields.is_empty() {
        writeln!(writer, "{}", fields.join("\t"))?;
    } else {
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn build_cut_columns(
    headers: &[String],
    selectors: &[ColumnSelector],
    no_header: bool,
) -> Result<Vec<CutColumn>> {
    let mut columns = Vec::new();
    for selector in selectors {
        match selector {
            ColumnSelector::Special(special) => columns.push(CutColumn::Injected(*special)),
            ColumnSelector::Range(start, end) => {
                if start
                    .as_deref()
                    .map_or(false, |sel| matches!(sel, ColumnSelector::Special(_)))
                    || end
                        .as_deref()
                        .map_or(false, |sel| matches!(sel, ColumnSelector::Special(_)))
                {
                    bail!("special columns cannot be used within a range selector");
                }
                let indices = resolve_selectors(headers, &[selector.clone()], no_header)?;
                columns.extend(indices.into_iter().map(CutColumn::Index));
            }
            _ => {
                let indices = resolve_selectors(headers, &[selector.clone()], no_header)?;
                columns.extend(indices.into_iter().map(CutColumn::Index));
            }
        }
    }
    Ok(columns)
}

#[derive(Clone)]
struct FileInfo {
    path: String,
    base: String,
}

impl FileInfo {
    fn from_path(path: &Path) -> Self {
        if path == Path::new("-") {
            return FileInfo {
                path: "-".to_string(),
                base: "-".to_string(),
            };
        }
        let path_str = path.to_string_lossy().into_owned();
        let base = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| path_str.clone());
        FileInfo {
            path: path_str,
            base,
        }
    }

    fn value_for(&self, special: SpecialColumn) -> &str {
        match special {
            SpecialColumn::FilePath => self.path.as_str(),
            SpecialColumn::FileBase => self.base.as_str(),
        }
    }
}

struct FileColumnConfig<'a> {
    rename: Option<&'a str>,
}

impl<'a> FileColumnConfig<'a> {
    fn new(rename: Option<&'a str>) -> Self {
        FileColumnConfig { rename }
    }

    fn header_for(&self, special: SpecialColumn) -> String {
        match self.rename {
            Some(name) => name.to_string(),
            None => special.default_header().to_string(),
        }
    }
}

enum CutColumn {
    Index(usize),
    Injected(SpecialColumn),
}
