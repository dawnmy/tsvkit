use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    ColumnSelector, FileTemplateContext, InputOptions, SpecialColumn, default_headers,
    parse_selector_list_with_templates, reader_for_path, render_file_template, resolve_selectors,
    resolve_selectors_allow_duplicates, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Select and reorder TSV columns",
    after_help = "Selector syntax:
  name,index,range,regex mix:  id,2,group:tech,~\"^IL\"
  negative indices/ranges:     -1,-2:     (last column, second-last to end)
  literal symbol names:        `2:4`      (avoid selector parsing)

Injected selectors:
  __file__   inject file path per row
  __base__   inject file basename per row
  {file}     inject rendered template value (also {base}, {dir}, modifiers)
  sample={base:!lower}   inline header+template alias

Injected header names:
  --inject-col-names sample,file_base
  (aliases: --file-col, --fc)

Template tokens:
  {file} {base} {dir}
  aliases: __file__ == file, __base__ == base, __dir__ == dir
  modifiers: : (strip all ext), . (strip last ext), % (basename), / (dirname)
  trims: ^suffix (remove trailing suffix), #prefix (remove leading prefix)
  case: !lower !upper !cap

Shell quoting note:
  Use single quotes when templates contain '!': '{base:!lower}'
  In double quotes, escape '!': \"{base:\\!lower}\"

Examples:
  tsvkit cut -f 'sample_id,group,purity' examples/samples.tsv
  tsvkit cut -f '1,group,~\"^IL\",-1' examples/cytokines.tsv
  tsvkit cut -f '{file},{base:},1:2' examples/qc.tsv
  tsvkit cut -f 'sample={base:#sample_!lower},1:' sample_A.tsv
  tsvkit cut -f '__base__,1:' examples/qc*.tsv
  tsvkit cut --inject-col-names sample -f '__base__,1:' examples/qc*.tsv
  tsvkit cut --inject-col-names file_name,sample -f '{base:},sample={base:#sample_!upper},1:2' sample_A.tsv
  tsvkit cut -H -f '3,1,-1' data.tsv
  tsvkit cut -C ';' -E -I -f '1:3' dirty.tsv
  tsvkit cut -D -f 'value,~\"^value$\"' duplicated_headers.tsv

Practical tips:
  - Use -f first, then pipe into filter/summarize/sort for analysis workflows.
  - Use regex selectors (~\"...\") to keep evolving column groups (e.g. assays).
  - Use template/injected selectors to preserve provenance when concatenating files."
)]
pub struct CutArgs {
    /// Input TSV file(s) (use '-' for stdin; supports gz/xz)
    #[arg(value_name = "FILES", num_args = 0.., default_values = ["-"])]
    pub files: Vec<PathBuf>,

    /// Fields to select, using names, 1-based indices, ranges (`colA:colD`, `2:5`), regex (`~"^sample"`), templates (`{base:}`), or mixes. Comma-separated list.
    #[arg(
        short = 'f',
        long = "fields",
        value_name = "COLS",
        required = true,
        allow_hyphen_values = true
    )]
    pub fields: String,

    /// Comma-separated header names for injected columns (template/special selectors in -f)
    #[arg(
        long = "inject-col-names",
        visible_aliases = ["file-col", "fc"],
        value_name = "NAMES"
    )]
    pub inject_col_names: Option<String>,

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

    /// Allow duplicate column matches when resolving names or regex selectors
    #[arg(short = 'D', long = "allow-dups")]
    pub allow_dups: bool,
}

pub fn run(args: CutArgs) -> Result<()> {
    let selectors = parse_selector_list_with_templates(&args.fields)?;
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut writer = BufWriter::new(io::stdout().lock());

    let file_column_config = FileColumnConfig::parse(args.inject_col_names.as_deref())?;
    let mut header_emitted = false;

    for path in &args.files {
        let mut reader = reader_for_path(path, args.no_header, &input_opts)?;
        let file_info = FileInfo::from_path(path);

        if args.no_header {
            process_no_header_file(
                &mut reader,
                path,
                &selectors,
                &file_info,
                &input_opts,
                &mut writer,
                args.allow_dups,
            )?;
        } else {
            process_header_file(
                &mut reader,
                path,
                &selectors,
                &file_info,
                &file_column_config,
                &input_opts,
                &mut writer,
                &mut header_emitted,
                args.allow_dups,
            )?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn process_no_header_file(
    reader: &mut csv::Reader<Box<dyn io::Read>>,
    path: &Path,
    selectors: &[ColumnSelector],
    file_info: &FileInfo,
    input_opts: &InputOptions,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
    allow_duplicates: bool,
) -> Result<()> {
    let mut records = reader.records();
    let first_record = loop {
        match records.next() {
            Some(record) => {
                let record = record.with_context(|| format!("failed reading from {:?}", path))?;
                if should_skip_record(&record, input_opts, None) {
                    continue;
                }
                break record;
            }
            None => return Ok(()),
        }
    };
    let expected_width = first_record.len();
    let headers = default_headers(expected_width);
    let columns = build_cut_columns(&headers, selectors, true, allow_duplicates)?;
    emit_record(&first_record, &columns, file_info, writer)?;
    for record in records {
        let record = record.with_context(|| format!("failed reading from {:?}", path))?;
        if should_skip_record(&record, input_opts, Some(expected_width)) {
            continue;
        }
        emit_record(&record, &columns, file_info, writer)?;
    }
    Ok(())
}

fn process_header_file(
    reader: &mut csv::Reader<Box<dyn io::Read>>,
    path: &Path,
    selectors: &[ColumnSelector],
    file_info: &FileInfo,
    file_column_config: &FileColumnConfig,
    input_opts: &InputOptions,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
    header_emitted: &mut bool,
    allow_duplicates: bool,
) -> Result<()> {
    let headers = reader
        .headers()
        .with_context(|| format!("failed reading header from {:?}", path))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let columns = build_cut_columns(&headers, selectors, false, allow_duplicates)?;
    let expected_width = headers.len();

    if !*header_emitted {
        file_column_config.validate_count(&columns)?;
        let mut injected_idx = 0usize;
        let header_fields: Vec<String> = columns
            .iter()
            .map(|column| match column {
                CutColumn::Index(idx) => headers
                    .get(*idx)
                    .map(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                CutColumn::Injected(special) => {
                    let name = file_column_config
                        .header_for(injected_idx)
                        .unwrap_or_else(|| special.default_header().to_string());
                    injected_idx += 1;
                    name
                }
                CutColumn::Template { header, .. } => {
                    let name = file_column_config
                        .header_for(injected_idx)
                        .unwrap_or_else(|| header.clone());
                    injected_idx += 1;
                    name
                }
            })
            .collect();
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("\t"))?;
        }
        *header_emitted = true;
    }

    for record in reader.records() {
        let record = record.with_context(|| format!("failed reading from {:?}", path))?;
        if should_skip_record(&record, input_opts, Some(expected_width)) {
            continue;
        }
        emit_record(&record, &columns, file_info, writer)?;
    }
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
            CutColumn::Index(idx) => fields.push(record.get(*idx).unwrap_or("").to_string()),
            CutColumn::Injected(special) => fields.push(file_info.rendered_value_for(*special)?),
            CutColumn::Template { template, .. } => {
                fields.push(render_file_template(template, &file_info.context)?)
            }
        }
    }
    writeln!(writer, "{}", fields.join("\t"))?;
    Ok(())
}

fn build_cut_columns(
    headers: &[String],
    selectors: &[ColumnSelector],
    no_header: bool,
    allow_duplicates: bool,
) -> Result<Vec<CutColumn>> {
    let mut columns = Vec::new();
    for selector in selectors {
        match selector {
            ColumnSelector::Special(special) => columns.push(CutColumn::Injected(*special)),
            ColumnSelector::Template(template) => {
                let raw = format!("{{{}}}", template);
                columns.push(CutColumn::Template {
                    header: raw.clone(),
                    template: raw,
                });
            }
            ColumnSelector::Range(start, end) => {
                if start.as_deref().map_or(false, |sel| {
                    matches!(
                        sel,
                        ColumnSelector::Special(_) | ColumnSelector::Template(_)
                    )
                }) || end.as_deref().map_or(false, |sel| {
                    matches!(
                        sel,
                        ColumnSelector::Special(_) | ColumnSelector::Template(_)
                    )
                }) {
                    bail!("special/template columns cannot be used within a range selector");
                }
                let indices = if allow_duplicates {
                    resolve_selectors_allow_duplicates(headers, &[selector.clone()], no_header)?
                } else {
                    resolve_selectors(headers, &[selector.clone()], no_header)?
                };
                columns.extend(indices.into_iter().map(CutColumn::Index));
            }
            _ => {
                if let ColumnSelector::Name(name) = selector {
                    if let Some((header, template)) = parse_inline_template_selector(name) {
                        columns.push(CutColumn::Template { header, template });
                        continue;
                    }
                }
                let indices = if allow_duplicates {
                    resolve_selectors_allow_duplicates(headers, &[selector.clone()], no_header)?
                } else {
                    resolve_selectors(headers, &[selector.clone()], no_header)?
                };
                columns.extend(indices.into_iter().map(CutColumn::Index));
            }
        }
    }
    Ok(columns)
}

#[derive(Clone)]
struct FileInfo {
    context: FileTemplateContext,
}

impl FileInfo {
    fn from_path(path: &Path) -> Self {
        FileInfo {
            context: FileTemplateContext::from_path(path),
        }
    }

    fn rendered_value_for(&self, special: SpecialColumn) -> Result<String> {
        Ok(match special {
            SpecialColumn::FilePath => self.context.path.clone(),
            SpecialColumn::FileBase => self.context.base.clone(),
        })
    }
}

struct FileColumnConfig<'a> {
    names: Vec<&'a str>,
}

impl<'a> FileColumnConfig<'a> {
    fn parse(spec: Option<&'a str>) -> Result<Self> {
        let names = spec
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(FileColumnConfig { names })
    }

    fn header_for(&self, injected_idx: usize) -> Option<String> {
        self.names.get(injected_idx).map(|s| s.to_string())
    }

    fn validate_count(&self, columns: &[CutColumn]) -> Result<()> {
        let injected_count = columns.iter().filter(|c| c.is_injected()).count();
        if self.names.len() > injected_count {
            bail!(
                "--inject-col-names provided {} name(s), but only {} injected column(s) are selected",
                self.names.len(),
                injected_count
            );
        }
        Ok(())
    }
}

enum CutColumn {
    Index(usize),
    Injected(SpecialColumn),
    Template { header: String, template: String },
}

impl CutColumn {
    fn is_injected(&self) -> bool {
        matches!(self, CutColumn::Injected(_) | CutColumn::Template { .. })
    }
}

fn parse_inline_template_selector(token: &str) -> Option<(String, String)> {
    let (header, template) = token.split_once('=')?;
    let header = header.trim();
    let template = template.trim();
    if header.is_empty()
        || template.len() < 2
        || !template.starts_with('{')
        || !template.ends_with('}')
    {
        return None;
    }
    Some((header.to_string(), template.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::common::SpecialColumn;

    use super::{CutColumn, FileColumnConfig, parse_inline_template_selector};

    #[test]
    fn parses_inline_template_selector() {
        let parsed = parse_inline_template_selector("sample={base:!lower}").unwrap();
        assert_eq!(parsed.0, "sample");
        assert_eq!(parsed.1, "{base:!lower}");
    }

    #[test]
    fn parses_injected_column_names() {
        let cfg = FileColumnConfig::parse(Some("a, b ,c")).unwrap();
        assert_eq!(cfg.header_for(0).as_deref(), Some("a"));
        assert_eq!(cfg.header_for(1).as_deref(), Some("b"));
        assert_eq!(cfg.header_for(2).as_deref(), Some("c"));
    }

    #[test]
    fn rejects_too_many_injected_names() {
        let cfg = FileColumnConfig::parse(Some("a,b")).unwrap();
        let columns = vec![CutColumn::Injected(SpecialColumn::FileBase)];
        assert!(cfg.validate_count(&columns).is_err());
    }
}
