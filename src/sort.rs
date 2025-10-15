use std::cmp::Ordering;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use csv::StringRecord;

use crate::common::{
    InputOptions, default_headers, inconsistent_width_error, parse_single_selector,
    reader_for_path, resolve_single_selector, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Sort TSV rows by column keys",
    long_about = "Sort TSV rows by one or more keys. Provide -k/--key with column selectors (names or 1-based indices) and optional modifiers: :n (numeric asc), :nr (numeric desc), :r (reverse text). Repeat -k for additional sort levels. Defaults to header-aware mode; add -H for headerless files.\n\nExamples:\n  tsvkit sort -k count:nr examples/abundance.tsv\n  tsvkit sort -k $1:nr -k $2:r examples/profiles.tsv"
)]
pub struct SortArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Sort key specification (`column[:modifier]`). Select columns by name, index, or range; modifiers include `:n` (numeric asc), `:nr` (numeric desc), `:r` (reverse text). Repeat flag for secondary keys.
    #[arg(short = 'k', long = "key", value_name = "SPEC", required = true)]
    pub keys: Vec<String>,

    /// Treat input as headerless (columns referenced by indices only)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Use an unstable sort (faster but does not preserve order of equal keys)
    #[arg(long = "unstable")]
    pub unstable: bool,

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortMode {
    Text,
    Numeric,
}

#[derive(Clone, Debug)]
struct KeySpec {
    selector: String,
    order: SortOrder,
    mode: SortMode,
}

#[derive(Clone, Debug)]
struct ResolvedKey {
    index: usize,
    order: SortOrder,
    mode: SortMode,
}

pub fn run(args: SortArgs) -> Result<()> {
    let specs = args
        .keys
        .iter()
        .map(|spec| parse_key_spec(spec))
        .collect::<Result<Vec<_>>>()?;

    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());
    let source_name = format!("\"{}\"", args.file.display());

    let mut records: Vec<StringRecord> = Vec::new();
    let headers = if args.no_header {
        let mut reference_width: Option<usize> = None;
        let mut row_number = 0usize;
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            row_number += 1;
            if let Some(width) = reference_width {
                if record.len() != width {
                    if input_opts.ignore_illegal {
                        continue;
                    } else {
                        return Err(inconsistent_width_error(
                            &source_name,
                            row_number,
                            width,
                            record.len(),
                        ));
                    }
                }
            }
            if should_skip_record(&record, &input_opts, reference_width) {
                continue;
            }
            if reference_width.is_none() {
                reference_width = Some(record.len());
            }
            records.push(record);
        }
        let len = reference_width.unwrap_or(0);
        default_headers(len)
    } else {
        let header_row = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut row_number = 0usize;
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            row_number += 1;
            if record.len() != header_row.len() {
                if input_opts.ignore_illegal {
                    continue;
                } else {
                    return Err(inconsistent_width_error(
                        &source_name,
                        row_number + 1,
                        header_row.len(),
                        record.len(),
                    ));
                }
            }
            if should_skip_record(&record, &input_opts, Some(header_row.len())) {
                continue;
            }
            records.push(record);
        }
        if !header_row.is_empty() {
            writeln!(writer, "{}", header_row.join("\t"))?;
        }
        header_row
    };

    if records.is_empty() {
        writer.flush()?;
        return Ok(());
    }

    let resolved = resolve_keys(&headers, &specs, args.no_header)?;

    if args.unstable {
        records.sort_unstable_by(|a, b| compare_records(a, b, &resolved));
    } else {
        records.sort_by(|a, b| compare_records(a, b, &resolved));
    }

    for record in records {
        writeln!(writer, "{}", record.iter().collect::<Vec<_>>().join("\t"))?;
    }

    writer.flush()?;
    Ok(())
}

fn parse_key_spec(spec: &str) -> Result<KeySpec> {
    let mut parts = spec.split(':');
    let selector_raw = parts
        .next()
        .ok_or_else(|| anyhow!("empty key specification"))?
        .trim();
    if selector_raw.is_empty() {
        bail!("empty key specification");
    }

    let mut selector = selector_raw.to_string();
    if let Some(stripped) = selector.strip_prefix('<') {
        selector = stripped.trim().to_string();
    }
    if let Some(stripped) = selector.strip_prefix('$') {
        selector = stripped.trim().to_string();
    }
    if selector.is_empty() {
        bail!("column selector after '<' must not be empty");
    }

    let mut order = SortOrder::Asc;
    let mut mode = SortMode::Text;

    for part in parts {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let lower = token.to_ascii_lowercase();
        match lower.as_str() {
            "asc" => order = SortOrder::Asc,
            "desc" => order = SortOrder::Desc,
            "num" | "numeric" => mode = SortMode::Numeric,
            "n" => mode = SortMode::Numeric,
            "str" | "text" | "s" => mode = SortMode::Text,
            "r" => {
                mode = SortMode::Text;
                order = SortOrder::Desc;
            }
            "nr" | "rn" => {
                mode = SortMode::Numeric;
                order = SortOrder::Desc;
            }
            other => bail!(
                "unknown sort modifier '{}': expected asc, desc, num, str, n, r, or nr",
                other
            ),
        }
    }

    Ok(KeySpec {
        selector,
        order,
        mode,
    })
}

fn resolve_keys(
    headers: &[String],
    specs: &[KeySpec],
    no_header: bool,
) -> Result<Vec<ResolvedKey>> {
    let mut resolved = Vec::with_capacity(specs.len());
    for spec in specs {
        let selector = parse_single_selector(&spec.selector)?;
        let index = resolve_single_selector(headers, selector, no_header)?;
        resolved.push(ResolvedKey {
            index,
            order: spec.order,
            mode: spec.mode,
        });
    }
    Ok(resolved)
}

fn compare_records(a: &StringRecord, b: &StringRecord, keys: &[ResolvedKey]) -> Ordering {
    for key in keys {
        let field_a = a.get(key.index).unwrap_or("");
        let field_b = b.get(key.index).unwrap_or("");

        let mut ordering = match key.mode {
            SortMode::Text => field_a.cmp(field_b),
            SortMode::Numeric => compare_numeric(field_a, field_b),
        };

        if key.order == SortOrder::Desc {
            ordering = ordering.reverse();
        }

        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_numeric(a: &str, b: &str) -> Ordering {
    let num_a = parse_number(a);
    let num_b = parse_number(b);

    match (num_a, num_b) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => a.cmp(b),
    }
}

fn parse_number(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_key_spec_with_modifiers() {
        let spec = parse_key_spec("score:desc:num").unwrap();
        assert_eq!(spec.selector, "score");
        assert_eq!(spec.order, SortOrder::Desc);
        assert_eq!(spec.mode, SortMode::Numeric);
    }

    #[test]
    fn compare_numeric_handles_missing() {
        assert_eq!(compare_numeric("10", ""), Ordering::Greater);
        assert_eq!(compare_numeric("", "10"), Ordering::Less);
        assert_eq!(compare_numeric("alpha", "beta"), Ordering::Less);
    }

    #[test]
    fn parse_shorthand_numeric_flags() {
        let spec = parse_key_spec("count:nr").unwrap();
        assert_eq!(spec.selector, "count");
        assert_eq!(spec.order, SortOrder::Desc);
        assert_eq!(spec.mode, SortMode::Numeric);
    }

    #[test]
    fn parse_shorthand_reverse_text() {
        let spec = parse_key_spec("name:r").unwrap();
        assert_eq!(spec.selector, "name");
        assert_eq!(spec.order, SortOrder::Desc);
        assert_eq!(spec.mode, SortMode::Text);
    }

    #[test]
    fn parse_index_selector_with_numeric_hint() {
        let spec = parse_key_spec("<3:n").unwrap();
        assert_eq!(spec.selector, "3");
        assert_eq!(spec.order, SortOrder::Asc);
        assert_eq!(spec.mode, SortMode::Numeric);
    }

    #[test]
    fn parse_dollar_prefix_selector() {
        let spec = parse_key_spec("$2:r").unwrap();
        assert_eq!(spec.selector, "2");
        assert_eq!(spec.order, SortOrder::Desc);
        assert_eq!(spec.mode, SortMode::Text);
    }
}
