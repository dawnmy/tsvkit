use std::cmp::Ordering;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use csv::StringRecord;
use indexmap::IndexMap;

use crate::common::{
    InputOptions, default_headers, inconsistent_width_error, parse_selector_list,
    parse_single_selector, reader_for_path, resolve_selectors, resolve_single_selector,
    should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Sort TSV rows by column keys",
    long_about = "Sort TSV rows by one or more keys. Provide -k/--key with column selectors (names or 1-based indices) and optional modifiers: :n (numeric asc), :nr (numeric desc), :r (reverse text). Repeat -k for additional sort levels. Defaults to header-aware mode; add -H for headerless files.\n\nExamples:\n  tsvkit sort -k count:nr examples/abundance.tsv\n  tsvkit sort -k $1:nr -k $2:r examples/profiles.tsv\n  tsvkit sort -k group -k score:nr --group-by group --group-head 1 examples/scores.tsv",
    after_help = "Key spec quick reference:
  -k col        text ascending
  -k col:r      text descending
  -k col:n      numeric ascending
  -k col:nr     numeric descending

Multi-key examples:
  tsvkit sort -k group -k score:nr data.tsv
  tsvkit sort -k date -k sample_id data.tsv
  tsvkit sort -H -k 3:n -k 1 raw.tsv
  tsvkit sort -k cohort -k metric:nr --group-by cohort --group-head 3 data.tsv
  tsvkit sort -k cohort -k metric:nr --group-by cohort --group-tail 2 data.tsv
  tsvkit sort -k cohort -k metric:nr --group-by cohort --group-head 0.2 --ceil data.tsv

Tips:
  Stable sort (default) preserves input order for equal keys.
  Use --unstable for speed when equal-key order does not matter.
  For N < 1, --group-head/--group-tail treat N as a fraction of each group size."
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

    /// Keep one row per group using these columns (comma-separated names/indices/ranges)
    #[arg(long = "group-by", value_name = "COLS")]
    pub group_by: Option<String>,

    /// When --group-by is set, keep first N rows per sorted group (N>=1 count, 0<N<1 fraction of group size)
    #[arg(
        long = "group-head",
        value_name = "N",
        conflicts_with = "group_tail",
        requires = "group_by"
    )]
    pub group_head: Option<f64>,

    /// When --group-by is set, keep last N rows per sorted group (N>=1 count, 0<N<1 fraction of group size)
    #[arg(
        long = "group-tail",
        value_name = "N",
        conflicts_with = "group_head",
        requires = "group_by"
    )]
    pub group_tail: Option<f64>,

    /// For fractional N (0 < N < 1), round up when converting fraction * group_size to row count
    #[arg(long = "ceil", conflicts_with = "floor", requires = "group_by")]
    pub ceil: bool,

    /// For fractional N (0 < N < 1), round down when converting fraction * group_size to row count
    #[arg(long = "floor", conflicts_with = "ceil", requires = "group_by")]
    pub floor: bool,

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GroupTake {
    Head,
    Tail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FractionRounding {
    Round,
    Ceil,
    Floor,
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

    if let Some(spec) = args.group_by.as_deref() {
        let (take_mode, n_spec) = match (args.group_head, args.group_tail) {
            (Some(n), None) => (GroupTake::Head, n),
            (None, Some(n)) => (GroupTake::Tail, n),
            (None, None) => {
                bail!("--group-by requires either --group-head N or --group-tail N");
            }
            (Some(_), Some(_)) => unreachable!("clap enforces conflicts"),
        };
        let rounding = if args.ceil {
            FractionRounding::Ceil
        } else if args.floor {
            FractionRounding::Floor
        } else {
            FractionRounding::Round
        };
        let group_indices = resolve_group_indices(&headers, spec, args.no_header)?;
        records = pick_n_records_per_group(records, &group_indices, n_spec, rounding, take_mode)?;
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

fn resolve_group_indices(headers: &[String], group_spec: &str, no_header: bool) -> Result<Vec<usize>> {
    let selectors = parse_selector_list(group_spec)?;
    let indices = resolve_selectors(headers, &selectors, no_header)?;
    if indices.is_empty() {
        bail!("--group-by must resolve at least one column");
    }
    Ok(indices)
}

fn pick_n_records_per_group(
    records: Vec<StringRecord>,
    group_indices: &[usize],
    n_spec: f64,
    rounding: FractionRounding,
    mode: GroupTake,
) -> Result<Vec<StringRecord>> {
    match mode {
        GroupTake::Head => {
            let mut picked: IndexMap<Vec<String>, Vec<StringRecord>> = IndexMap::new();
            for record in records {
                let key = group_indices
                    .iter()
                    .map(|&idx| record.get(idx).unwrap_or("").to_string())
                    .collect::<Vec<_>>();
                picked.entry(key).or_default().push(record);
            }
            let mut output = Vec::new();
            for group in picked.into_values() {
                let keep = rows_to_keep(n_spec, group.len(), rounding)?;
                output.extend(group.into_iter().take(keep));
            }
            Ok(output)
        }
        GroupTake::Tail => {
            let mut picked: IndexMap<Vec<String>, Vec<StringRecord>> = IndexMap::new();
            for record in records {
                let key = group_indices
                    .iter()
                    .map(|&idx| record.get(idx).unwrap_or("").to_string())
                    .collect::<Vec<_>>();
                picked.entry(key).or_default().push(record);
            }
            let mut output = Vec::new();
            for group in picked.into_values() {
                let keep = rows_to_keep(n_spec, group.len(), rounding)?;
                let skip = group.len().saturating_sub(keep);
                output.extend(group.into_iter().skip(skip));
            }
            Ok(output)
        }
    }
}

fn rows_to_keep(n_spec: f64, group_len: usize, rounding: FractionRounding) -> Result<usize> {
    if !n_spec.is_finite() {
        bail!("group row count N must be a finite number");
    }
    if n_spec <= 0.0 {
        bail!("group row count N must be > 0");
    }
    if n_spec < 1.0 {
        let raw = n_spec * group_len as f64;
        let rounded = match rounding {
            FractionRounding::Round => raw.round(),
            FractionRounding::Ceil => raw.ceil(),
            FractionRounding::Floor => raw.floor(),
        };
        return Ok(rounded.max(0.0) as usize);
    }

    if (n_spec.fract()).abs() > f64::EPSILON {
        bail!("group row count N must be an integer when N >= 1");
    }
    Ok(n_spec as usize)
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

    #[test]
    fn pick_group_head_records() {
        let records = vec![
            StringRecord::from(vec!["A", "10"]),
            StringRecord::from(vec!["A", "9"]),
            StringRecord::from(vec!["A", "8"]),
            StringRecord::from(vec!["B", "7"]),
        ];
        let selected = pick_n_records_per_group(
            records,
            &[0],
            2.0,
            FractionRounding::Round,
            GroupTake::Head,
        )
        .unwrap();
        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].get(1), Some("10"));
        assert_eq!(selected[1].get(1), Some("9"));
        assert_eq!(selected[2].get(1), Some("7"));
    }

    #[test]
    fn pick_group_tail_records() {
        let records = vec![
            StringRecord::from(vec!["A", "10"]),
            StringRecord::from(vec!["A", "9"]),
            StringRecord::from(vec!["A", "8"]),
            StringRecord::from(vec!["B", "7"]),
        ];
        let selected = pick_n_records_per_group(
            records,
            &[0],
            2.0,
            FractionRounding::Round,
            GroupTake::Tail,
        )
        .unwrap();
        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].get(1), Some("9"));
        assert_eq!(selected[1].get(1), Some("8"));
        assert_eq!(selected[2].get(1), Some("7"));
    }

    #[test]
    fn group_head_one_matches_first() {
        let records = vec![
            StringRecord::from(vec!["A", "10"]),
            StringRecord::from(vec!["A", "9"]),
            StringRecord::from(vec!["B", "7"]),
        ];
        let selected = pick_n_records_per_group(
            records,
            &[0],
            1.0,
            FractionRounding::Round,
            GroupTake::Head,
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].get(1), Some("10"));
        assert_eq!(selected[1].get(1), Some("7"));
    }

    #[test]
    fn group_tail_one_matches_last() {
        let records = vec![
            StringRecord::from(vec!["A", "10"]),
            StringRecord::from(vec!["A", "9"]),
            StringRecord::from(vec!["B", "7"]),
        ];
        let selected = pick_n_records_per_group(
            records,
            &[0],
            1.0,
            FractionRounding::Round,
            GroupTake::Tail,
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].get(1), Some("9"));
        assert_eq!(selected[1].get(1), Some("7"));
    }

    #[test]
    fn fraction_rounding_modes_affect_row_count() {
        assert_eq!(
            rows_to_keep(0.34, 3, FractionRounding::Round).unwrap(),
            1
        );
        assert_eq!(rows_to_keep(0.34, 3, FractionRounding::Ceil).unwrap(), 2);
        assert_eq!(rows_to_keep(0.34, 3, FractionRounding::Floor).unwrap(), 1);
    }

    #[test]
    fn rejects_non_integer_above_one() {
        let err = rows_to_keep(1.5, 10, FractionRounding::Round).unwrap_err();
        assert!(err
            .to_string()
            .contains("must be an integer when N >= 1"));
    }
}
