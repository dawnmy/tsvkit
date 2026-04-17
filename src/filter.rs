use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{InputOptions, default_headers, reader_for_path, should_skip_record};
use crate::expression::{
    EvalDiagnostics, bind_expression, collect_division_denominator_columns,
    evaluate_with_diagnostics, parse_expression, RowAccessor,
};

#[derive(Args, Debug)]
#[command(
    about = "Filter TSV rows using boolean expressions",
    long_about = r#"Filter rows using expressions with column references ($name or $index), comparisons, logical operators, arithmetic, regex (~ and !~), numeric functions (abs, sqrt, exp, exp2, ln, log, log10, log2), and row-wise aggregators (sum/mean/median/trimmean/iqr, sd/var, min/max/absmin/absmax, mode/antimode, count/unique/collapse, prod, entropy, argmin/argmax, quantiles via q*/p*). Wrap each -e argument in single quotes so the shell preserves $column selectors; inside the expression, use double quotes around string literals. Defaults to header-aware mode; add -H for headerless input.

Examples:
  tsvkit filter -e '$sample2>=5 & $sample3!=9' examples/profiles.tsv
  tsvkit filter -e '$kingdom ~ "^Bact"' examples/abundance.tsv
  tsvkit filter -e 'log2($coverage) > 10' reads.tsv"#,
    after_help = "More expression patterns:
  Numeric range:
    tsvkit filter -e '$score >= 0.8 & $score <= 0.95' data.tsv
  Membership:
    tsvkit filter -e '$group in [\"case\",\"control\"]' data.tsv
  Negation:
    tsvkit filter -e '!($status == \"failed\")' data.tsv
  Row-wise aggregate filter:
    tsvkit filter -e 'mean($sample1:$sample5) > 10' data.tsv

Tips:
  - Always quote expressions so your shell does not expand `$col`.
  - Prefer double quotes for string literals inside expressions.
  - In -H mode, use `$1`, `$2`, ... selectors.
  - `filter` emits headers only when at least one row matches."
)]
pub struct FilterArgs {
    /// Input TSV file (use '-' for stdin; compressed files supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Filter expression (e.g. `$purity>=0.9 & sum($dna_ug:$rna_ug)>6`); supports `$col`/`$1` selectors, comparisons, arithmetic, regex (~ / !~), numeric functions, and summarize-style aggregators (sum, mean, sd, var, min/max, mode, unique, q*/p*, etc.)
    #[arg(short = 'e', long = "expr", value_name = "EXPR", required = true)]
    pub expr: String,

    /// Treat input as headerless (columns become 1-based indices only)
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

    /// Replace NA literals with this numeric value before numeric evaluation (e.g. --na 0)
    #[arg(long = "na", value_name = "VALUE")]
    pub na_value: Option<f64>,
}

pub fn run(args: FilterArgs) -> Result<()> {
    let expr_ast = parse_expression(&args.expr)?;
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first_record = loop {
            match records.next() {
                Some(rec) => {
                    let record =
                        rec.with_context(|| format!("failed reading from {:?}", args.file))?;
                    if should_skip_record(&record, &input_opts, None) {
                        continue;
                    }
                    break record;
                }
                None => return Ok(()),
            }
        };
        let headers = default_headers(first_record.len());
        let bound = bind_expression(expr_ast, &headers, true)?;
        let denominator_cols = collect_division_denominator_columns(&bound);
        let mut zero_den_rows = 0usize;
        let mut eval_diag = EvalDiagnostics::default();

        if let Some(na_value) = args.na_value {
            let normalized = normalize_record_na(&first_record, na_value);
            if row_has_zero_denominator(&normalized, &denominator_cols) {
                zero_den_rows += 1;
            }
            if evaluate_with_diagnostics(&bound, &normalized, &mut eval_diag) {
                emit_record(&first_record, &mut writer)?;
            }
        } else {
            if row_has_zero_denominator(&first_record, &denominator_cols) {
                zero_den_rows += 1;
            }
            if evaluate_with_diagnostics(&bound, &first_record, &mut eval_diag) {
                emit_record(&first_record, &mut writer)?;
            }
        }
        let expected_width = first_record.len();
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            if let Some(na_value) = args.na_value {
                let normalized = normalize_record_na(&record, na_value);
                if row_has_zero_denominator(&normalized, &denominator_cols) {
                    zero_den_rows += 1;
                }
                if evaluate_with_diagnostics(&bound, &normalized, &mut eval_diag) {
                    emit_record(&record, &mut writer)?;
                }
            } else {
                if row_has_zero_denominator(&record, &denominator_cols) {
                    zero_den_rows += 1;
                }
                if evaluate_with_diagnostics(&bound, &record, &mut eval_diag) {
                    emit_record(&record, &mut writer)?;
                }
            }
        }
        emit_division_warning(
            zero_den_rows,
            &denominator_cols,
            &headers,
            true,
            eval_diag.divide_by_zero_count,
        );
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let bound = bind_expression(expr_ast, &headers, false)?;
        let denominator_cols = collect_division_denominator_columns(&bound);
        let expected_width = headers.len();
        let header_line = (!headers.is_empty()).then(|| headers.join("\t"));
        let mut header_written = false;
        let mut zero_den_rows = 0usize;
        let mut eval_diag = EvalDiagnostics::default();
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            if let Some(na_value) = args.na_value {
                let normalized = normalize_record_na(&record, na_value);
                if row_has_zero_denominator(&normalized, &denominator_cols) {
                    zero_den_rows += 1;
                }
                if evaluate_with_diagnostics(&bound, &normalized, &mut eval_diag) {
                    if !header_written {
                        if let Some(line) = header_line.as_ref() {
                            writeln!(writer, "{}", line)?;
                        }
                        header_written = true;
                    }
                    emit_record(&record, &mut writer)?;
                }
            } else {
                if row_has_zero_denominator(&record, &denominator_cols) {
                    zero_den_rows += 1;
                }
                if evaluate_with_diagnostics(&bound, &record, &mut eval_diag) {
                    if !header_written {
                        if let Some(line) = header_line.as_ref() {
                            writeln!(writer, "{}", line)?;
                        }
                        header_written = true;
                    }
                    emit_record(&record, &mut writer)?;
                }
            }
        }
        emit_division_warning(
            zero_den_rows,
            &denominator_cols,
            &headers,
            false,
            eval_diag.divide_by_zero_count,
        );
    }

    writer.flush()?;
    Ok(())
}

fn row_has_zero_denominator<R: RowAccessor + ?Sized>(record: &R, columns: &[usize]) -> bool {
    columns.iter().any(|&idx| {
        record
            .get(idx)
            .and_then(|value| value.trim().parse::<f64>().ok())
            .map(|v| v == 0.0)
            .unwrap_or(false)
    })
}

fn normalize_record_na(record: &csv::StringRecord, na_value: f64) -> Vec<String> {
    let replacement = na_value.to_string();
    record
        .iter()
        .map(|value| {
            if value.trim().eq_ignore_ascii_case("na") {
                replacement.clone()
            } else {
                value.to_string()
            }
        })
        .collect()
}

fn emit_division_warning(
    zero_den_rows: usize,
    denominator_cols: &[usize],
    headers: &[String],
    no_header: bool,
    divide_by_zero_count: usize,
) {
    if let Some(warning) = build_division_warning(
        zero_den_rows,
        denominator_cols,
        headers,
        no_header,
        divide_by_zero_count,
    ) {
        eprintln!("{}", warning);
    }
}

fn build_division_warning(
    zero_den_rows: usize,
    denominator_cols: &[usize],
    headers: &[String],
    no_header: bool,
    divide_by_zero_count: usize,
) -> Option<String> {
    if zero_den_rows == 0 || denominator_cols.is_empty() || divide_by_zero_count == 0 {
        return None;
    }
    let selector_text = denominator_cols
        .iter()
        .map(|idx| {
            if no_header {
                format!("${}", idx + 1)
            } else {
                let name = headers.get(*idx).map(|s| s.as_str()).unwrap_or("");
                format!("${{{}}}", name)
            }
        })
        .collect::<Vec<_>>();
    let guard = selector_text
        .iter()
        .map(|s| format!("{}!=0", s))
        .collect::<Vec<_>>()
        .join(" & ");
    Some(format!(
        "warning: encountered {} row(s) with denominator value exactly zero while evaluating filter ({} divide-by-zero evaluation(s)). Consider adding a guard such as: {}",
        zero_den_rows, divide_by_zero_count, guard
    ))
}

fn emit_record(
    record: &csv::StringRecord,
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    let line = record.iter().collect::<Vec<_>>().join("\t");
    writeln!(writer, "{}", line)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_division_warning, normalize_record_na};
    use crate::expression::{bind_expression, evaluate, parse_expression};
    use csv::StringRecord;

    #[test]
    fn suppresses_warning_when_no_divide_by_zero_evaluations() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let warning = build_division_warning(10, &[1], &headers, false, 0);
        assert!(warning.is_none());
    }

    #[test]
    fn includes_suggested_guard_when_divide_by_zero_occurs() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let warning = build_division_warning(3, &[1], &headers, false, 2)
            .expect("expected warning");
        assert!(warning.contains("2 divide-by-zero evaluation(s)"));
        assert!(warning.contains("${b}!=0"));
    }

    #[test]
    fn normalize_record_replaces_na_literals() {
        let row = StringRecord::from(vec!["NA", "na", "1.2"]);
        let normalized = normalize_record_na(&row, 0.0);
        assert_eq!(normalized, vec!["0", "0", "1.2"]);
    }

    #[test]
    fn parses_numeric_columns_with_minus_operator() {
        assert!(parse_expression("$3-$2").is_ok());
    }

    #[test]
    fn parses_named_columns_with_dash() {
        assert!(parse_expression("$foo-bar").is_ok());
    }

    #[test]
    fn regex_match_operator_evaluates() {
        let expr = parse_expression("$1 ~ \"^foo\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["foobar"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn regex_not_match_operator_evaluates() {
        let expr = parse_expression("$1 !~ \"bar\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["foo"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn log2_function_supported() {
        let expr = parse_expression("log2($1) > 3").unwrap();
        let headers = vec!["value".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["10"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn sum_aggregator_supported() {
        let expr = parse_expression("sum($1:$3) > 6").unwrap();
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["2", "3", "4"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn parentheses_with_addition_parse() {
        let expr = parse_expression("($1 + $2) > 6").unwrap();
        let headers = vec!["dna".to_string(), "rna".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let record = StringRecord::from(vec!["4", "3"]);
        assert!(evaluate(&bound, &record));
    }

    #[test]
    fn regex_literal_escape_preserved_for_special_characters() {
        let expr = parse_expression("$1 ~ \"\\\\.\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let matching = StringRecord::from(vec!["value.with.dot"]);
        let non_matching = StringRecord::from(vec!["value without dot"]);
        assert!(evaluate(&bound, &matching));
        assert!(!evaluate(&bound, &non_matching));
    }

    #[test]
    fn regex_literal_escape_handles_vertical_bar() {
        let expr = parse_expression("$1 ~ \"\\\\|\"").unwrap();
        let headers = vec!["col1".to_string()];
        let bound = bind_expression(expr, &headers, false).unwrap();
        let matching = StringRecord::from(vec!["left|right"]);
        let non_matching = StringRecord::from(vec!["left/right"]);
        assert!(evaluate(&bound, &matching));
        assert!(!evaluate(&bound, &non_matching));
    }
}
