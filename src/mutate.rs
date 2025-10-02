use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use regex::Regex;

use crate::common::{
    InputOptions, default_headers, parse_selector_list, reader_for_path, resolve_selectors,
    should_skip_record,
};
use crate::expression::{BoundValue, bind_value_expression, eval_value, parse_value_expression};

#[derive(Args, Debug)]
#[command(
    about = "Create or transform TSV columns",
    long_about = r#"Add derived columns or rewrite existing ones. Use -e/--expr to specify operations. Assignments can be arbitrary expressions (`name=EXPR`) using the filter expression language (column selectors, arithmetic, abs/sqrt/exp/ln/log/log2/log10/exp2, regex matches, etc.), row-wise aggregates (sum, mean, median, sd, q1–q4, q0.25, p95, etc.), or string helpers like sub(). Always prefix column references with `$`, including columns created earlier in the same invocation (e.g. `log_total=log2($total)`), and wrap each -e argument in single quotes so the shell leaves `$` selectors alone. You can also run in-place substitutions with sed-style syntax s/selectors/pattern/replacement/.

Examples:
  tsvkit mutate -e "coverage_sum=sum($1,$3:$5)" examples/profiles.tsv
  tsvkit mutate -e "log_counts=mean($count1:$count5)" counts.tsv
  tsvkit mutate -e "title_clean=sub($title,\"\\s+\", \"_\")" titles.tsv
  tsvkit mutate -e 's/$col1:$col3/NA/0/' data.tsv"#
)]
pub struct MutateArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Mutation expression, repeatable. Use `name=EXPR` for derived columns (expressions, aggregates, sub()) or `s/selectors/pattern/replacement/` for in-place substitution.
    #[arg(short = 'e', long = "expr", value_name = "EXPR", required = true)]
    pub exprs: Vec<String>,

    /// Treat input as headerless (columns referenced by 1-based indices only)
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

pub fn run(args: MutateArgs) -> Result<()> {
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
        let expected_width = first_record.len();
        let headers = default_headers(expected_width);
        let ops = parse_operations(&args.exprs, &headers, true)?;

        let mut row = first_record
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        process_row(&mut row, &ops)?;
        writer.write_all(row.join("\t").as_bytes())?;
        writer.write_all(b"\n")?;

        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let mut row = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            process_row(&mut row, &ops)?;
            writer.write_all(row.join("\t").as_bytes())?;
            writer.write_all(b"\n")?;
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let ops = parse_operations(&args.exprs, &headers, false)?;
        let expected_width = headers.len();

        let mut output_headers = headers.clone();
        for op in &ops {
            if let MutateOp::Create { name, .. } = op {
                output_headers.push(name.clone());
            }
        }
        if !output_headers.is_empty() {
            writer.write_all(output_headers.join("\t").as_bytes())?;
            writer.write_all(b"\n")?;
        }

        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            let mut row = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            process_row(&mut row, &ops)?;
            writer.write_all(row.join("\t").as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn process_row(row: &mut Vec<String>, ops: &[MutateOp]) -> Result<()> {
    for op in ops {
        match op {
            MutateOp::Create { func, .. } => {
                let value = evaluate_function(func, row)?;
                row.push(value);
            }
            MutateOp::Substitute {
                columns,
                pattern,
                replacement,
            } => {
                for &idx in columns {
                    if let Some(field) = row.get_mut(idx) {
                        let replaced = pattern.replace_all(field, replacement.as_str());
                        *field = replaced.into_owned();
                    }
                }
            }
        }
    }
    Ok(())
}

fn evaluate_function(func: &FunctionSpec, row: &[String]) -> Result<String> {
    match func {
        FunctionSpec::Expression { expr } => {
            let evaluated = eval_value(expr, row);
            Ok(evaluated.text.into_owned())
        }
        FunctionSpec::Aggregate { kind, columns } => {
            let values = collect_numeric(row, columns);
            if values.is_empty() {
                return Ok(String::new());
            }
            let result = match kind {
                AggregateKind::Sum => values.iter().sum::<f64>(),
                AggregateKind::Mean => values.iter().sum::<f64>() / values.len() as f64,
                AggregateKind::Sd => stddev(&values).unwrap_or(f64::NAN),
                AggregateKind::Quantile { fraction } => {
                    quantile(&values, *fraction).unwrap_or(f64::NAN)
                }
            };
            if result.is_finite() {
                Ok(format_number(result))
            } else {
                Ok(String::new())
            }
        }
        FunctionSpec::SubNew {
            column,
            pattern,
            replacement,
        } => {
            let value = row.get(*column).map(|s| s.as_str()).unwrap_or("");
            let replaced = pattern.replace_all(value, replacement.as_str());
            Ok(replaced.into_owned())
        }
    }
}

fn collect_numeric(row: &[String], indices: &[usize]) -> Vec<f64> {
    indices
        .iter()
        .filter_map(|&idx| row.get(idx))
        .filter_map(|value| parse_float(value))
        .collect()
}

fn parse_operations(
    exprs: &[String],
    headers: &[String],
    no_header: bool,
) -> Result<Vec<MutateOp>> {
    let mut ops = Vec::new();
    let mut current_headers = headers.to_vec();
    for expr in exprs {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            bail!("mutation expression must not be empty");
        }
        if trimmed.starts_with("s/") {
            ops.push(parse_substitution_expression(
                trimmed,
                &current_headers,
                no_header,
            )?);
        } else {
            let op = parse_assignment_expression(trimmed, &current_headers, no_header)?;
            if let MutateOp::Create { name, .. } = &op {
                if no_header {
                    current_headers.push(format!("col{}", current_headers.len() + 1));
                } else {
                    current_headers.push(name.clone());
                }
            }
            ops.push(op);
        }
    }
    Ok(ops)
}

fn parse_assignment_expression(
    expr: &str,
    headers: &[String],
    no_header: bool,
) -> Result<MutateOp> {
    let (name_part, value_part) = expr
        .split_once('=')
        .with_context(|| "assignment must use name=expression syntax")?;
    let name = name_part.trim();
    if name.is_empty() {
        bail!("missing column name on left-hand side");
    }
    let function = parse_function(value_part.trim(), headers, no_header)?;
    Ok(MutateOp::Create {
        name: name.to_string(),
        func: function,
    })
}

fn parse_function(value: &str, headers: &[String], no_header: bool) -> Result<FunctionSpec> {
    let trimmed = value.trim();

    if let Some(rest) = trimmed.strip_prefix("sub(") {
        let inner = rest
            .strip_suffix(')')
            .with_context(|| "sub() expression must end with ')'")?;
        let args = split_args(inner);
        if args.len() != 3 {
            bail!("sub() expects three arguments: column, pattern, replacement");
        }
        let selectors = parse_selector_list(&normalize_selector_spec(&args[0]))?;
        if selectors.len() != 1 {
            bail!("sub() requires exactly one column selector");
        }
        let column_indices = resolve_selectors(headers, &selectors, no_header)?;
        let column = column_indices[0];
        let pattern = Regex::new(&parse_string_literal(&args[1])?)
            .with_context(|| "invalid regex in sub() pattern")?;
        let replacement = parse_string_literal(&args[2])?;
        return Ok(FunctionSpec::SubNew {
            column,
            pattern,
            replacement,
        });
    }

    if let Some(open_idx) = trimmed.find('(') {
        let func_name = trimmed[..open_idx].trim();
        if let Ok(kind) = parse_aggregate_kind(func_name) {
            let inner = trimmed
                .strip_prefix(&format!("{}(", func_name))
                .and_then(|s| s.strip_suffix(')'))
                .with_context(|| "function call must end with ')'")?;
            let args_str = inner.trim();
            let selectors = parse_selector_list(&normalize_selector_spec(args_str))?;
            if selectors.is_empty() {
                bail!(
                    "function '{}' expects at least one column selector",
                    func_name
                );
            }
            let indices = resolve_selectors(headers, &selectors, no_header)?;
            return Ok(FunctionSpec::Aggregate {
                kind,
                columns: indices,
            });
        }
    }

    let expr = parse_value_expression(trimmed)?;
    let bound = bind_value_expression(expr, headers, no_header)?;
    Ok(FunctionSpec::Expression { expr: bound })
}

fn parse_substitution_expression(
    expr: &str,
    headers: &[String],
    no_header: bool,
) -> Result<MutateOp> {
    let content = expr.trim_start_matches("s/");
    let content = content
        .strip_suffix('/')
        .with_context(|| "substitution expression must end with '/'")?;
    let mut parts = content.splitn(3, '/');
    let selector_part = parts
        .next()
        .with_context(|| "missing selector list in substitution expression")?;
    let pattern_part = parts
        .next()
        .with_context(|| "missing pattern in substitution expression")?;
    let replacement_part = parts
        .next()
        .with_context(|| "missing replacement in substitution expression")?;

    let selectors = parse_selector_list(&normalize_selector_spec(selector_part))?;
    if selectors.is_empty() {
        bail!("substitution requires at least one target column");
    }
    let indices = resolve_selectors(headers, &selectors, no_header)?;
    let regex = Regex::new(pattern_part).with_context(|| "invalid regex in substitution")?;
    Ok(MutateOp::Substitute {
        columns: indices,
        pattern: regex,
        replacement: replacement_part.to_string(),
    })
}

fn normalize_selector_spec(spec: &str) -> String {
    spec.replace('$', "")
}

fn split_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            ',' if !in_quotes => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        args.push(current.trim().to_string());
    }
    args
}

fn parse_string_literal(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let mut result = String::new();
        let mut chars = inner.chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    result.push(match next {
                        'n' => '\n',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        other => other,
                    });
                }
            } else {
                result.push(ch);
            }
        }
        Ok(result)
    } else {
        bail!("expected string literal enclosed in double quotes")
    }
}

fn parse_aggregate_kind(name: &str) -> Result<AggregateKind> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "sum" => Ok(AggregateKind::Sum),
        "mean" | "avg" => Ok(AggregateKind::Mean),
        "median" | "med" => Ok(AggregateKind::Quantile { fraction: 0.5 }),
        "sd" | "std" | "stddev" => Ok(AggregateKind::Sd),
        _ if lower.starts_with('q') => {
            let fraction = parse_quantile_fraction(&lower)?;
            Ok(AggregateKind::Quantile { fraction })
        }
        _ if lower.starts_with('p') => {
            let fraction = parse_percent_fraction(&lower)?;
            Ok(AggregateKind::Quantile { fraction })
        }
        other => bail!(
            "unsupported function '{}': try sum, mean, median, sd, q*, or p*",
            other
        ),
    }
}

fn parse_quantile_fraction(token: &str) -> Result<f64> {
    let rest = &token[1..];
    if rest.is_empty() {
        bail!("quantile function must specify a value (e.g. q1 or q0.25)");
    }
    if rest.chars().all(|c| c.is_ascii_digit()) {
        let int_val: u32 = rest.parse().with_context(|| "invalid quantile index")?;
        if int_val <= 4 {
            return Ok(int_val as f64 / 4.0);
        } else if int_val <= 100 {
            return Ok(int_val as f64 / 100.0);
        } else {
            bail!("quantile integer must be between 1 and 100");
        }
    }
    let fractional = rest.replace('_', ".");
    let value: f64 = fractional
        .parse()
        .with_context(|| "invalid quantile fraction")?;
    if !(0.0..=1.0).contains(&value) {
        bail!("quantile fraction must lie between 0 and 1");
    }
    Ok(value)
}

fn parse_percent_fraction(token: &str) -> Result<f64> {
    let rest = &token[1..];
    if rest.is_empty() {
        bail!("percentile function must specify a value (e.g. p95)");
    }
    let value: f64 = rest
        .replace('_', ".")
        .parse()
        .with_context(|| "invalid percentile value")?;
    if !(0.0..=100.0).contains(&value) {
        bail!("percentile value must be between 0 and 100");
    }
    Ok(value / 100.0)
}

fn stddev(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|v| {
            let diff = v - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(variance.max(0.0).sqrt())
}

fn quantile(values: &[f64], fraction: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(values[0]);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let position = fraction.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        Some(sorted[lower])
    } else {
        let weight = position - lower as f64;
        let lower_value = sorted[lower];
        let upper_value = sorted[upper];
        Some(lower_value + (upper_value - lower_value) * weight)
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.6}", value)
    }
}

fn parse_float(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

#[derive(Debug)]
enum MutateOp {
    Create {
        name: String,
        func: FunctionSpec,
    },
    Substitute {
        columns: Vec<usize>,
        pattern: Regex,
        replacement: String,
    },
}

#[derive(Debug)]
enum FunctionSpec {
    Expression {
        expr: BoundValue,
    },
    Aggregate {
        kind: AggregateKind,
        columns: Vec<usize>,
    },
    SubNew {
        column: usize,
        pattern: Regex,
        replacement: String,
    },
}

#[derive(Debug)]
enum AggregateKind {
    Sum,
    Mean,
    Sd,
    Quantile { fraction: f64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quantile_aliases() {
        assert!((parse_quantile_fraction("q1").unwrap() - 0.25).abs() < f64::EPSILON);
        assert!((parse_quantile_fraction("q2").unwrap() - 0.5).abs() < f64::EPSILON);
        assert!((parse_quantile_fraction("q75").unwrap() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_percent_aliases() {
        assert!((parse_percent_fraction("p95").unwrap() - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn split_args_handles_quotes() {
        let args = split_args("$col1, \"a,b\", \"c\"");
        assert_eq!(args, vec!["$col1", "\"a,b\"", "\"c\""]);
    }

    #[test]
    fn expression_assignment_uses_filter_language() {
        let headers = vec!["dna_ug".to_string(), "rna_ug".to_string()];
        let ops = parse_operations(
            &vec!["ee=exp($dna_ug) - exp2($rna_ug)".to_string()],
            &headers,
            false,
        )
        .unwrap();
        let mut row = vec!["1.0".to_string(), "2.0".to_string()];
        process_row(&mut row, &ops).unwrap();
        assert_eq!(row.len(), 3);
        let value: f64 = row[2].parse().unwrap();
        let expected = 1.0f64.exp() - 2.0f64.exp2();
        assert!((value - expected).abs() < 1e-6);
    }

    #[test]
    fn subsequent_assignments_can_reference_new_columns() {
        let headers = vec![
            "sample_id".to_string(),
            "IL6".to_string(),
            "IL7".to_string(),
            "IL8".to_string(),
            "IL9".to_string(),
            "IL10".to_string(),
        ];
        let ops = parse_operations(
            &vec![
                "total=sum($IL6:$IL10)".to_string(),
                "log_total=log2($total)".to_string(),
            ],
            &headers,
            false,
        )
        .unwrap();
        let mut row = vec![
            "S1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
        ];
        process_row(&mut row, &ops).unwrap();
        assert_eq!(row.len(), 8);
        assert_eq!(row[6], "5");
        let log_total: f64 = row[7].parse().unwrap();
        assert!((log_total - 5f64.log2()).abs() < 1e-6);
    }
}
