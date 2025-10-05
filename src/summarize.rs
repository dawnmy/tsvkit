use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::Args;
use indexmap::IndexMap;

use crate::common::{
    InputOptions, default_headers, parse_selector_list, parse_single_selector, reader_for_path,
    resolve_selectors, resolve_single_selector, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Grouped statistics over TSV columns",
    long_about = "Group rows with -g/--group and compute statistics for selected columns via -s/--stat. Each --stat accepts COLUMN=ops, where COLUMN can be names, indices, ranges, or mixes, and ops include sum, mean, median, quantiles (q1, q50, q0.9), var, sd, mode, distinct, and more. Headers are used by default; add -H for headerless input.\n\nExamples:\n  tsvkit summarize -s 'sample1:sample3=mean' examples/profiles.tsv\n  tsvkit summarize -g group -s 'sample1=mean,sd' -s 'sample2:sample3=sum' examples/profiles.tsv\n  tsvkit summarize -s 'sample1=q1,q3,var' examples/profiles.tsv"
)]
pub struct SummarizeArgs {
    /// Input TSV file (use '-' for stdin; compressed files are detected automatically)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Columns to group by (comma-separated names/indices/ranges). Omit to aggregate the entire table as one group.
    #[arg(short = 'g', long = "group", value_name = "COLS")]
    pub group_cols: Option<String>,

    /// Statistics to compute (`COLUMN=ops`). Columns accept names, indices, ranges (e.g. `IL6:IL10`); operations are comma-separated (sum, mean, median, sd, var, min, max, mode, distinct, q*/p* aliases). Repeatable.
    #[arg(short = 's', long = "stat", value_name = "COLUMN=OPS", required = true)]
    pub stats: Vec<String>,

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

#[derive(Debug, Clone, PartialEq)]
struct QuantileSpec {
    fraction: f64,
    label: String,
}

#[derive(Debug, Clone, PartialEq)]
enum StatOp {
    Sum,
    Mean,
    Median,
    TrimMean,
    Iqr,
    First,
    Last,
    Count,
    Rand,
    UniqueValues,
    Collapse,
    CountUnique(&'static str),
    Sd,
    Var,
    Min,
    Max,
    AbsMin,
    AbsMax,
    Mode,
    AntiMode,
    Prod,
    Entropy,
    ArgMin,
    ArgMax,
    Quantile(QuantileSpec),
}

impl StatOp {
    fn from_str(op: &str) -> Result<Self> {
        if let Some(spec) = parse_quantile_spec(op) {
            return Ok(StatOp::Quantile(spec));
        }

        match op.to_lowercase().as_str() {
            "sum" => Ok(StatOp::Sum),
            "mean" | "avg" => Ok(StatOp::Mean),
            "median" | "med" => Ok(StatOp::Median),
            "trimmean" => Ok(StatOp::TrimMean),
            "iqr" => Ok(StatOp::Iqr),
            "first" => Ok(StatOp::First),
            "last" => Ok(StatOp::Last),
            "count" => Ok(StatOp::Count),
            "rand" | "random" => Ok(StatOp::Rand),
            "unique" => Ok(StatOp::UniqueValues),
            "collapse" => Ok(StatOp::Collapse),
            "countunique" => Ok(StatOp::CountUnique("countunique")),
            "distinct" => Ok(StatOp::CountUnique("distinct")),
            "sd" | "std" | "stddev" => Ok(StatOp::Sd),
            "var" | "variance" => Ok(StatOp::Var),
            "min" => Ok(StatOp::Min),
            "max" => Ok(StatOp::Max),
            "absmin" => Ok(StatOp::AbsMin),
            "absmax" => Ok(StatOp::AbsMax),
            "mode" => Ok(StatOp::Mode),
            "antimode" => Ok(StatOp::AntiMode),
            "prod" | "product" => Ok(StatOp::Prod),
            "entropy" => Ok(StatOp::Entropy),
            "argmin" => Ok(StatOp::ArgMin),
            "argmax" => Ok(StatOp::ArgMax),
            other => bail!(
                "unsupported stat '{}': sum, mean, median, trimmean, iqr, first, last, count, rand, unique, collapse, countunique, sd, var, min, max, absmin, absmax, mode, antimode, prod, entropy, argmin, argmax, quantiles",
                other
            ),
        }
    }

    fn label(&self) -> &str {
        match self {
            StatOp::Sum => "sum",
            StatOp::Mean => "mean",
            StatOp::Median => "median",
            StatOp::TrimMean => "trimmean",
            StatOp::Iqr => "iqr",
            StatOp::First => "first",
            StatOp::Last => "last",
            StatOp::Count => "count",
            StatOp::Rand => "rand",
            StatOp::UniqueValues => "unique",
            StatOp::Collapse => "collapse",
            StatOp::CountUnique(label) => label,
            StatOp::Sd => "sd",
            StatOp::Var => "var",
            StatOp::Min => "min",
            StatOp::Max => "max",
            StatOp::AbsMin => "absmin",
            StatOp::AbsMax => "absmax",
            StatOp::Mode => "mode",
            StatOp::AntiMode => "antimode",
            StatOp::Prod => "prod",
            StatOp::Entropy => "entropy",
            StatOp::ArgMin => "argmin",
            StatOp::ArgMax => "argmax",
            StatOp::Quantile(spec) => spec.label.as_str(),
        }
    }
}

fn parse_quantile_spec(token: &str) -> Option<QuantileSpec> {
    let trimmed = token.trim();
    if trimmed.len() < 2 {
        return None;
    }
    let prefix = trimmed.chars().next()?.to_ascii_lowercase();
    let rest_original = &trimmed[1..];
    let rest = rest_original.trim();
    if rest.is_empty() {
        return None;
    }

    match prefix {
        'q' => {
            let fraction = if rest.chars().all(|c| c.is_ascii_digit()) {
                let int_val = rest.parse::<u32>().ok()?;
                if int_val <= 4 {
                    int_val as f64 / 4.0
                } else if int_val <= 100 {
                    int_val as f64 / 100.0
                } else {
                    return None;
                }
            } else if let Ok(val) = rest.parse::<f64>() {
                if (0.0..=1.0).contains(&val) {
                    val
                } else if (1.0..=100.0).contains(&val) {
                    val / 100.0
                } else {
                    return None;
                }
            } else {
                return None;
            };
            Some(QuantileSpec {
                fraction,
                label: format!("q{}", sanitize_quantile_label(rest_original)),
            })
        }
        'p' => {
            let percent = rest.parse::<f64>().ok()?;
            if !(0.0..=100.0).contains(&percent) {
                return None;
            }
            Some(QuantileSpec {
                fraction: percent / 100.0,
                label: format!("p{}", sanitize_quantile_label(rest_original)),
            })
        }
        _ => None,
    }
}

fn sanitize_quantile_label(segment: &str) -> String {
    let mut label = segment.trim().to_ascii_lowercase();
    if label.is_empty() {
        label.push('0');
    }
    while label.starts_with('0') && label.len() > 1 && label.chars().nth(1) != Some('.') {
        label.remove(0);
    }
    label = label.replace('%', "");
    label = label.replace('.', "_");
    label.replace('-', "_")
}

struct StatRequest {
    column_index: usize,
    ops: Vec<StatOp>,
    output_labels: Vec<String>,
}

struct ColumnAggState {
    ops: Vec<StatOp>,
    needs_first: bool,
    needs_last: bool,
    needs_numeric: bool,
    needs_variance: bool,
    needs_sorted_values: bool,
    needs_minmax: bool,
    needs_value_counts: bool,
    needs_distinct: bool,
    needs_abs_extrema: bool,
    needs_product: bool,
    needs_all_values: bool,
    needs_argmin: bool,
    needs_argmax: bool,
    first: Option<String>,
    last: Option<String>,
    count: usize,
    numeric_count: usize,
    sum: f64,
    sum_sq: f64,
    values: Vec<f64>,
    min: Option<f64>,
    max: Option<f64>,
    abs_min: Option<(f64, f64)>,
    abs_max: Option<(f64, f64)>,
    product: f64,
    has_product: bool,
    argmin_value: Option<f64>,
    argmin_position: Option<usize>,
    argmax_value: Option<f64>,
    argmax_position: Option<usize>,
    value_counts: IndexMap<String, usize>,
    distinct_values: HashSet<String>,
    all_values: Vec<String>,
}

impl ColumnAggState {
    fn new(ops: Vec<StatOp>) -> Self {
        let needs_first = ops.iter().any(|op| matches!(op, StatOp::First));
        let needs_last = ops.iter().any(|op| matches!(op, StatOp::Last));
        let needs_numeric = ops.iter().any(|op| {
            matches!(
                op,
                StatOp::Sum
                    | StatOp::Mean
                    | StatOp::Median
                    | StatOp::TrimMean
                    | StatOp::Iqr
                    | StatOp::Var
                    | StatOp::Sd
                    | StatOp::Min
                    | StatOp::Max
                    | StatOp::AbsMin
                    | StatOp::AbsMax
                    | StatOp::Quantile(_)
                    | StatOp::Prod
                    | StatOp::ArgMin
                    | StatOp::ArgMax
            )
        });
        let needs_variance = ops.iter().any(|op| matches!(op, StatOp::Sd | StatOp::Var));
        let needs_sorted_values = ops.iter().any(|op| {
            matches!(
                op,
                StatOp::Median | StatOp::Quantile(_) | StatOp::TrimMean | StatOp::Iqr
            )
        });
        let needs_minmax = ops.iter().any(|op| matches!(op, StatOp::Min | StatOp::Max));
        let needs_value_counts = ops.iter().any(|op| {
            matches!(
                op,
                StatOp::Mode | StatOp::AntiMode | StatOp::Entropy | StatOp::UniqueValues
            )
        });
        let needs_distinct = ops.iter().any(|op| matches!(op, StatOp::CountUnique(_)));
        let needs_abs_extrema = ops
            .iter()
            .any(|op| matches!(op, StatOp::AbsMin | StatOp::AbsMax));
        let needs_product = ops.iter().any(|op| matches!(op, StatOp::Prod));
        let needs_all_values = ops
            .iter()
            .any(|op| matches!(op, StatOp::Collapse | StatOp::Rand));
        let needs_argmin = ops.iter().any(|op| matches!(op, StatOp::ArgMin));
        let needs_argmax = ops.iter().any(|op| matches!(op, StatOp::ArgMax));
        ColumnAggState {
            ops,
            needs_first,
            needs_last,
            needs_numeric,
            needs_variance,
            needs_sorted_values,
            needs_minmax,
            needs_value_counts,
            needs_distinct,
            needs_abs_extrema,
            needs_product,
            needs_all_values,
            needs_argmin,
            needs_argmax,
            first: None,
            last: None,
            count: 0,
            numeric_count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            values: Vec::new(),
            min: None,
            max: None,
            abs_min: None,
            abs_max: None,
            product: 1.0,
            has_product: false,
            argmin_value: None,
            argmin_position: None,
            argmax_value: None,
            argmax_position: None,
            value_counts: IndexMap::new(),
            distinct_values: HashSet::new(),
            all_values: Vec::new(),
        }
    }

    fn update(&mut self, value: &str) {
        if self.needs_first && self.first.is_none() {
            self.first = Some(value.to_string());
        }
        if self.needs_last {
            self.last = Some(value.to_string());
        }
        self.count += 1;

        if self.needs_all_values {
            self.all_values.push(value.to_string());
        }

        if self.needs_value_counts {
            let entry = self.value_counts.entry(value.to_string()).or_insert(0);
            *entry += 1;
        }
        if self.needs_distinct {
            self.distinct_values.insert(value.to_string());
        }

        if self.needs_numeric {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                if let Ok(number) = trimmed.parse::<f64>() {
                    if number.is_finite() {
                        self.numeric_count += 1;
                        self.sum += number;
                        if self.needs_variance {
                            self.sum_sq += number * number;
                        }
                        if self.needs_sorted_values {
                            self.values.push(number);
                        }
                        if self.needs_minmax {
                            self.min = Some(match self.min {
                                Some(current) => current.min(number),
                                None => number,
                            });
                            self.max = Some(match self.max {
                                Some(current) => current.max(number),
                                None => number,
                            });
                        }
                        if self.needs_abs_extrema {
                            let abs_value = number.abs();
                            if let Some((_, current_abs)) = self.abs_min {
                                if abs_value < current_abs {
                                    self.abs_min = Some((number, abs_value));
                                }
                            } else {
                                self.abs_min = Some((number, abs_value));
                            }
                            if let Some((_, current_abs)) = self.abs_max {
                                if abs_value > current_abs {
                                    self.abs_max = Some((number, abs_value));
                                }
                            } else {
                                self.abs_max = Some((number, abs_value));
                            }
                        }
                        if self.needs_product {
                            if self.has_product {
                                self.product *= number;
                            } else {
                                self.product = number;
                                self.has_product = true;
                            }
                        }
                        if self.needs_argmin {
                            let pos = self.numeric_count;
                            match self.argmin_value {
                                Some(current) => {
                                    if number < current {
                                        self.argmin_value = Some(number);
                                        self.argmin_position = Some(pos);
                                    }
                                }
                                None => {
                                    self.argmin_value = Some(number);
                                    self.argmin_position = Some(pos);
                                }
                            }
                        }
                        if self.needs_argmax {
                            let pos = self.numeric_count;
                            match self.argmax_value {
                                Some(current) => {
                                    if number > current {
                                        self.argmax_value = Some(number);
                                        self.argmax_position = Some(pos);
                                    }
                                }
                                None => {
                                    self.argmax_value = Some(number);
                                    self.argmax_position = Some(pos);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn finalize(&mut self) -> Vec<String> {
        if self.needs_sorted_values && !self.values.is_empty() {
            self.values
                .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        }

        let mut results = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            match op {
                StatOp::Sum => {
                    results.push(if self.numeric_count > 0 {
                        format_number(self.sum)
                    } else {
                        String::new()
                    });
                }
                StatOp::Mean => {
                    results.push(if self.numeric_count > 0 {
                        format_number(self.sum / self.numeric_count as f64)
                    } else {
                        String::new()
                    });
                }
                StatOp::Median => {
                    results.push(median(&self.values).map(format_number).unwrap_or_default());
                }
                StatOp::TrimMean => {
                    results.push(
                        trimmed_mean(&self.values)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
                StatOp::Iqr => {
                    results.push(
                        interquartile_range(&self.values)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
                StatOp::First => {
                    results.push(self.first.clone().unwrap_or_default());
                }
                StatOp::Last => {
                    results.push(self.last.clone().unwrap_or_default());
                }
                StatOp::Count => {
                    results.push(self.count.to_string());
                }
                StatOp::Rand => {
                    if let Some(choice) = random_choice(&self.all_values) {
                        results.push(choice);
                    } else {
                        results.push(String::new());
                    }
                }
                StatOp::UniqueValues => {
                    if self.value_counts.is_empty() {
                        results.push(String::new());
                    } else {
                        let unique: Vec<&String> = self.value_counts.keys().collect();
                        results.push(
                            unique
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                    }
                }
                StatOp::Collapse => {
                    if self.all_values.is_empty() {
                        results.push(String::new());
                    } else {
                        results.push(self.all_values.join(","));
                    }
                }
                StatOp::CountUnique(_) => {
                    results.push(self.distinct_values.len().to_string());
                }
                StatOp::Sd => {
                    results.push(
                        stddev(self.sum, self.sum_sq, self.numeric_count)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
                StatOp::Var => {
                    results.push(
                        variance(self.sum, self.sum_sq, self.numeric_count)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
                StatOp::Min => {
                    results.push(self.min.map(format_number).unwrap_or_default());
                }
                StatOp::Max => {
                    results.push(self.max.map(format_number).unwrap_or_default());
                }
                StatOp::AbsMin => {
                    results.push(
                        self.abs_min
                            .map(|(value, _)| format_number(value))
                            .unwrap_or_default(),
                    );
                }
                StatOp::AbsMax => {
                    results.push(
                        self.abs_max
                            .map(|(value, _)| format_number(value))
                            .unwrap_or_default(),
                    );
                }
                StatOp::Mode => {
                    results.push(mode_value(&self.value_counts));
                }
                StatOp::AntiMode => {
                    results.push(antimode_value(&self.value_counts));
                }
                StatOp::Prod => {
                    if self.has_product {
                        results.push(format_number(self.product));
                    } else {
                        results.push(String::new());
                    }
                }
                StatOp::Entropy => {
                    results.push(
                        entropy(&self.value_counts, self.count)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
                StatOp::ArgMin => {
                    results.push(
                        self.argmin_position
                            .map(|pos| pos.to_string())
                            .unwrap_or_default(),
                    );
                }
                StatOp::ArgMax => {
                    results.push(
                        self.argmax_position
                            .map(|pos| pos.to_string())
                            .unwrap_or_default(),
                    );
                }
                StatOp::Quantile(spec) => {
                    results.push(
                        quantile(&self.values, spec.fraction)
                            .map(format_number)
                            .unwrap_or_default(),
                    );
                }
            }
        }
        results
    }
}

struct GroupState {
    aggregators: Vec<ColumnAggState>,
}

impl GroupState {
    fn new(stat_requests: &[StatRequest]) -> Self {
        let aggregators = stat_requests
            .iter()
            .map(|req| ColumnAggState::new(req.ops.clone()))
            .collect();
        GroupState { aggregators }
    }
}

pub fn run(args: SummarizeArgs) -> Result<()> {
    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;
    let mut reader = reader_for_path(&args.file, args.no_header, &input_opts)?;
    let mut groups: IndexMap<Vec<String>, GroupState> = IndexMap::new();

    let headers: Vec<String>;
    let group_indices: Vec<usize>;
    let stat_requests: Vec<StatRequest>;

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
                None => {
                    bail!("input is empty; cannot infer columns without a header");
                }
            }
        };
        headers = default_headers(first_record.len());
        group_indices = parse_group_indices(args.group_cols.as_deref(), &headers, true)?;
        stat_requests = parse_stat_requests(&args.stats, &headers, true)?;

        process_record(&mut groups, &group_indices, &stat_requests, &first_record);
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(first_record.len())) {
                continue;
            }
            process_record(&mut groups, &group_indices, &stat_requests, &record);
        }
    } else {
        headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        group_indices = parse_group_indices(args.group_cols.as_deref(), &headers, false)?;
        stat_requests = parse_stat_requests(&args.stats, &headers, false)?;

        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(headers.len())) {
                continue;
            }
            process_record(&mut groups, &group_indices, &stat_requests, &record);
        }
    }

    output_summary(
        &headers,
        &group_indices,
        &stat_requests,
        &mut groups,
        args.no_header,
    )
}

fn parse_group_indices(
    spec: Option<&str>,
    headers: &[String],
    no_header: bool,
) -> Result<Vec<usize>> {
    if let Some(spec) = spec {
        let selectors = parse_selector_list(spec)?;
        resolve_selectors(headers, &selectors, no_header)
    } else {
        Ok(Vec::new())
    }
}

fn expand_target_columns(spec: &str, headers: &[String], no_header: bool) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for token in spec.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() == 1 {
            let selectors = parse_selector_list(token)?;
            let resolved = resolve_selectors(headers, &selectors, no_header)?;
            indices.extend(resolved);
        } else if parts.len() == 2 {
            let start_part = parts[0].trim();
            let end_part = parts[1].trim();
            let start_idx = if start_part.is_empty() {
                0
            } else {
                let start_selector = parse_single_selector(start_part)?;
                resolve_single_selector(headers, start_selector, no_header)?
            };
            let end_idx = if end_part.is_empty() {
                headers.len().checked_sub(1).ok_or_else(|| {
                    anyhow!("column range end cannot be determined in empty table")
                })?
            } else {
                let end_selector = parse_single_selector(end_part)?;
                resolve_single_selector(headers, end_selector, no_header)?
            };
            if start_idx <= end_idx {
                for idx in start_idx..=end_idx {
                    indices.push(idx);
                }
            } else {
                for idx in (end_idx..=start_idx).rev() {
                    indices.push(idx);
                }
            }
        } else {
            bail!("invalid column range '{}': use 'start:end'", token);
        }
    }
    if indices.is_empty() {
        bail!("column specification '{}' did not select any columns", spec);
    }
    Ok(indices)
}

fn parse_stat_requests(
    specs: &[String],
    headers: &[String],
    no_header: bool,
) -> Result<Vec<StatRequest>> {
    if specs.is_empty() {
        bail!("at least one --stat specification is required");
    }

    let mut result = Vec::new();
    for spec in specs {
        let (column_part, ops_part) = spec
            .split_once('=')
            .or_else(|| spec.split_once(':'))
            .with_context(|| format!("expected COLUMN=ops in '{}'", spec))?;
        let column_indices = expand_target_columns(column_part.trim(), headers, no_header)?;

        let ops = ops_part
            .split(',')
            .map(|op| StatOp::from_str(op.trim()))
            .collect::<Result<Vec<_>>>()?;

        if ops.is_empty() {
            bail!("no operations provided for '{}'", spec);
        }

        for column_index in column_indices {
            let base_name = headers
                .get(column_index)
                .cloned()
                .unwrap_or_else(|| format!("col{}", column_index + 1));

            let output_labels = ops
                .iter()
                .map(|op| format!("{}_{}", base_name, op.label()))
                .collect();

            result.push(StatRequest {
                column_index,
                ops: ops.clone(),
                output_labels,
            });
        }
    }
    Ok(result)
}

fn process_record(
    groups: &mut IndexMap<Vec<String>, GroupState>,
    group_indices: &[usize],
    stat_requests: &[StatRequest],
    record: &csv::StringRecord,
) {
    let key = build_group_key(group_indices, record);
    let entry = groups
        .entry(key)
        .or_insert_with(|| GroupState::new(stat_requests));

    for (agg, req) in entry.aggregators.iter_mut().zip(stat_requests.iter()) {
        let value = record.get(req.column_index).unwrap_or("");
        agg.update(value);
    }
}

fn build_group_key(group_indices: &[usize], record: &csv::StringRecord) -> Vec<String> {
    if group_indices.is_empty() {
        return Vec::new();
    }
    group_indices
        .iter()
        .map(|&idx| record.get(idx).unwrap_or("").to_string())
        .collect()
}

fn output_summary(
    headers: &[String],
    group_indices: &[usize],
    stat_requests: &[StatRequest],
    groups: &mut IndexMap<Vec<String>, GroupState>,
    no_header: bool,
) -> Result<()> {
    let mut writer = BufWriter::new(io::stdout().lock());

    if !no_header {
        let mut header_fields = Vec::new();
        for &idx in group_indices {
            let name = headers
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("group{}", idx + 1));
            header_fields.push(name);
        }
        for req in stat_requests {
            header_fields.extend(req.output_labels.clone());
        }
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("\t"))?;
        }
    }

    for (key, state) in groups.iter_mut() {
        let mut fields = Vec::new();
        fields.extend(key.iter().cloned());
        for agg in &mut state.aggregators {
            fields.extend(agg.finalize());
        }
        writeln!(writer, "{}", fields.join("\t"))?;
    }

    writer.flush()?;
    Ok(())
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.6}", value)
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        let mid = values.len() / 2;
        if values.len() % 2 == 0 {
            Some((values[mid - 1] + values[mid]) / 2.0)
        } else {
            Some(values[mid])
        }
    }
}

fn stddev(sum: f64, sum_sq: f64, count: usize) -> Option<f64> {
    variance(sum, sum_sq, count).map(|var| var.sqrt())
}

fn variance(sum: f64, sum_sq: f64, count: usize) -> Option<f64> {
    if count == 0 {
        return None;
    }
    let mean = sum / count as f64;
    let variance = (sum_sq / count as f64) - mean * mean;
    Some(variance.max(0.0))
}

fn quantile(values: &[f64], fraction: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len();
    if n == 1 {
        return Some(values[0]);
    }
    let clamped = fraction.clamp(0.0, 1.0);
    let position = clamped * (n - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        Some(values[lower])
    } else {
        let weight = position - lower as f64;
        let lower_value = values[lower];
        let upper_value = values[upper];
        Some(lower_value + (upper_value - lower_value) * weight)
    }
}

fn trimmed_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len();
    let trim_fraction = 0.1;
    let trim = ((n as f64) * trim_fraction).floor() as usize;
    if trim == 0 {
        let sum: f64 = values.iter().sum();
        return Some(sum / n as f64);
    }
    if trim * 2 >= n {
        let sum: f64 = values.iter().sum();
        return Some(sum / n as f64);
    }
    let slice = &values[trim..(n - trim)];
    if slice.is_empty() {
        return None;
    }
    let sum: f64 = slice.iter().sum();
    Some(sum / slice.len() as f64)
}

fn interquartile_range(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let q1 = quantile(values, 0.25)?;
    let q3 = quantile(values, 0.75)?;
    Some(q3 - q1)
}

fn random_choice(values: &[String]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    let mut hasher = DefaultHasher::new();
    values.len().hash(&mut hasher);
    for (idx, value) in values.iter().enumerate() {
        idx.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
        duration.as_nanos().hash(&mut hasher);
    }
    let index = (hasher.finish() as usize) % values.len();
    values.get(index).cloned()
}

fn mode_value(counts: &IndexMap<String, usize>) -> String {
    let mut best_value = String::new();
    let mut best_count = 0usize;
    for (value, count) in counts {
        if *count > best_count {
            best_count = *count;
            best_value = value.clone();
        }
    }
    best_value
}

fn antimode_value(counts: &IndexMap<String, usize>) -> String {
    if counts.is_empty() {
        return String::new();
    }
    let mut best_value = String::new();
    let mut best_count = usize::MAX;
    for (value, count) in counts {
        if *count < best_count {
            best_count = *count;
            best_value = value.clone();
        }
    }
    best_value
}

fn entropy(counts: &IndexMap<String, usize>, total: usize) -> Option<f64> {
    if total == 0 || counts.is_empty() {
        return None;
    }
    let total = total as f64;
    let mut entropy = 0.0;
    for &count in counts.values() {
        if count == 0 {
            continue;
        }
        let probability = count as f64 / total;
        entropy -= probability * probability.log2();
    }
    Some(entropy.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::{entropy, expand_target_columns, parse_quantile_spec, trimmed_mean};
    use indexmap::IndexMap;

    #[test]
    fn quantile_aliases_map_to_expected_fractions() {
        let q1 = parse_quantile_spec("q1").unwrap();
        assert!((q1.fraction - 0.25).abs() < f64::EPSILON);

        let q2 = parse_quantile_spec("q2").unwrap();
        assert!((q2.fraction - 0.5).abs() < f64::EPSILON);

        let q3 = parse_quantile_spec("q3").unwrap();
        assert!((q3.fraction - 0.75).abs() < f64::EPSILON);

        let q5 = parse_quantile_spec("q5").unwrap();
        assert!((q5.fraction - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn decimal_quantiles_pass_through() {
        let q = parse_quantile_spec("q0.6").unwrap();
        assert!((q.fraction - 0.6).abs() < f64::EPSILON);
    }

    #[test]
    fn out_of_range_quantiles_reject() {
        assert!(parse_quantile_spec("q250").is_none());
    }

    #[test]
    fn expand_target_columns_supports_open_ranges() {
        let headers = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let end_open = expand_target_columns("2:", &headers, false).unwrap();
        assert_eq!(end_open, vec![1, 2, 3]);
        let start_open = expand_target_columns(":3", &headers, false).unwrap();
        assert_eq!(start_open, vec![0, 1, 2]);
    }

    #[test]
    fn trimmed_mean_trims_extremes() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let tm = trimmed_mean(&values).unwrap();
        assert!((tm - 5.5).abs() < 1e-6);
    }

    #[test]
    fn entropy_matches_uniform_distribution() {
        let mut counts = IndexMap::new();
        counts.insert("a".to_string(), 2);
        counts.insert("b".to_string(), 2);
        let ent = entropy(&counts, 4).unwrap();
        assert!((ent - 1.0).abs() < 1e-6);
    }
}
