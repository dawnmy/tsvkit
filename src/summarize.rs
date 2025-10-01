use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use indexmap::IndexMap;

use crate::common::{
    default_headers, parse_selector_list, parse_single_selector, reader_for_path,
    resolve_selectors, resolve_single_selector,
};

#[derive(Args, Debug)]
pub struct SummarizeArgs {
    /// Input TSV file (use '-' for stdin)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Group by columns (comma-separated names or 1-based indices)
    #[arg(short = 'g', long = "group", value_name = "COLS")]
    pub group_cols: Option<String>,

    /// Statistics to compute, e.g. "value=sum,mean" (repeatable)
    #[arg(short = 's', long = "stat", value_name = "COLUMN=OPS", required = true)]
    pub stats: Vec<String>,

    /// Treat input as having no header row
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,
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
    First,
    Last,
    Count,
    Sd,
    Var,
    Min,
    Max,
    Mode,
    Distinct,
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
            "first" => Ok(StatOp::First),
            "last" => Ok(StatOp::Last),
            "count" => Ok(StatOp::Count),
            "sd" | "std" | "stddev" => Ok(StatOp::Sd),
            "var" | "variance" => Ok(StatOp::Var),
            "min" => Ok(StatOp::Min),
            "max" => Ok(StatOp::Max),
            "mode" => Ok(StatOp::Mode),
            "distinct" | "unique" => Ok(StatOp::Distinct),
            other => bail!(
                "unsupported stat '{}': sum, mean, median, first, last, count, sd, var, min, max, mode, distinct, quantiles",
                other
            ),
        }
    }

    fn label(&self) -> &str {
        match self {
            StatOp::Sum => "sum",
            StatOp::Mean => "mean",
            StatOp::Median => "median",
            StatOp::First => "first",
            StatOp::Last => "last",
            StatOp::Count => "count",
            StatOp::Sd => "sd",
            StatOp::Var => "var",
            StatOp::Min => "min",
            StatOp::Max => "max",
            StatOp::Mode => "mode",
            StatOp::Distinct => "distinct",
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
    needs_median: bool,
    needs_quantiles: bool,
    needs_minmax: bool,
    needs_mode: bool,
    needs_distinct: bool,
    first: Option<String>,
    last: Option<String>,
    count: usize,
    numeric_count: usize,
    sum: f64,
    sum_sq: f64,
    values: Vec<f64>,
    min: Option<f64>,
    max: Option<f64>,
    mode_counts: IndexMap<String, usize>,
    distinct_values: HashSet<String>,
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
                    | StatOp::Var
                    | StatOp::Sd
                    | StatOp::Min
                    | StatOp::Max
                    | StatOp::Quantile(_)
            )
        });
        let needs_variance = ops.iter().any(|op| matches!(op, StatOp::Sd | StatOp::Var));
        let needs_median = ops.iter().any(|op| matches!(op, StatOp::Median));
        let needs_quantiles = ops.iter().any(|op| matches!(op, StatOp::Quantile(_)));
        let needs_minmax = ops.iter().any(|op| matches!(op, StatOp::Min | StatOp::Max));
        let needs_mode = ops.iter().any(|op| matches!(op, StatOp::Mode));
        let needs_distinct = ops.iter().any(|op| matches!(op, StatOp::Distinct));
        ColumnAggState {
            ops,
            needs_first,
            needs_last,
            needs_numeric,
            needs_variance,
            needs_median,
            needs_quantiles,
            needs_minmax,
            needs_mode,
            needs_distinct,
            first: None,
            last: None,
            count: 0,
            numeric_count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            values: Vec::new(),
            min: None,
            max: None,
            mode_counts: IndexMap::new(),
            distinct_values: HashSet::new(),
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

        if self.needs_mode {
            let entry = self.mode_counts.entry(value.to_string()).or_insert(0);
            *entry += 1;
        }
        if self.needs_distinct {
            self.distinct_values.insert(value.to_string());
        }

        if self.needs_numeric {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                if let Ok(number) = trimmed.parse::<f64>() {
                    self.numeric_count += 1;
                    self.sum += number;
                    if self.needs_variance {
                        self.sum_sq += number * number;
                    }
                    if self.needs_median || self.needs_quantiles {
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
                }
            }
        }
    }

    fn finalize(&mut self) -> Vec<String> {
        if (self.needs_median || self.needs_quantiles) && !self.values.is_empty() {
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
                StatOp::First => {
                    results.push(self.first.clone().unwrap_or_default());
                }
                StatOp::Last => {
                    results.push(self.last.clone().unwrap_or_default());
                }
                StatOp::Count => {
                    results.push(self.count.to_string());
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
                StatOp::Mode => {
                    let mut best_value = String::new();
                    let mut best_count = 0usize;
                    for (value, count) in &self.mode_counts {
                        if *count > best_count {
                            best_count = *count;
                            best_value = value.clone();
                        }
                    }
                    results.push(best_value);
                }
                StatOp::Distinct => {
                    results.push(self.distinct_values.len().to_string());
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
    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut groups: IndexMap<Vec<String>, GroupState> = IndexMap::new();

    let headers: Vec<String>;
    let group_indices: Vec<usize>;
    let stat_requests: Vec<StatRequest>;

    if args.no_header {
        let mut records = reader.records();
        let first_record = match records.next() {
            Some(rec) => rec.with_context(|| format!("failed reading from {:?}", args.file))?,
            None => {
                bail!("input is empty; cannot infer columns without a header");
            }
        };
        headers = default_headers(first_record.len());
        group_indices = parse_group_indices(args.group_cols.as_deref(), &headers, true)?;
        stat_requests = parse_stat_requests(&args.stats, &headers, true)?;

        process_record(&mut groups, &group_indices, &stat_requests, &first_record);
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
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
            let start_selector = parse_single_selector(parts[0].trim())?;
            let end_selector = parse_single_selector(parts[1].trim())?;
            let start_idx = resolve_single_selector(headers, start_selector, no_header)?;
            let end_idx = resolve_single_selector(headers, end_selector, no_header)?;
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

#[cfg(test)]
mod tests {
    use super::parse_quantile_spec;

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
}
