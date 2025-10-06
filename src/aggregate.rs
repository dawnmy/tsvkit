use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use indexmap::{IndexMap, IndexSet};

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateKind {
    Sum,
    Mean,
    Median,
    TrimMean,
    Iqr,
    First,
    Last,
    Count,
    Rand,
    Unique,
    Collapse,
    CountUnique,
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
    Quantile { fraction: f64 },
}

#[derive(Debug, Clone)]
pub struct AggregateValue {
    pub text: String,
    pub numeric: Option<f64>,
}

impl AggregateValue {
    pub fn empty() -> Self {
        AggregateValue {
            text: String::new(),
            numeric: None,
        }
    }

    pub fn from_number(value: f64) -> Self {
        if value.is_finite() {
            AggregateValue {
                text: format_number(value),
                numeric: Some(value),
            }
        } else {
            AggregateValue::empty()
        }
    }

    pub fn from_text(text: String) -> Self {
        let numeric = parse_float(&text);
        AggregateValue { text, numeric }
    }
}

pub fn try_parse_aggregate_kind(name: &str) -> Result<Option<AggregateKind>> {
    let lower = name.to_ascii_lowercase();
    if let Some(fraction) = try_parse_quantile(&lower)? {
        return Ok(Some(AggregateKind::Quantile { fraction }));
    }

    let kind = match lower.as_str() {
        "sum" => Some(AggregateKind::Sum),
        "mean" | "avg" => Some(AggregateKind::Mean),
        "median" | "med" => Some(AggregateKind::Median),
        "trimmean" => Some(AggregateKind::TrimMean),
        "iqr" => Some(AggregateKind::Iqr),
        "first" => Some(AggregateKind::First),
        "last" => Some(AggregateKind::Last),
        "count" => Some(AggregateKind::Count),
        "rand" | "random" => Some(AggregateKind::Rand),
        "unique" => Some(AggregateKind::Unique),
        "collapse" => Some(AggregateKind::Collapse),
        "countunique" | "distinct" => Some(AggregateKind::CountUnique),
        "sd" | "std" | "stddev" => Some(AggregateKind::Sd),
        "var" | "variance" => Some(AggregateKind::Var),
        "min" => Some(AggregateKind::Min),
        "max" => Some(AggregateKind::Max),
        "absmin" => Some(AggregateKind::AbsMin),
        "absmax" => Some(AggregateKind::AbsMax),
        "mode" => Some(AggregateKind::Mode),
        "antimode" => Some(AggregateKind::AntiMode),
        "prod" | "product" => Some(AggregateKind::Prod),
        "entropy" => Some(AggregateKind::Entropy),
        "argmin" => Some(AggregateKind::ArgMin),
        "argmax" => Some(AggregateKind::ArgMax),
        _ => None,
    };
    Ok(kind)
}

pub fn parse_quantile_fraction(token: &str) -> Result<f64> {
    let rest = token.trim_start_matches('q');
    if rest.is_empty() {
        bail!("quantile function must specify a value (e.g. q1 or q0.25)");
    }
    if rest.chars().all(|c| c.is_ascii_digit()) {
        let int_val: u32 = rest.parse().with_context(|| "invalid quantile index")?;
        if int_val <= 4 {
            return Ok(int_val as f64 / 4.0);
        }
        if int_val <= 100 {
            return Ok(int_val as f64 / 100.0);
        }
        bail!("quantile integer must be between 1 and 100");
    }
    let cleaned = rest.replace('_', ".");
    let value: f64 = cleaned
        .parse()
        .with_context(|| "invalid quantile fraction")?;
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else if (1.0..=100.0).contains(&value) {
        Ok(value / 100.0)
    } else {
        bail!("quantile fraction must lie between 0 and 1 (or 0-100 for percentages)");
    }
}

pub fn parse_percent_fraction(token: &str) -> Result<f64> {
    let rest = token.trim_start_matches('p');
    if rest.is_empty() {
        bail!("percentile function must specify a value (e.g. p95)");
    }
    let cleaned = rest.replace('_', ".");
    let value: f64 = cleaned
        .parse()
        .with_context(|| "invalid percentile value")?;
    if !(0.0..=100.0).contains(&value) {
        bail!("percentile value must be between 0 and 100");
    }
    Ok(value / 100.0)
}

fn try_parse_quantile(token: &str) -> Result<Option<f64>> {
    let trimmed = token.trim();
    if trimmed.len() < 2 {
        return Ok(None);
    }
    let prefix = trimmed.chars().next().unwrap();
    match prefix {
        'q' => parse_quantile_fraction(trimmed).map(Some),
        'p' => parse_percent_fraction(trimmed).map(Some),
        _ => Ok(None),
    }
}

pub fn evaluate_row_aggregate(kind: &AggregateKind, values: &[&str]) -> AggregateValue {
    match kind {
        AggregateKind::Sum => numeric_result(values, |nums| Some(nums.iter().sum::<f64>())),
        AggregateKind::Mean => numeric_result(values, |nums| {
            if nums.is_empty() {
                None
            } else {
                Some(nums.iter().sum::<f64>() / nums.len() as f64)
            }
        }),
        AggregateKind::Median => numeric_result(values, |nums| median(nums)),
        AggregateKind::TrimMean => numeric_result(values, |nums| trimmed_mean(nums)),
        AggregateKind::Iqr => numeric_result(values, |nums| interquartile_range(nums)),
        AggregateKind::Sd => numeric_result(values, |nums| stddev(nums)),
        AggregateKind::Var => numeric_result(values, |nums| variance(nums)),
        AggregateKind::Min => numeric_result(values, |nums| nums.iter().cloned().reduce(f64::min)),
        AggregateKind::Max => numeric_result(values, |nums| nums.iter().cloned().reduce(f64::max)),
        AggregateKind::AbsMin => numeric_result(values, |nums| abs_min(nums)),
        AggregateKind::AbsMax => numeric_result(values, |nums| abs_max(nums)),
        AggregateKind::Prod => numeric_result(values, |nums| {
            if nums.is_empty() {
                None
            } else {
                let mut product = 1.0;
                for num in nums {
                    product *= num;
                }
                Some(product)
            }
        }),
        AggregateKind::Quantile { fraction } => {
            numeric_result(values, |nums| quantile(nums, *fraction))
        }
        AggregateKind::First => values.first().map_or_else(AggregateValue::empty, |v| {
            AggregateValue::from_text((*v).to_string())
        }),
        AggregateKind::Last => values.last().map_or_else(AggregateValue::empty, |v| {
            AggregateValue::from_text((*v).to_string())
        }),
        AggregateKind::Count => AggregateValue::from_number(values.len() as f64),
        AggregateKind::Rand => random_choice(values)
            .map(AggregateValue::from_text)
            .unwrap_or_else(AggregateValue::empty),
        AggregateKind::Unique => {
            let mut seen = IndexSet::new();
            for value in values {
                seen.insert((*value).to_string());
            }
            let text = seen.into_iter().collect::<Vec<_>>().join(",");
            AggregateValue::from_text(text)
        }
        AggregateKind::Collapse => {
            if values.is_empty() {
                AggregateValue::empty()
            } else {
                AggregateValue::from_text(values.join(","))
            }
        }
        AggregateKind::CountUnique => {
            let mut distinct = HashSet::new();
            for value in values {
                distinct.insert((*value).to_string());
            }
            AggregateValue::from_number(distinct.len() as f64)
        }
        AggregateKind::Mode => {
            let counts = build_counts(values);
            AggregateValue::from_text(mode_value(&counts))
        }
        AggregateKind::AntiMode => {
            let counts = build_counts(values);
            AggregateValue::from_text(antimode_value(&counts))
        }
        AggregateKind::Entropy => {
            let counts = build_counts(values);
            let total = values.len();
            entropy(&counts, total)
                .map(AggregateValue::from_number)
                .unwrap_or_else(AggregateValue::empty)
        }
        AggregateKind::ArgMin => {
            let mut best: Option<(f64, usize)> = None;
            let mut position = 0usize;
            for value in values {
                if let Some(num) = parse_float(value) {
                    if !num.is_finite() {
                        continue;
                    }
                    position += 1;
                    match best {
                        Some((current, _)) if num >= current => {}
                        _ => best = Some((num, position)),
                    }
                }
            }
            best.map(|(_, pos)| AggregateValue::from_number(pos as f64))
                .unwrap_or_else(AggregateValue::empty)
        }
        AggregateKind::ArgMax => {
            let mut best: Option<(f64, usize)> = None;
            let mut position = 0usize;
            for value in values {
                if let Some(num) = parse_float(value) {
                    if !num.is_finite() {
                        continue;
                    }
                    position += 1;
                    match best {
                        Some((current, _)) if num <= current => {}
                        _ => best = Some((num, position)),
                    }
                }
            }
            best.map(|(_, pos)| AggregateValue::from_number(pos as f64))
                .unwrap_or_else(AggregateValue::empty)
        }
    }
}

fn numeric_result<F>(values: &[&str], compute: F) -> AggregateValue
where
    F: Fn(&[f64]) -> Option<f64>,
{
    let numbers = collect_numeric(values);
    compute(&numbers)
        .map(AggregateValue::from_number)
        .unwrap_or_else(AggregateValue::empty)
}

fn collect_numeric(values: &[&str]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|value| parse_float(value))
        .filter(|num| num.is_finite())
        .collect()
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    } else {
        Some(sorted[mid])
    }
}

fn trimmed_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let trim = ((n as f64) * 0.1).floor() as usize;
    if trim == 0 || trim * 2 >= n {
        let sum: f64 = sorted.iter().sum();
        return Some(sum / n as f64);
    }
    let slice = &sorted[trim..(n - trim)];
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

fn stddev(values: &[f64]) -> Option<f64> {
    variance(values).map(|var| var.sqrt())
}

fn variance(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let diff = v - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(var.max(0.0))
}

fn abs_min(values: &[f64]) -> Option<f64> {
    let mut best: Option<(f64, f64)> = None;
    for &value in values {
        let abs_val = value.abs();
        match best {
            Some((_, current_abs)) if abs_val >= current_abs => {}
            _ => best = Some((value, abs_val)),
        }
    }
    best.map(|(value, _)| value)
}

fn abs_max(values: &[f64]) -> Option<f64> {
    let mut best: Option<(f64, f64)> = None;
    for &value in values {
        let abs_val = value.abs();
        match best {
            Some((_, current_abs)) if abs_val <= current_abs => {}
            _ => best = Some((value, abs_val)),
        }
    }
    best.map(|(value, _)| value)
}

fn quantile(values: &[f64], fraction: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
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

fn random_choice(values: &[&str]) -> Option<String> {
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
    values.get(index).map(|v| (*v).to_string())
}

fn build_counts(values: &[&str]) -> IndexMap<String, usize> {
    let mut counts = IndexMap::new();
    for value in values {
        let entry = counts.entry((*value).to_string()).or_insert(0);
        *entry += 1;
    }
    counts
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

pub fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.6}", value)
    }
}

pub fn parse_float(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::{AggregateKind, evaluate_row_aggregate, try_parse_aggregate_kind};

    #[test]
    fn parses_quantile_aliases() {
        let q1 = try_parse_aggregate_kind("q1").unwrap().unwrap();
        match q1 {
            AggregateKind::Quantile { fraction } => {
                assert!((fraction - 0.25).abs() < f64::EPSILON)
            }
            _ => panic!("expected quantile"),
        }
        let p95 = try_parse_aggregate_kind("p95").unwrap().unwrap();
        match p95 {
            AggregateKind::Quantile { fraction } => {
                assert!((fraction - 0.95).abs() < f64::EPSILON)
            }
            _ => panic!("expected quantile"),
        }
    }

    #[test]
    fn aggregate_sum_over_values() {
        let values = vec!["1", "2", "3"];
        let result = evaluate_row_aggregate(&AggregateKind::Sum, &values);
        assert_eq!(result.text, "6");
    }

    #[test]
    fn aggregate_mode_prefers_first_encounter() {
        let values = vec!["A", "B", "A", "C", "B", "A"];
        let result = evaluate_row_aggregate(&AggregateKind::Mode, &values);
        assert_eq!(result.text, "A");
    }

    #[test]
    fn aggregate_entropy_handles_empty() {
        let empty: Vec<&str> = Vec::new();
        let result = evaluate_row_aggregate(&AggregateKind::Entropy, &empty);
        assert!(result.text.is_empty());
    }

    #[test]
    fn try_parse_non_aggregate_returns_none() {
        assert!(try_parse_aggregate_kind("abs").unwrap().is_none());
    }
}
