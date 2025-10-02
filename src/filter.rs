use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::common::{default_headers, reader_for_path};
use crate::expression::{bind_expression, evaluate, parse_expression};

#[derive(Args, Debug)]
#[command(
    about = "Filter TSV rows using boolean expressions",
    long_about = r#"Filter rows using expressions with column references ($name or $index), comparisons, logical operators, arithmetic, regex (~ and !~), and numeric functions (abs, sqrt, exp, exp2, ln, log, log10, log2). Wrap each -e argument in single quotes so the shell preserves $column selectors; inside the expression, use double quotes around string literals. Defaults to header-aware mode; add -H for headerless input.

Examples:
  tsvkit filter -e '$sample2>=5 & $sample3!=9' examples/profiles.tsv
  tsvkit filter -e '$kingdom ~ "^Bact"' examples/abundance.tsv
  tsvkit filter -e 'log2($coverage) > 10' reads.tsv"#
)]
pub struct FilterArgs {
    /// Input TSV file (use '-' for stdin; compressed files supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Filter expression (e.g. `$purity>=0.9 & log2($dna_ug)>4`); supports `$col`/`$1` selectors, comparisons, arithmetic, regex (~ / !~), and functions (abs, sqrt, exp, exp2, ln, log, log10, log2)
    #[arg(short = 'e', long = "expr", value_name = "EXPR", required = true)]
    pub expr: String,

    /// Treat input as headerless (columns become 1-based indices only)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,
}

pub fn run(args: FilterArgs) -> Result<()> {
    let expr_ast = parse_expression(&args.expr)?;
    let mut reader = reader_for_path(&args.file, args.no_header)?;
    let mut writer = BufWriter::new(io::stdout().lock());

    if args.no_header {
        let mut records = reader.records();
        let first_record = match records.next() {
            Some(rec) => rec.with_context(|| format!("failed reading from {:?}", args.file))?,
            None => return Ok(()),
        };
        let headers = default_headers(first_record.len());
        let bound = bind_expression(expr_ast, &headers, true)?;

        if evaluate(&bound, &first_record) {
            emit_record(&first_record, &mut writer)?;
        }
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if evaluate(&bound, &record) {
                emit_record(&record, &mut writer)?;
            }
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let bound = bind_expression(expr_ast, &headers, false)?;

        if !headers.is_empty() {
            writeln!(writer, "{}", headers.join("\t"))?;
        }
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if evaluate(&bound, &record) {
                emit_record(&record, &mut writer)?;
            }
        }
    }

    writer.flush()?;
    Ok(())
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
    use crate::expression::{bind_expression, evaluate, parse_expression};
    use csv::StringRecord;

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
}
