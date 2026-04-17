use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    InputOptions, default_headers, parse_selector_list, reader_for_path, resolve_selectors,
    should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Rename header columns",
    long_about = "Rename header columns by selector assignment. Use `old=new` mappings separated by semicolons (e.g. `sample_id=id;group=cohort`) or map selector lists to name lists (`1,3,sample=[\"col1\",\"col3\",\"sample\"]`). You can also replace the entire header with `:=` (e.g. `:= [\"c1\",\"c2\"]`; `1:=...` is accepted as an alias). Selectors use normal tsvkit selector syntax (no `$` prefix).",
    after_help = "Examples:
  tsvkit rename -e 'sample_id=id;group=cohort' data.tsv
  tsvkit rename -e '1,3,sample=[\"col1\",\"col3\",\"sample\"]' data.tsv
  tsvkit rename -e ':=[\"newname1\",\"newname2\",\"newname3\"]' data.tsv
  tsvkit rename -H -e ':=col1,col2,col3' no_header_input.tsv"
)]
pub struct RenameArgs {
    /// Input TSV file (use '-' for stdin; gz/xz supported)
    #[arg(value_name = "FILE", default_value = "-")]
    pub file: PathBuf,

    /// Rename expression(s): `old=new`, `selectors=[n1,n2,...]`, and/or `:=["h1","h2",...]`
    #[arg(short = 'e', long = "expr", value_name = "EXPR", required = true)]
    pub expr: String,

    /// Treat input as headerless and create headers from --expr
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

pub fn run(args: RenameArgs) -> Result<()> {
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
        let mut headers = default_headers(expected_width);
        apply_rename_expr(&mut headers, &args.expr, true)?;
        writeln!(writer, "{}", headers.join("\t"))?;
        writeln!(
            writer,
            "{}",
            first_record.iter().collect::<Vec<_>>().join("\t")
        )?;
        for record in records {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            writeln!(writer, "{}", record.iter().collect::<Vec<_>>().join("\t"))?;
        }
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {:?}", args.file))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let expected_width = headers.len();
        let mut renamed = headers.clone();
        apply_rename_expr(&mut renamed, &args.expr, false)?;
        writeln!(writer, "{}", renamed.join("\t"))?;
        for record in reader.records() {
            let record = record.with_context(|| format!("failed reading from {:?}", args.file))?;
            if should_skip_record(&record, &input_opts, Some(expected_width)) {
                continue;
            }
            writeln!(writer, "{}", record.iter().collect::<Vec<_>>().join("\t"))?;
        }
    }

    writer.flush()?;
    Ok(())
}

fn apply_rename_expr(headers: &mut Vec<String>, expr: &str, no_header: bool) -> Result<()> {
    let clauses = split_clauses(expr)?;
    if clauses.is_empty() {
        bail!("rename expression must not be empty");
    }

    for clause in clauses {
        if let Some((left, right)) = clause.split_once(":=") {
            let lhs = left.trim();
            if !(lhs.is_empty() || lhs == ":" || lhs == "1") {
                bail!("`:=` assignment only supports ':=' or '1:=' on the left-hand side");
            }
            let names = parse_header_list(right)?;
            if names.len() != headers.len() {
                bail!(
                    "header list contains {} names but input has {} columns",
                    names.len(),
                    headers.len()
                );
            }
            *headers = names;
            continue;
        }

        let Some((left, right)) = clause.split_once('=') else {
            bail!(
                "invalid rename clause '{}': expected selector=new_name",
                clause
            );
        };
        let selectors = parse_selector_list(left.trim())?;
        let indices = resolve_selectors(headers, &selectors, no_header)?;
        if indices.is_empty() {
            bail!("selector '{}' resolved to no columns", left.trim());
        }
        let rhs = right.trim();
        if rhs.is_empty() {
            bail!("new header name must not be empty");
        }
        let names = parse_multi_names(rhs)?;
        if names.len() == 1 {
            for idx in indices {
                headers[idx] = names[0].clone();
            }
        } else {
            if names.len() != indices.len() {
                bail!(
                    "selector '{}' resolves to {} columns, but {} replacement names were provided",
                    left.trim(),
                    indices.len(),
                    names.len()
                );
            }
            for (idx, name) in indices.into_iter().zip(names.into_iter()) {
                headers[idx] = name;
            }
        }
    }

    Ok(())
}

fn split_clauses(expr: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut bracket_depth = 0usize;
    let mut prev_escape = false;

    for ch in expr.chars() {
        match ch {
            '\'' if !in_double && !prev_escape => in_single = !in_single,
            '"' if !in_single && !prev_escape => in_double = !in_double,
            '[' if !in_single && !in_double => bracket_depth += 1,
            ']' if !in_single && !in_double && bracket_depth > 0 => bracket_depth -= 1,
            ';' if !in_single && !in_double && bracket_depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    out.push(trimmed.to_string());
                }
                current.clear();
                prev_escape = false;
                continue;
            }
            _ => {}
        }
        current.push(ch);
        prev_escape = ch == '\\' && !prev_escape;
    }

    if in_single || in_double || bracket_depth != 0 {
        bail!("unterminated quote or bracket in rename expression");
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    Ok(out)
}

fn parse_header_list(raw: &str) -> Result<Vec<String>> {
    let trimmed = raw.trim();
    let inner = if trimmed.starts_with('[') && trimmed.ends_with(']') && trimmed.len() >= 2 {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };
    let parts = split_csv_like(inner)?;
    if parts.is_empty() {
        bail!("header list must not be empty");
    }
    Ok(parts)
}

fn parse_multi_names(raw: &str) -> Result<Vec<String>> {
    let names = parse_header_list(raw)?;
    if names.iter().any(|name| name.trim().is_empty()) {
        bail!("replacement names must not be empty");
    }
    Ok(names)
}

fn split_csv_like(input: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut prev_escape = false;

    for ch in input.chars() {
        match ch {
            '\'' if !in_double && !prev_escape => in_single = !in_single,
            '"' if !in_single && !prev_escape => in_double = !in_double,
            ',' if !in_single && !in_double => {
                let value = trim_quotes(current.trim())?;
                if !value.is_empty() {
                    out.push(value);
                }
                current.clear();
                prev_escape = false;
                continue;
            }
            _ => {}
        }
        current.push(ch);
        prev_escape = ch == '\\' && !prev_escape;
    }
    if in_single || in_double {
        bail!("unterminated quote in header list");
    }
    let value = trim_quotes(current.trim())?;
    if !value.is_empty() {
        out.push(value);
    }
    Ok(out)
}

fn trim_quotes(value: &str) -> Result<String> {
    if value.is_empty() {
        return Ok(String::new());
    }
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        if value.len() < 2 {
            bail!("invalid quoted header name");
        }
        return Ok(value[1..value.len() - 1].to_string());
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_mapping_and_bulk_replace() {
        let mut headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        apply_rename_expr(&mut headers, "a=alpha;c=gamma", false).unwrap();
        assert_eq!(headers, vec!["alpha", "b", "gamma"]);

        apply_rename_expr(&mut headers, ":=[\"x\",\"y\",\"z\"]", false).unwrap();
        assert_eq!(headers, vec!["x", "y", "z"]);
    }

    #[test]
    fn selector_list_can_map_to_name_list() {
        let mut headers = vec![
            "col1".to_string(),
            "col2".to_string(),
            "sample".to_string(),
            "col4".to_string(),
        ];
        apply_rename_expr(&mut headers, "1,3,sample=[\"a\",\"b\",\"c\"]", false).unwrap();
        assert_eq!(headers, vec!["a", "col2", "c", "col4"]);
    }

    #[test]
    fn split_clauses_handles_lists() {
        let parts = split_clauses("a=x; :=[\"b;c\",d]").unwrap();
        assert_eq!(parts, vec!["a=x", ":=[\"b;c\",d]"]);
    }
}
