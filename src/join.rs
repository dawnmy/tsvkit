use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    ColumnSelector, default_headers, parse_multi_selector_spec, reader_for_path, resolve_selectors,
};

#[derive(Args, Debug)]
pub struct JoinArgs {
    /// Input TSV files (use '-' for stdin)
    #[arg(value_name = "FILES", required = true)]
    pub files: Vec<PathBuf>,

    /// Join field specification, e.g. "id" or "id,name;id" for different files
    #[arg(short = 'f', long = "fields", value_name = "SPEC", required = true)]
    pub fields: String,

    /// Treat input as having no header row
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Join strategy: -1 (inner), 0 (full), N (keep rows from file N)
    #[arg(short = 'k', long = "keep", default_value = "-1")]
    pub keep: i32,
}

struct Table {
    headers: Vec<String>,
    join_indices: Vec<usize>,
    include_indices: Vec<usize>,
    rows: Vec<Vec<String>>,
    key_to_rows: HashMap<Vec<String>, Vec<usize>>,
    key_order: Vec<Vec<String>>,
    empty_row: Vec<String>,
}

pub fn run(args: JoinArgs) -> Result<()> {
    if args.files.len() < 2 {
        bail!("join requires at least two input files");
    }

    let column_specs = parse_multi_selector_spec(&args.fields, args.files.len())?;

    let mut tables = Vec::with_capacity(args.files.len());
    for (idx, path) in args.files.iter().enumerate() {
        let specs = column_specs
            .get(idx)
            .context("missing column specification for file")?;
        let table = load_table(path, specs, args.no_header)?;
        tables.push(table);
    }

    validate_keep_option(args.keep, tables.len())?;

    output_joined(tables, args.keep, !args.no_header)
}

fn load_table(path: &Path, selectors: &[ColumnSelector], no_header: bool) -> Result<Table> {
    let mut reader = reader_for_path(path, no_header)?;

    let (headers, rows) = if no_header {
        let mut collected = Vec::new();
        for record in reader.records() {
            let record =
                record.with_context(|| format!("failed reading from {}", path.display()))?;
            collected.push(record.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        }
        let header_len = collected.first().map(|row| row.len()).unwrap_or(0);
        let headers = default_headers(header_len);
        (headers, collected)
    } else {
        let headers = reader
            .headers()
            .with_context(|| format!("failed reading header from {}", path.display()))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut collected = Vec::new();
        for record in reader.records() {
            let record =
                record.with_context(|| format!("failed reading from {}", path.display()))?;
            collected.push(record.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        }
        (headers, collected)
    };

    if rows.iter().any(|row| row.len() != headers.len()) {
        bail!("rows in {} have inconsistent column counts", path.display());
    }

    let join_indices = resolve_selectors(&headers, selectors, no_header)?;

    let join_set: HashSet<usize> = join_indices.iter().cloned().collect();
    let include_indices = headers
        .iter()
        .enumerate()
        .filter_map(|(idx, _)| {
            if join_set.contains(&idx) {
                None
            } else {
                Some(idx)
            }
        })
        .collect::<Vec<_>>();

    let empty_row = vec![String::new(); headers.len()];
    let mut key_to_rows: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
    let mut key_order: Vec<Vec<String>> = Vec::new();

    for (idx, row) in rows.iter().enumerate() {
        let key = make_key(row, &join_indices);
        match key_to_rows.entry(key.clone()) {
            Entry::Vacant(v) => {
                key_order.push(key.clone());
                v.insert(vec![idx]);
            }
            Entry::Occupied(mut o) => {
                o.get_mut().push(idx);
            }
        }
    }

    Ok(Table {
        headers,
        join_indices,
        include_indices,
        rows,
        key_to_rows,
        key_order,
        empty_row,
    })
}

fn make_key(row: &[String], indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|&idx| row.get(idx).cloned().unwrap_or_default())
        .collect()
}

fn validate_keep_option(keep: i32, file_count: usize) -> Result<()> {
    if keep >= 1 && keep as usize > file_count {
        bail!(
            "--keep index {} exceeds number of files {}",
            keep,
            file_count
        );
    }
    Ok(())
}

fn output_joined(tables: Vec<Table>, keep: i32, has_header: bool) -> Result<()> {
    let mut writer = BufWriter::new(io::stdout().lock());

    if has_header {
        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut header_fields = Vec::new();

        if let Some(first_table) = tables.first() {
            for &idx in &first_table.join_indices {
                let original = first_table.headers.get(idx).cloned().unwrap_or_default();
                let entry = seen.entry(original.clone()).or_insert(0);
                if *entry == 0 {
                    *entry = 1;
                    header_fields.push(original);
                } else {
                    *entry += 1;
                    header_fields.push(format!("{}#{}", original, *entry));
                }
            }
        }

        for (table_idx, table) in tables.iter().enumerate() {
            for &col_idx in &table.include_indices {
                let original = table.headers.get(col_idx).cloned().unwrap_or_default();
                let entry = seen.entry(original.clone()).or_insert(0);
                if *entry == 0 {
                    *entry = 1;
                    header_fields.push(original);
                } else {
                    *entry += 1;
                    header_fields.push(format!("{}#{}", original, table_idx + 1));
                }
            }
        }
        if !header_fields.is_empty() {
            writeln!(writer, "{}", header_fields.join("	"))?;
        }
    }

    let key_list = build_key_list(&tables, keep);

    for key in key_list {
        let mut row_sets: Vec<Vec<&Vec<String>>> = Vec::with_capacity(tables.len());
        let mut skip = false;

        for table in &tables {
            if let Some(indices) = table.key_to_rows.get(&key) {
                let rows = indices
                    .iter()
                    .map(|&row_idx| &table.rows[row_idx])
                    .collect();
                row_sets.push(rows);
            } else {
                match keep {
                    -1 => {
                        skip = true;
                        break;
                    }
                    0 => {
                        row_sets.push(vec![&table.empty_row]);
                    }
                    _ => {
                        row_sets.push(vec![&table.empty_row]);
                    }
                }
            }
        }
        if skip {
            continue;
        }

        write_combinations(&key, &row_sets, &tables, &mut writer)?;
    }

    writer.flush()?;
    Ok(())
}

fn build_key_list(tables: &[Table], keep: i32) -> Vec<Vec<String>> {
    if keep == -1 {
        if let Some(first) = tables.first() {
            let mut result = Vec::new();
            'outer: for key in &first.key_order {
                for table in tables.iter().skip(1) {
                    if !table.key_to_rows.contains_key(key) {
                        continue 'outer;
                    }
                }
                result.push(key.clone());
            }
            result
        } else {
            Vec::new()
        }
    } else if keep == 0 {
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        let mut result = Vec::new();
        for table in tables {
            for key in &table.key_order {
                if seen.insert(key.clone()) {
                    result.push(key.clone());
                }
            }
        }
        result
    } else {
        let idx = (keep - 1) as usize;
        tables
            .get(idx)
            .map(|table| table.key_order.clone())
            .unwrap_or_default()
    }
}

fn write_combinations(
    key: &[String],
    row_sets: &[Vec<&Vec<String>>],
    tables: &[Table],
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
    if row_sets.is_empty() || row_sets.iter().any(|set| set.is_empty()) {
        return Ok(());
    }

    let mut indices = vec![0usize; row_sets.len()];
    loop {
        let mut fields: Vec<&str> = Vec::new();
        for value in key {
            fields.push(value.as_str());
        }
        for (table_idx, row_idx) in indices.iter().enumerate() {
            let row = row_sets[table_idx][*row_idx];
            for &col_idx in &tables[table_idx].include_indices {
                fields.push(row[col_idx].as_str());
            }
        }
        writeln!(writer, "{}", fields.join("	"))?;

        let mut level = row_sets.len();
        while level > 0 {
            level -= 1;
            indices[level] += 1;
            if indices[level] < row_sets[level].len() {
                break;
            }
            indices[level] = 0;
            if level == 0 {
                return Ok(());
            }
        }
    }
}
