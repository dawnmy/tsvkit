use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::common::{
    ColumnSelector, default_headers, parse_multi_selector_spec, reader_for_path, resolve_selectors,
};

#[derive(Args, Debug)]
#[command(
    about = "Join multiple TSV files on shared key columns",
    long_about = "Join two or more TSV files on one or more key columns. Provide selectors with -f/--fields (comma-separated list; use semicolons to give per-file specs). Each file must contribute the same number of key columns. Keys default to an inner join; adjust with -k/--keep. When inputs are pre-sorted by the key, add --sorted to stream without buffering.\n\nExamples:\n  tsvkit join -f id examples/metadata.tsv examples/abundance.tsv\n  tsvkit join -f 'sample_id,taxon;id,taxon_id' file1.tsv file2.tsv\n  tsvkit join -f id -k 0 examples/metadata.tsv examples/abundance.tsv"
)]
pub struct JoinArgs {
    /// Input TSV files (use '-' for stdin)
    #[arg(value_name = "FILES", required = true)]
    pub files: Vec<PathBuf>,

    /// Join field specification, e.g. "id" or "id;name;ID" for different files
    #[arg(short = 'f', long = "fields", value_name = "SPEC", required = true)]
    pub fields: String,

    /// Treat input as having no header row
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Join strategy: default inner; use 0 for full outer or comma-separated file numbers to keep keys from those files
    #[arg(short = 'k', long = "keep", value_name = "SPEC")]
    pub keep: Option<String>,

    /// Assume inputs are sorted by the join keys and stream the join without buffering entire files
    #[arg(long = "sorted")]
    pub sorted: bool,
}

#[derive(Debug, Clone)]
enum KeepStrategy {
    Inner,
    Full,
    Files(Vec<usize>),
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

struct StreamTable {
    reader: csv::Reader<Box<dyn io::Read>>,
    headers: Vec<String>,
    join_indices: Vec<usize>,
    include_indices: Vec<usize>,
    empty_row: Vec<String>,
    current: Option<RowGroup>,
    pending: Option<csv::StringRecord>,
    width: usize,
    source: String,
}

#[derive(Clone)]
struct RowGroup {
    key: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl StreamTable {
    fn new(path: &Path, selectors: &[ColumnSelector], no_header: bool) -> Result<Self> {
        let mut reader = reader_for_path(path, no_header)?;
        let source = path.display().to_string();

        let (headers, pending, width) = if no_header {
            let mut records = reader.records();
            match records.next() {
                Some(rec) => {
                    let record = rec.with_context(|| format!("failed reading from {}", source))?;
                    let width = record.len();
                    let headers = default_headers(width);
                    (headers, Some(record), width)
                }
                None => (Vec::new(), None, 0),
            }
        } else {
            let headers = reader
                .headers()
                .with_context(|| format!("failed reading header from {}", source))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            let width = headers.len();
            (headers, None, width)
        };

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
        let empty_row = vec![String::new(); width];

        let mut table = StreamTable {
            reader,
            headers,
            join_indices,
            include_indices,
            empty_row,
            current: None,
            pending,
            width,
            source,
        };

        if table.pending.is_some() {
            // ensure first pending record has the expected width
            if let Some(ref pending) = table.pending {
                if table.width == 0 {
                    table.width = pending.len();
                    table.empty_row = vec![String::new(); table.width];
                } else if pending.len() != table.width {
                    bail!("rows in {} have inconsistent column counts", table.source);
                }
            }
        }

        Ok(table)
    }

    fn advance(&mut self) -> Result<()> {
        let first_record = match self.next_record()? {
            Some(record) => record,
            None => {
                self.current = None;
                return Ok(());
            }
        };

        let key = make_key_from_record(&first_record, &self.join_indices);
        let mut rows = vec![record_to_vec(&first_record)];

        loop {
            match self.next_record()? {
                Some(record) => {
                    let record_key = make_key_from_record(&record, &self.join_indices);
                    if record_key == key {
                        rows.push(record_to_vec(&record));
                    } else {
                        self.pending = Some(record);
                        break;
                    }
                }
                None => break,
            }
        }

        self.current = Some(RowGroup { key, rows });
        Ok(())
    }

    fn next_record(&mut self) -> Result<Option<csv::StringRecord>> {
        if let Some(record) = self.pending.take() {
            return Ok(Some(record));
        }
        match self.reader.records().next() {
            Some(rec) => {
                let record = rec.with_context(|| format!("failed reading from {}", self.source))?;
                if self.width == 0 {
                    self.width = record.len();
                    self.empty_row = vec![String::new(); self.width];
                } else if record.len() != self.width {
                    bail!("rows in {} have inconsistent column counts", self.source);
                }
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }
}

pub fn run(args: JoinArgs) -> Result<()> {
    if args.files.len() < 2 {
        bail!("join requires at least two input files");
    }

    let column_specs = parse_multi_selector_spec(&args.fields, args.files.len())?;
    validate_join_width(&column_specs)?;

    let keep = parse_keep_option(args.keep.as_deref(), args.files.len())?;

    if args.sorted {
        return stream_join(
            &args.files,
            &column_specs,
            &keep,
            args.no_header,
            !args.no_header,
        );
    }

    let mut tables = Vec::with_capacity(args.files.len());
    for (idx, path) in args.files.iter().enumerate() {
        let specs = column_specs
            .get(idx)
            .context("missing column specification for file")?;
        let table = load_table(path, specs, args.no_header)?;
        tables.push(table);
    }

    output_joined(tables, &keep, !args.no_header)
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

fn validate_join_width(column_specs: &[Vec<ColumnSelector>]) -> Result<()> {
    if column_specs.is_empty() {
        return Ok(());
    }
    let expected = column_specs[0].len();
    for (idx, specs) in column_specs.iter().enumerate() {
        if specs.len() != expected {
            bail!(
                "join fields must select the same number of columns for each file (file {} selects {}, expected {})",
                idx + 1,
                specs.len(),
                expected
            );
        }
    }
    Ok(())
}

fn make_key(row: &[String], indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|&idx| row.get(idx).cloned().unwrap_or_default())
        .collect()
}

fn parse_keep_option(spec: Option<&str>, file_count: usize) -> Result<KeepStrategy> {
    match spec.map(|s| s.trim()) {
        None | Some("") => Ok(KeepStrategy::Inner),
        Some("-1") => Ok(KeepStrategy::Inner),
        Some("0") => Ok(KeepStrategy::Full),
        Some(raw) => {
            let mut indices = Vec::new();
            for part in raw.split(',') {
                let value = part.trim();
                if value.is_empty() {
                    bail!("empty value in --keep specification");
                }
                let idx: usize = value
                    .parse()
                    .with_context(|| format!("invalid --keep value '{}'", value))?;
                if idx == 0 {
                    bail!("--keep values are 1-based (minimum is 1)");
                }
                if idx > file_count {
                    bail!(
                        "--keep index {} exceeds number of files {}",
                        idx,
                        file_count
                    );
                }
                indices.push(idx - 1);
            }

            if indices.is_empty() {
                bail!("--keep requires at least one file index");
            }

            let mut seen = HashSet::new();
            let mut unique = Vec::new();
            for idx in indices {
                if seen.insert(idx) {
                    unique.push(idx);
                }
            }

            Ok(KeepStrategy::Files(unique))
        }
    }
}

fn output_joined(tables: Vec<Table>, keep: &KeepStrategy, has_header: bool) -> Result<()> {
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
                    KeepStrategy::Inner => {
                        skip = true;
                        break;
                    }
                    KeepStrategy::Full | KeepStrategy::Files(_) => {
                        let empty = &table.empty_row;
                        row_sets.push(vec![empty]);
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

fn build_key_list(tables: &[Table], keep: &KeepStrategy) -> Vec<Vec<String>> {
    match keep {
        KeepStrategy::Inner => {
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
        }
        KeepStrategy::Full => {
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
        }
        KeepStrategy::Files(indices) => {
            let mut seen: HashSet<Vec<String>> = HashSet::new();
            let mut result = Vec::new();
            for &idx in indices {
                if let Some(table) = tables.get(idx) {
                    for key in &table.key_order {
                        if seen.insert(key.clone()) {
                            result.push(key.clone());
                        }
                    }
                }
            }
            result
        }
    }
}

fn stream_join(
    files: &[PathBuf],
    column_specs: &[Vec<ColumnSelector>],
    keep: &KeepStrategy,
    no_header: bool,
    has_header: bool,
) -> Result<()> {
    let mut tables = Vec::with_capacity(files.len());
    for (idx, path) in files.iter().enumerate() {
        let selectors = column_specs
            .get(idx)
            .context("missing column specification for file")?;
        let table = StreamTable::new(path, selectors, no_header)?;
        tables.push(table);
    }

    for table in tables.iter_mut() {
        table.advance()?;
    }

    let mut writer = BufWriter::new(io::stdout().lock());

    if has_header {
        write_stream_header(&tables, &mut writer)?;
    }

    loop {
        if matches!(keep, KeepStrategy::Inner) && tables.iter().any(|table| table.current.is_none())
        {
            break;
        }

        let Some(key) = find_min_key(&tables) else {
            break;
        };

        let (row_sets, advance_indices, should_output) = gather_row_sets(&tables, &key, keep);

        if should_output {
            write_stream_combinations(&key, &row_sets, &tables, &mut writer)?;
        }

        if advance_indices.is_empty() {
            break;
        }

        for idx in advance_indices {
            if let Some(table) = tables.get_mut(idx) {
                table.advance()?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn find_min_key(tables: &[StreamTable]) -> Option<Vec<String>> {
    tables
        .iter()
        .filter_map(|table| table.current.as_ref().map(|group| &group.key))
        .min()
        .cloned()
}

fn gather_row_sets<'a>(
    tables: &'a [StreamTable],
    key: &[String],
    keep: &KeepStrategy,
) -> (Vec<Vec<&'a Vec<String>>>, Vec<usize>, bool) {
    let mut row_sets = Vec::with_capacity(tables.len());
    let mut advance_indices = Vec::new();
    let mut present = Vec::with_capacity(tables.len());

    for (idx, table) in tables.iter().enumerate() {
        if let Some(group) = table.current.as_ref() {
            if group.key == key {
                row_sets.push(group.rows.iter().collect());
                advance_indices.push(idx);
                present.push(true);
                continue;
            }
        }
        row_sets.push(vec![&table.empty_row]);
        present.push(false);
    }

    let should_output = match keep {
        KeepStrategy::Inner => present.iter().all(|&flag| flag),
        KeepStrategy::Full => true,
        KeepStrategy::Files(indices) => indices
            .iter()
            .any(|&idx| present.get(idx).copied().unwrap_or(false)),
    };

    (row_sets, advance_indices, should_output)
}

fn write_stream_header(
    tables: &[StreamTable],
    writer: &mut BufWriter<io::StdoutLock<'_>>,
) -> Result<()> {
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
        writeln!(writer, "{}", header_fields.join("\t"))?;
    }

    Ok(())
}

fn write_stream_combinations(
    key: &[String],
    row_sets: &[Vec<&Vec<String>>],
    tables: &[StreamTable],
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
        writeln!(writer, "{}", fields.join("\t"))?;

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

fn make_key_from_record(record: &csv::StringRecord, indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|&idx| record.get(idx).unwrap_or("").to_string())
        .collect()
}

fn record_to_vec(record: &csv::StringRecord) -> Vec<String> {
    record.iter().map(|s| s.to_string()).collect()
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
