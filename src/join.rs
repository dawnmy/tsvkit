use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use num_cpus;
use rayon::{ThreadPoolBuilder, prelude::*};

use crate::common::{
    ColumnSelector, InputOptions, default_headers, parse_multi_selector_spec, parse_selector_list,
    reader_for_path, resolve_selectors, should_skip_record,
};

#[derive(Args, Debug)]
#[command(
    about = "Join multiple TSV files on shared key columns",
    long_about = "Join two or more TSV files on one or more key columns. Provide selectors with -f/--fields (comma-separated list; use semicolons to give per-file specs). Each file must contribute the same number of key columns. Use -F/--select to control which non-key columns are emitted per file (wrap multi-file specs in quotes). Keys default to an inner join; adjust with -k/--keep. Control parallel input loading with -t/--threads (defaults to min(8, available CPUs)). When inputs are pre-sorted by the key, add --sorted to stream without buffering.\n\nExamples:\n  tsvkit join -f id examples/metadata.tsv examples/abundance.tsv\n  tsvkit join -f 'sample_id,taxon;id,taxon_id' file1.tsv file2.tsv\n  tsvkit join -f subject_id;subject_id -F 'sample_id,group;age,sex' examples/samples.tsv examples/subjects.tsv\n  tsvkit join -f id -k 0 examples/metadata.tsv examples/abundance.tsv"
)]
pub struct JoinArgs {
    /// Input TSV files to join (use '-' to read from stdin; `.tsv`, `.tsv.gz`, `.tsv.xz` all supported)
    #[arg(value_name = "FILES", required = true)]
    pub files: Vec<PathBuf>,

    /// Join field specification; provide comma-separated selectors per file and separate files with ';' (e.g. `id,name;id,name`). Each file must contribute the same number of join columns.
    #[arg(short = 'f', long = "fields", value_name = "SPEC", required = true)]
    pub fields: String,

    /// Columns to emit from each file after the join (same selector format as --fields). Wrap the spec in quotes when using ';'. Defaults to all non-key columns.
    #[arg(short = 'F', long = "select", value_name = "SPEC")]
    pub select: Option<String>,

    /// Treat input files as headerless (columns become 1-based indices like `1`, `2`, ...)
    #[arg(short = 'H', long = "no-header")]
    pub no_header: bool,

    /// Join strategy: omit for inner joins; use `0` for full outer joins or comma-separated 1-based file numbers (e.g. `1,3`) to keep keys from specific inputs
    #[arg(short = 'k', long = "keep", value_name = "SPEC")]
    pub keep: Option<String>,

    /// Assume inputs are pre-sorted by key columns and stream without loading entire files (inner/semi joins only)
    #[arg(long = "sorted")]
    pub sorted: bool,

    /// Number of worker threads to use when loading inputs (defaults to min(8, available CPUs))
    #[arg(short = 't', long = "threads", value_name = "N")]
    pub threads: Option<usize>,

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
    pending: Option<Vec<String>>,
    source_width: usize,
    source: String,
    projection: Vec<usize>,
    input_opts: InputOptions,
}

#[derive(Clone)]
struct RowGroup {
    key: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl StreamTable {
    fn new(
        path: &Path,
        selectors: &[ColumnSelector],
        selected_columns: Option<&[ColumnSelector]>,
        no_header: bool,
        input_opts: &InputOptions,
    ) -> Result<Self> {
        let mut reader = reader_for_path(path, no_header, input_opts)?;
        let source = path.display().to_string();

        if no_header {
            let mut records = reader.records();
            let mut first_record: Option<csv::StringRecord> = None;
            let mut source_width = 0usize;
            while let Some(rec) = records.next() {
                let record = rec.with_context(|| format!("failed reading from {}", source))?;
                if should_skip_record(
                    &record,
                    input_opts,
                    if source_width == 0 {
                        None
                    } else {
                        Some(source_width)
                    },
                ) {
                    continue;
                }
                if source_width != 0 && record.len() != source_width {
                    if input_opts.ignore_illegal {
                        continue;
                    } else {
                        bail!("rows in {} have inconsistent column counts", source);
                    }
                }
                source_width = record.len();
                first_record = Some(record);
                break;
            }

            let headers_orig = default_headers(source_width);
            let join_indices_orig = resolve_selectors(&headers_orig, selectors, true)?;
            let include_indices_orig =
                resolve_include_indices(&headers_orig, selected_columns, &join_indices_orig, true)?;
            let projection_plan =
                build_projection(&headers_orig, &join_indices_orig, &include_indices_orig);
            let pending_row = first_record
                .as_ref()
                .map(|record| project_record(record, &projection_plan.projection));
            return Ok(StreamTable {
                reader,
                headers: projection_plan.projected_headers,
                join_indices: projection_plan.join_indices,
                include_indices: projection_plan.include_indices,
                empty_row: vec![String::new(); projection_plan.projection.len()],
                current: None,
                pending: pending_row,
                source_width,
                source,
                projection: projection_plan.projection,
                input_opts: input_opts.clone(),
            });
        }

        let headers_orig = reader
            .headers()
            .with_context(|| format!("failed reading header from {}", source))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let source_width = headers_orig.len();
        let join_indices_orig = resolve_selectors(&headers_orig, selectors, false)?;
        let include_indices_orig =
            resolve_include_indices(&headers_orig, selected_columns, &join_indices_orig, false)?;
        let projection_plan =
            build_projection(&headers_orig, &join_indices_orig, &include_indices_orig);
        Ok(StreamTable {
            reader,
            headers: projection_plan.projected_headers,
            join_indices: projection_plan.join_indices,
            include_indices: projection_plan.include_indices,
            empty_row: vec![String::new(); projection_plan.projection.len()],
            current: None,
            pending: None,
            source_width,
            source,
            projection: projection_plan.projection,
            input_opts: input_opts.clone(),
        })
    }

    fn advance(&mut self) -> Result<()> {
        let first_row = match self.next_record()? {
            Some(row) => row,
            None => {
                self.current = None;
                return Ok(());
            }
        };

        let key = make_key(&first_row, &self.join_indices);
        let mut rows = vec![first_row];

        loop {
            match self.next_record()? {
                Some(row) => {
                    if make_key(&row, &self.join_indices) == key {
                        rows.push(row);
                    } else {
                        self.pending = Some(row);
                        break;
                    }
                }
                None => break,
            }
        }

        self.current = Some(RowGroup { key, rows });
        Ok(())
    }

    fn next_record(&mut self) -> Result<Option<Vec<String>>> {
        if let Some(record) = self.pending.take() {
            return Ok(Some(record));
        }
        loop {
            match self.reader.records().next() {
                Some(rec) => {
                    let record =
                        rec.with_context(|| format!("failed reading from {}", self.source))?;
                    if record.len() != self.source_width {
                        if self.input_opts.ignore_illegal {
                            continue;
                        } else {
                            bail!("rows in {} have inconsistent column counts", self.source);
                        }
                    }
                    if should_skip_record(&record, &self.input_opts, Some(self.source_width)) {
                        continue;
                    }
                    return Ok(Some(project_record(&record, &self.projection)));
                }
                None => return Ok(None),
            }
        }
    }
}

pub fn run(args: JoinArgs) -> Result<()> {
    if args.files.len() < 2 {
        bail!("join requires at least two input files");
    }

    let input_opts = InputOptions::from_flags(
        &args.comment_char,
        args.ignore_empty_row,
        args.ignore_illegal_row,
    )?;

    let threads = match args.threads {
        Some(value) if value == 0 => bail!("--threads must be greater than zero"),
        Some(value) => value,
        None => default_thread_count(),
    };

    ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .with_context(|| "failed to initialize thread pool")?
        .install(|| execute_join(&args, &input_opts))
}

fn execute_join(args: &JoinArgs, input_opts: &InputOptions) -> Result<()> {
    if args.sorted {
        let column_specs = parse_multi_selector_spec(&args.fields, args.files.len())?;
        validate_join_width(&column_specs)?;
        let select_specs = parse_select_specs(args.select.as_deref(), args.files.len())?;
        let keep = parse_keep_option(args.keep.as_deref(), args.files.len())?;
        return stream_join(
            &args.files,
            &column_specs,
            &select_specs,
            &keep,
            args.no_header,
            !args.no_header,
            input_opts,
        );
    }

    let column_specs = parse_multi_selector_spec(&args.fields, args.files.len())?;
    validate_join_width(&column_specs)?;
    let select_specs = parse_select_specs(args.select.as_deref(), args.files.len())?;

    let keep = parse_keep_option(args.keep.as_deref(), args.files.len())?;

    let tables: Vec<Table> = args
        .files
        .par_iter()
        .enumerate()
        .map(|(idx, path)| -> Result<Table> {
            let specs = column_specs
                .get(idx)
                .context("missing column specification for file")?;
            let select_for_file = select_specs[idx].as_ref().map(|v| v.as_slice());
            load_table(
                path.as_path(),
                specs,
                select_for_file,
                args.no_header,
                input_opts,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    output_joined(tables, &keep, !args.no_header)
}

fn load_table(
    path: &Path,
    selectors: &[ColumnSelector],
    selected_columns: Option<&[ColumnSelector]>,
    no_header: bool,
    input_opts: &InputOptions,
) -> Result<Table> {
    let mut reader = reader_for_path(path, no_header, input_opts)?;

    let (headers, rows, join_indices, include_indices) = if no_header {
        let mut records = reader.records();
        let mut first_record: Option<csv::StringRecord> = None;
        let mut expected_width: Option<usize> = None;
        while let Some(rec) = records.next() {
            let record = rec.with_context(|| format!("failed reading from {}", path.display()))?;
            if should_skip_record(&record, input_opts, expected_width) {
                continue;
            }
            if let Some(width) = expected_width {
                if record.len() != width {
                    if input_opts.ignore_illegal {
                        continue;
                    } else {
                        bail!("rows in {} have inconsistent column counts", path.display());
                    }
                }
            }
            expected_width = Some(record.len());
            first_record = Some(record);
            break;
        }

        let expected_width = expected_width.unwrap_or(0);
        let mut headers = default_headers(expected_width);
        let join_indices_orig = resolve_selectors(&headers, selectors, true)?;
        let include_indices_orig =
            resolve_include_indices(&headers, selected_columns, &join_indices_orig, true)?;
        let projection_plan = build_projection(&headers, &join_indices_orig, &include_indices_orig);
        headers = projection_plan.projected_headers.clone();
        let mut rows = Vec::new();

        if let Some(record) = first_record {
            rows.push(project_record(&record, &projection_plan.projection));
        }

        for rec in records {
            let record = rec.with_context(|| format!("failed reading from {}", path.display()))?;
            if record.len() != expected_width {
                if input_opts.ignore_illegal {
                    continue;
                } else {
                    bail!("rows in {} have inconsistent column counts", path.display());
                }
            }
            if should_skip_record(&record, input_opts, Some(expected_width)) {
                continue;
            }
            rows.push(project_record(&record, &projection_plan.projection));
        }

        (
            headers,
            rows,
            projection_plan.join_indices,
            projection_plan.include_indices,
        )
    } else {
        let headers_orig = reader
            .headers()
            .with_context(|| format!("failed reading header from {}", path.display()))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let expected_width = headers_orig.len();
        let join_indices_orig = resolve_selectors(&headers_orig, selectors, false)?;
        let include_indices_orig =
            resolve_include_indices(&headers_orig, selected_columns, &join_indices_orig, false)?;
        let projection_plan =
            build_projection(&headers_orig, &join_indices_orig, &include_indices_orig);
        let mut rows = Vec::new();
        for rec in reader.records() {
            let record = rec.with_context(|| format!("failed reading from {}", path.display()))?;
            if record.len() != expected_width {
                if input_opts.ignore_illegal {
                    continue;
                } else {
                    bail!("rows in {} have inconsistent column counts", path.display());
                }
            }
            if should_skip_record(&record, input_opts, Some(expected_width)) {
                continue;
            }
            rows.push(project_record(&record, &projection_plan.projection));
        }
        (
            projection_plan.projected_headers,
            rows,
            projection_plan.join_indices,
            projection_plan.include_indices,
        )
    };

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

fn parse_select_specs(
    spec: Option<&str>,
    file_count: usize,
) -> Result<Vec<Option<Vec<ColumnSelector>>>> {
    if file_count == 0 {
        return Ok(Vec::new());
    }

    let raw = match spec.map(|s| s.trim()) {
        None | Some("") => return Ok(vec![None; file_count]),
        Some(text) => text,
    };

    let parts: Vec<&str> = raw.split(';').map(|s| s.trim()).collect();

    if parts.len() == 1 && file_count > 1 {
        let part = parts[0];
        if part.is_empty() {
            return Ok(vec![None; file_count]);
        }
        let parsed = parse_selector_list(part)?;
        return Ok(vec![Some(parsed); file_count]);
    }

    if parts.len() != file_count {
        bail!(
            "expected column specification for each file, got {}",
            parts.len()
        );
    }

    let mut result = Vec::with_capacity(file_count);
    for part in parts {
        if part.is_empty() {
            result.push(None);
        } else {
            result.push(Some(parse_selector_list(part)?));
        }
    }
    Ok(result)
}

fn resolve_include_indices(
    headers: &[String],
    selected_columns: Option<&[ColumnSelector]>,
    join_indices: &[usize],
    no_header: bool,
) -> Result<Vec<usize>> {
    let join_set: HashSet<usize> = join_indices.iter().cloned().collect();
    let mut include_indices = if let Some(cols) = selected_columns {
        resolve_selectors(headers, cols, no_header)?
    } else {
        headers
            .iter()
            .enumerate()
            .filter_map(|(idx, _)| (!join_set.contains(&idx)).then_some(idx))
            .collect::<Vec<_>>()
    };
    include_indices.retain(|idx| !join_set.contains(idx));
    Ok(include_indices)
}

#[derive(Debug)]
struct ProjectionPlan {
    projection: Vec<usize>,
    projected_headers: Vec<String>,
    join_indices: Vec<usize>,
    include_indices: Vec<usize>,
}

fn build_projection(
    headers: &[String],
    join_indices: &[usize],
    include_indices: &[usize],
) -> ProjectionPlan {
    let mut projection = Vec::new();
    let mut projected_headers = Vec::new();
    let mut index_map: HashMap<usize, usize> = HashMap::new();

    for &idx in join_indices {
        if let Entry::Vacant(v) = index_map.entry(idx) {
            let pos = projection.len();
            projection.push(idx);
            projected_headers.push(headers.get(idx).cloned().unwrap_or_default());
            v.insert(pos);
        }
    }

    for &idx in include_indices {
        if let Entry::Vacant(v) = index_map.entry(idx) {
            let pos = projection.len();
            projection.push(idx);
            projected_headers.push(headers.get(idx).cloned().unwrap_or_default());
            v.insert(pos);
        }
    }

    let join_positions = join_indices
        .iter()
        .map(|idx| index_map[idx])
        .collect::<Vec<_>>();
    let include_positions = include_indices
        .iter()
        .map(|idx| index_map[idx])
        .collect::<Vec<_>>();

    ProjectionPlan {
        projection,
        projected_headers,
        join_indices: join_positions,
        include_indices: include_positions,
    }
}

fn project_record(record: &csv::StringRecord, projection: &[usize]) -> Vec<String> {
    projection
        .iter()
        .map(|&idx| record.get(idx).unwrap_or("").to_string())
        .collect()
}

fn default_thread_count() -> usize {
    let available = num_cpus::get();
    std::cmp::max(1, std::cmp::min(8, available))
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
    select_specs: &[Option<Vec<ColumnSelector>>],
    keep: &KeepStrategy,
    no_header: bool,
    has_header: bool,
    input_opts: &InputOptions,
) -> Result<()> {
    let mut tables = Vec::with_capacity(files.len());
    for (idx, path) in files.iter().enumerate() {
        let selectors = column_specs
            .get(idx)
            .context("missing column specification for file")?;
        let select_for_file = select_specs
            .get(idx)
            .and_then(|opt| opt.as_ref().map(|v| v.as_slice()));
        let table = StreamTable::new(path, selectors, select_for_file, no_header, input_opts)?;
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
