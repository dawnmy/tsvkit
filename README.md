# tsvkit

`tsvkit` is a fast, ergonomic toolkit for working with tab-separated values. Written in Rust, it brings familiar data-wrangling verbs (join, cut, filter, mutate, summarize, reshape, slice, pretty-print) to the command line with consistent column selection, rich expressions, and streaming-friendly performance. All commands accept input from files or `-` for stdin and transparently read `.tsv`, `.tsv.gz`, and `.tsv.xz` archives.

## Installation

```bash
cargo build --release
# binary available at target/release/tsvkit
```

Run any command with `--help` to see detailed usage and concrete examples:

```bash
tsvkit --help
tsvkit join --help
```

## Sample Data

The repository ships curated example tables under `examples/` that are used throughout this guide.

| File | Description |
| ---- | ----------- |
| `samples.tsv` | Sample-level metrics (purity, yields, contamination) for six RNA-seq libraries. |
| `subjects.tsv` | Subject demographics linked to samples via `subject_id`. |
| `expression.tsv` | Long-form gene expression measurements (`sample_id`, `gene`, `expression`). |
| `cytokines.tsv` | Wide cytokine panel with one row per sample. |
| `qc.tsv` | Sequencing QC metrics (reads, mapped percentage, duplication rate). |

You can download the repository, inspect the files directly, or adapt them to your own pipelines.

## Quick Start Pipeline

Join sample and subject metadata, derive a total cytokine score, filter high-purity case samples, and pretty-print the result:

```bash
cat examples/cytokines.tsv \
  | tsvkit mutate -e 'total=sum($IL6:$IL10)' -e 'log_total=log2($total)' \
  | tsvkit join -f 'sample_id;sample_id' -k 0 - examples/samples.tsv \
  | tsvkit filter -e '$group == "case" & $purity >= 0.94' \
  | tsvkit cut -f 'sample_id:IL10,log_total:purity' \
  | tsvkit pretty
```

> Tip: wrap every `-e` expression in single quotes so your shell keeps `$column` selectors intact. Inside an expression, always prefix column references with `$` (e.g. `$total`, `$1`).

_Output_
```
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
| sample_id | IL6 | TNF | IFNG | IL10 | log_total | subject_id | group | timepoint | purity |
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
| S01       | 4.2 | 3.1 | 6.8  | 2.4  | 4.044394  | P001       | case  | baseline  | 0.94   |
| S03       | 4.9 | 3.6 | 7.4  | 2.6  | 4.209453  | P001       | case  | week4     | 0.96   |
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
```

The same pipeline works if the cytokine table is compressed (`examples/cytokines.tsv.gz`).

## Command Overview

The table below lists every `tsvkit` subcommand and a one-line purpose summary; each item links to the detailed section later in this guide.

- [`cut`](#cut) — select or reorder columns via names, indices, or ranges.
- [`filter`](#filter) — keep rows matching an expression (math, logic, regex, functions).
- [`join`](#join) — join multiple TSVs on key columns with parallel loading and fill values.
- [`mutate`](#mutate) — create or overwrite columns using expressions, aggregates, and string helpers.
- [`summarize`](#summarize) — group rows and compute aggregates (mean, median, quantiles, etc.).
- [`sort`](#sort) — sort rows by one or more keys with numeric/text modifiers.
- [`melt`](#melt) — convert wide tables into tidy long form with `variable/value` pairs.
- [`pivot`](#pivot) — convert long form back to wide with optional fill value for missing cells.
- [`slice`](#slice) — extract rows by 1-based indices or ranges.
- [`pretty`](#pretty) — render aligned, boxed tables for quick inspection or sharing.
- [`excel`](#excel) — inspect, preview, export, or build `.xlsx` workbooks.
- [`csv`](#csv) — convert delimited text to TSV with custom separators.

The following sections cover shared concepts first, then dive into each command with practical examples.

## Core Concepts

These conventions appear across the toolkit; understanding them once makes each subcommand predictable.

### Column selectors

- **Names**: `sample_id,purity`
- **1-based indices**: `1,4,9`
- **Ranges**: `IL6:IL10`, `2:5`
- **Mixed lists**: `sample_id,3:5,tech`
- **Multi-file specs**: separate selectors for each input with semicolons, e.g. `sample_id;subject_id` for `join -f`.

Anywhere you access column *values* inside an expression, prefix the selector with `$` (`$purity`, `$1`, `$IL6:$IL10`).

### Expression language

- Quote each `-e` argument with single quotes so the shell leaves `$column` references untouched.
- Use double quotes inside expressions for string literals (`"case"`).
- Arithmetic operators: `+ - * /`; comparisons: `== != < <= > >=`.
- Logical combinators: `&` and `|` (or `and`/`or`); negation: `!`/`not`.
- Regex operators: `~` (match) and `!~` (does not match). Patterns follow Rust's `regex` crate syntax.
- Numeric helpers: `abs`, `sqrt`, `exp`, `exp2`, `ln`, `log`/`log10`, `log2`.
- String helpers in `mutate`: `sub(column, pattern, replacement)` and the sed-inspired `s/cols/pattern/replacement/` syntax.

### Aggregations

Aggregators support descriptive statistics in `summarize` and row-wise calculations in `mutate`:

- Totals and averages: `sum`, `mean`/`avg`
- Spread: `sd`/`std`
- Medians: `median`/`med`
- Quantiles: `q1`, `q2`, `q3`, `q4`, `q0.25`, `p95`, etc. (`q` = fraction, `p` = percentile)

### CLI conventions

- Every command accepts files or `-` (stdin) and auto-detects `.tsv`, `.tsv.gz`, and `.tsv.xz` inputs.
- Add `-H/--no-header` when your data lacks a header row; selectors fall back to 1-based indices.
- Commands are stream-friendly—pipe them freely to build larger workflows.
- Use `-C/--comment-char`, `-E/--ignore-empty-row`, and `-I/--ignore-illegal-row` on any subcommand to control how input lines are filtered before processing.
- `tsvkit join` parallelizes input loading; control the worker count with `-t/--threads` (defaults to the lesser of 8 and the available CPU cores).
- `--fill TEXT` lets join, melt, pivot (and others) swap empty cells for a custom placeholder.

---

## Command Reference

Each subsection highlights the core options, shows realistic invocations, and calls out relevant selectors or expressions.

### `cut`

Select or reorder columns by name, index, or range.

```bash
tsvkit cut -f 'sample_id,group,purity,tech' examples/samples.tsv
```

Ranges expand consecutive columns automatically:

```bash
tsvkit cut -f 'sample_id,IL6:IL10' examples/cytokines.tsv
```

### `filter`

Filter rows with boolean logic, arithmetic, and regexes.

```bash
tsvkit filter -e '$group == "case" & $purity >= 0.94' examples/samples.tsv
```

Regex operators `~` / `!~` make pattern filters concise:

```bash
tsvkit filter -e '$tech ~ "sRNA"' examples/samples.tsv
```

### `join`

Merge tables on shared keys. Provide selectors with `-f/--fields`; when all inputs use the same key you can specify it once.

```bash
tsvkit join -f subject_id examples/samples.tsv examples/subjects.tsv
```

Control join type with `-k` (`-k 0` = full outer). Use `-F/--select` to specify output columns (defaults to all non-key columns); syntax mirrors `-f`. `--fill TEXT` supplies placeholders for missing combinations, while `--sorted` streams pre-sorted data. `tsvkit join` trims unused columns before indexing, and `-t/--threads` (default up to 8) balances throughput and resource usage.

### `mutate`

Create derived columns or rewrite values using expressions.

```bash
tsvkit mutate \
  -e 'total=sum($IL6:$IL10)' \
  -e 'log_total=log2($total)' \
  -e 'label=sub($sample_id,"S","Sample_")' \
  examples/cytokines.tsv
```

Apply in-place edits with the sed-style form:

```bash
tsvkit mutate -e 's/$group/ctrl/control/' examples/samples.tsv
```

### `summarize`

Group rows and compute descriptive statistics.

```bash
tsvkit summarize \
  -g group \
  -s 'purity=mean,sd' \
  -s 'dna_ug:contamination_pct=q1,q3' \
  examples/samples.tsv
```

### `sort`

Sort rows by one or more keys. Modifiers: `:n` (numeric), `:nr` (numeric descending), `:r` (reverse text).

```bash
tsvkit sort -k purity:nr -k contamination_pct examples/samples.tsv
```

### `melt`

Convert wide tables into tidy long form. Add `--fill TEXT` to substitute blanks with a chosen value.

```bash
tsvkit melt -i sample_id -v IL6:IL10 examples/cytokines.tsv
```

### `pivot`

Promote long-form values to columns. `-c/--column` also accepts the short alias `-f`, and `--fill TEXT` sets a default value for missing combinations.

```bash
tsvkit pivot -i gene -c sample_id -v expression examples/expression.tsv
```

### `slice`

Take specific rows (1-based indices or ranges).

```bash
tsvkit slice -r 1,4:5 examples/samples.tsv
```

### `pretty`

Render aligned, boxed output for quick inspection.

```bash
tsvkit filter -e '$group == "case"' examples/samples.tsv | tsvkit pretty
```

### `excel`

Inspect `.xlsx` workbooks, preview sheets, export ranges as TSV, or assemble new workbooks from TSV inputs.

- **List sheets** with row/column counts and inferred column types:

  ```bash
  tsvkit excel --sheets reports.xlsx
  ```

- **Preview** the first rows of every sheet (header + N rows). Use `-s` to focus on one sheet, `-n` to change the window, `--formulas` to show Excel formulas instead of values, and `--dates raw|excel|iso` to control date rendering (`iso` is the default):

  ```bash
  tsvkit excel --preview reports.xlsx -n 5 -s Summary
  tsvkit excel --preview reports.xlsx --formulas --dates raw
  ```

- **Dump** a sheet (or subset) to TSV. Columns accept names, indices, or Excel letters/ranges (e.g. `A:C,Expr`). Rows accept 1-based indices or inclusive ranges (`1,10:20,100:`). `--na` replaces blanks, `--escape-*` makes TSV-safe output, and the same `--values/--formulas` + `--dates` controls apply:

  ```bash
  tsvkit excel --dump reports.xlsx -s Data -f 'A:C,Expr' -r 1:100 --na NA > data.tsv
  ```

- **Load** one or more TSV files into a new workbook. Each `--load TSV` must be followed by `-s SHEETNAME`. Use `-H` when the TSV lacks headers, `--fields` to supply header names in that case, `--types infer|string` to control numeric inference, `--dates excel|iso|raw` to influence how date strings are written, `--na` to treat specific tokens as blanks, and `--max-rows-per-sheet` to split very tall sheets (defaults to Excel's 1,048,576 rows):

  ```bash
  tsvkit excel --load expr.tsv -s Expr meta.tsv -s Metadata -o result.xlsx
  tsvkit excel --load ids.tsv -s IDs -o ids.xlsx --types string
  ```

Only `.xlsx` files are supported at the moment. Sheets created via `--load` are renamed `Name (2)`, `Name (3)`, … when row limits force splits.
When `-s` is omitted the sheet falls back to its 1-based position (`1`, `2`, …) in the load order, so a mixture of named and unnamed inputs still yields deterministic sheet names.

### `csv`

Convert delimited text into TSV. Use `--delim` to specify the input delimiter (default `,`) and `-H` when the source has no header row. The converter also mirrors common TSV-reader switches:

- `-C/--comment-char` skips comment lines.
- `-E/--ignore-empty-row` drops blank lines.
- `-I/--ignore-illegal-row` skips rows whose width differs from the header/first row.
- `--na STR` substitutes a string for empty fields (left blank by default).
- `--lazy-quotes` treats stray quotes literally rather than erroring out.

Compressed inputs work via the same auto-detection as other commands.

```bash
tsvkit csv examples/data.csv > examples/data.tsv
tsvkit csv examples/semicolon.csv --delim ';' -H > tmp.tsv
```

## Additional Tips

- `tsvkit` automatically detects `.tsv`, `.tsv.gz`, and `.tsv.xz`. Pipe from `curl`/`zcat` for other formats.
- Numeric functions treat empty cells as missing; regex syntax follows Rust's `regex` crate.
- For massive joins, pre-sort inputs and use `join --sorted` to keep memory usage flat.

## Contributing

Issues and pull requests are welcome. If you extend the toolkit, please add regression tests (`cargo test`) and update the documentation. For large feature ideas (new subcommands, storage backends), start a discussion first.
