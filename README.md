# tsvkit

`tsvkit` is a fast, ergonomic toolkit for working with TSV tables. Written in Rust, it brings familiar data-wrangling verbs (join, cut, filter, mutate, summarize, reshape, slice, pretty-print) to the command line with consistent column selection, rich expressions, and streaming-friendly performance. `tsvkit` is inspired by tools such as `csvtk`, `csvkit`, `datamash`, `awk`, `xsv`, and `mlr`. Many of its options are designed to be compatible with `csvtk` (https://github.com/shenwei356/csvtk), making it easier for existing users to adopt. 

Compared with existing tools, `tsvkit` offers a more powerful and flexible way to select rows and columns. It combines versatile in- and output column selectors with an expression engine for statistics, filtering, and data transformation. This makes it possible, for example, to generate a gene expression matrix from `samtools idxstats` or `featureCounts` outputs of multiple samples in a single command:

`tsvkit join -f <Gene> -F <Count> <all sample files>`, 

to compute different summary statistics by groups across multiple columns, each with one or more stats functions:

`tsvkit summarize -g group -s 'purity=mean,sd' -s 'dna_ug:contamination_pct=max,q3' examples/samples.tsv`.

In addition, it natively supports previewing and processing multi-sheet Excel files, making it easier to work with complex datasets across both TSV and spreadsheet formats.

All commands accept input from files or `-` for stdin and transparently read `.tsv`, `.tsv.gz`, and `.tsv.xz` archives.

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
| `abundance.tsv` | Mock metagenomic counts with taxonomic kingdoms for quick pivot/stack demos. |
| `cytokines.tsv` | Wide cytokine panel with one row per sample. |
| `expression.tsv` | Long-form gene expression measurements (`sample_id`, `gene`, `expression`). |
| `metadata.tsv` | Minimal ID → group lookup for join/filter illustrations. |
| `profiles.tsv` | Wide expression profiles suitable for melt/pivot walkthroughs. |
| `qc.tsv` | Sequencing QC metrics (reads, mapped percentage, duplication rate). |
| `samples.tsv` | Sample-level metrics (purity, yields, contamination) for six RNA-seq libraries. |
| `scores.tsv` | Tidy student scores table for summarize/group-by coverage. |
| `subjects.tsv` | Subject demographics linked to samples via `subject_id`. |
| `bioinfo_example.xlsx` | Two-sheet workbook (`Samples`, `Cytokines`) built from the TSVs above for the Excel tooling. |



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

- [`info`](#info) — inspect table shape, inferred column types, and sample values.
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
- **Ranges**: `IL6:IL10`, `2:5`, open-ended forms like `:IL10` (from the first column) or `IL6:` (through the last column)
- **Whole-table**: `:` selects every column in order
- **Mixed lists**: `sample_id,3:5,tech`
- **Clustered sequences**: combine selectors such as `sample_id,quality:` or `:purity,tech`
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
- Quantiles: `q1`, `q2`, `q3`, `q0.25`, `p95`, etc. (`q` = fraction, `p` = percentile)

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

### `info`

Get a quick, structured summary of any TSV: the overall shape plus one row per column with inferred types and sample values. The preview column defaults to the first three rows, but you can raise or lower it with `-n` (e.g. `-n 5`). Combine with `-H` when the input has no header row so the summary omits the name column.

```bash
tsvkit info examples/samples.tsv
```

_Output_
```
#shape(6, 9)
index   name            type    first3
1       sample_id       str     [S01, S02, S03]
2       subject_id      str     [P001, P002, P001]
3       group           str     [case, control, case]
4       timepoint       str     [baseline, baseline, week4]
5       purity          num     [0.94, 0.90, 0.96]
6       dna_ug          num     [25.3, 22.8, 27.4]
7       rna_ug          num     [18.1, 17.5, 19.8]
8       contamination_pct       num     [0.02, 0.03, 0.01]
9       tech            str     [sRNA-seq, sRNA-seq, sRNA-seq]
```

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

Regex operators `~` / `!~` make pattern filters concise and now work across column ranges:

```bash
tsvkit filter -e '$tech ~ "sRNA"' examples/samples.tsv
tsvkit filter -e '$gene:$notes ~ "kinase"' data.tsv   # match either column
tsvkit filter -e '$expr:, $status ~ "fail"' qc.tsv     # mixed open range + column list
tsvkit filter -e '~ "control"' results.tsv             # search all columns
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

Take specific rows (1-based indices or ranges, including open-ended forms like `:10`, `10:`, or even `:` for everything).

```bash
tsvkit slice -r 1,4:5 examples/samples.tsv
```

### `pretty`

Render aligned, boxed output for quick inspection.

```bash
tsvkit filter -e '$group == "case"' examples/samples.tsv | tsvkit pretty
```

### `excel`

Inspect `.xlsx` workbooks, preview sheets, export ranges as TSV, or assemble new workbooks from TSV inputs. Unless `-H/--no-header` is supplied, the first row of each sheet is treated as the header row; use that flag when you need to preview or export raw rows.

- **List sheets** (`--sheets`) with row/column counts and inferred column types:

  ```bash
  tsvkit excel --sheets examples/bioinfo_example.xlsx
  ```

  _Output_
  ```
  1	Samples	rows=21	cols=4	types=[string,string,mixed,mixed]
  2	Variants	rows=21	cols=4	types=[string,mixed,mixed,mixed]
  3	Expression	rows=21	cols=3	types=[string,mixed,mixed]
  4	Pathways	rows=11	cols=3	types=[string,string,mixed]
  5	QC	rows=21	cols=4	types=[string,mixed,mixed,mixed]
  6	ClinMetadata	rows=21	cols=4	types=[string,mixed,string,string]
  7	Taxonomy	rows=21	cols=4	types=[string,mixed,mixed,mixed]
  8	Coverage	rows=11	cols=2	types=[string,mixed]
  9	Proteomics	rows=21	cols=3	types=[string,mixed,mixed]
  10	Metabolites	rows=21	cols=2	types=[string,mixed]
  ```

- **Preview** (`--preview`) the first rows of every sheet (header + N rows by default). Use `-s` to focus on one sheet, `-n` to change the window, `--formulas` to show Excel formulas instead of values, `--dates raw|excel|iso` to control date rendering (`iso` is the default), and `--pretty` to render the preview with aligned borders. Add `-H/--no-header` when the sheet lacks a header row so the preview shows raw rows only:

  ```bash
  tsvkit excel --preview reports.xlsx -n 5 -s Summary
  tsvkit excel --preview reports.xlsx --formulas --dates raw
  tsvkit excel --preview reports.xlsx --pretty
  ```

  ```bash
  tsvkit excel --preview examples/bioinfo_example.xlsx -n 3 --pretty
  ```

  _Output_ (Only the first three sheets are shown below, the actual output has ten)
  ```
  #1 Samples
  +----------+-------+--------+--------+
  | SampleID | Group | Purity | DNA_ug |
  +----------+-------+--------+--------+
  | S001     | Case  | 0.744  | 3.05   |
  | S002     | Case  | 0.837  | 8.59   |
  | S003     | Case  | 0.703  | 1.15   |
  +----------+-------+--------+--------+
  
  #2 Variants
  +----------+------+--------+------+
  | SampleID | SNPs | Indels | CNVs |
  +----------+------+--------+------+
  | S001     | 1217 | 130    | 13   |
  | S002     | 3191 | 241    | 19   |
  | S003     | 2463 | 210    | 8    |
  +----------+------+--------+------+
  
  #3 Expression
  +-------+-----------+--------------+
  | Gene  | Expr_Case | Expr_Control |
  +-------+-----------+--------------+
  | Gene1 | 12.46     | 9.49         |
  | Gene2 | 5.6       | 14.43        |
  | Gene3 | 6.99      | 5.05         |
  +-------+-----------+--------------+
  ...
  ```
  

- **Dump** (`--dump`) a sheet (or subset) to TSV. Columns accept names, indices, or Excel letters/ranges (e.g. `A:C,Expr`, `:C`, `C:`). Rows accept 1-based indices or inclusive ranges (`1,10:20,:25,100:`). `--na` replaces blanks, `--escape-*` makes TSV-safe output, and the same `--values/--formulas` + `--dates` controls apply. Use `-H/--no-header` when the sheet lacks a header row so column names fall back to indices:

  ```bash
  tsvkit excel --dump examples/bioinfo_example.xlsx -s Samples -f 'SampleID,Group,Purity' -r 2:3 | tsvkit pretty
  ```

  _Output_
  ```
  +----------+-------+--------+
  | SampleID | Group | Purity |
  +----------+-------+--------+
  | S002     | Case  | 0.837  |
  | S003     | Case  | 0.703  |
  +----------+-------+--------+
  ```

- **Load** (`--load`) one or more TSV files into a new workbook. Each `TSV` can be followed by `-s SHEETNAME`. Use `-H` when the TSV lacks headers, `--fields` to supply header names in that case, `--types infer|string` to control numeric inference, `--dates excel|iso|raw` to influence how date strings are written, `--na` to treat specific tokens as blanks, and `--max-rows-per-sheet` to split very tall sheets (defaults to Excel's 1,048,576 rows):

  ```bash
  tsvkit excel --load examples/samples.tsv -s Samples --load examples/cytokines.tsv -s Cytokines -o examples/testout.xlsx
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
