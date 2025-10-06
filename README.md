# tsvkit

`tsvkit` is a fast, ergonomic toolkit for working with TSV tables. Written in Rust, it brings familiar data-wrangling verbs (join, cut, filter, mutate, summarize, reshape, slice, pretty-print) to the command line with consistent column selection, rich expressions, and streaming-friendly performance. The CLI is inspired by projects such as `csvtk`, `csvkit`, `datamash`, `awk`, `xsv`, and `mlr`, and many options are intentionally compatible with `csvtk` so existing users can transition quickly.

## Table of Contents
- [Overview](#overview)
  - [Key features](#key-features)
- [Installation](#installation)
- [Sample data](#sample-data)
- [Quick start pipeline](#quick-start-pipeline)
- [Command overview](#command-overview)
- [Core concepts](#core-concepts)
  - [Column selectors](#column-selectors)
  - [Streaming and file handling](#streaming-and-file-handling)
  - [Expression language essentials](#expression-language-essentials)
- [Command reference](#command-reference)
  - [`info`](#info)
  - [`cut`](#cut)
  - [`filter`](#filter)
  - [`join`](#join)
  - [`mutate`](#mutate)
  - [`summarize`](#summarize)
  - [`sort`](#sort)
  - [`melt`](#melt)
  - [`pivot`](#pivot)
  - [`slice`](#slice)
  - [`pretty`](#pretty)
  - [`excel`](#excel)
  - [`csv`](#csv)
- [Additional tips](#additional-tips)

## Overview
`tsvkit` combines versatile column selection with an expression engine for statistics, filtering, and data transformation. This makes it straightforward to generate matrices from `samtools idxstats` or `featureCounts`, compute multi-column summaries, or pipe TSV/Excel data through complex workflows without leaving the shell. Multi-sheet Excel workbooks are supported alongside `.tsv`, `.tsv.gz`, and `.tsv.xz` files.

### Key features
- Stream-friendly processing; every command reads from files or standard input and writes to standard output.
- Column selectors that accept names, 1-based indices, ranges, and multi-file specifications.
- Expression language with arithmetic, comparisons, logical operators, regex matching, and numeric helper functions.
- Aggregations for grouped summaries (`summarize`) and row-wise calculations (`mutate`).
- Excel tooling to inspect, preview, export, and assemble `.xlsx` workbooks.

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

## Sample data
Curated example tables live under `examples/` and power the walkthroughs below.

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

## Quick start pipeline
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
```text
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
| sample_id | IL6 | TNF | IFNG | IL10 | log_total | subject_id | group | timepoint | purity |
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
| S01       | 4.2 | 3.1 | 6.8  | 2.4  | 4.044394  | P001       | case  | baseline  | 0.94   |
| S03       | 4.9 | 3.6 | 7.4  | 2.6  | 4.209453  | P001       | case  | week4     | 0.96   |
+-----------+-----+-----+------+------+-----------+------------+-------+-----------+--------+
```
The same pipeline works if the cytokine table is compressed (`examples/cytokines.tsv.gz`).

## Command overview
The list below provides a one-line description of every `tsvkit` subcommand. Each item links to the detailed section later in this guide.

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

## Core concepts
These conventions appear across the toolkit; understanding them once makes each subcommand predictable.

### Column selectors
Selectors are reused in `cut`, `filter`, `join`, `mutate`, `summarize`, and others.

| Pattern | Meaning | Example |
| ------- | ------- | ------- |
| `name` | Column by header name. | `sample_id,purity` |
| `index` | 1-based column index. | `1,4,9` |
| `-index` | Column counted from the end (1 = last). | `-1,-2` |
| `start:end` | Inclusive range by name or index. Supports open ends. | `IL6:IL10`, `2:5`, `:IL10`, `IL6:` |
| `:` | Select every column in order. | `-f ':'` |
| `mixed` | Combine names, indices, and ranges. | `sample_id,3:5,tech` |
| `multi-file` | Separate selectors for each input with semicolons (primarily `join`). | `sample_id;subject_id` |
| `range in expressions` | Prefixed with `$` to access a slice of values. | `$IL6:$IL10` |

> Wrap selectors in backticks or braces to treat punctuation literally. For example, ``-f '`IL6:IL10`,`total,reads`'`` or `-f '{IL6:IL10},{total,reads}'` selects columns named `IL6:IL10` and `total,reads` instead of expanding a range or splitting on the comma.

Negative indices are also valid inside ranges: `:-2` selects every column except the final two, while `-3:` keeps the last three columns.

Anywhere you access column *values* inside an expression, prefix the selector with `$` (`$purity`, `$1`, `$IL6:$IL10`).

### Streaming and file handling
- Every command accepts files or `-` (stdin) and auto-detects `.tsv`, `.tsv.gz`, and `.tsv.xz` inputs.
- Add `-H/--no-header` when your data lacks a header row; selectors fall back to 1-based indices.
- Use `-C/--comment-char`, `-E/--ignore-empty-row`, and `-I/--ignore-illegal-row` on any subcommand to control how input lines are filtered before processing.
- `tsvkit join` parallelizes input loading; control the worker count with `-t/--threads` (defaults to the lesser of 8 and the available CPU cores).
- `--fill TEXT` lets join, melt, pivot (and others) swap empty cells for a custom placeholder.

### Expression language essentials
The same expression language powers `filter -e`, `mutate -e name=EXPR`, and regex substitutions. Wrap expressions in single quotes to protect `$columns` from the shell.

**Operators and comparisons**

| Symbol / keyword | Description | Works on |
| ---------------- | ----------- | -------- |
| `+ - * / ^` | Arithmetic operators (`^` is exponentiation, right-associative). | Numbers |
| `== != < <= > >=` | Comparisons. | Numbers or strings |
| `&` / `and` | Logical AND. | Booleans |
| `|` / `or` | Logical OR. | Booleans |
| `!` / `not` | Logical negation. | Booleans |
| `~` | Regex match. Right-hand side can be literal text or a `$range`. | Strings |
| `!~` | Regex does *not* match. | Strings |

> Reference columns whose names contain operators or punctuation with `${column-name}` inside expressions (e.g. `${dna-} - $rna_ug`). This prevents the parser from treating the characters as arithmetic.

**Numeric helper functions**

| Function | Description |
| -------- | ----------- |
| `abs(expr)` | Absolute value |
| `sqrt(expr)` | Square root |
| `exp(expr)` / `exp2(expr)` | Exponential (`e^x`) / base-2 exponential |
| `ln(expr)` | Natural logarithm |
| `log(expr)` / `log10(expr)` | Base-10 logarithm |
| `log2(expr)` | Base-2 logarithm |

Functions accept column references (`abs($purity - 1)`), constants, or subexpressions. Empty or non-numeric values yield blanks.

**Row-wise aggregation helpers**

Available within `mutate` expressions via functions such as `sum($col1:$col5)`; see the [Mutate](#mutate) section for the full list.

## Command reference
Each subsection highlights the core options, shows realistic invocations, and calls out relevant selectors or expressions.

### `info`
Get a quick, structured summary of any TSV: the overall shape plus one row per column with inferred types and sample values. The preview column defaults to the first three rows, but you can raise or lower it with `-n` (e.g. `-n 5`). Combine with `-H` when the input has no header row so the summary omits the name column.

```bash
tsvkit info examples/samples.tsv
```

_Output_
```text
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
Filter rows with boolean logic, arithmetic, column ranges, and regexes.

```bash
tsvkit filter -e '$group == "case" & $purity >= 0.94' examples/samples.tsv
```

**Expression building blocks for `filter`**

| Building block | Examples | Notes |
| -------------- | -------- | ----- |
| Column values | `$purity`, `$1`, `$IL6:$IL10` | Ranges produce a list; use in regex matches or aggregate helpers. |
| Literals | `1.25`, `"case"` | Strings use double quotes; escape inner quotes with `\"`. |
| Arithmetic | `($rna_ug - $dna_ug) / $rna_ug` | Standard precedence applies (parentheses for clarity). |
| Comparisons | `$purity >= 0.9`, `$group != "control"` | Works on numeric or string data. |
| Logical | `($purity >= 0.9) & ($group == "case")` | `&`, `|`, and `!` (or `and`, `or`, `not`). |
| Numeric functions | `log2($total)`, `sqrt($reads)` | See [Expression language essentials](#expression-language-essentials). |
| Row-wise aggregators | `sum($dna_ug:$rna_ug)`, `mode($1,$3)`, `countunique($gene:)` | Same catalog as [`summarize`](#summarize): totals, quantiles (`q*` / `p*`), variance/SD, products, entropy, argmin/argmax, membership stats. Works with ranges, lists, and open selectors. |
| Regex match | `$tech ~ "sRNA"`, `$notes !~ "(?i)fail"` | Patterns follow Rust `regex` syntax. `(?i)` enables case-insensitive matching. |
| Regex across ranges | `$gene:$notes ~ "kinase"`, `~ "control"` | When the left-hand side is omitted, `~` scans all columns. |

**Regex usage at a glance**

| Pattern | Description |
| ------- | ----------- |
| `$col ~ "^ABC"` | Keep rows where the column starts with `ABC`. |
| `$col !~ "xyz$"` | Exclude rows where the column ends with `xyz`. |
| `$A:$C ~ "kinase"` | Match if *any* column in the range contains `kinase`. |
| `~ "(?i)na"` | Match if any column (entire row) contains `na`, case-insensitive. |

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

**Mutation building blocks**

| Form | Meaning | Example |
| ---- | ------- | ------- |
| `name=EXPR` | Append a new column containing the evaluated expression. | `mean_signal=mean($sig1:$sig4)` |
| `existing=EXPR` | Overwrite an existing column with the expression result. | `purity=round($purity,2)` (via custom helper script) |
| `s/$selectors/pattern/replacement/` | Regex substitution on one or more columns (`$` optional). | `s/$group/ctrl/control/` |

**Row-wise aggregators shared by `filter` and `mutate`**

| Category | Functions (aliases) | Description |
| -------- | ------------------- | ----------- |
| Totals & centers | `sum`, `mean`/`avg`, `median`/`med`, `trimmean`, `iqr` | Numeric summaries that ignore blanks; `iqr` computes `q3 - q1`. |
| Dispersion | `sd`/`std`/`stddev`, `var`/`variance`, `entropy` | Spread metrics and Shannon entropy of the value distribution. |
| Extremes & positions | `min`, `max`, `absmin`, `absmax`, `argmin`, `argmax` | `arg*` return the 1-based position among numeric entries. |
| Membership & counts | `count`, `first`, `last`, `rand`/`random`, `unique`, `collapse`, `countunique`/`distinct`, `mode`, `antimode` | Operate on the original strings (including duplicates and blanks). |
| Products | `prod`/`product` | Multiply all numeric inputs (skips blanks and NaNs). |
| Quantiles | `q*` (`q1`, `q0.9`, `q_0_25`), `p*` (`p95`, `p99.5`) | Fractions `0–1` and percents `0–100`; underscores may replace dots. |

Aggregators accept any range, list, or open-ended selector (`sum($1,$3:)`). Non-numeric cells are skipped for numeric summaries. Results are appended as new columns unless you assign them back to an existing name.

### `summarize`
Group rows and compute descriptive statistics. Without `-g/--group`, the entire table is treated as a single group.

```bash
tsvkit summarize \
  -g group \
  -s 'purity=mean,sd' \
  -s 'dna_ug:contamination_pct=q1,q3' \
  examples/samples.tsv
```

**Aggregators supported by `summarize`**

_Counts & membership_

| Aggregator (aliases) | Description | Output type |
| -------------------- | ----------- | ----------- |
| `count` | Number of rows in the group (ignores blanks). | Numeric |
| `first` | First non-empty value encountered. | Original type |
| `last` | Last non-empty value encountered. | Original type |
| `rand` / `random` | Random value from the group. | Original type |
| `unique` | Comma-separated list of distinct values in encounter order. | String |
| `collapse` | Concatenate every value (comma-separated, includes duplicates). | String |
| `countunique` / `distinct` | Count of distinct values. | Numeric |
| `mode` | Most frequent value (ties resolved by first occurrence). | Original type |
| `antimode` | Least frequent value (ties resolved by first occurrence). | Original type |
| `entropy` | Shannon entropy calculated from value frequencies. | Numeric |

_Numeric summaries_

| Aggregator (aliases) | Description |
| -------------------- | ----------- |
| `sum` | Sum of numeric values. |
| `mean` / `avg` | Arithmetic mean. |
| `median` / `med` | Median (50th percentile). |
| `trimmean` | Mean of values after trimming 25% from each tail. |
| `iqr` | Interquartile range (`q3 - q1`). |
| `sd` / `std` / `stddev` | Sample standard deviation. |
| `var` / `variance` | Sample variance. |
| `min` / `max` | Minimum / maximum value. |
| `absmin` / `absmax` | Value with the smallest / largest absolute magnitude (returned with original sign). |
| `prod` / `product` | Product of numeric values. |
| `argmin` / `argmax` | 1-based row index within the group where the min/max occurs. |

_Quantiles_

| Pattern | Description |
| ------- | ----------- |
| `q1`, `q2`, `q3`, `q4` | Quartiles (`q2` equals the median). |
| `q0`, `q0.25`, `q0.75`, `q0.9` | Fractional quantiles between 0 and 1 (underscores allowed instead of dots). |
| `p0`, `p25`, `p95`, `p99.5` | Percentiles between 0 and 100. |

Quantile aggregators accept any `q*` (fraction) or `p*` (percent) token. Values may include decimals (`q0.05`, `p99.5`) or integers. Non-numeric cells are ignored for numeric summaries and quantiles. `absmin`, `absmax`, `mode`, `antimode`, and `entropy` inspect the original string values, so they work even without numeric conversion.

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
Take specific rows (1-based indices or ranges, including open-ended forms like `:10`, `10:`, or even `:` for everything). Negative indices count from the end, so `:-2` emits every row except the final two and `-3:` keeps the last three rows.

```bash
tsvkit slice -r 1,4:5 examples/samples.tsv
```

### `pretty`
Render aligned, boxed output for quick inspection.

```bash
tsvkit filter -e '$group == "case"' examples/samples.tsv | tsvkit pretty
```

- `--round DIGITS` (or `-r`) rounds numeric cells to the requested precision. Tiny magnitudes automatically switch to scientific
  notation so columns stay legible even when values approach zero.

### `excel`
Inspect `.xlsx` workbooks, preview sheets, export ranges as TSV, or assemble new workbooks from TSV inputs. Unless `-H/--no-header` is supplied, the first row of each sheet is treated as the header row; use that flag when you need to preview or export raw rows.

- **List sheets** (`--sheets`) with row/column counts and inferred column types:
  ```bash
  tsvkit excel --sheets examples/bioinfo_example.xlsx
  ```
  _Output_
  ```text
  1     Samples rows=21 cols=4  types=[string,string,mixed,mixed]
  2     Variants        rows=21 cols=4  types=[string,mixed,mixed,mixed]
  3     Expression      rows=21 cols=3  types=[string,mixed,mixed]
  4     Pathways        rows=11 cols=3  types=[string,string,mixed]
  5     QC      rows=21 cols=4  types=[string,mixed,mixed,mixed]
  6     ClinMetadata    rows=21 cols=4  types=[string,mixed,string,string]
  7     Taxonomy        rows=21 cols=4  types=[string,mixed,mixed,mixed]
  8     Coverage        rows=11 cols=2  types=[string,mixed]
  9     Proteomics      rows=21 cols=3  types=[string,mixed,mixed]
  10    Metabolites     rows=21 cols=2  types=[string,mixed]
  ```

- **Preview** (`--preview`) the first rows of every sheet (header + N rows by default). Use `-s` to focus on one sheet, `-n` to change the window, `--formulas` to show Excel formulas instead of values, `--dates raw|excel|iso` to control date rendering (`iso` is the default), and `--pretty` to render the preview with aligned borders. Add `-H/--no-header` when the sheet lacks a header row so the preview shows raw rows only:
  ```bash
  tsvkit excel --preview reports.xlsx -n 5 -s Summary
  tsvkit excel --preview reports.xlsx --formulas --dates raw
  tsvkit excel --preview reports.xlsx --pretty
  tsvkit excel --preview examples/bioinfo_example.xlsx -n 3 --pretty
  ```

  _Output_ (Only the first three sheets are shown below; the actual output has ten.)
  ```text
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
  ```text
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

Only `.xlsx` files are supported at the moment. Sheets created via `--load` are renamed `Name (2)`, `Name (3)`, … when row limits force splits. When `-s` is omitted the sheet falls back to its 1-based position (`1`, `2`, …) in the load order, so a mixture of named and unnamed inputs still yields deterministic sheet names.

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

## Additional tips
- `tsvkit` automatically detects `.tsv`, `.tsv.gz`, and `.tsv.xz`. Pipe from `curl`/`zcat` for other formats.
- Numeric functions treat empty cells as missing; regex syntax follows Rust's `regex` crate.
- For massive joins, pre-sort inputs and use `join --sorted` to keep memory usage flat.
- Combine `mutate`, `filter`, and `summarize` to build complete pipelines directly on the command line.

## Contributing
Issues and pull requests are welcome!
