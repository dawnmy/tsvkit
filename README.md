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

Control join type with `-k` (`-k 0` = full outer). Pick which columns from each file to emit with `-F/--select`; by default every non-key column from every file is included. The selector syntax matches `-f`: separate per-file specs with semicolons (`samples_cols;subjects_cols`), and within each spec use commas/ranges to list the columns you want to keep. Add `--sorted` to stream when inputs are pre-sorted on the key.

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

Convert wide tables into tidy long form.

```bash
tsvkit melt -i sample_id -v IL6:IL10 examples/cytokines.tsv
```

### `pivot`

Promote long-form values to columns. `-c/--column` also accepts the short alias `-f` for consistency with other commands.

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

## Additional Tips

- `tsvkit` automatically detects `.tsv`, `.tsv.gz`, and `.tsv.xz`. Pipe from `curl`/`zcat` for other formats.
- Numeric functions treat empty cells as missing; regex syntax follows Rust's `regex` crate.
- For massive joins, pre-sort inputs and use `join --sorted` to keep memory usage flat.

## Contributing

Issues and pull requests are welcome. If you extend the toolkit, please add regression tests (`cargo test`) and update the documentation. For large feature ideas (new subcommands, storage backends), start a discussion first.
