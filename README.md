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
  | tsvkit mutate -e "total=sum($IL6:$IL10)" -e "log_total=log2(total)" \
  | tsvkit join -f sample_id;sample_id -k 0 - examples/samples.tsv \
  | tsvkit filter -e '$group == "case" & $purity >= 0.94' \
  | tsvkit pretty
```

The same pipeline works if the cytokine table is compressed (`examples/cytokines.tsv.gz`).

---

## Command Reference

Each subsection highlights the core options and shows realistic invocations using the example dataset.

### `join`

Merge TSV files on key columns. Provide selectors with `-f/--fields`; when all files use the same key, specify it once. Every file must contribute the same number of key columns. Use `-s/--select` to control which non-key columns are emitted per file (defaults to all). Provide selectors with `-f/--fields`; when all files use the same key, specify it once. Every file must contribute the same number of key columns.

```bash
tsvkit join \
  -f subject_id;subject_id \
  examples/samples.tsv examples/subjects.tsv
```
_Output (first rows)_
```
subject_id	sample_id	group	timepoint	purity	dna_ug	rna_ug	contamination_pct	tech	age	sex	site	bmi	smoker_status
P001	S01	case	baseline	0.94	25.3	18.1	0.02	sRNA-seq	45	F	Seattle	27.1	former
P001	S03	case	week4	0.96	27.4	19.8	0.01	sRNA-seq	45	F	Seattle	27.1	former
```

Use `-k 0` for a full outer join, or comma-separated indices (e.g., `-k 1,3`) to retain keys from selected files. Add `--sorted` when all inputs are pre-sorted by the join key to stream without buffering entire tables.

Limit the output columns per file with `-s/--select` (same syntax as `-f/--fields`):

```bash
tsvkit join \
  -f subject_id;subject_id \
  -s 'sample_id,group;age,sex' \
  examples/samples.tsv examples/subjects.tsv
```

### `mutate`

Create new columns or rewrite existing ones with row-wise expressions. Numeric helpers accept column ranges (`$IL6:$IL10`) or individual columns.

```bash
tsvkit mutate \
  -e "total_cytokines=sum($IL6:$IL10)" \
  -e "scaled=mean($IL6:$IL10)" \
  -e "label=sub($sample_id,\"S\",\"Sample_\")" \
  examples/cytokines.tsv
```
_Output_
```
sample_id	IL6	TNF	IFNG	IL10	total_cytokines	scaled	label
S01	4.2	3.1	6.8	2.4	16.5	4.125000	Sample_01
S02	3.9	2.7	5.5	2.0	14.1	3.525000	Sample_02
```

In-place substitution uses `s/columns/pattern/replacement/` syntax:

```bash
tsvkit mutate -e 's/$group/ctrl/control/' examples/samples.tsv
```

### `slice`

Select rows by index or range (headers are always preserved when present).

```bash
tsvkit slice -r 1,4:5 examples/samples.tsv
```

Use ranges to grab larger spans, e.g. `tsvkit slice -r 10:20,100 data.tsv`.

---

### `summarize`

Group rows and compute descriptive statistics. Omitting `-g` aggregates across the whole table.

```bash
tsvkit summarize \
  -g group \
  -s 'purity=mean,sd' \
  -s 'dna_ug:contamination_pct=q1,q3' \
  examples/samples.tsv
```
_Output_
```
group	purity_mean	purity_sd	dna_ug_q1	dna_ug_q3	contamination_pct_q1	contamination_pct_q3
case	0.926667	0.034435	20.700000	26.650000	0.015000	0.045000
control	0.910000	0.010000	22.850000	24.000000	0.020000	0.032500
```

Quantiles accept aliases (`q1`, `q2`, `q75`, `p95`) or fractions (`q0.25`).

### `filter`

Filter with boolean expressions, regex matches, arithmetic, and numeric functions.

```bash
tsvkit filter \
  -e '$group == "case" & $purity >= 0.94 & log2($dna_ug) > 4.6' \
  examples/samples.tsv
```

Regex operators `~` and `!~` make pattern filters concise:

```bash
tsvkit filter -e '$tech ~ ^sRNA' examples/samples.tsv
```

### `cut`

Select/reorder columns by name, index, or range.

```bash
tsvkit cut -f 'sample_id,group,purity,tech' examples/samples.tsv
```

Ranges expand automatically:

```bash
tsvkit cut -f 'sample_id,IL6:IL10' examples/cytokines.tsv
```

### `sort`

Sort by one or more keys. Modifiers: `:n` (numeric asc), `:nr` (numeric desc), `:r` (reverse text).

```bash
tsvkit sort -k purity:nr -k contamination_pct examples/samples.tsv
```

### `mutate`

Create new columns or rewrite existing ones using row-wise expressions. Numeric helpers accept column ranges and ignore non-numeric values. String helpers such as `sub()` or `s/.../.../.../` make cleanup easy.

```bash
tsvkit mutate \
  -e "total=sum($IL6:$IL10)" \
  -e "label=sub($sample_id,\"S\",\"Sample_\")" \
  examples/cytokines.tsv
```
_Output_
```
sample_id	IL6	TNF	IFNG	IL10	total	label
S01	4.2	3.1	6.8	2.4	16.5	Sample_01
S02	3.9	2.7	5.5	2.0	14.1	Sample_02
```

In-place substitution uses a sed-like syntax:

```bash
tsvkit mutate -e 's/$group/control/CTRL/' examples/samples.tsv
```

---

### `melt`

Convert wide tables into tidy long form.

```bash
tsvkit melt -i sample_id -v IL6:IL10 examples/cytokines.tsv
```
_Output_
```
sample_id	variable	value
S01	IL6	4.2
S01	TNF	3.1
```

### `pivot`

Promote long-form values to columns.

```bash
tsvkit pivot -i gene -c sample_id -v expression examples/expression.tsv
```

### `slice`

Select rows by index or range (headers are always preserved when present).

```bash
tsvkit slice -r 1,4:5 examples/samples.tsv
```

### `pretty`

Render aligned, boxed tables for quick inspection.

```bash
tsvkit filter -e '$group == "case"' examples/samples.tsv \
  | tsvkit pretty
```

---

## Advanced Tips

- **Compressed input**: `tsvkit` automatically detects `.gz` and `.xz` files. Pipe from `curl`/`zcat` for other formats.
- **Column selectors**: Anywhere you select columns you can use names, 1-based indices, ranges (`colA:colD`, `2:6`), or mixes (`$sample1:$sample3,$dna_ug`).
- **Expressions**: Numeric functions ignore empty/non-numeric values; string helpers understand double-quoted literals with standard escapes.
- **Streaming joins**: Combine `sort` + `join --sorted` to handle very large datasets with constant memory.

## Contributing

Issues and pull requests are welcome. If you extend the toolkit, please add regression tests (`cargo test`) and update the documentation. For large feature ideas (new subcommands, storage backends), start a discussion first.
