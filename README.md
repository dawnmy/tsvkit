# tsvkit

`tsvkit` is a command–line toolkit for fast manipulation, querying, and reporting on tab-separated value (TSV) data. It is inspired by projects like `csvkit` and `csvtk`, but focuses on high–performance workflows over TSV files. The binary is written in Rust and ships five complementary subcommands:

- `join` – merge multiple TSV files on key columns.
- `summarize` – compute grouped descriptive statistics.
- `cut` – select and reorder columns by index or name.
- `pretty` – render aligned, boxed tables for on-screen inspection.
- `filter` – filter rows using an expression language similar to `awk`.

Every subcommand works with headers by default and can be toggled into headerless (`-H`) mode. The tool reads from files or `-` for standard input.

---

## Installation & Building

```
cargo build --release
# binary will be at target/release/tsvkit
```

Run without arguments to see the top-level help:

```
./target/release/tsvkit --help
```

Each subcommand also ships rich help text, e.g. `./target/release/tsvkit summarize --help`.

---

## Sample Data

The examples below reuse the following miniature datasets. You can copy them into your workspace for experimentation.

`profiles.tsv`
```
id	sample1	sample2	sample3
A	1	2	3
B	4	5	6
C	7	8	9
```

`metadata.tsv`
```
id	group
A	case
B	control
C	case
```

`abundance.tsv`
```
id	count	kingdom
B	2	Bacteria
C	1	Bacteria
D	5	Archaea
```

Save the snippets exactly as shown (tabs between fields) and run the commands from the repository root.

---

## Subcommand Reference & Examples

### `join`

Join multiple TSV files on one or more key columns. Column selectors can be header names or 1-based indices. Join fields are printed once, even when sourced from several tables. If all files share the same key specification you can provide it once (e.g. `-f id`). Use semicolons to provide different selectors per file and commas to list multiple columns within a selector.

````
./target/release/tsvkit join \
  -f id \
  metadata.tsv abundance.tsv
```
_Output_
```
id	group	count	kingdom
B	control	2	Bacteria
C	case	1	Bacteria
```

Control the join type with `-k`:

- `-k -1` (default) – inner join (only shared keys)
- `-k 0` – full outer join (keys from any file)
- `-k N` – keep all keys from the Nth file (1-based)

Full join example:

```
./target/release/tsvkit join -f id -k 0 metadata.tsv abundance.tsv
```
_Output_
```
id	group	count	kingdom
A	case		
B	control	2	Bacteria
C	case	1	Bacteria
D		5	Archaea
```

Left join keeping all metadata rows:

```
./target/release/tsvkit join -f id -k 1 metadata.tsv abundance.tsv
```

To join on multiple columns, separate them with commas. Different selectors per file require semicolons:

```
./target/release/tsvkit join -f 'sample_id,taxon;id,taxon_id' file1.tsv file2.tsv
```

Pass `-H` when the inputs have no header row.

---

### `summarize`

Group rows and compute statistics. The `-g` flag selects grouping columns, while `-s` describes which columns to summarize and which operations to apply. Column targets can include comma-separated lists (`sample1,sample3`), ranges (`sample1:sample3` or `2:5`), or mixtures. Operations are also comma-separated and can include `sum`, `mean`, `median`, `first`, `last`, `count`, `sd`, `min`, `max`.

Average all measurement columns:

```
./target/release/tsvkit summarize \
  -s 'sample1:sample3=mean' \
  profiles.tsv
```
_Output_
```
sample1_mean	sample2_mean	sample3_mean
4	5	6
```

Combine ranges and single columns with an operator list:

```
./target/release/tsvkit summarize \
  -s 'sample1,sample3=sd' \
  profiles.tsv
```

Group by metadata and calculate multi-operator summaries:

```
./target/release/tsvkit summarize \
  -g group \
  -s 'sample1=mean,sd' \
  -s 'sample2:sample3=sum' \
  profiles.tsv
```
_Output_
```
group	sample1_mean	sample1_sd	sample2_sum	sample3_sum
case	4	4.242641	10	12
control	4	0	5	6
```

Headerless data requires `-H`, and indices become the default (`1:3=mean`).

---

### `cut`

Select and reorder columns. Works on headers or 1-based indices.

```
./target/release/tsvkit cut \
  -f id,sample3,sample1 \
  profiles.tsv
```
_Output_
```
id	sample3	sample1
A	3	1
B	6	4
C	9	7
```

With no headers:

```
./target/release/tsvkit cut -H -f 3,1 data.tsv
```

---

### `pretty`

Render a table with aligned columns and box drawing characters for quick inspection.

```
./target/release/tsvkit pretty profiles.tsv
```
_Output_
```
+----+---------+---------+---------+
| id | sample1 | sample2 | sample3 |
+----+---------+---------+---------+
| A  | 1       | 2       | 3       |
| B  | 4       | 5       | 6       |
| C  | 7       | 8       | 9       |
+----+---------+---------+---------+
```

Use `-H` to suppress the header banner for raw data.

---

### `filter`

Filter rows using a mini expression language. Expressions reference columns with `$name` or `$index`, combine predicates with `&`, `|`, and `!`, and support parentheses plus comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`). Numeric literals understand scientific notation and leading minus signs; string literals require double quotes.

You can build arithmetic expressions with `+`, `-`, `*`, and `/`, chain inequalities (`1.5 < $depth/$total < 0.75`), and call single-argument numeric functions such as `abs()`, `sqrt()`, `exp()`, `ln()`, and `log()`/`log10()`.

Select rows where `sample2 >= 5` **and** `sample3 != 9`:

```
./target/release/tsvkit filter \
  -e '$sample2>=5 & $sample3!=9' \
  profiles.tsv
```

Slice headerless data by index:

```
./target/release/tsvkit filter -H -e '$2>=40' measurements.tsv
```

Use arithmetic and functions together:

```
./target/release/tsvkit filter \
  -e 'abs($v7 - $v8) > 0.5 & sqrt($v7) >= 1' \
  data.tsv
```

Boolean expressions follow short-circuit semantics. Every column reference that cannot be parsed as a number automatically falls back to string comparison.

---

## Tips

- All subcommands accept `-H` for headerless files. In that mode, column names such as `col1`, `col2`, … are auto-generated when needed.
- Use shell quoting for arguments that include spaces, commas, parentheses, or semicolons (e.g. `-f 'sample_id,taxon;id,taxon_id'`).
- `tsvkit` streams data; piping between subcommands enables complex pipelines: `tsvkit cut ... | tsvkit filter ... | tsvkit summarize ...`.
- If you hit performance bottlenecks on huge files, prefer column indices over names to skip the name lookup cost.

---

## License

This project inherits the license of the surrounding repository. Check the repository root for licensing details.
