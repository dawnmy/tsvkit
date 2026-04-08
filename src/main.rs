use anyhow::Result;
use clap::{Parser, Subcommand};
use std::env;
use std::io;

mod aggregate;
mod common;
mod csv;
mod cut;
mod excel;
mod expression;
mod filter;
mod head;
mod info;
mod join;
mod melt;
mod mutate;
mod pivot;
mod pretty;
mod slice;
mod sort;
mod summarize;
mod transpose;

#[derive(Parser)]
#[command(
    name = "tsvkit",
    version,
    about = "High-level TSV toolkit: join, filter, reshape, summarize.",
    long_about = "tsvkit is a Swiss-army knife for tab-separated data. It ships focused subcommands for joins, filtering, column selection, statistics, reshaping, spreadsheet interop, and pretty-printing.\n\nQuick start workflow:\n  1) Inspect columns and value types\n     tsvkit info data.tsv\n  2) Keep only columns you need\n     tsvkit cut -f 'sample_id,group,score' data.tsv\n  3) Filter rows with an expression\n     tsvkit filter -e '$score >= 80 & $group == \"case\"' data.tsv\n  4) Sort results and preview\n     tsvkit sort -k score:nr data.tsv | tsvkit head -n 20 | tsvkit pretty\n\nMost commands are header-aware by default and share these safety flags:\n  -H/--no-header        treat input as headerless\n  -C/--comment-char     skip comment-prefixed lines (default '#')\n  -E/--ignore-empty-row skip fully empty rows\n  -I/--ignore-illegal-row skip inconsistent-width rows\n\nFile inputs are auto-detected as plain `.tsv`, `.tsv.gz`, or `.tsv.xz` whenever a subcommand reads TSV data.\n\nGet detailed usage for any subcommand:\n  tsvkit <subcommand> --help\n\nExamples:\n  tsvkit join --help\n  tsvkit summarize --help\n  tsvkit excel --help",
    author = "tsvkit"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Join multiple TSV files on matching keys (inner/left/right/full)
    Join(join::JoinArgs),
    /// Summarize numeric/text columns with grouped statistics
    Summarize(summarize::SummarizeArgs),
    /// Select, reorder, and inject columns (including file metadata)
    Cut(cut::CutArgs),
    /// Pretty-print TSV as an aligned terminal table
    Pretty(pretty::PrettyArgs),
    /// Filter rows using boolean expressions on column values
    Filter(filter::FilterArgs),
    /// Pivot long/tidy data into wide format
    Pivot(pivot::PivotArgs),
    /// Melt wide tables into long/tidy format
    Melt(melt::MeltArgs),
    /// Sort rows by one or more text/numeric key columns
    Sort(sort::SortArgs),
    /// Add derived columns or rewrite values with expressions/substitutions
    Mutate(mutate::MutateArgs),
    /// Print the first N rows (header-aware)
    Head(head::HeadArgs),
    /// Transpose rows and columns
    Transpose(transpose::TransposeArgs),
    /// Slice rows by 1-based index/ranges (supports from-end selectors)
    Slice(slice::SliceArgs),
    /// Excel helpers: inspect sheets, preview, dump TSV, or load TSV into xlsx
    Excel(excel::ExcelArgs),
    /// CSV utilities (convert CSV/TSV-like delimited text into TSV)
    Csv(csv::CsvArgs),
    /// Inspect table shape, schema hints, and sample values
    Info(info::InfoArgs),
}

fn main() -> Result<()> {
    let raw_args: Vec<_> = env::args_os().collect();
    let cli = Cli::parse_from(raw_args.clone());
    let result = match cli.command {
        Commands::Join(args) => join::run(args),
        Commands::Summarize(args) => summarize::run(args),
        Commands::Cut(args) => cut::run(args),
        Commands::Pretty(args) => pretty::run(args),
        Commands::Filter(args) => filter::run(args),
        Commands::Pivot(args) => pivot::run(args),
        Commands::Melt(args) => melt::run(args),
        Commands::Sort(args) => sort::run(args),
        Commands::Mutate(args) => mutate::run(args),
        Commands::Head(args) => head::run(args),
        Commands::Transpose(args) => transpose::run(args),
        Commands::Slice(args) => slice::run(args),
        Commands::Excel(args) => excel::run(args, &raw_args),
        Commands::Csv(args) => csv::run(args),
        Commands::Info(args) => info::run(args),
    };

    if let Err(err) = &result {
        if is_broken_pipe(err) {
            return Ok(());
        }
    }

    result
}

fn is_broken_pipe(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            io_err.kind() == io::ErrorKind::BrokenPipe
        } else {
            false
        }
    })
}
