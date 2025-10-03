use anyhow::Result;
use clap::{Parser, Subcommand};
use std::env;

mod common;
mod csv;
mod cut;
mod excel;
mod expression;
mod filter;
mod join;
mod melt;
mod mutate;
mod pivot;
mod pretty;
mod slice;
mod sort;
mod summarize;

#[derive(Parser)]
#[command(
    name = "tsvkit",
    version,
    about = "High-level TSV toolkit: join, filter, reshape, summarize.",
    long_about = "tsvkit is a Swiss-army knife for tab-separated data. It ships focused subcommands for joins, filtering, column selection, statistics, reshaping, and pretty-printing. Every subcommand reads from files or standard input and respects headers by default.",
    author = "tsvkit"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Join multiple TSV files on matching keys
    Join(join::JoinArgs),
    /// Summarize columns after grouping rows
    Summarize(summarize::SummarizeArgs),
    /// Select and reorder columns
    Cut(cut::CutArgs),
    /// Pretty-print the table with aligned columns
    Pretty(pretty::PrettyArgs),
    /// Filter rows using expressions on column values
    Filter(filter::FilterArgs),
    /// Pivot long data into wide format
    Pivot(pivot::PivotArgs),
    /// Melt wide data into long format
    Melt(melt::MeltArgs),
    /// Sort rows by one or more key columns
    Sort(sort::SortArgs),
    /// Add derived columns or rewrite existing ones
    Mutate(mutate::MutateArgs),
    /// Slice rows by 1-based index
    Slice(slice::SliceArgs),
    /// Excel-focused helpers (inspect, preview, convert, load)
    Excel(excel::ExcelArgs),
    /// CSV utilities (convert to TSV)
    Csv(csv::CsvArgs),
}

fn main() -> Result<()> {
    let raw_args: Vec<_> = env::args_os().collect();
    let cli = Cli::parse_from(raw_args.clone());
    match cli.command {
        Commands::Join(args) => join::run(args),
        Commands::Summarize(args) => summarize::run(args),
        Commands::Cut(args) => cut::run(args),
        Commands::Pretty(args) => pretty::run(args),
        Commands::Filter(args) => filter::run(args),
        Commands::Pivot(args) => pivot::run(args),
        Commands::Melt(args) => melt::run(args),
        Commands::Sort(args) => sort::run(args),
        Commands::Mutate(args) => mutate::run(args),
        Commands::Slice(args) => slice::run(args),
        Commands::Excel(args) => excel::run(args, &raw_args),
        Commands::Csv(args) => csv::run(args),
    }
}
