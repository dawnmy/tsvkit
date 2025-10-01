use anyhow::Result;
use clap::{Parser, Subcommand};

mod common;
mod cut;
mod filter;
mod join;
mod melt;
mod pivot;
mod pretty;
mod sort;
mod summarize;

#[derive(Parser)]
#[command(
    name = "tsvkit",
    version,
    about = "Toolkit for working with TSV files",
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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Join(args) => join::run(args),
        Commands::Summarize(args) => summarize::run(args),
        Commands::Cut(args) => cut::run(args),
        Commands::Pretty(args) => pretty::run(args),
        Commands::Filter(args) => filter::run(args),
        Commands::Pivot(args) => pivot::run(args),
        Commands::Melt(args) => melt::run(args),
        Commands::Sort(args) => sort::run(args),
    }
}
