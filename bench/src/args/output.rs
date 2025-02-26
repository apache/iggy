use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkOutputCommand {
    /// Output results to a directory subcommand
    Output(BenchmarkOutputArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct BenchmarkOutputArgs {
    /// Output directory path for storing benchmark results
    #[arg(long, short = 'o', default_value = "performance_results")]
    pub output_dir: String,

    /// Identifier for the benchmark run (defaults to hostname if not provided)
    #[arg(long, default_value_t = hostname::get().unwrap().to_string_lossy().to_string())]
    pub identifier: String,

    /// Additional remark for the benchmark (e.g., no-cache)
    #[arg(long)]
    pub remark: Option<String>,

    /// Extra information
    #[arg(long)]
    pub extra_info: Option<String>,

    /// Git reference (commit hash, branch or tag) used for note in the benchmark results
    #[arg(long)]
    pub gitref: Option<String>,

    /// Git reference date used for note in the benchmark results, preferably merge date of the commit
    #[arg(long)]
    pub gitref_date: Option<String>,

    /// Open generated charts in browser after benchmark is finished
    #[arg(long, short = 'c', default_value_t = false)]
    pub open_charts: bool,
}
