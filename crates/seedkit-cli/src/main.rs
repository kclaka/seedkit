use clap::Parser;
use tracing_subscriber::EnvFilter;

mod args;
mod commands;

use args::{Cli, Command};

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();

    // Load .env file if present
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    if cli.verbose {
        // Re-init with debug level
        // (In practice, would need to handle this before init, but this is fine for now)
    }

    let result = match &cli.command {
        Command::Generate(args) => commands::generate::run(args).await,
        Command::Introspect(args) => commands::introspect::run(args).await,
        Command::Preview(args) => commands::preview::run(args).await,
        Command::Check(args) => commands::check::run(args).await,
        Command::Graph(args) => commands::graph::run(args).await,
        Command::Sample(args) => commands::sample::run(args).await,
    };

    if let Err(err) = result {
        eprintln!("Error: {:#}", err);
        std::process::exit(1);
    }
}
