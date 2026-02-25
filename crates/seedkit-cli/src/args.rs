use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "seedkit",
    about = "Generate realistic, constraint-safe seed data for any database",
    version,
    after_help = "Examples:\n  seedkit generate --db postgres://localhost/myapp --rows 1000 --output seed.sql\n  seedkit generate --rows 100              # auto-detect DB from .env\n  seedkit introspect --db postgres://localhost/myapp\n  seedkit preview --db postgres://localhost/myapp\n  seedkit check --db postgres://localhost/myapp\n  seedkit graph --db postgres://localhost/myapp --format mermaid"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Generate seed data for a database
    Generate(GenerateArgs),

    /// Introspect a database schema and display analysis
    Introspect(IntrospectArgs),

    /// Preview sample generated data without inserting
    Preview(PreviewArgs),

    /// Check for schema drift against a lock file
    Check(CheckArgs),

    /// Visualize table dependency graph
    Graph(GraphArgs),
}

#[derive(Parser, Debug)]
pub struct GenerateArgs {
    /// Database connection URL (postgres://, mysql://, sqlite://)
    /// Falls back to DATABASE_URL env var or .env file
    #[arg(long, env = "DATABASE_URL")]
    pub db: Option<String>,

    /// Number of rows to generate per table
    #[arg(long, default_value = "100")]
    pub rows: usize,

    /// Output file path (.sql, .json, .csv) or "direct" for DB insertion
    #[arg(short, long)]
    pub output: Option<String>,

    /// Output format (auto-detected from file extension if not specified)
    #[arg(long)]
    pub format: Option<OutputFormat>,

    /// Random seed for deterministic generation
    #[arg(long)]
    pub seed: Option<u64>,

    /// Per-table row count overrides (e.g., users=500,orders=2000)
    #[arg(long, value_delimiter = ',')]
    pub table_rows: Vec<String>,

    /// Only generate data for these tables
    #[arg(long, value_delimiter = ',')]
    pub include: Vec<String>,

    /// Exclude these tables from generation
    #[arg(long, value_delimiter = ',')]
    pub exclude: Vec<String>,

    /// Schema name to introspect (default: public for Postgres)
    #[arg(long)]
    pub schema: Option<String>,

    /// Use LLM for enhanced semantic classification
    #[arg(long)]
    pub ai: bool,

    /// LLM model to use with --ai
    #[arg(long)]
    pub model: Option<String>,

    /// Regenerate from a lock file
    #[arg(long)]
    pub from_lock: bool,

    /// Force regeneration even if schema has changed
    #[arg(long)]
    pub force: bool,

    /// Use PostgreSQL COPY format for output (faster for large datasets)
    #[arg(long)]
    pub copy: bool,
}

#[derive(Parser, Debug)]
pub struct IntrospectArgs {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub db: Option<String>,

    /// Schema name to introspect
    #[arg(long)]
    pub schema: Option<String>,

    /// Output format
    #[arg(long, default_value = "table")]
    pub format: IntrospectFormat,
}

#[derive(Parser, Debug)]
pub struct PreviewArgs {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub db: Option<String>,

    /// Number of sample rows to preview per table
    #[arg(long, default_value = "5")]
    pub rows: usize,

    /// Schema name
    #[arg(long)]
    pub schema: Option<String>,
}

#[derive(Parser, Debug)]
pub struct CheckArgs {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub db: Option<String>,

    /// Schema name
    #[arg(long)]
    pub schema: Option<String>,

    /// Output format for drift report
    #[arg(long, default_value = "text")]
    pub format: CheckFormat,
}

#[derive(Parser, Debug)]
pub struct GraphArgs {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub db: Option<String>,

    /// Schema name
    #[arg(long)]
    pub schema: Option<String>,

    /// Output format for the dependency graph
    #[arg(long, default_value = "mermaid")]
    pub format: GraphFormat,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Sql,
    Json,
    Csv,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum IntrospectFormat {
    Table,
    Json,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum CheckFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum GraphFormat {
    Mermaid,
    Dot,
}

impl GenerateArgs {
    /// Determine output format from file extension or explicit format flag.
    pub fn output_format(&self) -> OutputFormat {
        if let Some(ref fmt) = self.format {
            return fmt.clone();
        }
        if let Some(ref path) = self.output {
            if path.ends_with(".json") {
                return OutputFormat::Json;
            } else if path.ends_with(".csv") {
                return OutputFormat::Csv;
            }
        }
        OutputFormat::Sql
    }

    /// Parse table row overrides like "users=500,orders=2000".
    /// Returns a BTreeMap for deterministic lock file serialization.
    pub fn parse_table_rows(&self) -> std::collections::BTreeMap<String, usize> {
        let mut map = std::collections::BTreeMap::new();
        for entry in &self.table_rows {
            if let Some((table, count_str)) = entry.split_once('=') {
                if let Ok(count) = count_str.parse::<usize>() {
                    map.insert(table.to_string(), count);
                }
            }
        }
        map
    }
}
