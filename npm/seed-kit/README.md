# SeedKit

**Generate realistic, constraint-safe seed data for any database.**

SeedKit connects to your PostgreSQL, MySQL, or SQLite database, reads the schema, and generates seed data that respects foreign keys, unique constraints, check constraints, and enum types -- all without copying production data.

## Install

```bash
npm install -g @seed-kit/cli
```

Or run without installing:

```bash
npx @seed-kit/cli generate --db postgres://localhost/myapp --rows 1000
```

## Quick Start

```bash
# Generate 1000 rows per table as SQL
seedkit generate --db postgres://localhost/myapp --rows 1000 --output seed.sql

# Insert directly into database
seedkit generate --db postgres://localhost/myapp --rows 1000

# JSON or CSV output
seedkit generate --db postgres://localhost/myapp --rows 100 --output data.json

# Deterministic output with seed
seedkit generate --db postgres://localhost/myapp --rows 100 --seed 42 --output seed.sql
```

## Database Connection

SeedKit automatically finds your database URL by checking (in order):

1. `--db` CLI flag
2. `DATABASE_URL` environment variable
3. `.env` file in the current directory
4. `seedkit.toml` config file

Supported URL formats:

```bash
# PostgreSQL
seedkit generate --db postgres://user:pass@localhost:5432/mydb

# MySQL
seedkit generate --db mysql://user:pass@localhost:3306/mydb

# SQLite
seedkit generate --db sqlite://path/to/db.sqlite
```

## AI-Enhanced Classification

SeedKit can use an LLM to improve column classification beyond the built-in 50+ regex rules. This helps with ambiguous column names that the rule engine classifies as `Unknown`.

```bash
# Set one of these environment variables:
export ANTHROPIC_API_KEY=sk-ant-...    # Uses Claude Sonnet (default)
export OPENAI_API_KEY=sk-...           # Uses GPT-4o (default)

# Run with --ai flag
seedkit generate --db postgres://localhost/myapp --rows 1000 --ai --output seed.sql

# Override the model
seedkit generate --db postgres://localhost/myapp --rows 1000 --ai --model claude-opus-4-20250514
```

The AI classification is cached locally so subsequent runs with the same schema don't re-query the LLM. Results are also stored in the lock file for team reproducibility.

## Smart Sampling

Extract statistical distributions from a production database to generate data that mirrors real patterns:

```bash
# Sample distributions (read-only, PII auto-masked)
seedkit sample --db postgres://readonly-replica:5432/myapp

# Generate using sampled distributions
seedkit generate --db postgres://localhost/myapp --rows 1000 --subset seedkit.distributions.json
```

## All Commands

| Command | Description |
|---|---|
| `seedkit generate` | Generate seed data (SQL, JSON, CSV, or direct insert) |
| `seedkit sample` | Extract production distributions with PII masking |
| `seedkit introspect` | Analyze schema and show classification results |
| `seedkit preview` | Preview sample rows without full generation |
| `seedkit check` | Detect schema drift against lock file (CI-friendly) |
| `seedkit graph` | Visualize table dependencies (Mermaid or Graphviz) |

## Configuration

Create a `seedkit.toml` in your project root:

```toml
[database]
url = "postgres://localhost/myapp"

[generate]
rows = 500
seed = 42

[tables.users]
rows = 1000

[tables.orders]
rows = 5000

# Custom value lists with optional weights
[columns."products.color"]
values = ["red", "blue", "green", "black", "white"]
weights = [0.25, 0.20, 0.20, 0.20, 0.15]
```

## Supported Platforms

| Platform | Architecture |
|---|---|
| Linux | x64, ARM64 |
| macOS | Intel, Apple Silicon |
| Windows | x64 |

## Documentation

Full documentation, architecture details, and benchmarks: [github.com/kclaka/seedkit](https://github.com/kclaka/seedkit)

## License

MIT
