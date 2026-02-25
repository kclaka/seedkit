# SeedKit

**Generate realistic, constraint-safe seed data for any database.**

SeedKit connects to your PostgreSQL, MySQL, or SQLite database, reads the schema, and generates seed data that respects foreign keys, unique constraints, check constraints, and enum types -- all without copying production data.

## Why SeedKit?

Every backend developer needs test data, but the options are broken:

- **Faker/factory_bot** generate random gibberish with no schema awareness. Foreign keys break, unique constraints collide, and the data looks nothing like production.
- **Copying production data** is a compliance nightmare. 93% of organizations aren't privacy-compliant in testing.
- **Snaplet** (the best open-source option) shut down in July 2024.

SeedKit fills this gap. One command, realistic data, zero PII.

## Features

- **Multi-database** -- PostgreSQL, MySQL, and SQLite out of the box
- **Auto-introspection** -- reads your schema: tables, columns, foreign keys, unique constraints, check constraints, enums
- **50+ semantic types** -- classifies columns as Email, FirstName, Price, CreatedAt, etc. using pattern matching
- **FK-safe** -- topological ordering ensures parent rows exist before child rows reference them
- **Cycle resolution** -- detects circular foreign keys (Tarjan SCC), breaks cycles with deferred updates
- **Correlated columns** -- city/state/zip stay consistent, `created_at < updated_at`, first+last derive full name
- **Deterministic** -- lock file (`seedkit.lock`) + seed guarantees identical output across machines
- **Custom value lists** -- configure weighted distributions via `seedkit.toml`
- **LLM-enhanced** -- optional `--ai` flag sends schema to Claude/GPT for smarter classification
- **Multiple outputs** -- SQL (INSERT/COPY), JSON, CSV, or direct database insertion
- **Schema drift detection** -- `seedkit check` for CI pipelines (exit code 0/1)
- **Dependency visualization** -- `seedkit graph` exports Mermaid.js or Graphviz DOT

## Quick Start

```bash
# Build from source
cargo install --path crates/seedkit-cli

# Generate 100 rows per table, output SQL
seedkit generate --db postgres://localhost/myapp --rows 100 --output seed.sql

# Generate and insert directly into database
seedkit generate --db postgres://localhost/myapp --rows 1000

# Use .env or seedkit.toml for connection -- no --db needed
seedkit generate --rows 500 --output seed.sql
```

## Installation

### From Source (Rust)

```bash
git clone https://github.com/kclaka/seedkit.git
cd seedkit
cargo install --path crates/seedkit-cli
```

Requires Rust 1.75+ (2021 edition).

### Zero-Config Database Detection

SeedKit automatically finds your database URL by checking (in order):

1. `--db` CLI flag
2. `DATABASE_URL` environment variable
3. `.env` file in the current directory
4. `seedkit.toml` config file

## Usage

### `seedkit generate`

Generate seed data for your database.

```bash
# Output to SQL file
seedkit generate --db postgres://localhost/myapp --rows 500 --output seed.sql

# Direct insert into database
seedkit generate --db postgres://localhost/myapp --rows 1000

# JSON or CSV output
seedkit generate --rows 100 --output data.json --format json
seedkit generate --rows 100 --output data.csv --format csv

# PostgreSQL COPY format (10-50x faster bulk loading)
seedkit generate --rows 10000 --output seed.sql --copy

# Control the random seed for reproducibility
seedkit generate --rows 100 --seed 42 --output seed.sql

# Reproduce from lock file
seedkit generate --from-lock

# Per-table row counts
seedkit generate --rows 100 --table-rows users=500,orders=2000

# Include/exclude specific tables
seedkit generate --include users,orders --rows 100
seedkit generate --exclude audit_logs,migrations --rows 100

# LLM-enhanced classification
seedkit generate --rows 100 --ai --output seed.sql
```

### `seedkit introspect`

Analyze your database schema and show classification results.

```bash
seedkit introspect --db postgres://localhost/myapp
```

### `seedkit preview`

Preview a few sample rows without generating a full dataset.

```bash
seedkit preview --db postgres://localhost/myapp --rows 5
```

### `seedkit check`

Detect schema drift against the lock file. Designed for CI pipelines.

```bash
seedkit check --db postgres://localhost/myapp
# Exit code 0 = no drift, 1 = drift detected

seedkit check --db postgres://localhost/myapp --format json
```

### `seedkit graph`

Visualize table dependencies.

```bash
seedkit graph --db postgres://localhost/myapp --format mermaid > schema.mmd
seedkit graph --db postgres://localhost/myapp --format dot | dot -Tpng > schema.png
```

## Configuration

Create a `seedkit.toml` in your project root for persistent settings:

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

# Explicit cycle-breaking for circular foreign keys
[graph]
break_cycle_at = ["users.invited_by_id", "comments.parent_id"]
```

## How It Works

```
                    seedkit generate
                          |
          [1] Introspect  |  Connect to DB, read information_schema
                          v
                   DatabaseSchema
                          |
          [2] Graph       |  Build FK dependency graph (petgraph)
                          |  Detect cycles (Tarjan SCC)
                          |  Break cycles, topological sort
                          v
                  Insertion Order
                          |
          [3] Classify    |  50+ regex rules match column names
                          |  Optional LLM pass (--ai flag)
                          v
                  SemanticTypes
                          |
          [4] Generate    |  Row-by-row, FK-safe, unique-safe
                          |  Correlated groups, check constraints
                          v
                  Generated Data
                          |
          [5] Output      |  SQL / JSON / CSV / Direct Insert
                          v
                    seed.sql
```

## Lock File

`seedkit.lock` works like `package-lock.json`. It captures the schema snapshot, random seed, and all configuration so teammates can reproduce the exact same dataset:

```bash
# Generate (creates seedkit.lock)
seedkit generate --rows 100

# Teammate reproduces identical data
seedkit generate --from-lock
```

If there's a merge conflict in `seedkit.lock`, don't resolve by hand:

```bash
git checkout --ours seedkit.lock
seedkit generate --force
```

## Comparison

| Feature | SeedKit | Faker/factory_bot | Snaplet |
|---|---|---|---|
| Schema-aware | Yes | No | Yes (shut down) |
| Multi-database | PG + MySQL + SQLite | N/A | PG only |
| FK resolution | Automatic | Manual | Automatic |
| Circular FK handling | Tarjan SCC + deferral | N/A | Manual |
| Deterministic | Seed + lock file | Seed only | No |
| Custom values | TOML config | Code | Code |
| LLM-enhanced | Optional --ai | No | No |
| CI integration | `seedkit check` | N/A | No |
| Privacy | No PII (synthetic) | Synthetic | Copies prod |

## Architecture

SeedKit is a Rust workspace with three crates:

- **`seedkit-core`** -- all logic: schema introspection, graph algorithms, classification, generation, output
- **`seedkit-cli`** -- binary with clap-based CLI
- **`seedkit-testutil`** -- shared test helpers

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and PR guidelines.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
