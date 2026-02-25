<p align="center">
  <h1 align="center">SeedKit</h1>
  <p align="center">
    <strong>Generate realistic, constraint-safe seed data for any database.</strong>
  </p>
  <p align="center">
    <a href="https://github.com/kclaka/seedkit/actions/workflows/ci.yml"><img src="https://github.com/kclaka/seedkit/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
    <img src="https://img.shields.io/badge/tests-221_passing-brightgreen" alt="Tests">
    <img src="https://img.shields.io/badge/version-1.2.1-blue" alt="Version">
    <img src="https://img.shields.io/badge/rust-1.75%2B-orange?logo=rust" alt="Rust">
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License: MIT"></a>
    <img src="https://img.shields.io/badge/databases-PostgreSQL%20%7C%20MySQL%20%7C%20SQLite-blueviolet" alt="Databases">
  </p>
</p>

---

SeedKit connects to your PostgreSQL, MySQL, or SQLite database, reads the schema, and generates seed data that respects foreign keys, unique constraints, check constraints, and enum types -- all without copying production data.

```bash
seedkit generate --db postgres://localhost/myapp --rows 1000 --output seed.sql
```

## Why SeedKit?

Every backend developer needs test data, but the options are broken:

- **Faker/factory_bot** generate random gibberish with no schema awareness. Foreign keys break, unique constraints collide, and the data looks nothing like production.
- **Copying production data** is a compliance nightmare. 93% of organizations aren't privacy-compliant in testing.
- **Snaplet** (the best open-source option) shut down in July 2024.

SeedKit fills this gap. One command, realistic data, zero PII.

## Features

| Category | Feature |
|---|---|
| **Databases** | PostgreSQL, MySQL, and SQLite out of the box |
| **Introspection** | Auto-reads tables, columns, FKs, unique constraints, check constraints, enums |
| **Classification** | 50+ semantic types (Email, FirstName, Price, CreatedAt, etc.) via pattern matching |
| **FK Safety** | Topological ordering ensures parent rows exist before child rows reference them |
| **Cycle Resolution** | Detects circular FKs (Tarjan SCC), breaks cycles with deferred `UPDATE` statements |
| **Correlations** | city/state/zip stay consistent, `created_at < updated_at`, first+last derive full name |
| **Determinism** | Lock file (`seedkit.lock`) + seed guarantees identical output across machines |
| **Custom Values** | Weighted value lists via `seedkit.toml` config |
| **Smart Sampling** | Extract production distributions and generate data that mirrors real patterns (with PII masking) |
| **LLM-Enhanced** | Optional `--ai` flag sends schema to Claude/GPT for smarter classification |
| **Output Formats** | SQL (`INSERT`/`COPY`), JSON, CSV, or direct database insertion |
| **CI Integration** | `seedkit check` detects schema drift (exit code 0/1) |
| **Visualization** | `seedkit graph` exports Mermaid.js or Graphviz DOT dependency diagrams |

## Quick Start

```bash
# Install from source
cargo install --path crates/seedkit-cli

# Generate 1000 rows per table, output SQL
seedkit generate --db postgres://localhost/myapp --rows 1000 --output seed.sql

# Insert directly into database
seedkit generate --db postgres://localhost/myapp --rows 1000

# Use .env or seedkit.toml for connection -- no --db needed
seedkit generate --rows 500 --output seed.sql
```

## Installation

### From Source

```bash
git clone https://github.com/kclaka/seedkit.git
cd seedkit
cargo install --path crates/seedkit-cli
```

**Requirements:** Rust 1.75+ (2021 edition)

### Verify Installation

```bash
seedkit --version
# seedkit 1.2.1
```

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
# SQL file output
seedkit generate --db postgres://localhost/myapp --rows 500 --output seed.sql

# Direct insert into database
seedkit generate --db postgres://localhost/myapp --rows 1000

# JSON or CSV
seedkit generate --rows 100 --output data.json
seedkit generate --rows 100 --output data.csv

# PostgreSQL COPY format (10-50x faster bulk loading)
seedkit generate --rows 10000 --output seed.sql --copy

# Deterministic with seed
seedkit generate --rows 100 --seed 42 --output seed.sql

# Reproduce from lock file
seedkit generate --from-lock

# Per-table row counts
seedkit generate --rows 100 --table-rows users=500,orders=2000

# Include/exclude tables
seedkit generate --include users,orders --rows 100
seedkit generate --exclude audit_logs,migrations --rows 100

# LLM-enhanced classification
seedkit generate --rows 100 --ai --output seed.sql

# Production-like with sampled distributions
seedkit generate --rows 1000 --subset seedkit.distributions.json
```

### `seedkit sample`

Extract statistical distributions from a production database (read-only replica recommended). Automatically masks PII columns.

```bash
# Sample all tables
seedkit sample --db postgres://readonly-replica:5432/myapp

# Sample specific tables with custom limits
seedkit sample --db postgres://localhost/myapp --tables users,orders --categorical-limit 100

# Custom output path
seedkit sample --db postgres://localhost/myapp -o profiles.json
```

This creates `seedkit.distributions.json` with:
- **Categorical distributions** -- value frequencies for text/enum columns (PII columns auto-masked)
- **Numeric distributions** -- min, max, mean, stddev for numeric columns
- **FK ratios** -- child-to-parent row count ratios (e.g., 3.2 orders per user)

Then use with `seedkit generate --subset seedkit.distributions.json` to produce data that mirrors production patterns.

### `seedkit introspect`

Analyze your database schema and show classification results.

```bash
seedkit introspect --db postgres://localhost/myapp
seedkit introspect --db postgres://localhost/myapp --format json
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
                          |  Optional distribution profiles (--subset)
                          v
                  SemanticTypes
                          |
          [4] Generate    |  Row-by-row, FK-safe, unique-safe
                          |  Correlated groups, check constraints
                          |  Distribution-aware (normal, categorical)
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

## Performance

Benchmarked with [criterion](https://github.com/bheisler/criterion.rs) on Apple Silicon (M-series). Run `cargo bench` to reproduce.

| Operation | Throughput |
|---|---|
| Generation (10 cols, semantic providers) | ~480K rows/sec |
| Generation (FK references only) | ~3.7M rows/sec |
| Generation (weighted value lists) | ~6.9M rows/sec |
| Generation (distribution sampling) | ~8.6M rows/sec |
| Classification (100 tables x 20 cols) | ~2.1M cols/sec |
| SQL output formatting | ~1.5M rows/sec |
| JSON output formatting | ~1.1M rows/sec |
| CSV output formatting | ~1.5M rows/sec |

## Comparison

| Feature | SeedKit | Faker/factory_bot | Snaplet |
|---|---|---|---|
| Schema-aware | Yes | No | Yes (shut down) |
| Multi-database | PG + MySQL + SQLite | N/A | PG only |
| FK resolution | Automatic | Manual | Automatic |
| Circular FK handling | Tarjan SCC + deferral | N/A | Manual |
| Deterministic | Seed + lock file | Seed only | No |
| Custom values | TOML config | Code | Code |
| Smart sampling | Production distributions | No | No |
| LLM-enhanced | Optional --ai | No | No |
| CI integration | `seedkit check` | N/A | No |
| Privacy | Synthetic + PII masking | Synthetic | Copies prod |

## Architecture

SeedKit is a Rust workspace with three crates:

```
seedkit/
  crates/
    seedkit-core/     # Library: introspection, graph, classification, generation, output, sampling
    seedkit-cli/      # Binary: clap-based CLI with 6 subcommands
    seedkit-testutil/  # Shared test helpers
  tests/
    fixtures/         # SQL schema fixtures for integration tests
```

**Test suite:** 221 tests (201 unit + 13 PostgreSQL integration + 7 MySQL integration)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and PR guidelines.

```bash
# Run the full test suite
cargo test

# Run integration tests (requires Docker)
docker compose -f docker/docker-compose.test.yml up -d
TEST_POSTGRES_URL=postgres://seedkit:seedkit@localhost:5432/seedkit_test \
TEST_MYSQL_URL=mysql://seedkit:seedkit@localhost:3307/seedkit_test \
  cargo test --test '*' -- --test-threads=1
```

## License

Licensed under the [MIT License](LICENSE).
