# Contributing to SeedKit

Thanks for your interest in contributing to SeedKit!

## Prerequisites

- [Rust](https://rustup.rs/) 1.75 or later
- [Docker](https://docs.docker.com/get-docker/) (for integration tests)

## Development Setup

```bash
git clone https://github.com/kclaka/seedkit.git
cd seedkit
cargo build
cargo test
```

## Running Tests

### Unit Tests

```bash
cargo test
```

### Integration Tests (requires Docker)

```bash
docker-compose -f docker/docker-compose.test.yml up -d
cargo test --test '*'
docker-compose -f docker/docker-compose.test.yml down
```

## Code Style

- Run `cargo fmt --all` before committing
- Run `cargo clippy --all-targets` and fix any warnings
- Write tests for new functionality (TDD preferred)
- Use `thiserror` for error types with actionable context (include table name, column name, row index)

## Project Structure

```
crates/
  seedkit-core/     # Library: schema, graph, classify, generate, output
  seedkit-cli/      # Binary: CLI commands (generate, introspect, preview, check, graph)
  seedkit-testutil/  # Shared test fixtures
```

## Pull Request Process

1. Fork the repo and create a feature branch
2. Write tests for your changes
3. Ensure `cargo test`, `cargo fmt --check`, and `cargo clippy --all-targets` pass
4. Open a PR with a clear description of what changed and why
