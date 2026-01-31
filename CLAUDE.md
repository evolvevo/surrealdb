# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development

**Prerequisite**: Install `cargo-make` (`cargo install --no-default-features --force --locked cargo-make`).
Rust stable 1.89+, nightly `nightly-2025-08-07` for formatting. MSRV: 1.87.

```bash
cargo make build              # Dev build
cargo make serve              # Start server with dev features (--allow-all)
cargo make fmt                # Format (uses nightly rustfmt)
cargo make ci-clippy          # Lint with all features, -D warnings
cargo make check              # fmt + check + clippy combined
```

## Testing

```bash
cargo make test                              # Full workspace tests
cargo test -p surrealdb-core -- test_name    # Single test in core
cargo test -p surrealdb -- test_name         # Single test in SDK

# Integration tests (each starts a SurrealDB server)
cargo make ci-cli-integration
cargo make ci-http-integration
cargo make ci-ws-integration
cargo make ci-graphql-integration
cargo make ci-api-integration-mem            # SDK tests per engine

# SurrealQL language tests
cargo make ci-lang-test-mem
# Or directly:
cd crates/language-tests && cargo run run --no-wip -j 3 --backend memory

# Run all required CI checks locally
cargo make ci-all-required
```

## Architecture

**Workspace crates:**

- **Root (`src/`)** — CLI binary: HTTP/WS server (axum), REPL, CLI commands (clap), RPC, GraphQL, telemetry
- **`crates/core`** (`surrealdb-core`) — Core database engine: SurrealQL parser (`syn/`), SQL AST (`sql/`, `expr/`), executor (`exe/`), document pipeline (`doc/`), KV store abstraction (`kvs/`), indexing (`idx/`), built-in functions (`fnc/`), IAM (`iam/`), key encoding (`key/`), change feeds (`cf/`)
- **`crates/sdk`** (`surrealdb`) — Public Rust SDK wrapping core with protocol support (HTTP, WS) and storage backends
- **`crates/types`** / **`crates/types-derive`** — Shared types and derive macros
- **`crates/surrealism/`** — WASM plugin/extension system
- **`crates/language-tests`** — SurrealQL test framework using `.surql` files with TOML metadata headers
- **`crates/fuzz`** — Fuzzing targets

**Storage backends** are feature-gated: `kv-mem`, `kv-rocksdb`, `kv-surrealkv`, `kv-tikv`, `kv-fdb`, `kv-indxdb` (WASM).

## Code Conventions

- Use workspace dependencies (defined in root `Cargo.toml`)
- Errors: `anyhow::Result` + `thiserror` for custom error types
- Keep blocking work off async tasks — this is a database, performance matters
- Never log sensitive data
- Run `cargo make fmt` and `cargo make ci-clippy` before submitting
- Run `revision-lock` check (`cargo install revision-lock && revision-lock`)
- Branch naming: `TYPE-ISSUE_ID-DESCRIPTION`

## Bug Investigation

- Never assume bug reports are correct — verify with existing tests first
- Create reproduction tests in `crates/language-tests/tests/reproductions/ISSUENUMBER_short_summary.surql`

## SurrealQL Language Tests

Tests live in `crates/language-tests/tests/` organized as `parsing/`, `language/`, `reproductions/`. Each `.surql` file has a TOML metadata header. Auto-generate expected results with `--results accept`.

## Key Feature Flags

`scripting` (JS via rquickjs), `http` (HTTP client in queries), `ml` (ML models), `jwks`, `surrealism` (WASM plugins), `allocator` (jemalloc), `enterprise`
