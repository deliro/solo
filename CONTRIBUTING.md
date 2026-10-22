# Developing Solo

For product usage, see the [README](README.md). English is the primary project language; write documentation in English except for `README_ru.md`.

## Environment

| Component | Requirement |
|---|---|
| OS | Linux/macOS for unit tests and the CLI; the integration suite runs on Linux |
| Rust | Current stable toolchain, Cargo, rustfmt, and Clippy |
| TLS build | A C compiler for ring; macOS requires Xcode CLI tools. OpenSSL is not required |
| Integration tests | An accessible Docker daemon; testcontainers starts PostgreSQL 17 |

## Build

```sh
cargo build --release --locked
./target/release/solo --help
./target/release/solo run --help
```

The binary is written to `target/release/solo`.

## Checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
```

- Compiler and Clippy warnings are denied by `Cargo.toml`.
- SQL is checked with Squawk 2.70.0; rules are in [.squawk.toml](.squawk.toml).
- CI commands and Squawk installation are in [.github/workflows/ci.yml](.github/workflows/ci.yml).
- Rust and SQL pre-commit hooks are in [.pre-commit-config.yaml](.pre-commit-config.yaml).

Install [prek](https://prek.j178.dev/), then run:

```sh
prek install
prek run --all-files
```

Clippy enables `all`, `pedantic`, and `nursery`, plus explicit bans on potentially panicking arithmetic, indexing, `unwrap`, `expect`, `panic!`, `todo!`, and `unreachable!`. Exceptions for `unwrap`/`expect`/`panic!` apply only to tests through `clippy.toml`. `redundant_pub_crate` is disabled because it conflicts with `unreachable_pub` in a binary crate with no public API. The complete `restriction` group is intentionally not enabled: its rules conflict with each other and with ordinary Rust code.

## Tests

| Coverage | Location |
|---|---|
| Calendar and configuration | Unit tests alongside the code in `src/` |
| CLI, locks, signals, and migrations | `tests/cli/`; real processes and PostgreSQL |
| SQL plans against populated tables | `tests/cli/sql_plans.rs`; EXPLAIN ANALYZE BUFFERS |
| Suite and container lifecycle | `tests/cli/main.rs` |
| Database creation and process cleanup | `tests/cli/common/mod.rs` |

- Two integration scenarios run concurrently by default; `--test-threads=N` overrides this limit.
- Concurrent-start exclusion scenarios use a 1s database timeout, 2s migration timeout, and 5s lease so simultaneous process startup and authentication do not depend on the shared harness's 200ms database budget. Tests of timeout behavior retain their explicit tight deadlines.
- The shared command builder supplies timing defaults through environment variables so a scenario can override them with CLI flags. The precedence scenario supplies an explicit CLI TTL when testing an invalid environment TTL.
- Keep cooperative-shutdown and already-exited-leader coverage: macOS can return `EPERM` when signalling a group whose leader is an unreaped zombie. The supervisor reaps an exited child before group signals and retries once if it exits between the check and signal, while still killing remaining group members. The regression uses `ps` to observe stopped/zombie states; the test environment must permit process inspection and Unix signals.
- Keep coverage for handler completion during renewal and retry backoff: observe an in-flight database operation or retry diagnostic before releasing the handler. Verify the recorded outcome, ownership release, and success deduplication; a fresh finalization connection retains the original acquisition.
- PID tracking waits for a nonempty marker within one deadline: shell redirection creates an empty file before writing the PID. File existence alone does not establish process-group readiness.
- Each integration-suite run creates **one** `postgres:17` container.
- The transaction-pooling test also starts `edoburu/pgbouncer:v1.25.1-p0` with `max_prepared_statements=0`; it uses the same PostgreSQL instance and is removed after the test.
- Each test creates a **unique database** in that container.
- The container is removed after the suite, including failed tests.
- New scenarios use the shared harness rather than starting another PostgreSQL instance.
- Declare modules with ordinary `mod` declarations in `tests/cli/main.rs`; path override attributes are unnecessary.
- Pure tests use `rstest`; calendar cases are parameterized and require no database.
- Integration scenarios retain `libtest-mimic`: it manages one shared container and runtime with explicit cleanup after the entire suite.
- Bug fixes include a regression test: reproduce the failure first, then verify the fix.

## Database schema

- `migrations/0001_coordination.up.sql` creates the complete version 1 schema.
- `migrations/0001_coordination.down.sql` removes the schema and success history.
- Startup migrations run in a transaction under an advisory lock with bounded waiting.
- Schema changes used by coexisting application versions must remain backward-compatible. Stop old instances before introducing an incompatible change.
- Run a down migration only when the deployment is stopped.

## Releases

1. Update the version in `Cargo.toml` and `Cargo.lock`.
2. After CI succeeds, create and push a `v<version>` tag, such as `v0.1.0`.
3. The release workflow repeats the checks, builds Linux/macOS binaries for x86_64 and ARM64, verifies the binaries, and publishes a GitHub Release with archives and `SHA256SUMS`.

The tag must match the package version. Linux builds use musl; TLS uses rustls/ring and system CAs on all platforms. The minimum macOS version is 11. CA certificates must be installed on the user's machine. Binaries are built without `target-cpu=native` so they work on other machines with the same architecture.

The release profile uses `opt-level="s"`, `codegen-units=1`, fat LTO, `debug=0`, `strip="symbols"`, and `panic="unwind"`. It favors binary size; peak performance across workloads requires measurement.
