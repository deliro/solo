# Heartbeat retries implementation plan

**Goal:** Survive transient PostgreSQL renewal failures while the confirmed lease remains safe.

**Architecture:** The supervisor retries guarded heartbeat transactions on fresh connections. The last confirmed local deadline remains authoritative until commit is acknowledged. Backoff starts at a configurable positive interval and doubles to max(interval, 2s); shutdown retains its grace reserve.

**Tech stack:** Rust, Tokio, tokio-postgres, clap, PostgreSQL 17 integration harness.

**Constraints:** Preserve owner/interval/expiry guards, sanitized diagnostics, signals and runtime deadlines. Additive CLI configuration; no schema changes. Existing checkout files are untracked and must be preserved.

**Non-goals:** Acquisition/completion retries, failover orchestration, fencing or exactly-once effects.

### Task 1: Exercise renewal recovery and failure boundaries

Files: `tests/cli/heartbeat_budget.rs`, `tests/cli/scenarios.rs`, `tests/fixtures/renewal_failures.sql`.

1. Add a renewal-only trigger using a sequence (survives rollback), raising transient/permanent errors or sleeping on the first attempts.
2. Run `cargo test --locked --test cli heartbeat_retry`: recovery must fail before implementation because the first renewal failure kills the handler.
3. Cover persistent failure deadline, ownership loss (existing tests), timeout recovery and backend disconnection. Update database_loss to expect recovery after backend termination.

### Task 2: Implement bounded retries and configuration

Files: `src/config.rs:15`, `src/store.rs:14`, `src/supervisor.rs:50`, `tests/cli/common/mod.rs:60`.

1. Add `heartbeat_retry_interval: Duration`, default 200ms, CLI/env help and positive bounded validation.
2. Preserve retryability as typed sanitized database causes. Retry timeouts, connection failures, unavailable/read-only server, resource limits and lock/deadlock/cancellation errors; fail closed for unclassified errors and ownership loss.
3. Track retry backoff in the supervisor; reconnect before each retry, bound connect plus renewal by one db-timeout and lease reserve. Never move valid_until on failure. Keep signal/runtime supervision active and log retry/recovery.
4. Run `cargo test --locked --test cli heartbeat_retry`, then `cargo test --locked --bin solo`: expect PASS.

### Task 3: Document and verify

Files: `README.md:98`, `README_ru.md:98`, `docs/usage.md:59`, `src/config.rs` help tests.

1. Document flag/env/default, retry classification/backoff, shutdown budget and confirmed-commit semantics; verify `cargo run --locked -- run --help`.
2. Run `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, `cargo test --locked`.
3. Route actual changes through Rust/PostgreSQL references; review security and reliability. Rollback is removal of additive configuration/retry logic; schema stays compatible.
