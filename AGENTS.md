# Solo: agent context

`AGENTS.md` is the canonical project context; `CLAUDE.md` is a relative symlink to it. Maintain this context in one place.

## Purpose and boundaries

Solo is a Rust CLI that runs one trusted foreground command under a PostgreSQL lease. An external scheduler invokes Solo; the UTC cron expression selects the current interval. A busy key or an interval that already succeeded produces a normal skip. Solo does not wait for the next tick or backfill missed intervals.

`solo status` reads a key's state without migrations, handler execution, or changes to ownership or history.

The lease and success history coordinate instances sharing a key through one writable PostgreSQL primary. Handlers must be idempotent: leases provide neither fencing nor exactly-once external effects.

## Architecture and invariants

- `src/schedule.rs`: `nom` parses field items; `split(',').try_fold()` combines their bit masks without an intermediate list. `latest` searches allowed months backward, with separate day matching and time selection. Its search limit covers a complete 400-year Gregorian cycle, including the current partial month.
- Cron: restricted day-of-month and weekday fields combine with OR; a wildcard uses AND. Preserve the distinction between `*`, `*/1`, `*/2`, and an explicit full range: identical bit masks can have different semantics.
- `src/store.rs`: PostgreSQL determines interval time and atomically checks the owner, scheduled interval, and lease expiry. Only a valid owner may update the lease or record success. Success requires a confirmed COMMIT. Error classification uses the first recognized cause in the `source()` chain and determines retry eligibility.
- `src/supervisor.rs`: the local lease deadline is conservative and monotonic. Only a confirmed renewal advances it. Retries use fresh connections and one shared connection-and-renewal budget, preserving the `grace` reserve, signal handling, and max-runtime. Monitor child completion during renewal; cancelled renewals invalidate the connection. Finalization may reconnect once within its shared connection-and-completion budget, retaining the original owner and interval. Neither path reacquires ownership or retries a completion transaction.
- Shutdown targets the process group: SIGTERM, followed by SIGKILL after grace. Reap exited leaders before signalling while still stopping remaining group members. Ownership is released after group shutdown. The command must remain in the foreground and keep its descendants in the same group.
- `src/diagnostics.rs`: diagnostics are best-effort, with a bounded queue and line length. Slow or closed stderr must not delay heartbeats or handler shutdown.
- The connection string comes only from `SOLO_DATABASE_URL`, which is removed from the child's environment. All other variables, including `PG*`, are inherited unchanged for the handler's own configuration. Diagnostics expose only reviewed error categories. `sslmode` defaults to `prefer`, allowing plaintext when the server rejects TLS. TLS verifies the CA and hostname; verification errors fail without plaintext fallback. `require` forbids plaintext and `disable` skips TLS.

Before changing public behavior, timing, or the schema, read [docs/usage.md](docs/usage.md). For SQL changes, preserve PgBouncer transaction-pooling compatibility: transaction-scoped locks and settings, unnamed typed queries, and no reliance on session state.

## Implementation preferences

- Prefer iterator pipelines for searches and data transformations: `successors`, `filter`, `find_map`, and `try_fold`. Choose them for readability; keep event waits, process control, and effect sequencing explicit.
- Separate syntax parsing, invariant validation, and computation. Keep the pure core apart from I/O; small functions should express distinct concepts.
- Prefer standard operations over manual algorithms, such as `is_some_and` for an `Option` predicate and `floor_char_boundary` for UTF-8 truncation. Preserve short-circuiting and existing errors.
- Avoid intermediate collections when one pass is sufficient. Keep a clear `match` when a combinator chain would obscure it.
- Preserve the strict Rust/Clippy checks in `Cargo.toml`; fix the cause of a warning. Comments explain non-obvious reasons and constraints.
- English is the primary project language. Write code, CLI help, documentation, agent instructions, and commit messages in English. `README_ru.md` is the sole documentation language exception. Match the user's language in conversation. When CLI behavior changes, update `--help`, `README.md`, `README_ru.md`, and `docs/usage.md`.
- Lead READMEs with a short usage example and practical behavior. Keep detailed installation scripts in a collapsed section after the quick start.
- Use Conventional Commits: `feat: ...`, `fix: ...`, `refactor: ...`, `docs: ...`, `test: ...`, or `chore: ...`. Add a scope when it accurately describes the change.

## Verification

Before completing a Rust change, run the Checks section in [CONTRIBUTING.md](CONTRIBUTING.md). That document also defines the shared PostgreSQL harness, SQL lint, and release workflow. Integration tests use real processes and databases; preserve resource isolation and cleanup.

For calendar changes, retain independent forward-oracle tests over a complete 400-year cycle and every minute of the day. Cover cron OR/AND rules, impossible dates, leap centuries, day/year transitions, and Chrono limits. For parser refactors, compare accepted/rejected expressions and observable results with the previous implementation. Remove temporary comparison implementations after verification.

## Tools

- Check current external-library APIs with Context7: `resolve-library-id`, then `query-docs`. If unavailable, consult a primary source for the actual dependency version and state the verification limitation.
- Use CodeGraph MCP for symbols, callers, and impact analysis, accounting for index state. If the index is unavailable or the language unsupported, use `rg` and read files.
- Use Jira MCP for Jira tasks; project instructions and the user's request determine the tracker. Publish documents to April through April MCP when the user requests publication.

## Keeping context current

Updating context is part of completing every task:

1. Compare the actual diff, verification results, and user decisions with the current context.
2. Correct stale information and retain newly verified invariants, decision rationale, preferences, and material limitations. Record conclusions useful to future changes together with their applicability conditions.
3. Keep general rules here. Put development and testing details in `CONTRIBUTING.md`, and user-facing behavior in the READMEs and `docs/usage.md`. Link to details with explicit reading conditions rather than duplicating them.
4. Remove disproven claims and merge repetitions. Context describes the current project, not the conversation history. Exclude secrets, temporary paths, current commit hashes, and test-run counters.
5. Check that context agrees with the code, follows the English documentation policy, and that `CLAUDE.md` still points to `AGENTS.md`. Include context updates in the corresponding change's commit. When there are no new durable facts, keep the context concise and current.
