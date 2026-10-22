# Solo

[![CI](https://github.com/deliro/solo/actions/workflows/ci.yml/badge.svg)](https://github.com/deliro/solo/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/deliro/solo)](https://github.com/deliro/solo/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | [Russian](README_ru.md)

Solo runs a command under a shared PostgreSQL lease so that instances of the same job can run on different hosts or in different data centers.
It records successful cron intervals in the database so that repeated invocations skip them.

## Installation

Download `solo-v<version>-<target>.tar.gz` and `SHA256SUMS` from [GitHub Releases](https://github.com/deliro/solo/releases).

| OS | x86_64 | ARM64 |
|---|---|---|
| Linux | `x86_64-unknown-linux-musl` | `aarch64-unknown-linux-musl` |
| macOS 11+ | `x86_64-apple-darwin` | `aarch64-apple-darwin` |

Verify the archive's SHA-256 against `SHA256SUMS` (`sha256sum` on Linux, `shasum -a 256` on macOS), extract it, and install `solo` in a directory on `PATH`. Linux binaries are static; TLS requires system root certificates. Windows is not supported: handler supervision uses Unix signals and process groups.

For example, install v0.1.1 for Linux x86_64 below. Set `target` to the value from the table for your machine; macOS uses the same commands. Run in an empty directory:

```sh
(
  set -eu
  version=0.1.1
  target=x86_64-unknown-linux-musl
  archive="solo-v$version-$target"
  release="https://github.com/deliro/solo/releases/download/v$version"
  curl --fail --location "$release/$archive.tar.gz" --output "$archive.tar.gz"
  curl --fail --location "$release/SHA256SUMS" --output SHA256SUMS
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --ignore-missing --check SHA256SUMS
  else
    shasum -a 256 --ignore-missing --check SHA256SUMS
  fi
  tar -xzf "$archive.tar.gz"
  mkdir -p "$HOME/.local/bin"
  install -m 755 "$archive/solo" "$HOME/.local/bin/solo"
  "$HOME/.local/bin/solo" --version
)
```

Add `$HOME/.local/bin` to your shell's `PATH` if needed: `export PATH="$HOME/.local/bin:$PATH"`. Persist this line in your shell's startup file. Solo still needs the PostgreSQL connection and scheduler described below.

Build from source:

```sh
cargo build --release --locked
./target/release/solo --help
```

You need stable Rust and a system C compiler. TLS uses rustls with ring; OpenSSL is not required. Development and release instructions are in [CONTRIBUTING.md](CONTRIBUTING.md). License: [MIT](LICENSE).

## Quick start

You need Linux or macOS, a shared writable PostgreSQL primary without split-brain, and an external scheduler using UTC.
The database role needs schema DDL privileges and read/write access to Solo's tables.
These examples assume `solo` is on `PATH`.

```sh
export SOLO_DATABASE_URL='postgresql://solo:change-me@db.example.com/jobs?sslmode=require'
solo run \
  --key 'billing/prod/reconcile' \
  --schedule '*/5 * * * *' \
  --max-runtime 4m \
  -- /bin/echo 'reconcile example'
```

Replace the database endpoint and credentials using your secret manager, and replace `/bin/echo` with your job's handler.
Both URLs and `key=value` connection strings are supported; pass credentials only through `SOLO_DATABASE_URL`.
PgBouncer transaction pooling with `max_prepared_statements=0` is supported: Solo requires neither cached prepared statements nor a connection pinned to one backend.
`sslmode` is optional and defaults to `prefer`: try TLS, allowing plaintext if the server rejects TLS. TLS verifies the certificate and server hostname against system CAs; certificate or hostname errors fail the connection without plaintext fallback. Use `sslmode=require` to forbid plaintext or `sslmode=disable` to skip TLS. Install private CAs in the system store or select them with `SSL_CERT_FILE` / `SSL_CERT_DIR`.

Solo performs one attempt. An external scheduler controls invocations and retries.
Arguments after `--` are passed directly; explicitly use `/bin/sh -c` for a trusted shell script.

## Task keys and schedules

`--key` names a task in the shared database. Matching keys share a lease and success history, even when their commands differ.

| Task | Keys |
|---|---|
| One job in two data centers | Identical: `billing/prod/reconcile` |
| Independent production and development jobs | Different: `billing/prod/reconcile`, `billing/dev/reconcile` |

`--schedule` selects the execution interval. It does not start a timer.
The current interval begins at the latest scheduled instant not after PostgreSQL time.
For example, with `*/5 * * * *`, success at 00:00 suppresses a retry at 00:03; a new interval starts at 00:05.

- Five-field Kubernetes-style cron in UTC is supported; detailed syntax is in the [operations guide](docs/usage.md#schedule-and-outcomes).
- A late invocation executes the current interval; missed intervals are not backfilled.
- An active lease covers the entire key, including the next interval.
- All instances of a key must use the same schedule; stop old instances before changing it.

## Status

```sh
solo status --key hello
```

Only the key and `SOLO_DATABASE_URL` are required. No schedule or command is needed. The database timeout is `--db-timeout` / `SOLO_DB_TIMEOUT`, default `3s`.

| Field | Meaning |
|---|---|
| `State` | `unknown`: key not recorded; `free`: unowned; `held`: lease active; `expired`: lease expired |
| Owner | UUID, hostname, and PID of the Solo process |
| Latest attempt | Time of the last lease acquisition, not the start of the cron interval |
| Lease age and remaining lifetime | Time since acquisition and until expiry, using the database clock |
| Last completion | Time and recorded outcome of the last finished attempt: `success` / `failure` |
| Last success | Confirmed completion time and its scheduled interval |

- Status only reads the database: it does not run migrations, renew or release leases, or launch a handler.
- `held` confirms a lease, not handler liveness. A crash can leave an attempt without a completion record.
- Missing fields in older records are shown as `not recorded`.

## Run options

CLI arguments override environment variables. There is no configuration file.

| CLI | Environment | Default | Purpose |
|---|---|---|---|
| `--key` | `SOLO_KEY` | Required | Task key |
| `--schedule` | `SOLO_SCHEDULE` | Required | UTC schedule selecting the interval |
| `--ttl` | `SOLO_TTL` | `30s` | Lease lifetime without a successful renewal |
| `--heartbeat` | `SOLO_HEARTBEAT` | `5s` | Interval between renewal starts |
| `--heartbeat-retry-interval` | `SOLO_HEARTBEAT_RETRY_INTERVAL` | `200ms` | Initial backoff after a transient renewal failure |
| `--max-runtime` | `SOLO_MAX_RUNTIME` | Unlimited | Handler runtime before shutdown begins |
| `--db-timeout` | `SOLO_DB_TIMEOUT` | `3s` | Connection and per-operation coordination deadline |
| `--grace` | `SOLO_GRACE` | `2s` | Wait between SIGTERM and SIGKILL |
| `--migration-timeout` | `SOLO_MIGRATION_TIMEOUT` | `10s` | Deadline for the entire startup migration, including lock acquisition |
| — | `SOLO_DATABASE_URL` | Required | Connection to the shared PostgreSQL primary |

Durations accept units such as `200ms`, `30s`, `5m`, `1h`; values must be positive and at most 365 days.
The required relationship is `ttl > heartbeat + db-timeout + grace`.
TTL limits the lease, not the handler's lifetime: successful renewals allow it to run for hours.
`--max-runtime` begins shutdown when its deadline expires: SIGTERM, then SIGKILL after `grace`.

Transient renewal failures retry on fresh connections: connection/DNS errors, timeouts, PostgreSQL shutdown/startup, read-only primary, connection limits, lock conflicts and deadlocks. The pause doubles from `--heartbeat-retry-interval` to `max(2s, heartbeat-retry-interval)` and resets after recovery. Each attempt, including reconnection, shares one `db-timeout`, clipped to the last confirmed lease deadline minus `grace`. Attempts stop when another backoff no longer fits; failures never extend that deadline. Signals and `--max-runtime` remain active. Diagnostics report `phase=heartbeat result=retry` and `result=recovered`.
Ownership loss, authentication, permission, TLS, schema and unclassified errors stop immediately. Acquisition and final completion are not retried; a handler finishing during an outage still needs a confirmed completion commit.

Handler completion is monitored during renewal as well as retry backoff. If the database connection needs replacing before recording the outcome, Solo reconnects once; connection and final completion share one `db-timeout`. This does not reacquire the lease or retry the completion transaction.

## Outcomes and failures

| Outcome | Solo exit code | stderr |
|---|---|---|
| Success committed to the database | 0 | `result=success` |
| Key busy | 0 | `result=busy` |
| Interval already successful | 0 | `result=completed` |
| Configuration, database, handler, ownership, signal, or runtime failure | Nonzero | Phase and reason |

Exit code 0 also covers normal skips; distinguish outcomes using stderr.
Diagnostics do not block handler supervision: a full or unavailable stderr may lose messages, while heartbeats and shutdown continue.
A handler exit code of 0 becomes a success only after COMMIT is confirmed: recording success and releasing the lease are atomic.

- Exhausted heartbeat retry budget, permanent renewal errors or ownership loss stop the process group; no success is recorded and Solo exits nonzero.
- A timed-out final COMMIT also produces a nonzero exit, even if the database saved the success. The next attempt checks the history.
- When a handler fails, Solo attempts to release the lease. If the database is unavailable, the lease expires by TTL.
- Lease expiry does not trigger another attempt; the external scheduler does that.

## Guarantees and limits

- The command never starts without an available database and an acquired lease; an expired owner cannot renew it or confirm success.
- Exactly-once external effects and fencing are not provided. Handlers must be idempotent: an external action can complete before success recording fails.
- The command must remain in the foreground, wait for its children, and keep them in its process group. Solo stops the group with SIGTERM/SIGKILL.
- On Linux, SIGKILL of Solo kills only its direct child through a parent-death signal. Use a container or orchestrator to clean up the entire job.
- Long process pauses, uninterruptible kernel work, and submitted requests can outlive the lease; failover with data loss can erase success history.
- Solo runs trusted code. Keys do not isolate applications. `SOLO_DATABASE_URL` and `PG*` variables are removed from the child's environment; do not duplicate credentials under other names.

## History retention

Retention bounds success-history growth. The current horizon is **2928 days**, shared by all schedules.

| Question | Behavior |
|---|---|
| Configurable? | No CLI/environment setting yet; this is an implementation limit, not a PostgreSQL limit |
| Whose records are deleted? | Only those belonging to the supplied `--key`; other jobs are unaffected |
| When? | In the transaction that acquires a new lease; no cleanup on `busy` or `completed` |
| How many? | Up to 1024 records strictly older than the retention horizon, using database time |
| How selected? | By key and timestamp using the `(key, scheduled_at)` index; selected rows are locked and deleted individually |
| Background cleanup? | None; without new acquisitions, old records remain |

## Operations

Solo automatically prepares the database schema during `run` startup. Errors and unknown schema versions prevent the handler from starting.
Checking an up-to-date schema does not take a global migration lock; first-time initialization is serialized and rechecks the schema under the lock.
Schema upgrades, key deletion, and storage planning are covered in the [operations guide](docs/usage.md#schema-and-operations).

CLI reference: `solo --help`, `solo run --help`, `solo status --help`.
