# Solo

Solo runs one trusted foreground command on Linux or macOS, coordinated by a common writable PostgreSQL primary. The external scheduler owns retries and must use UTC. For binary downloads and checksum-verified installation, see the [README](../README.md#installation).

## Run

The `solo` executable must be available in `PATH`.

```sh
export SOLO_DATABASE_URL='postgresql://solo:password@db.example/solo?sslmode=require'
solo run --key 'billing/prod/daily' --schedule '0 0 * * *' --max-runtime 30m -- /opt/jobs/billing --daily
```

Set the connection string through your secret manager/environment. Do not put it on the Solo command line. `sslmode=require` uses native TLS certificate and hostname verification; install the issuing CA in the system trust store. For a trusted local test database, explicitly use `sslmode=disable`. The implicit `prefer` mode is rejected because it permits a plaintext fallback.

PgBouncer transaction pooling is supported with `max_prepared_statements=0`. Coordination uses parameterized unnamed protocol operations and does not require a statement cache or backend affinity across transactions.

Commands and arguments after `--` are passed directly, without shell expansion. To request a shell explicitly, use `-- /bin/sh -c 'your trusted script'`. Solo is not a sandbox. The child does not receive `SOLO_DATABASE_URL` or PostgreSQL `PG*` environment variables. Other inherited environment variables remain the operator's responsibility; do not duplicate the coordination credentials under other names.

## Schedule and outcomes

The required key is an opaque string. Reusing a key means sharing both exclusion and successful intervals, even if the commands differ. Distinct keys run independently. All executors of a key must use the same schedule; pause old executors before changing it.

Use Kubernetes five-field cron in UTC: numbers, lists, ranges, steps, month/day names, `?`, and `@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly`, `@annually`. Restricted day-of-month and day-of-week fields have Kubernetes OR semantics. Seconds, Quartz extensions, `@every` and embedded time zones are rejected. Search is bounded to a 400-year Gregorian cycle; impossible schedules fail closed.

The latest scheduled instant at or before PostgreSQL's current time defines the attempt. Late starts do not backfill. The interval is frozen throughout the attempt. A live lease excludes even a different interval of the same key.

| Outcome | Exit status | Diagnostic |
|---|---|---|
| Child succeeds and success is committed | 0 | `result=success` |
| Key is busy | 0 | `result=busy` |
| Interval already succeeded | 0 | `result=completed` |
| Configuration, child, DB, ownership, signal or runtime failure | nonzero | phase/reason on stderr |

Do not infer confirmed success from the handler's exit alone: Solo must atomically record it and release ownership. A failure after a business side effect can replay that effect. Handlers must be idempotent; there is no exactly-once or business-resource fencing guarantee.

Run diagnostics use a bounded best-effort queue. A full or unavailable stderr may lose messages, but cannot block lease management or child shutdown. Final diagnostic flushing is limited to 100ms.

## Status

```sh
solo status --key billing/prod/daily
```

Status requires `SOLO_DATABASE_URL` and a key; it does not require a schedule or a command. `--db-timeout` / `SOLO_DB_TIMEOUT` bounds the operation (default 3s). Run-only configuration is ignored.

- Reads through a read-only transaction; never runs migrations or modifies ownership/history.
- Distinguishes unknown, free, held and expired ownership. A held lease does not prove child liveness.
- Shows owner UUID, host and Solo PID; acquisition time, elapsed time and remaining lease use database time.
- Latest acquisition describes an attempt. The last recorded completion/outcome and latest successful completion are separate from scheduled interval timestamps.
- Historical metadata that was never recorded stays unknown. A supervisor crash or unavailable DB can leave completion unrecorded.
- A role with SELECT on `solo_schema_version` and `solo_leases` can inspect an initialized schema without DDL or write privileges. A missing or outdated schema returns an error instead of being modified.

## Timing

| Flag | Default | Meaning |
|---|---|---|
| `--ttl` | 30s | Lease duration |
| `--heartbeat` | 5s | Interval between renewal starts |
| `--heartbeat-retry-interval` | 200ms | Initial backoff for transient renewal failures |
| `--db-timeout` | 3s | Bound for one coordination operation |
| `--grace` | 2s | SIGTERM-to-SIGKILL delay |
| `--migration-timeout` | 10s | Entire startup migration budget |
| `--max-runtime` | absent | Time until child shutdown begins |

Durations accept units such as `200ms`, `30s`, `5m`, `1h`; each must be positive and at most 365 days. TTL must exceed heartbeat + DB timeout + grace. Solo reserves time to stop the child before its conservative local lease deadline. On failure it sends SIGTERM to the managed process group, then SIGKILL after grace and reaps the direct child.

Renewal retries reconnect without reacquiring ownership or migrating. Retryable errors include connection/DNS failures, timeouts, unavailable/read-only PostgreSQL, connection limits, lock conflicts and deadlocks. Backoff doubles from `--heartbeat-retry-interval` up to `max(2s, heartbeat-retry-interval)` and resets after recovery. Each reconnection plus renewal attempt shares one DB timeout and is clipped to the last confirmed lease deadline minus grace. Failures never extend the lease budget; stop when another pause cannot fit. Ownership loss and permanent/unclassified errors stop immediately. Signals and runtime deadlines remain active. Acquisition and final completion are not retried.

Environment equivalents are `SOLO_KEY`, `SOLO_SCHEDULE`, `SOLO_MAX_RUNTIME`, `SOLO_TTL`, `SOLO_HEARTBEAT`, `SOLO_HEARTBEAT_RETRY_INTERVAL`, `SOLO_DB_TIMEOUT`, `SOLO_GRACE` and `SOLO_MIGRATION_TIMEOUT`. Explicit CLI flags take precedence. The command still follows `--`; the DSN is accepted only through `SOLO_DATABASE_URL`.

Handler completion remains monitored during renewal and retry backoff. A handler that finishes before `--max-runtime` can proceed to final completion without waiting for an in-flight renewal. If its connection needs replacing, one reconnection and final completion share a single `db-timeout`; the same owner and scheduled interval remain in use. The completion transaction is attempted once, and success still requires a confirmed commit.

Handlers must remain foreground, wait for descendants, and keep them in the managed process group. Linux parent-death signaling kills the direct child if Solo is killed; it does **not** guarantee cleanup of all grandchildren. Use an orchestrator/container process boundary for whole-workload cleanup after supervisor death. A paused executor, uninterruptible kernel work, already-sent external requests and primary failover data loss remain limitations of leases.

## Schema and operations

Startup migrations run transactionally under a bounded PostgreSQL advisory lock. The role needs schema/DDL privileges as well as runtime read/write privileges. Unknown schema versions and failed or blocked migrations prevent handler launch. Startup migration is intentional, as required by the Solo specification.

An already-current schema is checked without the exclusive migration lock. Initialization takes the lock and rechecks the schema before creating it, so independent task starts do not queue behind a global lock once initialization is complete.

`public.solo_leases` retains one lease/diagnostic row per key. Successful intervals are exact `(key, scheduled_at)` pairs in `public.solo_successes`, shared across schedule expressions. Each new acquisition prunes at most 1024 entries for that key older than PostgreSQL current time minus 2928 days; the indexed primary key supports cleanup. Retention is fixed and shared by all schedules. Entries at the cutoff boundary survive.

Minute resolution bounds history to roughly 4.2 million successful intervals per active key within the horizon; budget PostgreSQL storage and vacuum capacity accordingly. Cleanup drains expired history on later acquisitions; inactive keys retain their previous bounded history until retirement. Keep the database clock disciplined: finite retention cannot deduplicate intervals selected after a rollback beyond deleted history. Stop every executor before retiring a key, then delete its rows from both tables. Distinct task-key count remains an operator capacity responsibility.

Stop old executors before an incompatible schema upgrade. A schema reset deletes deduplication state and must not be performed on live work.
