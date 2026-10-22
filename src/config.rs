use crate::schedule::{Schedule, ScheduleError};
use clap::{Args, Parser, Subcommand};
use std::{ffi::OsString, time::Duration};

pub(super) enum Invocation {
    Run(Config),
    Status(StatusConfig),
}

pub(super) struct StatusConfig {
    pub(super) key: String,
    pub(super) database_url: String,
    pub(super) db_timeout: Duration,
}

// Deliberately no Debug: database_url contains credentials.
pub(super) struct Config {
    pub(super) key: String,
    pub(super) schedule: Schedule,
    pub(super) command: Vec<OsString>,
    pub(super) database_url: String,
    pub(super) max_runtime: Option<Duration>,
    pub(super) ttl: Duration,
    pub(super) heartbeat: Duration,
    pub(super) heartbeat_retry_interval: Duration,
    pub(super) db_timeout: Duration,
    pub(super) grace: Duration,
    pub(super) migration_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ConfigError {
    #[error("{0}")]
    Cli(#[from] clap::Error),
    #[error("{0}")]
    Schedule(#[from] ScheduleError),
    #[error("SOLO_DATABASE_URL must contain a nonempty PostgreSQL connection string")]
    DatabaseUrl,
    #[error(
        "key and command must be nonempty; durations must be positive and at most 365 days; ttl must exceed heartbeat + db-timeout + grace"
    )]
    Invalid,
    #[error("key must be nonempty; db-timeout must be positive and at most 365 days")]
    InvalidStatus,
}

#[derive(Parser)]
#[command(
    name = "solo",
    version,
    about = "Run a command once per UTC cron interval across hosts using a PostgreSQL lease",
    long_about = "Solo coordinates one trusted foreground command across hosts that share a
writable PostgreSQL primary. A lease prevents concurrent attempts for the same
key; recorded successes suppress repeated attempts in the same UTC cron
interval.

Solo performs one attempt and exits. Use cron, a Kubernetes CronJob, or
another external scheduler to invoke it. The schedule selects the current
interval; Solo does not wait for the next tick or backfill missed intervals.

Handlers must be idempotent: leases do not provide exactly-once external
effects or fencing. Linux and macOS are supported.",
    after_help = "DATABASE:
  Set SOLO_DATABASE_URL to a PostgreSQL URL or key=value connection string.
  sslmode defaults to prefer: try TLS, allow plaintext if the server rejects TLS.
  TLS verifies the system CA and hostname; use sslmode=require to forbid plaintext
  or sslmode=disable to skip TLS. Credentials are never accepted as CLI arguments.
  Private CA overrides: SSL_CERT_FILE and SSL_CERT_DIR.

EXIT STATUS:
  0: status printed, or run succeeded/skipped (busy or already completed).
  Nonzero: configuration, database, handler, ownership, signal, or runtime failure.
  For run, stderr result=success|busy|completed distinguishes outcomes.

EXAMPLES:
  solo run --key billing/prod/reconcile --schedule '*/5 * * * *' --max-runtime 4m -- /bin/echo reconcile
  solo status --key billing/prod/reconcile"
)]
struct Cli {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    #[command(
        about = "Inspect a key's lease and last recorded outcomes without changing the database",
        long_about = "Read the lease owner, database time, expiry, last attempt and completion, and
last successful interval for one key. A held lease does not prove that its
handler is alive.

This command does not run migrations, launch a handler, renew a lease, or
release ownership. The schema must already exist. Only --key and
SOLO_DATABASE_URL are required.",
        after_help = "EXAMPLE:
  solo status --key billing/prod/reconcile

DATABASE:
  sslmode defaults to prefer: try TLS, allow plaintext if the server rejects TLS.
  sslmode=require verifies the server certificate and hostname using system CAs."
    )]
    Status {
        #[arg(
            long,
            env = "SOLO_KEY",
            help = "Task key shared by all instances of the same job"
        )]
        key: String,
        #[arg(long, env = "SOLO_DB_TIMEOUT", default_value = "3s", value_parser = humantime::parse_duration, help = "Deadline for connection and each database operation (positive, at most 365 days)")]
        db_timeout: Duration,
    },
    #[command(
        about = "Acquire a lease, run one foreground command, and record its outcome",
        long_about = "Attempt the current UTC cron interval once. If the key is busy or this
interval has already succeeded, skip the handler and exit with code 0.
Otherwise renew the lease while the command runs, then atomically record
success and release ownership.

Transient heartbeat failures retry on fresh connections while the confirmed
lease has time remaining, reserving --grace for shutdown. Ownership loss and
permanent errors stop immediately. Handler completion remains monitored during
renewal. A needed reconnection and final completion share one DB timeout;
the completion transaction is not retried. On exhausted renewal budget, SIGTERM,
SIGINT, or --max-runtime, stop
the process group with SIGTERM, then SIGKILL after --grace. A handler exit
code of 0 counts as success only after the database commit is confirmed.

The command must remain in the foreground, wait for its children, and keep
them in its process group. Arguments after -- are passed directly; no shell is
inserted. SOLO_DATABASE_URL and PG* variables are removed from the child's
environment.",
        after_help = "CONSTRAINTS:
  Durations accept units such as 200ms, 30s, 5m, 1h; range: (0, 365 days].
  ttl must exceed heartbeat + db-timeout + grace. Renewals can keep a command
  running beyond one TTL; use --max-runtime to bound its lifetime.
  All instances of a key must use the same schedule. Stop old instances before
  changing it. Handlers must be idempotent; exactly-once effects are not guaranteed.

DATABASE:
  sslmode defaults to prefer: try TLS, allow plaintext if the server rejects TLS.
  sslmode=require verifies the server certificate and hostname using system CAs.
  The role needs schema DDL and table read/write privileges for automatic setup.

EXAMPLE:
  solo run --key billing/prod/reconcile --schedule '*/5 * * * *' --max-runtime 4m -- /bin/echo reconcile"
    )]
    Run(RunArgs),
}

#[derive(Args)]
struct RunArgs {
    #[arg(
        long,
        env = "SOLO_KEY",
        help = "Task key sharing one lease and success history across hosts"
    )]
    key: String,
    #[arg(
        long,
        env = "SOLO_SCHEDULE",
        help = "Five-field Kubernetes-style cron in UTC, or @hourly/@daily/@weekly/@monthly/@yearly",
        long_help = "Select the latest scheduled instant not after PostgreSQL time.
Supports lists, ranges, steps, JAN-DEC, SUN-SAT (0-6), and ? as *. A
restricted day-of-month and weekday use OR; otherwise they use AND.
Aliases: @hourly, @daily, @midnight, @weekly, @monthly, @yearly,
@annually. No seconds, time zones, L, W, or #. This selects an
interval; it does not start a timer."
    )]
    schedule: String,
    #[arg(long, env = "SOLO_MAX_RUNTIME", value_parser = humantime::parse_duration, help = "Maximum handler runtime before shutdown begins (unlimited when omitted)")]
    max_runtime: Option<Duration>,
    #[arg(long, env = "SOLO_TTL", default_value = "30s", value_parser = humantime::parse_duration, help = "Lease lifetime without a successful renewal; not a handler runtime limit")]
    ttl: Duration,
    #[arg(long, env = "SOLO_HEARTBEAT", default_value = "5s", value_parser = humantime::parse_duration, help = "Interval between the starts of lease renewal attempts")]
    heartbeat: Duration,
    #[arg(long, env = "SOLO_HEARTBEAT_RETRY_INTERVAL", default_value = "200ms", value_parser = humantime::parse_duration, help = "Initial pause before retrying a transient renewal failure", long_help = "Initial pause before retrying a transient renewal failure on a fresh connection. Doubles up to max(this interval, 2s). Retries are bounded by the last confirmed lease deadline minus grace; each connection plus renewal attempt is bounded by db-timeout. Ownership loss and permanent errors are never retried.")]
    heartbeat_retry_interval: Duration,
    #[arg(long, env = "SOLO_DB_TIMEOUT", default_value = "3s", value_parser = humantime::parse_duration, help = "Deadline for connection and each coordination operation")]
    db_timeout: Duration,
    #[arg(long, env = "SOLO_GRACE", default_value = "2s", value_parser = humantime::parse_duration, help = "Wait between SIGTERM and SIGKILL when stopping the process group")]
    grace: Duration,
    #[arg(long, env = "SOLO_MIGRATION_TIMEOUT", default_value = "10s", value_parser = humantime::parse_duration, help = "Deadline for the entire startup migration, including lock acquisition")]
    migration_timeout: Duration,
    #[arg(last = true, required = true, num_args = 1.., help = "Trusted foreground executable and exact arguments; use /bin/sh -c explicitly for shell scripts")]
    command: Vec<OsString>,
}

impl Invocation {
    pub(super) fn parse() -> Result<Self, ConfigError> {
        let cli = Cli::try_parse()?;
        let database_url = std::env::var("SOLO_DATABASE_URL").ok();
        match cli.action {
            Action::Status { key, db_timeout } => {
                if key.is_empty() || db_timeout.is_zero() || db_timeout > Duration::from_hours(8760)
                {
                    return Err(ConfigError::InvalidStatus);
                }
                let database_url = database_url
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(ConfigError::DatabaseUrl)?;
                Ok(Self::Status(StatusConfig {
                    key,
                    database_url,
                    db_timeout,
                }))
            }
            Action::Run(args) => Config::from_args(args, database_url).map(Self::Run),
        }
    }
}

impl Config {
    fn from_args(args: RunArgs, database_url: Option<String>) -> Result<Self, ConfigError> {
        let RunArgs {
            key,
            schedule,
            max_runtime,
            ttl,
            heartbeat,
            heartbeat_retry_interval,
            db_timeout,
            grace,
            migration_timeout,
            command,
        } = args;
        let budget = heartbeat
            .checked_add(db_timeout)
            .and_then(|value| value.checked_add(grace))
            .ok_or(ConfigError::Invalid)?;
        if key.is_empty()
            || command.first().is_none_or(|value| value.is_empty())
            || [
                ttl,
                heartbeat,
                heartbeat_retry_interval,
                db_timeout,
                grace,
                migration_timeout,
            ]
            .iter()
            .any(|value| value.is_zero() || *value > Duration::from_hours(8760))
            || max_runtime
                .is_some_and(|value| value.is_zero() || value > Duration::from_hours(8760))
            || ttl <= budget
        {
            return Err(ConfigError::Invalid);
        }
        let schedule = Schedule::parse(&schedule)?;
        let database_url = database_url
            .filter(|value| !value.trim().is_empty())
            .ok_or(ConfigError::DatabaseUrl)?;
        Ok(Self {
            key,
            schedule,
            command,
            database_url,
            max_runtime,
            ttl,
            heartbeat,
            heartbeat_retry_interval,
            db_timeout,
            grace,
            migration_timeout,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError, RunArgs};
    use std::{ffi::OsString, time::Duration};

    #[rstest::rstest]
    #[case::runtime(Some(Duration::MAX), Duration::from_millis(200))]
    #[case::zero_retry(None, Duration::ZERO)]
    #[case::unrepresentable_retry(None, Duration::MAX)]
    fn rejects_invalid_durations(
        #[case] max_runtime: Option<Duration>,
        #[case] heartbeat_retry_interval: Duration,
    ) {
        let args = RunArgs {
            key: "duration-bound".to_owned(),
            schedule: "* * * * *".to_owned(),
            command: vec![OsString::from("true")],
            max_runtime,
            ttl: Duration::from_secs(30),
            heartbeat: Duration::from_secs(5),
            heartbeat_retry_interval,
            db_timeout: Duration::from_secs(3),
            grace: Duration::from_secs(2),
            migration_timeout: Duration::from_secs(10),
        };
        assert!(matches!(
            Config::from_args(args, Some("unused".to_owned())),
            Err(ConfigError::Invalid)
        ));
    }

    #[rstest::rstest]
    #[case::root(vec!["solo", "--help"], vec!["external scheduler", "sslmode=require", "exactly-once", "EXIT STATUS"])]
    #[case::run(vec!["solo", "run", "--help"], vec!["SIGTERM", "PG*", "ttl must exceed", "--migration-timeout", "No seconds", "--heartbeat-retry-interval", "SOLO_HEARTBEAT_RETRY_INTERVAL", "last confirmed lease deadline"])]
    #[case::status(vec!["solo", "status", "--help"], vec!["does not run migrations", "held lease", "--db-timeout", "SOLO_DATABASE_URL"])]
    fn help_explains_contract(
        #[case] arguments: Vec<&str>,
        #[case] required: Vec<&str>,
    ) -> anyhow::Result<()> {
        use clap::Parser as _;
        let error = super::Cli::try_parse_from(arguments)
            .err()
            .ok_or_else(|| anyhow::anyhow!("help must exit before parsing configuration"))?;
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        let help = error.to_string();
        let normalized = help.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            required.into_iter().all(|text| normalized.contains(text)),
            "{help}"
        );
        assert!(
            !help
                .chars()
                .any(|character| ('\u{0400}'..='\u{04ff}').contains(&character))
        );
        Ok(())
    }
}
