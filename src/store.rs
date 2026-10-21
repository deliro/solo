use crate::schedule::Schedule;
use chrono::{DateTime, Utc};
use rustls::{ClientConfig, RootCertStore};
use std::{error::Error, future::Future, io, time::Duration};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_postgres::{Client, Config, Transaction, config::SslMode, error::SqlState, types::Type};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(super) enum StoreError {
    #[error("{0}")]
    Database(DatabaseCause),
    #[error("code=timeout database operation exceeded {}; check database availability or increase the operation timeout", humantime::format_duration(*.0))]
    Timeout(Duration),
    #[error("lease ownership lost")]
    Lost,
    #[error(
        "code=schema unsupported or missing database schema; initialize an empty database with solo run; update an existing schema to match this binary"
    )]
    Schema,
    #[error(
        "code=sslmode invalid database configuration; set sslmode=require or explicit sslmode=disable"
    )]
    Configuration,
    #[error(
        "code=invalid_dsn invalid database connection string; check connection string syntax and sslmode values"
    )]
    InvalidDsn,
    #[error("invalid lease duration or schedule")]
    Invalid,
}

// Only fixed, reviewed text crosses the CLI boundary. Driver/server text and
// configuration values may contain credentials, even inside source errors.
#[derive(Debug, thiserror::Error)]
pub(super) enum DatabaseCause {
    #[error("code=authentication SQLSTATE=28P01 authentication failed; check database credentials")]
    Authentication,
    #[error(
        "code=database_missing SQLSTATE=3D000 database does not exist; check database name or create the database"
    )]
    MissingDatabase,
    #[error(
        "code=permission SQLSTATE=42501 permission denied; grant the database role the required privileges"
    )]
    Permission,
    #[error(
        "code=connection_refused connection refused; check database host and port and whether PostgreSQL is listening"
    )]
    Refused,
    #[error("code=dns address lookup failed; check database hostname and DNS configuration")]
    Dns,
    #[error(
        "code=endpoint_not_found local database connection endpoint does not exist; check the Unix socket path and whether PostgreSQL is listening"
    )]
    EndpointNotFound,
    #[error(
        "code=timeout connection timed out; check network reachability and connection timeout settings"
    )]
    Timeout,
    #[error(
        "code=tls TLS negotiation failed; check server TLS support, certificate trust and hostname"
    )]
    Tls,
    #[error("code=connection database connection interrupted; check database availability")]
    Connection,
    #[error(
        "code=unavailable database temporarily unavailable or read-only; check primary availability and connection limits"
    )]
    Unavailable,
    #[error(
        "code=contention database transaction interrupted by a lock, deadlock or statement timeout"
    )]
    Contention,
    #[error("code=database database operation failed; check database availability and server logs")]
    Other,
}

#[derive(Debug)]
#[must_use]
pub(super) enum Acquire {
    Acquired(Lease),
    Busy,
    Completed,
}
#[derive(Debug)]
#[must_use]
pub(super) struct Lease {
    key: String,
    owner: Uuid,
    scheduled: DateTime<Utc>,
    valid_until: Instant,
}
impl Lease {
    #[must_use]
    pub(super) const fn scheduled(&self) -> DateTime<Utc> {
        self.scheduled
    }
    #[must_use]
    pub(super) const fn valid_until(&self) -> Instant {
        self.valid_until
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Completion {
    Success,
    Failure,
}

#[derive(Debug)]
#[must_use]
pub(super) enum TaskStatus {
    Missing,
    Found(TaskSnapshot),
}
#[derive(Debug)]
pub(super) struct TaskSnapshot {
    pub(super) observed_at: DateTime<Utc>,
    pub(super) owner: Option<Uuid>,
    pub(super) owner_host: Option<String>,
    pub(super) owner_pid: Option<i64>,
    pub(super) scheduled_at: Option<DateTime<Utc>>,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) acquired_at: Option<DateTime<Utc>>,
    pub(super) last_completed_at: Option<DateTime<Utc>>,
    pub(super) last_outcome: Option<Completion>,
    pub(super) last_success_at: Option<DateTime<Utc>>,
    pub(super) last_success: Option<DateTime<Utc>>,
}

pub(super) struct Store {
    client: Client,
    driver: JoinHandle<()>,
    db_timeout: Duration,
}
impl Drop for Store {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl From<tokio_postgres::Error> for StoreError {
    fn from(error: tokio_postgres::Error) -> Self {
        let cause = match error.code().map(SqlState::code) {
            Some("28P01") => DatabaseCause::Authentication,
            Some("3D000") => DatabaseCause::MissingDatabase,
            Some("42501") => DatabaseCause::Permission,
            Some("57P01" | "57P02" | "57P03" | "53300" | "25006") => DatabaseCause::Unavailable,
            Some("40P01" | "55P03" | "57014") => DatabaseCause::Contention,
            Some(code) if code.starts_with("08") => DatabaseCause::Connection,
            _ if error.is_closed() => DatabaseCause::Connection,
            _ => classify_source(&error),
        };
        Self::Database(cause)
    }
}

impl StoreError {
    pub(super) const fn is_retryable(&self) -> bool {
        match self {
            Self::Timeout(_)
            | Self::Database(
                DatabaseCause::Refused
                | DatabaseCause::Dns
                | DatabaseCause::EndpointNotFound
                | DatabaseCause::Timeout
                | DatabaseCause::Connection
                | DatabaseCause::Unavailable
                | DatabaseCause::Contention,
            ) => true,
            Self::Lost
            | Self::Schema
            | Self::Configuration
            | Self::InvalidDsn
            | Self::Invalid
            | Self::Database(
                DatabaseCause::Authentication
                | DatabaseCause::MissingDatabase
                | DatabaseCause::Permission
                | DatabaseCause::Tls
                | DatabaseCause::Other,
            ) => false,
        }
    }
}

fn classify_source(error: &(dyn Error + 'static)) -> DatabaseCause {
    let mut source = error.source();
    while let Some(error) = source {
        if error.is::<rustls::Error>() {
            return DatabaseCause::Tls;
        }
        if let Some(error) = error.downcast_ref::<io::Error>() {
            if let Some(cause) = error.get_ref()
                && cause.is::<rustls::Error>()
            {
                return DatabaseCause::Tls;
            }
            // std's resolver wraps platform-specific getaddrinfo errors in an
            // io::Error. Inspect its fixed prefix only; never emit OS text.
            if error
                .to_string()
                .starts_with("failed to lookup address information:")
            {
                return DatabaseCause::Dns;
            }
            return match error.kind() {
                io::ErrorKind::ConnectionRefused => DatabaseCause::Refused,
                io::ErrorKind::TimedOut => DatabaseCause::Timeout,
                io::ErrorKind::NotFound => DatabaseCause::EndpointNotFound,
                io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::NotConnected => DatabaseCause::Connection,
                _ => DatabaseCause::Other,
            };
        }
        // tokio-postgres 0.7.18 uses this fixed local error for an SSLRequest
        // rejection. Equality is classification only; never print source text.
        if error.to_string() == "server does not support TLS" {
            return DatabaseCause::Tls;
        }
        source = error.source();
    }
    DatabaseCause::Other
}

async fn tls_connector(mode: SslMode) -> Result<MakeRustlsConnect, StoreError> {
    let mut roots = RootCertStore::empty();
    if mode == SslMode::Require {
        let certificates = tokio::task::spawn_blocking(rustls_native_certs::load_native_certs)
            .await
            .map_err(|_| StoreError::Database(DatabaseCause::Tls))?;
        let (added, _) = roots.add_parsable_certificates(certificates.certs);
        if added == 0 {
            return Err(StoreError::Database(DatabaseCause::Tls));
        }
    }
    // Select the provider explicitly: feature unification must not choose it or panic.
    let config =
        ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|_| StoreError::Database(DatabaseCause::Tls))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

fn status_error(error: tokio_postgres::Error) -> StoreError {
    match error.code() {
        Some(&SqlState::UNDEFINED_TABLE | &SqlState::UNDEFINED_COLUMN) => StoreError::Schema,
        _ => error.into(),
    }
}

async fn bounded<T>(
    duration: Duration,
    operation: impl Future<Output = Result<T, StoreError>>,
) -> Result<T, StoreError> {
    timeout(duration, operation)
        .await
        .map_err(|_| StoreError::Timeout(duration))?
}
fn ttl_seconds(ttl: Duration) -> Result<f64, StoreError> {
    if ttl.is_zero() || ttl > Duration::from_hours(8760) {
        return Err(StoreError::Invalid);
    }
    Ok(ttl.as_secs_f64())
}
async fn server_deadline(tx: &Transaction<'_>, duration: Duration) -> Result<(), StoreError> {
    // PostgreSQL timeout GUCs are signed 32-bit milliseconds; zero disables them.
    let milliseconds = duration.as_millis().clamp(1, 2_147_483_647).to_string();
    tx.query_typed_one(
        "SELECT set_config('statement_timeout', $1, true), set_config('lock_timeout', $1, true)",
        &[(&milliseconds, Type::TEXT)],
    )
    .await?;
    Ok(())
}

// Typed calls send unnamed Parse/Bind/Execute together. No prepare round trip
// or named statement survives a PgBouncer transaction-pool backend handover.
impl Store {
    pub(super) fn disconnect(&self) {
        self.driver.abort();
    }

    pub(super) async fn connect(
        database_url: &str,
        db_timeout: Duration,
    ) -> Result<Self, StoreError> {
        if db_timeout.is_zero() {
            return Err(StoreError::Configuration);
        }
        let config: Config = database_url.parse().map_err(|_| StoreError::InvalidDsn)?;
        // Prefer permits plaintext fallback. Require authenticated TLS, or make
        // the operator opt in explicitly to local/plaintext transport.
        if !matches!(config.get_ssl_mode(), SslMode::Require | SslMode::Disable) {
            return Err(StoreError::Configuration);
        }
        let (client, connection) = bounded(db_timeout, async {
            let tls = tls_connector(config.get_ssl_mode()).await?;
            config.connect(tls).await.map_err(Into::into)
        })
        .await?;
        let driver = tokio::spawn(async move {
            // Connection failure is delivered by pending/subsequent client calls.
            // Never log its raw error because diagnostics may disclose secrets.
            let _connection_result = connection.await;
        });
        Ok(Self {
            client,
            driver,
            db_timeout,
        })
    }

    pub(super) async fn status(&mut self, key: &str) -> Result<TaskStatus, StoreError> {
        let result = bounded(self.db_timeout, async {
            let tx = self.client.build_transaction().read_only(true).start().await?;
            server_deadline(&tx, self.db_timeout).await?;
            let versions = tx.query_typed("SELECT version FROM public.solo_schema_version WHERE singleton = TRUE", &[]).await.map_err(status_error)?;
            match versions.as_slice() {
                [row] if row.try_get::<_, i64>(0)? == 1 => {},
                _ => return Err(StoreError::Schema),
            }
            let rows = tx.query_typed("SELECT statement_timestamp() AS observed_at, owner, owner_host, owner_pid, scheduled_at, expires_at, acquired_at, last_completed_at, last_outcome, last_success_at, last_success FROM public.solo_leases WHERE key=$1", &[(&key, Type::TEXT)]).await.map_err(status_error)?;
            let status = match rows.as_slice() {
                [] => TaskStatus::Missing,
                [row] => {
                    let last_outcome = match row.try_get::<_, Option<&str>>("last_outcome")? {
                        Some("success") => Some(Completion::Success),
                        Some("failure") => Some(Completion::Failure),
                        None => None,
                        Some(_) => return Err(StoreError::Schema),
                    };
                    TaskStatus::Found(TaskSnapshot {
                        observed_at: row.try_get("observed_at")?,
                        owner: row.try_get("owner")?,
                        owner_host: row.try_get("owner_host")?,
                        owner_pid: row.try_get("owner_pid")?,
                        scheduled_at: row.try_get("scheduled_at")?,
                        expires_at: row.try_get("expires_at")?,
                        acquired_at: row.try_get("acquired_at")?,
                        last_completed_at: row.try_get("last_completed_at")?,
                        last_outcome,
                        last_success_at: row.try_get("last_success_at")?,
                        last_success: row.try_get("last_success")?,
                    })
                },
                _ => return Err(StoreError::Schema),
            };
            tx.commit().await?;
            Ok(status)
        }).await;
        if result.is_err() {
            self.driver.abort();
        }
        result
    }

    pub(super) async fn migrate(&mut self, migration_timeout: Duration) -> Result<(), StoreError> {
        let result = bounded(migration_timeout, async {
            let tx = self.client.transaction().await?;
            server_deadline(&tx, migration_timeout).await?;
            // Existing supported schemas need no global migration lock. Only
            // initialization serializes, and rechecks after acquiring the lock
            // because another startup may have committed while we waited.
            let version = if let Some(version) = schema_version(&tx).await? {
                version
            } else {
                tx.query_typed_one("SELECT pg_advisory_xact_lock(1936682095, 1)", &[])
                    .await?;
                if let Some(version) = schema_version(&tx).await? {
                    version
                } else {
                    tx.batch_execute(include_str!("../migrations/0001_coordination.up.sql"))
                        .await?;
                    1
                }
            };
            if version != 1 {
                return Err(StoreError::Schema);
            }
            tx.commit().await?;
            Ok(())
        })
        .await;
        if result.is_err() {
            self.driver.abort();
        }
        result
    }

    pub(super) async fn acquire(
        &mut self,
        key: &str,
        schedule: &Schedule,
        ttl: Duration,
    ) -> Result<Acquire, StoreError> {
        let seconds = ttl_seconds(ttl)?;
        let valid_until = Instant::now().checked_add(ttl).ok_or(StoreError::Invalid)?;
        let owner = Uuid::new_v4();
        let host = nix::unistd::gethostname()
            .map_err(|_| StoreError::Invalid)?
            .to_string_lossy()
            .into_owned();
        let pid = i64::from(std::process::id());
        let result = bounded(self.db_timeout, async {
            let tx = self.client.transaction().await?;
            server_deadline(&tx, self.db_timeout).await?;
            let locked: bool = tx.query_typed_one("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 1936682095))", &[(&key, Type::TEXT)]).await?.try_get(0)?;
            if !locked { return Ok(Acquire::Busy); }
            let now: DateTime<Utc> = tx.query_typed_one("SELECT clock_timestamp()", &[]).await?.try_get(0)?;
            let scheduled = schedule.latest(now).map_err(|_| StoreError::Invalid)?;
            // NOWAIT prevents queueing behind a concurrent heartbeat/completion.
            let rows = match tx.query_typed("SELECT owner, expires_at FROM public.solo_leases WHERE key=$1 FOR UPDATE NOWAIT", &[(&key, Type::TEXT)]).await {
                Ok(rows) => rows,
                Err(error) if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => return Ok(Acquire::Busy),
                Err(error) => return Err(error.into()),
            };
            if let [row] = rows.as_slice() {
                let expires: Option<DateTime<Utc>> = row.try_get(1)?;
                if expires.is_some_and(|expiry| expiry > now) { return Ok(Acquire::Busy); }
            }
            let completed: bool = tx.query_typed_one("SELECT (SELECT TRUE FROM public.solo_successes WHERE key=$1 AND scheduled_at=$2) IS TRUE", &[(&key, Type::TEXT), (&scheduled, Type::TIMESTAMPTZ)]).await?.try_get(0)?;
            if completed { return Ok(Acquire::Completed); }
            // A universal horizon survives schedule changes and the Gregorian
            // 2096-to-2104 leap-day gap. Statement time makes the cutoff indexable;
            // locked ctids stay valid within this statement and bound deletion too.
            tx.execute_typed("WITH expired AS MATERIALIZED (SELECT ctid FROM public.solo_successes WHERE key=$1 AND scheduled_at < statement_timestamp()-interval '2928 days' ORDER BY scheduled_at LIMIT 1024 FOR UPDATE) DELETE FROM public.solo_successes AS target USING expired WHERE target.ctid=expired.ctid", &[(&key, Type::TEXT)]).await?;
            tx.execute_typed("INSERT INTO public.solo_leases (key,owner,scheduled_at,expires_at,acquired_at,owner_host,owner_pid) VALUES ($1,$2,$3,clock_timestamp()+$4::double precision * interval '1 second',clock_timestamp(),$5,$6) ON CONFLICT (key) DO UPDATE SET owner=EXCLUDED.owner,scheduled_at=EXCLUDED.scheduled_at,expires_at=EXCLUDED.expires_at,acquired_at=EXCLUDED.acquired_at,owner_host=EXCLUDED.owner_host,owner_pid=EXCLUDED.owner_pid", &[(&key, Type::TEXT), (&owner, Type::UUID), (&scheduled, Type::TIMESTAMPTZ), (&seconds, Type::FLOAT8), (&host, Type::TEXT), (&pid, Type::INT8)]).await?;
            tx.commit().await?;
            if Instant::now() >= valid_until { return Err(StoreError::Lost); }
            Ok(Acquire::Acquired(Lease { key: key.to_owned(), owner, scheduled, valid_until }))
        }).await;
        if result.is_err() {
            self.driver.abort();
        }
        result
    }

    pub(super) async fn heartbeat(
        &mut self,
        lease: &Lease,
        ttl: Duration,
    ) -> Result<(), StoreError> {
        let seconds = ttl_seconds(ttl)?;
        let result = bounded(self.db_timeout, async {
            let tx = self.client.transaction().await?;
            server_deadline(&tx, self.db_timeout).await?;
            tx.query_typed("SELECT key FROM public.solo_leases WHERE key=$1 FOR UPDATE", &[(&lease.key, Type::TEXT)]).await?;
            let count = tx.execute_typed("UPDATE public.solo_leases SET expires_at=clock_timestamp()+$4::double precision * interval '1 second' WHERE key=$1 AND owner=$2 AND scheduled_at=$3 AND expires_at>clock_timestamp()", &[(&lease.key, Type::TEXT), (&lease.owner, Type::UUID), (&lease.scheduled, Type::TIMESTAMPTZ), (&seconds, Type::FLOAT8)]).await?;
            if count != 1 { return Err(StoreError::Lost); }
            tx.commit().await?;
            Ok(())
        }).await;
        if result.is_err() {
            self.driver.abort();
        }
        result
    }

    pub(super) async fn finish(
        &mut self,
        lease: &Lease,
        completion: Completion,
    ) -> Result<(), StoreError> {
        let success = match completion {
            Completion::Success => true,
            Completion::Failure => false,
        };
        let result = bounded(self.db_timeout, async {
            let tx = self.client.transaction().await?;
            server_deadline(&tx, self.db_timeout).await?;
            tx.query_typed("SELECT key FROM public.solo_leases WHERE key=$1 FOR UPDATE", &[(&lease.key, Type::TEXT)]).await?;
            let count = tx.execute_typed("UPDATE public.solo_leases SET last_success=CASE WHEN $4 THEN scheduled_at ELSE last_success END,last_completed_at=statement_timestamp(),last_outcome=CASE WHEN $4 THEN 'success' ELSE 'failure' END,last_success_at=CASE WHEN $4 THEN statement_timestamp() ELSE last_success_at END,owner=NULL,owner_host=NULL,owner_pid=NULL,scheduled_at=NULL,expires_at=NULL WHERE key=$1 AND owner=$2 AND scheduled_at=$3 AND expires_at>clock_timestamp()", &[(&lease.key, Type::TEXT), (&lease.owner, Type::UUID), (&lease.scheduled, Type::TIMESTAMPTZ), (&success, Type::BOOL)]).await?;
            if count != 1 { return Err(StoreError::Lost); }
            if success {
                tx.execute_typed("INSERT INTO public.solo_successes (key,scheduled_at) VALUES ($1,$2) ON CONFLICT (key,scheduled_at) DO NOTHING", &[(&lease.key, Type::TEXT), (&lease.scheduled, Type::TIMESTAMPTZ)]).await?;
            }
            tx.commit().await?;
            Ok(())
        }).await;
        if result.is_err() {
            self.driver.abort();
        }
        result
    }
}

async fn schema_version(tx: &Transaction<'_>) -> Result<Option<i64>, StoreError> {
    let exists: bool = tx
        .query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='solo_schema_version')",
            &[],
        )
        .await?
        .try_get(0)?;
    if !exists {
        return Ok(None);
    }
    let rows = tx
        .query_typed(
            "SELECT version FROM public.solo_schema_version WHERE singleton = TRUE",
            &[],
        )
        .await?;
    match rows.as_slice() {
        [row] => Ok(Some(row.try_get(0)?)),
        _ => Err(StoreError::Schema),
    }
}
