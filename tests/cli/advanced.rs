use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use nix::sys::signal::Signal;
use std::{
    fs,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

macro_rules! cases {
    ($h:expr; $($name:ident),+ $(,)?) => { vec![$({
        let harness = Arc::clone($h);
        Trial::test(concat!("advanced_", stringify!($name)), move || {
            let runtime = harness.runtime.clone();
            runtime.block_on($name(harness)).map_err(|error| format!("{error:#}").into())
        })
    }),+] };
}
pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    cases!(harness; stale_finish, stale_renew, malformed_migration, hostile_dsn)
}

async fn stale_finish(harness: Arc<Harness>) -> Result<()> {
    stale_after_lock(harness, "finish").await
}
async fn stale_renew(harness: Arc<Harness>) -> Result<()> {
    stale_after_lock(harness, "renew").await
}

async fn stale_after_lock(harness: Arc<Harness>, operation: &str) -> Result<()> {
    let db = harness.database().await?;
    let release = db.path("release");
    let script = format!(
        "echo $$ > '{}'; while [ ! -f '{}' ]; do sleep 0.01; done",
        db.path("pid").display(),
        release.display()
    );
    // Larger budgets make the observed lock interleaving robust under suite load.
    let mut command = Command::new(env!("CARGO_BIN_EXE_solo"));
    command.env("SOLO_DATABASE_URL", &db.dsn).args([
        "run",
        "--key",
        "locked",
        "--schedule",
        "@yearly",
        "--ttl",
        "10s",
        "--heartbeat",
        "200ms",
        "--db-timeout",
        "3s",
        "--grace",
        "100ms",
        "--migration-timeout",
        "500ms",
        "--",
        "/bin/sh",
        "-c",
        &script,
    ]);
    let mut process = db.start(command)?;
    process.track_group(&db.path("pid")).await?;
    // Freeze the supervisor while its lease ages. The child stays foreground.
    process.signal(Signal::SIGSTOP)?;
    db.client.execute("UPDATE public.solo_leases SET expires_at=clock_timestamp()+interval '2 seconds' WHERE key='locked'", &[]).await?;
    let lock = common::connect(&db.dsn).await?;
    lock.batch_execute("BEGIN; SELECT key FROM public.solo_leases WHERE key='locked' FOR UPDATE")
        .await?;
    // This transaction deliberately never changes the tuple: PostgreSQL's UPDATE
    // predicate recheck on a changed tuple must not mask stale-time evaluation.
    db.client.query_one("SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM expires_at-clock_timestamp())-1.0)) FROM public.solo_leases WHERE key='locked'", &[]).await?;
    if operation == "finish" {
        fs::write(&release, "go")?;
        common::stopped(common::pid(&db.path("pid"))?).await?;
    }
    process.signal(Signal::SIGCONT)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(900))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let waiting: bool = db.client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT key FROM public.solo_leases%')", &[]).await?.try_get(0)?;
        if waiting {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "{operation} did not reach row-lock wait; {}",
            process.logs()?
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    db.client.query_one("SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM expires_at-clock_timestamp())+0.01)) FROM public.solo_leases WHERE key='locked'", &[]).await?;
    lock.batch_execute("COMMIT").await?;
    ensure!(
        !process.wait().await?.success(),
        "stale {operation} succeeded: {}",
        process.logs()?
    );
    common::stopped(common::pid(&db.path("pid"))?).await?;
    let row = db.client.query_one("SELECT owner IS NOT NULL AND expires_at<clock_timestamp() AND last_success IS NULL FROM public.solo_leases WHERE key='locked'", &[]).await?;
    ensure!(
        row.try_get::<_, bool>(0)?,
        "stale {operation} changed lease or recorded success"
    );
    let marker = db.path("retry");
    ensure!(
        db.spawn("locked", &format!("touch '{}'", marker.display()))?
            .wait()
            .await?
            .success(),
        "retry failed"
    );
    ensure!(marker.exists(), "expired lease prevented retry");
    Ok(())
}

async fn malformed_migration(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.client
        .batch_execute("CREATE TABLE public.solo_leases (unexpected TEXT)")
        .await?;
    let mut process = db.spawn(
        "malformed",
        &format!("touch '{}'", db.path("unexpected").display()),
    )?;
    ensure!(
        !process.wait().await?.success(),
        "malformed schema accepted"
    );
    ensure!(
        !db.path("unexpected").exists(),
        "child ran after migration failure"
    );
    let installed: bool = db
        .client
        .query_one(
            "SELECT to_regclass('public.solo_schema_version') IS NOT NULL",
            &[],
        )
        .await?
        .try_get(0)?;
    ensure!(!installed, "failed migration left version advancement");
    db.client
        .batch_execute("DROP TABLE public.solo_leases")
        .await?;
    ensure!(
        db.spawn("malformed", "exit 0")?.wait().await?.success(),
        "migration retry failed"
    );
    Ok(())
}

async fn hostile_dsn(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    for dsn in [
        "host=127.0.0.1 port=1 user=hostile_user_92 password=hostile_secret_73 sslmode=disable connect_timeout=1",
        "postgresql://hostile_user_92:hostile_secret_73@127.0.0.1:1/db?sslmode=disable&hostile_option=hostile_value_61",
    ] {
        let mut command = db.command("secret", "@yearly");
        command
            .env("SOLO_DATABASE_URL", dsn)
            .args(["--", "/usr/bin/touch"])
            .arg(db.path("unexpected"));
        let mut process = db.start(command)?;
        ensure!(!process.wait().await?.success(), "hostile DSN succeeded");
        let logs = process.logs()?;
        for secret in [
            dsn,
            "hostile_user_92",
            "hostile_secret_73",
            "hostile_value_61",
        ] {
            ensure!(
                !logs.contains(secret),
                "database diagnostic exposed hostile DSN content"
            );
        }
        ensure!(
            !db.path("unexpected").exists(),
            "child ran with unusable database"
        );
    }
    Ok(())
}
