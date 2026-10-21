use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let heartbeat_harness = Arc::clone(harness);
    let finish_harness = Arc::clone(harness);
    let harness = Arc::clone(harness);
    vec![
        Trial::test("migration_wait_backend_exits", move || {
            harness
                .runtime
                .clone()
                .block_on(migration_wait_backend_exits(harness))
                .map_err(|error| format!("{error:#}").into())
        }),
        Trial::test("heartbeat_wait_backend_exits", move || {
            heartbeat_harness
                .runtime
                .clone()
                .block_on(row_wait_backend_exits(heartbeat_harness, "heartbeat"))
                .map_err(|error| format!("{error:#}").into())
        }),
        Trial::test("finish_wait_backend_exits", move || {
            finish_harness
                .runtime
                .clone()
                .block_on(row_wait_backend_exits(finish_harness, "finish"))
                .map_err(|error| format!("{error:#}").into())
        }),
    ]
}

async fn waiter(client: &tokio_postgres::Client) -> Result<Option<i32>> {
    Ok(client.query_opt("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock(1936682095, 1)%'", &[]).await?.map(|row| row.get(0)))
}

async fn migration_wait_backend_exits(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut blocker = common::connect(&db.dsn).await?;
    let tx = blocker.transaction().await?;
    tx.query_one("SELECT pg_advisory_xact_lock(1936682095, 1)", &[])
        .await?;
    let mut cli = db.spawn("blocked-migration", "exit 0")?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    let backend = loop {
        if let Some(pid) = waiter(&db.client).await? {
            break pid;
        }
        ensure!(
            Instant::now() < deadline,
            "migration lock waiter was never observed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    ensure!(
        !cli.wait().await?.success(),
        "blocked migration unexpectedly succeeded"
    );
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        if !db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                &[&backend],
            )
            .await?
            .get::<_, bool>(0)
        {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "migration backend {backend} remains after CLI timeout while blocker is held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tx.rollback().await?;
    Ok(())
}

async fn row_wait_backend_exits(h: Arc<Harness>, stage: &str) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    let ready = db.path("ready");
    let release = db.path("release");
    let script = format!(
        "echo $$ > '{}'; while [ ! -f '{}' ]; do sleep 0.01; done",
        ready.display(),
        release.display()
    );
    let mut cli = db.spawn(stage, &script)?;
    cli.track_group(&ready).await?;
    let mut blocker = common::connect(&db.dsn).await?;
    let tx = blocker.transaction().await?;
    tx.query_one(
        "SELECT key FROM public.solo_leases WHERE key=$1 FOR UPDATE",
        &[&stage],
    )
    .await?;
    if stage == "finish" {
        std::fs::write(&release, "")?;
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    let backend = loop {
        let rows = db.client.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT key FROM public.solo_leases%FOR UPDATE'", &[]).await?;
        if let [row] = rows.as_slice() {
            break row.get::<_, i32>(0);
        }
        ensure!(
            Instant::now() < deadline,
            "{stage} lock waiter was never observed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    ensure!(
        !cli.wait().await?.success(),
        "blocked {stage} unexpectedly succeeded"
    );
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                &[&backend],
            )
            .await?
            .get(0);
        if !exists {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "{stage} backend {backend} remains after CLI timeout while blocker is held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tx.rollback().await?;
    Ok(())
}
