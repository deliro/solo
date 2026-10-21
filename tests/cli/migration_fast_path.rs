use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::sync::Arc;

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let current_harness = Arc::clone(harness);
    let malformed_harness = Arc::clone(harness);
    vec![
        Trial::test("migration_fast_path_current_schema", move || {
            current_harness
                .runtime
                .clone()
                .block_on(current_schema(current_harness))
                .map_err(|error| format!("{error:#}").into())
        }),
        Trial::test("migration_fast_path_missing_version", move || {
            malformed_harness
                .runtime
                .clone()
                .block_on(missing_version(malformed_harness))
                .map_err(|error| format!("{error:#}").into())
        }),
    ]
}

async fn current_schema(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    let mut blocker = common::connect(&db.dsn).await?;
    let tx = blocker.transaction().await?;
    tx.query_one("SELECT pg_advisory_xact_lock(1936682095, 1)", &[])
        .await?;
    let mut first = db.spawn(
        "independent-first",
        &format!("touch '{}'", db.path("first").display()),
    )?;
    let mut second = db.spawn(
        "independent-second",
        &format!("touch '{}'", db.path("second").display()),
    )?;
    ensure!(
        first.wait().await?.success(),
        "current schema start blocked by migration lock: {}",
        first.logs()?
    );
    ensure!(
        second.wait().await?.success(),
        "second current schema start blocked: {}",
        second.logs()?
    );
    ensure!(db.path("first").exists(), "first handler did not execute");
    ensure!(db.path("second").exists(), "second handler did not execute");
    tx.rollback().await?;
    Ok(())
}

async fn missing_version(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute("DELETE FROM public.solo_schema_version")
        .await?;
    let mut cli = db.spawn(
        "missing-version",
        &format!("touch '{}'", db.path("unexpected").display()),
    )?;
    ensure!(!cli.wait().await?.success(), "missing version accepted");
    ensure!(
        cli.logs()?.contains("reason=migration"),
        "unexpected failure"
    );
    ensure!(
        !db.path("unexpected").exists(),
        "invalid-schema handler ran"
    );
    Ok(())
}
