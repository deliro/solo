use crate::common::{Database, Harness};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use libtest_mimic::Trial;
use std::{fs, sync::Arc, time::Duration};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let harness = Arc::clone(harness);
    vec![Trial::test(
        "windows_completed_five_minute_window",
        move || {
            harness
                .runtime
                .clone()
                .block_on(completed_five_minute_window(harness))
                .map_err(|error| format!("{error:#}").into())
        },
    )]
}

async fn current_window(db: &Database) -> Result<DateTime<Utc>> {
    Ok(db.client.query_one(
        "SELECT date_bin(interval '5 minutes', clock_timestamp(), timestamptz '2000-01-01 00:00:00+00')",
        &[],
    ).await?.try_get(0)?)
}

async fn run(db: &Database) -> Result<String> {
    let mut command = db.command("five-minute", "*/5 * * * *");
    command
        .args(["--", "/bin/sh", "-c", "echo run >> \"$1\"", "handler"])
        .arg(db.path("markers"));
    let mut process = db.start(command)?;
    ensure!(
        process.wait().await?.success(),
        "CLI failed: {}",
        process.logs()?
    );
    process.logs()
}

async fn assert_committed(db: &Database, scheduled: DateTime<Utc>) -> Result<()> {
    let row = db.client.query_one(
        "SELECT owner IS NULL AND scheduled_at IS NULL AND expires_at IS NULL, last_success=$1, (SELECT count(*) FROM public.solo_successes WHERE key='five-minute' AND scheduled_at=$1) FROM public.solo_leases WHERE key='five-minute'",
        &[&scheduled],
    ).await?;
    ensure!(
        row.try_get::<_, bool>(0)? && row.try_get::<_, bool>(1)? && row.try_get::<_, i64>(2)? == 1,
        "expected released ownership and exactly one committed success for {scheduled}"
    );
    Ok(())
}

async fn completed_five_minute_window(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    // Avoid beginning the short two-process probe immediately before a boundary.
    let remaining: f64 = db.client.query_one(
        "SELECT extract(epoch FROM (date_bin(interval '5 minutes', clock_timestamp(), timestamptz '2000-01-01 00:00:00+00') + interval '5 minutes' - clock_timestamp()))::double precision",
        &[],
    ).await?.try_get(0)?;
    if remaining < 10.0 {
        tokio::time::sleep(Duration::from_secs_f64(remaining.max(0.0) + 0.1)).await;
    }
    let scheduled = current_window(&db).await?;
    run(&db).await?;
    assert_committed(&db, scheduled).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    ensure!(
        current_window(&db).await? == scheduled,
        "probe crossed a real DB window boundary"
    );
    let duplicate = run(&db).await?;
    ensure!(
        duplicate.contains("completed"),
        "duplicate did not report completed: {duplicate}"
    );
    ensure!(
        current_window(&db).await? == scheduled,
        "duplicate crossed a real DB window boundary"
    );
    assert_committed(&db, scheduled).await?;
    ensure!(
        fs::read_to_string(db.path("markers"))?.lines().count() == 1,
        "completed five-minute window executed handler twice"
    );

    // Simulate the state seen in the next window: the successful pair is now
    // five minutes older than the current DB window. No production clock override.
    let previous = scheduled
        .checked_sub_signed(chrono::Duration::minutes(5))
        .ok_or_else(|| anyhow::anyhow!("test window overflow"))?;
    db.client.execute(
        "UPDATE public.solo_successes SET scheduled_at=$1 WHERE key='five-minute' AND scheduled_at=$2",
        &[&previous, &scheduled],
    ).await?;
    db.client
        .execute(
            "UPDATE public.solo_leases SET last_success=$1 WHERE key='five-minute'",
            &[&previous],
        )
        .await?;
    run(&db).await?;
    assert_committed(&db, scheduled).await?;
    ensure!(
        fs::read_to_string(db.path("markers"))?.lines().count() == 2,
        "previous window success suppressed current window"
    );
    let count: i64 = db.client.query_one(
        "SELECT count(*) FROM public.solo_successes WHERE key='five-minute' AND scheduled_at IN ($1,$2)",
        &[&previous, &scheduled],
    ).await?.try_get(0)?;
    ensure!(
        count == 2,
        "both distinct successful window identities must remain recorded"
    );
    Ok(())
}
