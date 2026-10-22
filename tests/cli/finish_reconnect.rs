use crate::common::Harness;
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::{fs, sync::Arc, time::Duration};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    [
        ("finish_reconnect_success_during_retry", "57P03", 0),
        ("finish_reconnect_failure_during_retry", "57P03", 1),
        ("finish_reconnect_success_after_timeout", "timeout", 0),
    ]
    .into_iter()
    .map(|(name, code, exit)| {
        let harness = Arc::clone(harness);
        Trial::test(name, move || {
            harness
                .runtime
                .clone()
                .block_on(finish_during_retry(harness, code, exit))
                .map_err(|error| format!("{error:#}").into())
        })
    })
    .collect()
}

async fn finish_during_retry(harness: Arc<Harness>, code: &str, exit: u8) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute(include_str!("../fixtures/renewal_failures.sql"))
        .await?;
    db.client
        .execute(
            "INSERT INTO renewal_fault (code, failures) VALUES ($1, $2)",
            &[&code, &1_i64],
        )
        .await?;
    let ready = db.path("ready");
    let finish = db.path("finish");
    let script = format!(
        "echo $$ > '{}'; while [ ! -f '{}' ]; do sleep 0.02; done; exit {exit}",
        ready.display(),
        finish.display()
    );
    let mut command = db.command("finish-reconnect", "@yearly");
    command.args([
        "--ttl",
        "5s",
        "--db-timeout",
        "500ms",
        "--heartbeat-retry-interval",
        "2s",
        "--",
        "/bin/sh",
        "-c",
        &script,
    ]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(3))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    while !cli.logs()?.contains("result=retry") {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "retry was not observed: {}",
            cli.logs()?
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let acquired: String = db
        .client
        .query_one(
            "SELECT acquired_at::text FROM public.solo_leases WHERE key=$1",
            &[&"finish-reconnect"],
        )
        .await?
        .try_get(0)?;
    fs::write(&finish, "finish")?;
    let status = cli.wait().await?;
    ensure!(
        status.success() == (exit == 0),
        "unexpected completion status: {}",
        cli.logs()?
    );
    let row = db
        .client
        .query_one(
            "SELECT owner IS NULL, last_outcome, acquired_at::text, (SELECT count(*) FROM public.solo_successes WHERE key=$1) FROM public.solo_leases WHERE key=$1",
            &[&"finish-reconnect"],
        )
        .await?;
    ensure!(row.try_get::<_, bool>(0)?, "ownership was not released");
    let expected = if exit == 0 { "success" } else { "failure" };
    ensure!(row.try_get::<_, &str>(1)? == expected, "wrong outcome");
    ensure!(
        row.try_get::<_, String>(2)? == acquired,
        "finalization reacquired ownership"
    );
    ensure!(
        row.try_get::<_, i64>(3)? == i64::from(exit == 0),
        "wrong success history"
    );
    let attempts: i64 = db
        .client
        .query_one("SELECT last_value FROM renewal_attempt", &[])
        .await?
        .try_get(0)?;
    ensure!(attempts == 1, "handler did not finish during retry backoff");
    if exit == 0 {
        let mut duplicate = db.spawn("finish-reconnect", "exit 1")?;
        ensure!(
            duplicate.wait().await?.success() && duplicate.logs()?.contains("result=completed"),
            "completed interval was not deduplicated: {}",
            duplicate.logs()?
        );
    }
    Ok(())
}
