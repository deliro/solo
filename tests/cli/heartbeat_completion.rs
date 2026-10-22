use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use nix::sys::signal::Signal;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Failure,
    Runtime,
    Signal,
}

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    [
        ("heartbeat_completion_before_runtime", Outcome::Success),
        ("heartbeat_completion_failed_child", Outcome::Failure),
        ("heartbeat_completion_runtime", Outcome::Runtime),
        ("heartbeat_completion_signal", Outcome::Signal),
    ]
    .into_iter()
    .map(|(name, outcome)| {
        let harness = Arc::clone(harness);
        Trial::test(name, move || {
            harness
                .runtime
                .clone()
                .block_on(completion_during_renewal(harness, outcome))
                .map_err(|error| format!("{error:#}").into())
        })
    })
    .collect()
}

async fn completion_during_renewal(harness: Arc<Harness>, outcome: Outcome) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    // Only renewal updates sleep; acquisition and final completion stay fast.
    db.client
        .batch_execute(include_str!("../fixtures/slow_renewal.sql"))
        .await?;
    let ready = db.path("ready");
    let release = db.path("release");
    let exit = match outcome {
        Outcome::Failure => 7,
        Outcome::Success | Outcome::Runtime | Outcome::Signal => 0,
    };
    let script = format!(
        "echo $$ > '{}'; while [ ! -f '{}' ]; do sleep 0.01; done; exit {exit}",
        ready.display(),
        release.display()
    );
    let mut command = db.command("completion-during-renewal", "@yearly");
    command.args([
        "--ttl",
        "3s",
        "--heartbeat",
        "100ms",
        "--db-timeout",
        "1s",
        "--max-runtime",
        "500ms",
        "--",
        "/bin/sh",
        "-c",
        &script,
    ]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    wait_for_renewal(&db.client).await?;
    match outcome {
        Outcome::Success | Outcome::Failure => std::fs::write(&release, "")?,
        Outcome::Signal => cli.signal(Signal::SIGTERM)?,
        Outcome::Runtime => {}
    }
    let status = cli.wait().await?;
    let succeeds = matches!(outcome, Outcome::Success);
    ensure!(
        status.success() == succeeds,
        "unexpected child result: {}",
        cli.logs()?
    );
    let reason = match outcome {
        Outcome::Success => "result=success",
        Outcome::Failure => "reason=handler_failure",
        Outcome::Runtime => "reason=runtime_or_lease_deadline",
        Outcome::Signal => "reason=sigterm",
    };
    ensure!(
        cli.logs()?.contains(reason),
        "incorrect completion reason: {}",
        cli.logs()?
    );
    common::stopped(common::pid(&ready)?).await?;
    let row = db.client.query_one(
        "SELECT owner IS NULL, last_outcome, (SELECT count(*) FROM public.solo_successes WHERE key=$1) FROM public.solo_leases WHERE key=$1",
        &[&"completion-during-renewal"],
    ).await?;
    ensure!(
        row.get::<_, bool>(0),
        "completion did not release ownership"
    );
    ensure!(
        row.get::<_, String>(1) == if succeeds { "success" } else { "failure" },
        "unexpected persisted outcome"
    );
    ensure!(
        row.get::<_, i64>(2) == i64::from(succeeds),
        "unexpected success history"
    );
    if succeeds {
        let mut duplicate = db.spawn("completion-during-renewal", "exit 9")?;
        ensure!(
            duplicate.wait().await?.success() && duplicate.logs()?.contains("result=completed"),
            "completion was not deduplicated: {}",
            duplicate.logs()?
        );
    }
    Ok(())
}

async fn wait_for_renewal(client: &tokio_postgres::Client) -> Result<()> {
    // Synchronize on the actual in-flight renewal instead of assuming startup speed.
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let sleeping: bool = client.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='PgSleep')", &[],
        ).await?.get(0);
        if sleeping {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "renewal did not start"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}
