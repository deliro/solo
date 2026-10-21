use crate::common::Harness;
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use nix::sys::signal::Signal;
use std::{process::Command, sync::Arc, time::Duration};

struct BudgetCase {
    ttl: &'static str,
    heartbeat: &'static str,
    db_timeout: &'static str,
    delay_ms: u32,
    handler_ms: u64,
}

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let mut trials: Vec<_> = [
        (
            "heartbeat_budget_delayed_renewals_complete_handler",
            BudgetCase {
                ttl: "1s",
                heartbeat: "500ms",
                db_timeout: "300ms",
                delay_ms: 150,
                handler_ms: 2200,
            },
        ),
        (
            "heartbeat_budget_long_renewal_completes_finished_handler",
            BudgetCase {
                ttl: "1100ms",
                heartbeat: "100ms",
                db_timeout: "800ms",
                delay_ms: 500,
                handler_ms: 550,
            },
        ),
        (
            "heartbeat_budget_long_repeated_renewals_complete_handler",
            BudgetCase {
                ttl: "1100ms",
                heartbeat: "100ms",
                db_timeout: "800ms",
                delay_ms: 450,
                handler_ms: 2200,
            },
        ),
    ]
    .into_iter()
    .map(|(name, budget)| {
        let harness = Arc::clone(harness);
        Trial::test(name, move || {
            harness
                .runtime
                .clone()
                .block_on(delayed_renewals_complete_handler(harness, budget))
                .map_err(|error| format!("{error:#}").into())
        })
    })
    .collect();
    trials.extend(
        [
            (
                "heartbeat_retry_recovers_after_transient_errors",
                "57P01",
                2,
                true,
            ),
            ("heartbeat_retry_recovers_after_timeout", "timeout", 1, true),
            ("heartbeat_retry_rejects_permanent_error", "42501", 1, false),
            ("heartbeat_retry_rejects_unknown_error", "P0001", 1, false),
            (
                "heartbeat_retry_recovers_read_only_primary",
                "25006",
                1,
                true,
            ),
            (
                "heartbeat_retry_stops_at_lease_reserve",
                "57P03",
                100,
                false,
            ),
        ]
        .into_iter()
        .map(|(name, code, failures, succeeds)| {
            let harness = Arc::clone(harness);
            Trial::test(name, move || {
                harness
                    .runtime
                    .clone()
                    .block_on(renewal_failure(harness, code, failures, succeeds))
                    .map_err(|error| format!("{error:#}").into())
            })
        }),
    );
    trials.extend(
        [
            ("heartbeat_retry_sigterm", Some(Signal::SIGTERM)),
            ("heartbeat_retry_sigint", Some(Signal::SIGINT)),
            ("heartbeat_retry_max_runtime", None),
        ]
        .into_iter()
        .map(|(name, signal)| {
            let harness = Arc::clone(harness);
            Trial::test(name, move || {
                harness
                    .runtime
                    .clone()
                    .block_on(retry_shutdown(harness, signal))
                    .map_err(|error| format!("{error:#}").into())
            })
        }),
    );
    trials
}

async fn retry_shutdown(harness: Arc<Harness>, signal: Option<Signal>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute(include_str!("../fixtures/renewal_failures.sql"))
        .await?;
    db.client
        .execute(
            "INSERT INTO renewal_fault (code, failures) VALUES ($1, $2)",
            &[&"57P03", &100_i64],
        )
        .await?;
    let ready = db.path("ready");
    let script = format!("echo $$ > '{}'; sleep 10", ready.display());
    let mut command = db.command("retry-shutdown", "@yearly");
    command.args([
        "--heartbeat-retry-interval",
        "1s",
        "--max-runtime",
        "700ms",
        "--",
        "/bin/sh",
        "-c",
        &script,
    ]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    while !cli.logs()?.contains("result=retry") {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "retry was not observed: {}",
            cli.logs()?
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if let Some(signal) = signal {
        cli.signal(signal)?;
    }
    ensure!(
        !cli.wait().await?.success(),
        "shutdown unexpectedly succeeded"
    );
    let reason = match signal {
        Some(Signal::SIGTERM) => "reason=sigterm",
        Some(Signal::SIGINT) => "reason=sigint",
        None => "reason=max_runtime",
        Some(_) => anyhow::bail!("unsupported test signal"),
    };
    ensure!(
        cli.logs()?.contains(reason),
        "shutdown lost its reason: {}",
        cli.logs()?
    );
    crate::common::stopped(crate::common::pid(&ready)?).await?;
    Ok(())
}

async fn renewal_failure(
    harness: Arc<Harness>,
    code: &str,
    failures: i64,
    succeeds: bool,
) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute(include_str!("../fixtures/renewal_failures.sql"))
        .await?;
    db.client
        .execute(
            "INSERT INTO renewal_fault (code, failures) VALUES ($1, $2)",
            &[&code, &failures],
        )
        .await?;
    let ready = db.path("ready");
    let completed = db.path("completed");
    let script = format!(
        "echo $$ > '{}'; sleep 2.5; touch '{}'",
        ready.display(),
        completed.display()
    );
    let mut command = db.command("renewal-fault", "@yearly");
    command.args(["--", "/bin/sh", "-c", &script]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    ensure!(
        cli.wait().await?.success() == succeeds,
        "unexpected renewal result: {}",
        cli.logs()?
    );
    ensure!(
        completed.exists() == succeeds,
        "unexpected handler completion: {}",
        cli.logs()?
    );
    let attempts: i64 = db
        .client
        .query_one("SELECT last_value FROM renewal_attempt", &[])
        .await?
        .get(0);
    if succeeds {
        ensure!(
            attempts > failures && cli.logs()?.contains("result=recovered"),
            "renewal did not recover: {}",
            cli.logs()?
        );
    } else if failures == 1 {
        ensure!(
            attempts == 1 && !cli.logs()?.contains("result=retry"),
            "permanent error was retried"
        );
    } else {
        ensure!(
            attempts > 1 && !completed.exists(),
            "retry budget was not enforced"
        );
    }
    Ok(())
}

async fn delayed_renewals_complete_handler(
    harness: Arc<Harness>,
    budget: BudgetCase,
) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    // Delay only renewals: acquisition is an INSERT and completion clears owner.
    // Only numeric constants from the cases above enter the SQL fixture.
    db.client
        .batch_execute(&format!(
            "CREATE FUNCTION delay_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.owner IS NOT NULL AND NEW.owner = OLD.owner THEN
                     PERFORM pg_sleep({} / 1000.0);
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER delay_renewal BEFORE UPDATE ON public.solo_leases
             FOR EACH ROW EXECUTE FUNCTION delay_renewal();",
            budget.delay_ms
        ))
        .await?;
    let ready = db.path("ready");
    let completed = db.path("completed");
    let script = format!(
        "echo $$ > '{}'; sleep {}; echo completed > '{}'",
        ready.display(),
        Duration::from_millis(budget.handler_ms).as_secs_f64(),
        completed.display()
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_solo"));
    command
        .env_remove("SOLO_MAX_RUNTIME")
        .env("SOLO_DATABASE_URL", &db.dsn)
        .args([
            "run",
            "--key",
            "heartbeat-budget",
            "--schedule",
            "@yearly",
            "--migration-timeout",
            "500ms",
            "--ttl",
            budget.ttl,
            "--heartbeat",
            budget.heartbeat,
            "--db-timeout",
            budget.db_timeout,
            "--grace",
            "100ms",
            "--",
            "/bin/sh",
            "-c",
            &script,
        ]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    ensure!(
        cli.wait().await?.success() && completed.exists(),
        "handler must complete across repeated in-budget renewals; logs: {}",
        cli.logs()?
    );
    Ok(())
}
