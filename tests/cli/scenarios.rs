use crate::common::{self, Database, Harness};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use libtest_mimic::Trial;
use nix::sys::signal::Signal;
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

macro_rules! cases {
    ($h:expr; $($name:ident),+ $(,)?) => { vec![$({
        let harness = Arc::clone($h);
        Trial::test(stringify!($name), move || {
            let runtime = harness.runtime.clone();
            runtime.block_on($name(harness)).map_err(|error| format!("{error:#}").into())
        })
    }),+] };
}
pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    cases!(harness; contention, simultaneous_first_acquisition, delayed_pid_marker, separate_keys, success_dedup_failure_retry, atomic_success, finish_transaction_failure, replaced_owner, expired_owner, database_unavailable, database_loss, runtime_kills_group, runtime_sends_term, external_sigterm, external_sigint, supervisor_death, heartbeat_frozen_interval, delayed_current_interval, cron_accepted, cron_rejected, migration_race, migration_lock_timeout, migration_version_error, arguments_and_credentials, non_shell_handler_and_no_scrape, environment_configuration)
}
async fn delayed_pid_marker(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let marker = db.path("pid");
    let mut process = db.spawn(
        "marker",
        &format!(
            ": > '{}'; sleep 0.2; echo $$ > '{}'; exec sleep 0.2",
            marker.display(),
            marker.display()
        ),
    )?;
    process.track_group(&marker).await?;
    ensure!(common::pid(&marker)? > 0, "PID marker was not complete");
    ensure!(
        process.wait().await?.success(),
        "marker handler failed: {}",
        process.logs()?
    );
    Ok(())
}
fn hold(db: &Database, marker: &str) -> String {
    format!(
        "echo $$ > '{}'; trap '' TERM; while :; do sleep 1; done",
        db.path(marker).display()
    )
}
fn spawn_contender(db: &Database, key: &str, script: &str) -> Result<common::Process> {
    let mut command = db.command(key, "@yearly");
    // Concurrent process starts compete for CPU and PostgreSQL authentication.
    // These tests assert exclusion, not subsecond connection or migration deadlines.
    command.args([
        "--ttl",
        "5s",
        "--db-timeout",
        "1s",
        "--migration-timeout",
        "2s",
        "--",
        "/bin/sh",
        "-c",
        script,
    ]);
    db.start(command)
}
async fn contention(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut owner = spawn_contender(&db, "shared", &hold(&db, "owner"))?;
    owner.track_group(&db.path("owner")).await?;
    let mut contenders = (0..5)
        .map(|n| {
            spawn_contender(
                &db,
                "shared",
                &format!("touch '{}'", db.path(&format!("unexpected{n}")).display()),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    for contender in &mut contenders {
        ensure!(
            contender.wait().await?.success(),
            "busy must skip: {}",
            contender.logs()?
        );
    }
    ensure!(
        fs::read_dir(db.directory.path())?.all(|entry| entry.is_ok_and(|entry| !entry
            .file_name()
            .to_string_lossy()
            .starts_with("unexpected"))),
        "contender ran"
    );
    Ok(())
}
async fn simultaneous_first_acquisition(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    let barrier = std::sync::Barrier::new(6);
    let attempts = std::thread::scope(|scope| {
        let starts = (0..6)
            .map(|number| {
                let db = &db;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    spawn_contender(db, "first-race", &hold(db, &format!("winner{number}")))
                })
            })
            .collect::<Vec<_>>();
        starts
            .into_iter()
            .map(|start| {
                start
                    .join()
                    .map_err(|_| anyhow::anyhow!("attempt launcher panicked"))?
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let mut attempts = attempts;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let busy = attempts
            .iter()
            .map(common::Process::logs)
            .collect::<Result<Vec<_>>>()?
            .iter()
            .filter(|log| log.contains("phase=acquire result=busy"))
            .count();
        if busy == 5 && (0..6).any(|number| db.path(&format!("winner{number}")).exists()) {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "first-acquisition contenders did not settle; logs: {:?}",
            attempts
                .iter()
                .map(common::Process::logs)
                .collect::<Result<Vec<_>>>()?
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(
        (0..6)
            .filter(|number| db.path(&format!("winner{number}")).exists())
            .count()
            == 1,
        "simultaneous first acquisition launched multiple handlers"
    );
    for (number, process) in attempts.iter_mut().enumerate() {
        let marker = db.path(&format!("winner{number}"));
        if marker.exists() {
            process.track_group(&marker).await?;
            process.signal(Signal::SIGTERM)?;
            ensure!(
                !process.wait().await?.success(),
                "signalled winner succeeded"
            );
            common::stopped(common::pid(&marker)?).await?;
        } else {
            ensure!(
                process.wait().await?.success(),
                "busy contender failed: {}",
                process.logs()?
            );
            ensure!(
                process.logs()?.contains("phase=acquire result=busy"),
                "missing busy diagnostic"
            );
        }
    }
    Ok(())
}
async fn separate_keys(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut a = db.spawn("a", &hold(&db, "a"))?;
    let mut b = db.spawn("b", &hold(&db, "b"))?;
    a.track_group(&db.path("a")).await?;
    b.track_group(&db.path("b")).await?;
    // A different interval for the same key still cannot overlap a live owner.
    let mut command = db.command("a", "* * * * *");
    command
        .args(["--", "/usr/bin/touch"])
        .arg(db.path("overlap"));
    ensure!(
        db.start(command)?.wait().await?.success(),
        "different interval skip failed"
    );
    ensure!(!db.path("overlap").exists(), "same key overlaps");
    Ok(())
}
async fn success_dedup_failure_retry(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let marker = db.path("count");
    let script = format!("echo run >> '{}'", marker.display());
    ensure!(
        db.spawn("success", &script)?.wait().await?.success(),
        "first failed"
    );
    let mut duplicate = db.spawn("success", &script)?;
    ensure!(duplicate.wait().await?.success(), "dedup status");
    ensure!(
        fs::read_to_string(&marker)?.lines().count() == 1,
        "duplicate child"
    );
    ensure!(
        duplicate.logs()?.contains("completed"),
        "missing completed diagnostic"
    );
    ensure!(
        !db.spawn("retry", "exit 7")?.wait().await?.success(),
        "failure status"
    );
    ensure!(
        db.spawn("retry", &script)?.wait().await?.success(),
        "retry blocked"
    );
    ensure!(
        fs::read_to_string(marker)?.lines().count() == 2,
        "retry did not run"
    );
    Ok(())
}
async fn atomic_success(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let release = db.path("release");
    let mut child = db.spawn(
        "atomic",
        &format!(
            "touch '{}'; while [ ! -f '{}' ]; do sleep 0.02; done",
            db.path("ready").display(),
            release.display()
        ),
    )?;
    common::wait_file(&db.path("ready")).await?;
    let observer = common::connect(&db.dsn).await?;
    fs::write(release, "go")?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(4))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let row = observer.query_one("SELECT owner IS NULL, last_success IS NOT NULL FROM public.solo_leases WHERE key='atomic'", &[]).await?;
        let free: bool = row.try_get(0)?;
        let success: bool = row.try_get(1)?;
        ensure!(free == success, "partial completion observed");
        if free {
            break;
        }
        ensure!(Instant::now() < deadline, "completion not published");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    ensure!(child.wait().await?.success(), "completion failed");
    ensure!(
        db.spawn("atomic", "exit 91")?.wait().await?.success(),
        "independent retry not suppressed"
    );
    Ok(())
}
async fn finish_transaction_failure(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    db.client.batch_execute("CREATE FUNCTION public.reject_success() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.last_success IS NOT NULL THEN RAISE EXCEPTION 'test completion fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_success BEFORE UPDATE ON public.solo_leases FOR EACH ROW EXECUTE FUNCTION public.reject_success()").await?;
    let mut failed = db.spawn("fault", "exit 0")?;
    ensure!(
        !failed.wait().await?.success(),
        "failed commit reported success"
    );
    ensure!(
        failed.logs()?.contains("phase=run reason=finish"),
        "failed commit diagnostic missing"
    );
    let row=db.client.query_one("SELECT owner IS NOT NULL,last_success IS NULL FROM public.solo_leases WHERE key='fault'", &[]).await?;
    ensure!(
        row.try_get::<_, bool>(0)? && row.try_get::<_, bool>(1)?,
        "partial transaction state"
    );
    db.client.batch_execute("DROP TRIGGER reject_success ON public.solo_leases; UPDATE public.solo_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE key='fault'").await?;
    ensure!(
        db.spawn(
            "fault",
            &format!("touch '{}'", db.path("retried").display())
        )?
        .wait()
        .await?
        .success(),
        "failed completion prevented retry"
    );
    ensure!(
        db.path("retried").exists(),
        "retry skipped handler execution"
    );
    Ok(())
}
async fn owner_fault(h: Arc<Harness>, replacement: bool) -> Result<()> {
    let db = h.database().await?;
    let mut process = db.spawn("stale", &hold(&db, "pid"))?;
    process.track_group(&db.path("pid")).await?;
    let new_owner = uuid::Uuid::new_v4();
    if replacement {
        db.client.execute("UPDATE public.solo_leases SET owner=$1,expires_at=clock_timestamp()+interval '1 hour' WHERE key='stale'", &[&new_owner]).await?;
    } else {
        db.client.execute("UPDATE public.solo_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE key='stale'",&[]).await?;
    }
    ensure!(!process.wait().await?.success(), "lost owner succeeded");
    ensure!(
        process.logs()?.contains("reason=heartbeat"),
        "primary ownership diagnostic missing: {}",
        process.logs()?
    );
    ensure!(
        process.logs()?.contains("phase=cleanup reason=finish"),
        "cleanup diagnostic missing"
    );
    common::stopped(common::pid(&db.path("pid"))?).await?;
    let row = db
        .client
        .query_one(
            "SELECT owner,last_success IS NULL FROM public.solo_leases WHERE key='stale'",
            &[],
        )
        .await?;
    ensure!(row.try_get::<_, bool>(1)?, "stale success recorded");
    if replacement {
        ensure!(
            row.try_get::<_, uuid::Uuid>(0)? == new_owner,
            "stale owner changed replacement"
        );
    }
    Ok(())
}
async fn replaced_owner(h: Arc<Harness>) -> Result<()> {
    owner_fault(h, true).await
}
async fn expired_owner(h: Arc<Harness>) -> Result<()> {
    owner_fault(h, false).await
}
async fn database_unavailable(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut command = db.command("offline", "@yearly");
    command
        .env("SOLO_DATABASE_URL", format!("{} port=1", db.dsn))
        .args(["--", "/usr/bin/touch"])
        .arg(db.path("unexpected"));
    let mut process = db.start(command)?;
    ensure!(!process.wait().await?.success(), "unavailable DB succeeded");
    ensure!(!db.path("unexpected").exists(), "unprotected execution");
    ensure!(
        !process.logs()?.contains("s4_test_secret"),
        "secret in error"
    );
    Ok(())
}
async fn database_loss(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let release = db.path("release");
    let script = format!(
        "echo $$ > '{}'; while [ ! -f '{}' ]; do sleep 0.01; done",
        db.path("pid").display(),
        release.display()
    );
    let mut process = db.spawn("loss", &script)?;
    process.track_group(&db.path("pid")).await?;
    // Only terminate connections to this test's isolated database, never shared PG.
    db.client.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()",&[]).await?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(3))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    while !process.logs()?.contains("result=recovered") {
        ensure!(
            Instant::now() < deadline,
            "connection was not restored: {}",
            process.logs()?
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    fs::write(release, "")?;
    ensure!(
        process.wait().await?.success(),
        "recovered handler failed: {}",
        process.logs()?
    );
    ensure!(
        !process.logs()?.contains("s4_test_secret"),
        "secret in diagnostic"
    );
    common::stopped(common::pid(&db.path("pid"))?).await?;
    Ok(())
}
async fn termination(h: Arc<Harness>, mode: &str) -> Result<()> {
    let db = h.database().await?;
    let script = if mode == "term" {
        format!(
            "echo $$ > '{}'; trap 'touch {}; exit 0' TERM; while :; do sleep 0.02; done",
            db.path("pid").display(),
            db.path("term").display()
        )
    } else {
        format!(
            "echo $$ > '{}'; trap '' TERM; /bin/sh -c 'trap \"\" TERM; echo $$ > \"$1\"; while :; do sleep 1; done' sh '{}' & wait",
            db.path("pid").display(),
            db.path("descendant").display()
        )
    };
    let mut command = db.command("stop", "@yearly");
    if mode == "term" || mode == "kill" {
        command.args(["--max-runtime", "500ms"]);
    }
    command.args(["--", "/bin/sh", "-c", &script]);
    let mut process = db.start(command)?;
    process.track_group(&db.path("pid")).await?;
    if mode != "term" {
        common::wait_file(&db.path("descendant")).await?;
    }
    match mode {
        "sigterm" => process.signal(Signal::SIGTERM)?,
        "sigint" => process.signal(Signal::SIGINT)?,
        _ => {}
    }
    ensure!(!process.wait().await?.success(), "termination succeeded");
    common::stopped(common::pid(&db.path("pid"))?).await?;
    if mode == "term" {
        ensure!(db.path("term").exists(), "SIGTERM was not delivered");
    } else {
        common::stopped(common::pid(&db.path("descendant"))?).await?;
    }
    Ok(())
}
async fn runtime_kills_group(h: Arc<Harness>) -> Result<()> {
    termination(h, "kill").await
}
async fn runtime_sends_term(h: Arc<Harness>) -> Result<()> {
    termination(h, "term").await
}
async fn external_sigterm(h: Arc<Harness>) -> Result<()> {
    termination(h, "sigterm").await
}
async fn external_sigint(h: Arc<Harness>) -> Result<()> {
    termination(h, "sigint").await
}
async fn supervisor_death(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut process = db.spawn("death", &hold(&db, "pid"))?;
    process.track_group(&db.path("pid")).await?;
    process.signal(Signal::SIGKILL)?;
    ensure!(!process.wait().await?.success(), "SIGKILL status");
    common::stopped(common::pid(&db.path("pid"))?).await?;
    Ok(())
}
async fn heartbeat_frozen_interval(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut process = db.spawn("long", &hold(&db, "pid"))?;
    process.track_group(&db.path("pid")).await?;
    let before = db
        .client
        .query_one(
            "SELECT scheduled_at,expires_at FROM public.solo_leases WHERE key='long'",
            &[],
        )
        .await?;
    let interval: DateTime<Utc> = before.try_get(0)?;
    let expiry: DateTime<Utc> = before.try_get(1)?;
    tokio::time::sleep(Duration::from_millis(2400)).await;
    let row=db.client.query_one("SELECT scheduled_at,expires_at,expires_at>clock_timestamp() FROM public.solo_leases WHERE key='long'",&[]).await?;
    ensure!(
        row.try_get::<_, DateTime<Utc>>(0)? == interval,
        "interval changed"
    );
    ensure!(
        row.try_get::<_, DateTime<Utc>>(1)? > expiry && row.try_get::<_, bool>(2)?,
        "heartbeat not extended"
    );
    ensure!(
        db.spawn("long", "exit 89")?.wait().await?.success(),
        "live owner not skipped"
    );
    Ok(())
}
async fn delayed_current_interval(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    db.client.batch_execute("INSERT INTO public.solo_leases(key,last_success) VALUES ('delayed',date_trunc('minute',clock_timestamp())-interval '2 minutes')").await?;
    let before: DateTime<Utc> = db
        .client
        .query_one("SELECT date_trunc('minute',clock_timestamp())", &[])
        .await?
        .try_get(0)?;
    let mut command = db.command("delayed", "* * * * *");
    command.args(["--", "/usr/bin/true"]);
    let mut process = db.start(command)?;
    ensure!(
        process.wait().await?.success(),
        "delayed run failed: {}",
        process.logs()?
    );
    let row=db.client.query_one("SELECT last_success,date_trunc('minute',clock_timestamp()) FROM public.solo_leases WHERE key='delayed'",&[]).await?;
    let success: DateTime<Utc> = row.try_get(0)?;
    let after: DateTime<Utc> = row.try_get(1)?;
    ensure!(
        success >= before && success <= after,
        "backfilled old interval"
    );
    Ok(())
}
async fn cron_accepted(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    for (n, schedule) in [
        "*/5 * * * *",
        "0,15,30 1-23/2 * * *",
        "0 0 ? JAN MON",
        "0 0 13 * FRI",
        "0 0 29 FEB *",
        "0 0 31 DEC *",
        "@hourly",
        "@daily",
        "@midnight",
        "@weekly",
        "@monthly",
        "@yearly",
        "@annually",
    ]
    .iter()
    .enumerate()
    {
        let mut command = db.command(&format!("calendar{n}"), schedule);
        command
            .args(["--", "/usr/bin/touch"])
            .arg(db.path(&format!("calendar{n}")));
        let mut process = db.start(command)?;
        ensure!(
            process.wait().await?.success(),
            "{schedule}: {}",
            process.logs()?
        );
        ensure!(
            db.path(&format!("calendar{n}")).exists(),
            "accepted schedule child missing"
        );
    }
    Ok(())
}
async fn cron_rejected(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    for schedule in [
        "* * * * * *",
        "@every 1m",
        "TZ=UTC * * * * *",
        "0 0 L * *",
        "0 0 * * MON#2",
        "61 * * * *",
    ] {
        let mut command = db.command("invalid", schedule);
        command
            .args(["--", "/usr/bin/touch"])
            .arg(db.path("unexpected"));
        ensure!(
            !db.start(command)?.wait().await?.success(),
            "accepted {schedule}"
        );
    }
    ensure!(
        !db.path("unexpected").exists(),
        "invalid schedule child ran"
    );
    Ok(())
}
async fn migration_race(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut processes = (0..5)
        .map(|n| {
            db.spawn(
                &format!("migration{n}"),
                &format!("touch '{}'", db.path(&format!("ran{n}")).display()),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    for process in &mut processes {
        ensure!(
            process.wait().await?.success(),
            "migration race: {}",
            process.logs()?
        );
    }
    for n in 0..5 {
        ensure!(db.path(&format!("ran{n}")).exists(), "migration lost run");
    }
    ensure!(
        db.client
            .query_one(
                "SELECT count(*) FROM public.solo_schema_version WHERE version=1",
                &[]
            )
            .await?
            .try_get::<_, i64>(0)?
            == 1,
        "invalid schema version"
    );
    Ok(())
}
async fn migration_lock_timeout(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.client
        .batch_execute("BEGIN; SELECT pg_advisory_xact_lock(1936682095,1)")
        .await?;
    let began = Instant::now();
    let mut process = db.spawn(
        "blocked",
        &format!("touch '{}'", db.path("unexpected").display()),
    )?;
    ensure!(!process.wait().await?.success(), "migration wait succeeded");
    ensure!(
        began.elapsed() < Duration::from_secs(3),
        "migration unbounded"
    );
    ensure!(!db.path("unexpected").exists(), "unmigrated child ran");
    db.client.batch_execute("ROLLBACK").await?;
    Ok(())
}
async fn migration_version_error(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute("UPDATE public.solo_schema_version SET version=999")
        .await?;
    ensure!(
        !db.spawn(
            "version",
            &format!("touch '{}'", db.path("unexpected").display())
        )?
        .wait()
        .await?
        .success(),
        "unsupported version accepted"
    );
    ensure!(!db.path("unexpected").exists(), "version error child ran");
    Ok(())
}
async fn arguments_and_credentials(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let injection = format!("$(touch {})", db.path("injected").display());
    let mut command = db.command("args", "@yearly");
    command.args([
        "--",
        "/usr/bin/printf",
        "%s\\n",
        "space value",
        "*",
        "; exit 9",
        &injection,
    ]);
    let mut process = db.start(command)?;
    ensure!(process.wait().await?.success(), "argv command failed");
    let log = process.logs()?;
    ensure!(
        log.contains(&format!("space value\n*\n; exit 9\n{injection}\n")),
        "arguments changed: {log}"
    );
    ensure!(!db.path("injected").exists(), "implicit shell");
    let mut env = db.command("environment", "@yearly");
    env.env("PGPASSWORD", "handler_password")
        .env("PGUSER", "handler_user")
        .env("PGHOST", "handler_host")
        .env("PG_CUSTOM", "application_value")
        .args(["--", "/usr/bin/env"]);
    let mut process = db.start(env)?;
    ensure!(process.wait().await?.success(), "env handler failed");
    let log = process.logs()?;
    ensure!(
        !log.contains("SOLO_DATABASE_URL") && !log.contains("s4_test_secret"),
        "coordination credentials leaked"
    );
    ensure!(
        [
            "PGPASSWORD=handler_password",
            "PGUSER=handler_user",
            "PGHOST=handler_host",
            "PG_CUSTOM=application_value",
        ]
        .into_iter()
        .all(|expected| log.lines().any(|line| line == expected)),
        "handler PG environment variables were removed or changed"
    );
    Ok(())
}
async fn non_shell_handler_and_no_scrape(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut command = db.command("native", "@yearly");
    command
        .env("PATH", "/nonexistent")
        .args(["--", "/usr/bin/true"]);
    let start = Instant::now();
    let mut process = db.start(command)?;
    ensure!(
        process.wait().await?.success(),
        "native child needs Python or PATH: {}",
        process.logs()?
    );
    ensure!(
        start.elapsed() < Duration::from_secs(2),
        "waited after child completion"
    );
    let start = Instant::now();
    ensure!(
        db.spawn("child-wait", "sleep 0.3")?.wait().await?.success(),
        "child waiting failed"
    );
    ensure!(
        start.elapsed() >= Duration::from_millis(300),
        "returned before handler exited"
    );
    Ok(())
}

async fn environment_configuration(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
    command
        .env("SOLO_DATABASE_URL", &db.dsn)
        .env("SOLO_KEY", "environment")
        .env("SOLO_SCHEDULE", "@yearly")
        .env("SOLO_TTL", "2s")
        .env("SOLO_HEARTBEAT", "200ms")
        .env("SOLO_DB_TIMEOUT", "200ms")
        .env("SOLO_GRACE", "100ms")
        .env("SOLO_MIGRATION_TIMEOUT", "500ms")
        .env_remove("SOLO_MAX_RUNTIME")
        .args(["run", "--", "/usr/bin/touch"])
        .arg(db.path("environment"));
    ensure!(
        db.start(command)?.wait().await?.success(),
        "environment configuration failed"
    );
    ensure!(db.path("environment").exists(), "env handler missing");
    let mut command = db.command("cli-precedence", "@yearly");
    command
        .env("SOLO_KEY", "environment")
        .env("SOLO_SCHEDULE", "invalid")
        .env("SOLO_TTL", "1ms")
        .args(["--ttl", "2s", "--", "/usr/bin/touch"])
        .arg(db.path("precedence"));
    ensure!(
        db.start(command)?.wait().await?.success(),
        "CLI did not override environment"
    );
    ensure!(
        db.path("precedence").exists(),
        "CLI key did not override completed environment key"
    );
    Ok(())
}
