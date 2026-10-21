use crate::common::{Database, Harness, wait_file};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::{
    process::{Command, Output},
    sync::Arc,
    time::Duration,
};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    macro_rules! case {
        ($test:ident) => {{
            let harness = Arc::clone(harness);
            Trial::test(concat!("status_", stringify!($test)), move || {
                harness
                    .runtime
                    .clone()
                    .block_on($test(harness))
                    .map_err(|error| format!("{error:#}").into())
            })
        }};
    }
    vec![
        case!(missing_key),
        case!(schema_errors),
        case!(held_and_completion),
        case!(expired_legacy),
        case!(readonly_role),
    ]
}

fn status(db: &Database, key: &str, dsn: Option<&str>) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_solo"))
        .env("SOLO_SCHEDULE", "invalid schedule")
        .env("SOLO_MAX_RUNTIME", "invalid")
        .env("SOLO_TTL", "invalid")
        .env("SOLO_HEARTBEAT", "invalid")
        .env("SOLO_GRACE", "invalid")
        .env("SOLO_MIGRATION_TIMEOUT", "invalid")
        .env("SOLO_DATABASE_URL", dsn.unwrap_or(&db.dsn))
        .args(["status", "--key", key, "--db-timeout", "1s"])
        .output()?)
}
fn readable(output: &Output) -> Result<String> {
    ensure!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout.clone())?)
}
fn field<'a>(text: &'a str, label: &str) -> Result<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(label))
        .ok_or_else(|| anyhow::anyhow!("missing field {label}: {text}"))
}
async fn snapshot(db: &Database) -> Result<String> {
    Ok(db.client.query_one("SELECT json_build_array((SELECT json_agg(l ORDER BY key) FROM public.solo_leases l),(SELECT json_agg(s ORDER BY key,scheduled_at) FROM public.solo_successes s))::text", &[]).await?.try_get(0)?)
}
async fn missing_key(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    let before = snapshot(&db).await?;
    let text = readable(&status(&db, "missing", None)?)?;
    ensure!(
        text.contains("unknown") && text.contains("missing"),
        "missing key not identified: {text}"
    );
    ensure!(snapshot(&db).await? == before, "status mutated rows");
    Ok(())
}
async fn schema_errors(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    let output = status(&db, "missing", None)?;
    ensure!(!output.status.success(), "missing schema succeeded");
    let diagnostic = String::from_utf8(output.stderr)?;
    ensure!(
        diagnostic.contains("schema") && !diagnostic.contains("s4_test_secret"),
        "unsafe/unhelpful schema error: {diagnostic}"
    );
    ensure!(
        diagnostic.contains("initialize an empty database with solo run"),
        "missing schema initialization guidance: {diagnostic}"
    );
    let absent: bool = db.client.query_one("SELECT to_regclass('public.solo_leases') IS NULL AND to_regclass('public.solo_schema_version') IS NULL", &[]).await?.try_get(0)?;
    ensure!(absent, "status created schema");
    db.client
        .batch_execute(include_str!("../../migrations/0001_coordination.up.sql"))
        .await?;
    db.client
        .batch_execute("ALTER TABLE public.solo_leases DROP COLUMN acquired_at")
        .await?;
    let output = status(&db, "missing", None)?;
    ensure!(!output.status.success(), "outdated schema succeeded");
    let diagnostic = String::from_utf8(output.stderr)?;
    ensure!(
        diagnostic.contains("update an existing schema to match this binary")
            && !diagnostic.contains("upgrade Solo schema with a compatible solo run"),
        "misleading outdated schema remedy: {diagnostic}"
    );
    db.client
        .batch_execute("UPDATE public.solo_schema_version SET version=2")
        .await?;
    ensure!(
        !status(&db, "missing", None)?.status.success(),
        "unsupported version succeeded"
    );
    Ok(())
}
async fn held_and_completion(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    let marker = db.path("started");
    let release = db.path("release");
    let group = db.path("group");
    let mut command = db.command("held", "@yearly");
    command.args([
        "--max-runtime",
        "6s",
        "--",
        "/bin/sh",
        "-c",
        &format!(
            "echo $$ > '{}'; echo $PPID > '{}'; while [ ! -e '{}' ]; do sleep 0.02; done",
            group.display(),
            marker.display(),
            release.display()
        ),
    ]);
    let mut child = db.start(command)?;
    child.track_group(&group).await?;
    wait_file(&marker).await?;
    let first = readable(&status(&db, "held", None)?)?;
    ensure!(
        first.contains("State: held")
            && !field(&first, "Owner host: ")?.contains("not recorded")
            && !field(&first, "Owner Solo PID: ")?.contains("not recorded"),
        "owner identity missing: {first}"
    );
    ensure!(
        field(&first, "Owner Solo PID: ")? == std::fs::read_to_string(&marker)?.trim(),
        "recorded PID is not Solo supervisor"
    );
    let acquired = field(&first, "Latest attempt / acquired at: ")?.to_owned();
    tokio::time::sleep(Duration::from_millis(450)).await;
    db.client
        .batch_execute(
            "BEGIN; UPDATE public.solo_leases SET owner_host='uncommitted writer' WHERE key='held'",
        )
        .await?;
    let second = readable(&status(&db, "held", None)?)?;
    ensure!(
        field(&second, "Latest attempt / acquired at: ")? == acquired,
        "heartbeat changed acquisition"
    );
    ensure!(
        !second.contains("uncommitted writer"),
        "status read an uncommitted writer"
    );
    db.client.batch_execute("ROLLBACK").await?;
    std::fs::write(&release, "release")?;
    ensure!(
        child.wait().await?.success(),
        "held handler failed: {}",
        child.logs()?
    );
    let success = readable(&status(&db, "held", None)?)?;
    ensure!(
        success.contains("State: free")
            && success.contains("Last outcome: success")
            && field(&success, "Latest attempt / acquired at: ")? == acquired,
        "completion metadata missing: {success}"
    );
    let confirmed = field(&success, "Latest actual successful completion: ")?.to_owned();
    ensure!(
        !confirmed.contains("not recorded")
            && confirmed != field(&success, "Successful scheduled interval: ")?,
        "actual completion confused with interval: {success}"
    );
    let mut fail = db.command("held", "@hourly");
    let failed_started = db.path("failed_started");
    fail.args([
        "--",
        "/bin/sh",
        "-c",
        &format!("touch '{}'; sleep 0.5; exit 7", failed_started.display()),
    ]);
    let mut process = db.start(fail)?;
    wait_file(&failed_started).await?;
    let running_again = readable(&status(&db, "held", None)?)?;
    ensure!(
        running_again.contains("State: held")
            && running_again.contains("Last outcome: success")
            && field(&running_again, "Last completed at: ")?
                == field(&success, "Last completed at: ")?,
        "new attempt erased previous completion: {running_again}"
    );
    ensure!(!process.wait().await?.success(), "failed child succeeded");
    let failure = readable(&status(&db, "held", None)?)?;
    ensure!(
        failure.contains("Last outcome: failure")
            && field(&failure, "Latest actual successful completion: ")? == confirmed
            && field(&failure, "Last completed at: ")? != field(&success, "Last completed at: ")?,
        "failure lost prior success: {failure}"
    );
    Ok(())
}
async fn expired_legacy(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.client
        .batch_execute(include_str!("../../migrations/0001_coordination.up.sql"))
        .await?;
    db.client.batch_execute("INSERT INTO public.solo_leases(key,owner,scheduled_at,expires_at) VALUES ('legacy','00000000-0000-0000-0000-000000000001','2020-01-01 UTC','2020-01-02 UTC')").await?;
    let before = snapshot(&db).await?;
    let text = readable(&status(&db, "legacy", None)?)?;
    ensure!(
        text.contains("State: expired")
            && field(&text, "Latest attempt / acquired at: ")?.contains("not recorded")
            && field(&text, "Owner host: ")?.contains("not recorded"),
        "invented old metadata: {text}"
    );
    ensure!(
        snapshot(&db).await? == before,
        "expired status mutated rows"
    );
    Ok(())
}
async fn readonly_role(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    // A unique role per isolated database avoids cluster-global role collisions.
    let role = format!("reader_{}", uuid::Uuid::new_v4().simple());
    db.client.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'status_read_secret'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.solo_schema_version, public.solo_leases TO {role}")).await?;
    let dsn = format!(
        "{} user={role} password=status_read_secret options='-c default_transaction_read_only=on'",
        db.dsn
    );
    let before = snapshot(&db).await?;
    let text = readable(&status(&db, "bootstrap", Some(&dsn))?)?;
    ensure!(
        text.contains("State: free")
            && !text.contains("status_read_secret")
            && !text.contains("s4_test_secret"),
        "readonly status unsafe: {text}"
    );
    ensure!(
        snapshot(&db).await? == before,
        "readonly status mutated state"
    );
    Ok(())
}
