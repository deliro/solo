use crate::common::{Database, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::{fs, sync::Arc};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    macro_rules! case {
        ($test:ident) => {{
            let harness = Arc::clone(harness);
            Trial::test(concat!("history_", stringify!($test)), move || {
                let runtime = harness.runtime.clone();
                runtime
                    .block_on($test(harness))
                    .map_err(|error| format!("{error:#}").into())
            })
        }};
    }
    vec![
        case!(schedule_change),
        case!(roundtrip),
        case!(initial_schema),
        case!(retention),
        case!(insert_failure),
    ]
}

async fn run(db: &Database, schedule: &str) -> Result<()> {
    let mut command = db.command("history", schedule);
    command.args([
        "--",
        "/bin/sh",
        "-c",
        &format!("echo run >> '{}'", db.path("markers").display()),
    ]);
    let mut process = db.start(command)?;
    ensure!(
        process.wait().await?.success(),
        "run failed: {}",
        process.logs()?
    );
    Ok(())
}

async fn schedule_change(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    // During January's first hour select a prior year's December interval.
    let distinct: bool = db
        .client
        .query_one(
            "SELECT date_trunc('hour',clock_timestamp()) > date_trunc('year',clock_timestamp())",
            &[],
        )
        .await?
        .try_get(0)?;
    let yearly = if distinct { "@yearly" } else { "0 0 1 DEC *" };
    run(&db, "@hourly").await?;
    run(&db, yearly).await?;
    run(&db, yearly).await?;
    let markers = fs::read_to_string(db.path("markers"))?;
    ensure!(
        markers.lines().count() == 2,
        "unsucceeded yearly interval must run once after hourly success; marker count={}",
        markers.lines().count()
    );
    Ok(())
}

async fn roundtrip(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    // A previous month's annual occurrence is always distinct from this hour.
    let month: i32 = db.client.query_one("SELECT CASE WHEN extract(month FROM clock_timestamp())::int=1 THEN 12 ELSE extract(month FROM clock_timestamp())::int-1 END", &[]).await?.try_get(0)?;
    let annual = format!("0 0 1 {month} *");
    run(&db, &annual).await?;
    run(&db, "@hourly").await?;
    run(&db, &annual).await?;
    // Same instant through a different expression shares the same identity.
    run(&db, "0 * * * *").await?;
    ensure!(
        fs::read_to_string(db.path("markers"))?.lines().count() == 2,
        "roundtrip or equivalent expression replayed a success"
    );
    Ok(())
}

async fn initial_schema(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.client
        .batch_execute(include_str!("../../migrations/0001_coordination.up.sql"))
        .await?;
    db.client.batch_execute("INSERT INTO public.solo_successes(key,scheduled_at) VALUES ('history',date_trunc('year',clock_timestamp()))").await?;
    run(&db, "@yearly").await?;
    ensure!(
        !db.path("markers").exists(),
        "initial schema did not suppress an exact successful interval"
    );
    Ok(())
}

async fn retention(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    // Simulate a rare annual/leap success alongside recent frequent successes.
    // 2927-day age includes the longest Gregorian leap gap without changing DB time.
    db.client.batch_execute("INSERT INTO public.solo_successes(key,scheduled_at) VALUES ('history',clock_timestamp()-interval '2927 days'),('history',clock_timestamp()-interval '2929 days')").await?;
    db.client.batch_execute("INSERT INTO public.solo_successes(key,scheduled_at) SELECT 'history',make_date(y,2,29)::timestamptz FROM generate_series(extract(year FROM clock_timestamp())::int-8,extract(year FROM clock_timestamp())::int) y WHERE y%4=0 AND (y%100<>0 OR y%400=0) AND make_date(y,3,1)::timestamptz<=clock_timestamp() ORDER BY y DESC LIMIT 1").await?;
    run(&db, "@hourly").await?;
    run(&db, "0 0 29 FEB *").await?;
    let row = db.client.query_one("SELECT count(*) FILTER (WHERE scheduled_at < clock_timestamp()-interval '2926 days'), count(*) FILTER (WHERE scheduled_at < clock_timestamp()-interval '2928 days') FROM public.solo_successes WHERE key='history'", &[]).await?;
    ensure!(
        row.try_get::<_, i64>(0)? == 1 && row.try_get::<_, i64>(1)? == 0,
        "retention erased rare success or retained expired pair"
    );
    run(&db, "@hourly").await?;
    ensure!(
        fs::read_to_string(db.path("markers"))?.lines().count() == 1,
        "cleanup replayed current success"
    );
    Ok(())
}

async fn insert_failure(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    db.client.batch_execute("CREATE FUNCTION reject_success() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER reject_success BEFORE INSERT ON public.solo_successes FOR EACH ROW EXECUTE FUNCTION reject_success()").await?;
    let mut command = db.command("history", "@yearly");
    command.args([
        "--",
        "/bin/sh",
        "-c",
        &format!("echo run >> '{}'", db.path("markers").display()),
    ]);
    let mut process = db.start(command)?;
    ensure!(
        !process.wait().await?.success(),
        "success INSERT failure was reported successful"
    );
    let row = db.client.query_one("SELECT owner IS NOT NULL, (SELECT count(*) FROM public.solo_successes WHERE key='history') FROM public.solo_leases WHERE key='history'", &[]).await?;
    ensure!(
        row.try_get::<_, bool>(0)? && row.try_get::<_, i64>(1)? == 0,
        "failed INSERT partially committed release or success"
    );
    db.client.batch_execute("DROP TRIGGER reject_success ON public.solo_successes; UPDATE public.solo_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE key='history'").await?;
    run(&db, "@yearly").await?;
    run(&db, "@yearly").await?;
    ensure!(
        fs::read_to_string(db.path("markers"))?.lines().count() == 2,
        "failed success could not retry exactly once"
    );
    Ok(())
}
