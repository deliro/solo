//! SQL/PG boundary performance probes: production SQL is read verbatim, never reimplemented.
use crate::common::Harness;
use anyhow::{Context, Result, ensure};
use libtest_mimic::Trial;
use std::sync::Arc;
use tokio_postgres::Client;

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let harness = Arc::clone(harness);
    vec![Trial::test("sql_plans_and_cleanup_bound", move || {
        let runtime = harness.runtime.clone();
        runtime
            .block_on(audit(harness))
            .map_err(|error| format!("{error:#}").into())
    })]
}

fn statements() -> Vec<&'static str> {
    include_str!("../../src/store.rs")
        .split('"')
        .filter(|part| {
            ["SELECT ", "UPDATE ", "INSERT ", "WITH expired "]
                .iter()
                .any(|prefix| part.starts_with(prefix))
        })
        .collect()
}

async fn explain(client: &Client, label: &str, sql: &str) -> Result<String> {
    client.batch_execute("BEGIN").await?;
    let rows = client
        .query(&format!("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}"), &[])
        .await?;
    let mut plan = format!("\n=== {label} ===\n{sql}\n");
    for row in rows {
        plan.push_str(row.try_get::<_, &str>(0)?);
        plan.push('\n');
    }
    client.batch_execute("ROLLBACK").await?;
    eprintln!("{plan}");
    Ok(plan)
}

async fn audit(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.client
        .batch_execute(include_str!("../../migrations/0001_coordination.up.sql"))
        .await?;
    // Cardinality is deliberately substantial; no planner settings are disabled.
    db.client.batch_execute("INSERT INTO public.solo_leases(key,last_success) SELECT 'other-'||n,clock_timestamp()-interval '1 day' FROM generate_series(1,50000) n; INSERT INTO public.solo_leases(key,owner,scheduled_at,expires_at) VALUES ('dense','00000000-0000-0000-0000-000000000001','2026-01-01 UTC','2100-01-01 UTC'); ANALYZE public.solo_leases; ANALYZE public.solo_schema_version").await?;
    db.client.batch_execute("INSERT INTO public.solo_successes(key,scheduled_at) SELECT 'dense',clock_timestamp()-interval '1 day'-n*interval '1 minute' FROM generate_series(1,100000) n; INSERT INTO public.solo_successes(key,scheduled_at) SELECT 'other-'||n,'2026-01-01 UTC'::timestamptz+n*interval '1 minute' FROM generate_series(1,50000) n").await?;
    db.client
        .batch_execute("VACUUM ANALYZE public.solo_successes")
        .await?;
    let version: String = db
        .client
        .query_one("SHOW server_version", &[])
        .await?
        .try_get(0)?;
    eprintln!(
        "PG={version}; leases=50001; successes=150000 (dense retained=100000); ANALYZE executed; default planner settings"
    );
    let mut cleanup_plan = None;
    for (number, query) in statements().iter().enumerate() {
        let fourth = if query.contains("CASE WHEN $4") {
            "TRUE"
        } else {
            "2.0"
        };
        let second = if query.contains("scheduled_at=$2")
            || query.starts_with("INSERT INTO public.solo_successes")
        {
            "'2026-01-01 UTC'::timestamptz"
        } else {
            "'00000000-0000-0000-0000-000000000001'::uuid"
        };
        let sql = query
            .replace("$6", "1234")
            .replace("$5", "'test-host'")
            .replace("$4", fourth)
            .replace("$3", "'2026-01-01 UTC'::timestamptz")
            .replace("$2", second)
            .replace(
                "$1",
                if query.contains("set_config") {
                    "'3000'"
                } else {
                    "'dense'"
                },
            );
        let plan = explain(
            &db.client,
            &format!("store SQL {}", number.saturating_add(1)),
            &sql,
        )
        .await?;
        if query.starts_with("SELECT statement_timestamp() AS observed_at") {
            verify_status_work(&plan)?;
        }
        if query.starts_with("WITH expired") {
            cleanup_plan = Some(plan);
        }
    }
    db.client.batch_execute("INSERT INTO public.solo_successes(key,scheduled_at) SELECT 'dense',clock_timestamp()-interval '3000 days'-n*interval '1 minute' FROM generate_series(1,4096) n; ANALYZE public.solo_successes").await?;
    let cleanup = statements()
        .into_iter()
        .find(|query| query.starts_with("WITH expired"))
        .context("cleanup SQL missing")?;
    let expired_plan = explain(
        &db.client,
        "cleanup with 4096 expired and 100000 retained",
        &cleanup.replace("$1", "'dense'"),
    )
    .await?;
    let plan = cleanup_plan.context("production cleanup SQL missing")?;
    // Work bound, independent of machine speed: retained rows cannot be filtered one by one.
    let filtered = plan
        .lines()
        .filter_map(|line| line.trim().strip_prefix("Rows Removed by Filter: "))
        .filter_map(|count| count.parse::<u64>().ok())
        .max()
        .unwrap_or_default();
    let scanned = expired_plan
        .lines()
        .filter(|line| {
            line.contains("Scan on public.solo_successes")
                || line.contains("Scan using solo_successes_pkey")
        })
        .filter_map(|line| {
            line.rsplit_once("rows=")
                .and_then(|(_, tail)| tail.split_whitespace().next())
        })
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or_default();
    ensure!(
        filtered <= 1024 && scanned <= 2048,
        "cleanup work is not bounded: filtered {filtered} retained rows for an empty 1024-row batch, expired-batch scan returned {scanned} rows; see EXPLAIN above"
    );
    verify_cleanup(&db.client, cleanup).await?;
    verify_success_lookup(&db.client).await
}

fn verify_status_work(plan: &str) -> Result<()> {
    let largest_buffer_count = plan
        .lines()
        .filter_map(|line| line.split("shared hit=").nth(1))
        .filter_map(|tail| tail.split_whitespace().next())
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or_default();
    let filtered = plan
        .lines()
        .filter_map(|line| line.trim().strip_prefix("Rows Removed by Filter: "))
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or_default();
    ensure!(
        largest_buffer_count <= 64 && filtered <= 8,
        "status lookup examined unrelated lease rows: buffers={largest_buffer_count}, filtered={filtered}; {plan}"
    );
    Ok(())
}

async fn verify_success_lookup(client: &Client) -> Result<()> {
    let lookup = statements()
        .into_iter()
        .find(|sql| {
            sql.starts_with("SELECT ")
                && sql.contains("FROM public.solo_successes WHERE key=$1 AND scheduled_at=$2")
        })
        .context("success lookup SQL missing")?;
    // Shared cron schedules naturally concentrate many keys at the same timestamp.
    client
        .batch_execute(
            "UPDATE public.solo_successes SET scheduled_at='2026-01-01 UTC' WHERE key<>'dense'",
        )
        .await?;
    client
        .batch_execute("VACUUM ANALYZE public.solo_successes")
        .await?;
    for (key, expected) in [("dense", false), ("other-1", true), ("absent", false)] {
        let sql = lookup
            .replace("$1", &format!("'{key}'"))
            .replace("$2", "'2026-01-01 UTC'::timestamptz");
        let result: bool = client.query_one(&sql, &[]).await?.try_get(0)?;
        let plan = explain(client, &format!("shared cron exact pair {key}"), &sql).await?;
        ensure!(
            result == expected,
            "exact success lookup for {key}: expected {expected}, got {result}"
        );
        let filtered = plan
            .lines()
            .filter_map(|line| line.trim().strip_prefix("Rows Removed by Filter: "))
            .map(str::parse::<u64>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or_default();
        ensure!(
            filtered <= 1024,
            "exact success lookup examined unrelated history: {filtered} rows filtered; see EXPLAIN above"
        );
    }
    let count: i64 = client.query_one("SELECT count(*) FROM public.solo_successes WHERE key='other-1' AND scheduled_at='2026-01-01 UTC'", &[]).await?.try_get(0)?;
    ensure!(count == 1, "known completed pair must remain unique");
    Ok(())
}

async fn verify_cleanup(client: &Client, cleanup: &str) -> Result<()> {
    let deleted = client.execute(cleanup, &[&"dense"]).await?;
    let row = client.query_one("SELECT count(*) FILTER (WHERE key='dense' AND scheduled_at >= statement_timestamp()-interval '2928 days'), count(*) FILTER (WHERE key<>'dense') FROM public.solo_successes", &[]).await?;
    ensure!(
        deleted == 1024
            && row.try_get::<_, i64>(0)? == 100_000
            && row.try_get::<_, i64>(1)? == 50_000,
        "cleanup must delete exactly one expired batch and preserve retained/other-key rows"
    );
    Ok(())
}
