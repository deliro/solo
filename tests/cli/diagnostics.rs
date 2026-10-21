use crate::common::Harness;
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::sync::Arc;

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let failures_harness = Arc::clone(harness);
    let harness = Arc::clone(harness);
    vec![
        Trial::test("diagnostics_sslmode", move || {
            harness
                .runtime
                .clone()
                .block_on(sslmode(harness))
                .map_err(|error| format!("{error:#}").into())
        }),
        Trial::test("diagnostics_connection_failures", move || {
            failures_harness
                .runtime
                .clone()
                .block_on(failures(failures_harness))
                .map_err(|error| format!("{error:#}").into())
        }),
    ]
}

async fn sslmode(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let mut command = db.command("diagnostics", "@yearly");
    command.env("SOLO_DATABASE_URL", db.dsn.replace("sslmode=disable", ""));
    command.args(["--", "echo", "handler-must-not-run"]);
    let mut cli = db.start(command)?;
    ensure!(!cli.wait().await?.success(), "invalid TLS policy succeeded");
    let logs = cli.logs()?;
    ensure!(
        logs.contains("set sslmode=require or explicit sslmode=disable"),
        "missing actionable TLS policy explanation: {logs}"
    );
    ensure!(!logs.contains("handler-must-not-run"), "handler ran");
    Ok(())
}

async fn failures(h: Arc<Harness>) -> Result<()> {
    let db = h.database().await?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let stalled = listener.local_addr()?.port();
    let refused_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let refused = refused_listener.local_addr()?.port();
    drop(refused_listener);
    let cases = [
        (
            format!(
                "host={} sslmode=disable",
                db.path("absent_socket").display()
            ),
            "endpoint_not_found",
            "Unix socket path",
        ),
        (
            "host=solo-diagnostics.invalid sslmode=disable".to_owned(),
            "dns",
            "hostname",
        ),
        (
            format!("{} password=hostile_password_sentinel", db.dsn),
            "authentication",
            "credentials",
        ),
        (
            format!("{} dbname=hostile_database_sentinel", db.dsn),
            "database_missing",
            "database name",
        ),
        (
            format!(
                "host=127.0.0.1 port={refused} sslmode=disable password=hostile_password_sentinel"
            ),
            "connection_refused",
            "host and port",
        ),
        (
            "hostile_password_sentinel".to_owned(),
            "invalid_dsn",
            "connection string",
        ),
        (
            format!("{} sslmode=hostile_password_sentinel", db.dsn),
            "invalid_dsn",
            "connection string",
        ),
        (
            db.dsn.replace("sslmode=disable", "sslmode=require"),
            "tls",
            "TLS",
        ),
        (
            format!(
                "host=127.0.0.1 port={stalled} sslmode=disable password=hostile_password_sentinel"
            ),
            "timeout",
            "200ms",
        ),
    ];
    for (dsn, code, remedy) in cases {
        let mut command = db.command("diagnostics", "@yearly");
        command
            .env("SOLO_DATABASE_URL", dsn)
            .args(["--", "echo", "handler-must-not-run"]);
        let mut cli = db.start(command)?;
        ensure!(
            !cli.wait().await?.success(),
            "{code} unexpectedly succeeded"
        );
        let logs = cli.logs()?;
        ensure!(
            logs.contains(code) && logs.contains(remedy),
            "missing {code} explanation/remedy: {logs}"
        );
        if code == "endpoint_not_found" {
            ensure!(
                !logs.contains("code=dns"),
                "missing local endpoint misclassified as DNS: {logs}"
            );
        }
        ensure!(
            !logs.contains("hostile_")
                && !logs.contains("s4_test_secret")
                && !logs.contains("handler-must-not-run"),
            "diagnostic disclosed input or executed handler: {logs}"
        );
    }
    Ok(())
}
