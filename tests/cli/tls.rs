use crate::common::Harness;
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::{path::Path, sync::Arc};
use tokio_postgres::config::Host;

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let harness = Arc::clone(harness);
    vec![Trial::test("tls_trust_and_hostname", move || {
        harness
            .runtime
            .clone()
            .block_on(trust_and_hostname(harness))
            .map_err(|error| format!("{error:#}").into())
    })]
}

async fn trust_and_hostname(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    db.bootstrap().await?;
    let config: tokio_postgres::Config = db.dsn.parse()?;
    let Some(Host::Tcp(host)) = config.get_hosts().first() else {
        anyhow::bail!("TCP harness required");
    };
    let port = *config
        .get_ports()
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing port"))?;
    let address = tokio::net::lookup_host((host.as_str(), port))
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing host address"))?
        .ip();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    // These fixtures expire in 2036; the committed private key is only for local tests.
    for (name, ca, mode, success) in [
        ("solo.test", "ca.crt", "require", true),
        ("wrong.test", "ca.crt", "require", false),
        ("solo.test", "unrelated.crt", "require", false),
        ("solo.test", "missing.crt", "require", false),
        ("solo.test", "missing.crt", "disable", true),
    ] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
        command
            .env(
                "SOLO_DATABASE_URL",
                format!(
                    "{} sslmode={mode}",
                    db.dsn.replacen(
                        &format!("host={host} "),
                        &format!("host={name} hostaddr={address} "),
                        1
                    )
                ),
            )
            .env("SSL_CERT_FILE", fixtures.join(ca))
            .env("SSL_CERT_DIR", fixtures.join("absent"))
            .args(["status", "--key", "tls", "--db-timeout", "3s"]);
        let mut cli = db.start(command)?;
        ensure!(
            cli.wait().await?.success() == success,
            "host={name} ca={ca} mode={mode}: {}",
            cli.logs()?
        );
        if !success {
            ensure!(
                cli.logs()?.contains("code=tls"),
                "missing TLS diagnostic: {}",
                cli.logs()?
            );
        }
    }
    let fifo = db.path("blocked-ca");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )?;
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
    command
        .env(
            "SOLO_DATABASE_URL",
            db.dsn.replace("sslmode=disable", "sslmode=require"),
        )
        .env("SSL_CERT_FILE", &fifo)
        .env("SSL_CERT_DIR", fixtures.join("absent"))
        .args(["status", "--key", "tls", "--db-timeout", "100ms"]);
    let started = std::time::Instant::now();
    let mut cli = db.start(command)?;
    ensure!(!cli.wait().await?.success(), "blocked CA loading succeeded");
    ensure!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "CA loading outlived its deadline"
    );
    ensure!(
        cli.logs()?.contains("code=timeout"),
        "missing CA timeout diagnostic: {}",
        cli.logs()?
    );
    Ok(())
}
