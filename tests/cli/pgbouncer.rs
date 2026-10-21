use crate::common::{Database, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let a = Arc::clone(harness);
    let b = Arc::clone(harness);
    vec![
        Trial::test("pgbouncer_unnamed_protocol", move || {
            a.runtime
                .clone()
                .block_on(protocol(a))
                .map_err(|e| format!("{e:#}").into())
        }),
        Trial::test("pgbouncer_transaction_backend_reassignment", move || {
            b.runtime
                .clone()
                .block_on(pooling(b))
                .map_err(|e| format!("{e:#}").into())
        }),
    ]
}

async fn protocol(harness: Arc<Harness>) -> Result<()> {
    let mut db = harness.database().await?;
    let config: tokio_postgres::Config = db.dsn.parse()?;
    let host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
        _ => anyhow::bail!("TCP harness required"),
    };
    let port = *config
        .get_ports()
        .first()
        .ok_or_else(|| anyhow::anyhow!("port missing"))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_port = listener.local_addr()?.port();
    let named = Arc::new(AtomicUsize::new(0));
    let parses = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&named);
    let p = Arc::clone(&parses);
    let proxy = tokio::spawn(serve_proxy(listener, host, port, n, p));
    db.dsn = format!(
        "host=127.0.0.1 port={proxy_port} user=solo password=s4_test_secret sslmode=disable dbname={}",
        config
            .get_dbname()
            .ok_or_else(|| anyhow::anyhow!("database missing"))?
    );
    let result = async {
        let mut cli = db.spawn("protocol", "sleep 0.7")?;
        ensure!(cli.wait().await?.success(), "run failed: {}", cli.logs()?);
        let mut status = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
        status.env("SOLO_DATABASE_URL", &db.dsn).args(["status", "--key", "protocol"]);
        let mut cli = db.start(status)?;
        ensure!(cli.wait().await?.success(), "status failed: {}", cli.logs()?);
        let prepared = named.load(Ordering::SeqCst);
        let total = parses.load(Ordering::SeqCst);
        eprintln!("b016 protocol parses={total} named_preparations={prepared}");
        ensure!(total > 0 && prepared == 0, "coordination must use unnamed extended protocol; observed {prepared} named preparations / {total} parses");
        Ok(())
    }.await;
    if proxy.is_finished() {
        proxy.await??;
    } else {
        proxy.abort();
        let joined = proxy.await;
        ensure!(
            joined.is_err_and(|error| error.is_cancelled()),
            "proxy shutdown failed"
        );
    }
    result
}

async fn pooling(harness: Arc<Harness>) -> Result<()> {
    let mut db = harness.database().await?;
    db.bootstrap().await?;
    db.client
        .batch_execute(include_str!("../fixtures/pgbouncer_backends.sql"))
        .await?;
    let pool = GenericImage::new("edoburu/pgbouncer", "v1.25.1-p0")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("process up"))
        .with_env_var("DB_HOST", &harness.backend_host)
        .with_env_var("DB_USER", "solo")
        .with_env_var("DB_PASSWORD", "s4_test_secret")
        .with_env_var("AUTH_TYPE", "scram-sha-256")
        .with_env_var("POOL_MODE", "transaction")
        .with_env_var("MAX_PREPARED_STATEMENTS", "0")
        .with_env_var("DEFAULT_POOL_SIZE", "1")
        .with_env_var("SERVER_LIFETIME", "1")
        .start()
        .await?;
    let result = async {
        let host = pool.get_host().await?;
        let port = pool.get_host_port_ipv4(5432.tcp()).await?;
        let config: tokio_postgres::Config = db.dsn.parse()?;
        db.dsn = format!(
            "host={host} port={port} user=solo password=s4_test_secret sslmode=disable dbname={}",
            config
                .get_dbname()
                .ok_or_else(|| anyhow::anyhow!("database missing"))?
        );
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
        command.env("SOLO_DATABASE_URL", &db.dsn).args([
            "run",
            "--key",
            "pooling",
            "--schedule",
            "@yearly",
        ]);
        command.args([
            "--ttl",
            "6s",
            "--heartbeat",
            "1200ms",
            "--db-timeout",
            "500ms",
            "--migration-timeout",
            "2s",
            "--",
            "/bin/sh",
            "-c",
            "sleep 4.2",
        ]);
        let mut cli = db.start(command)?;
        ensure!(
            cli.wait().await?.success(),
            "pooled run failed: {}",
            cli.logs()?
        );
        verify_pooled_completion(&db).await?;
        let count: i64 = db
            .client
            .query_one("SELECT count(DISTINCT pid) FROM observed_backends", &[])
            .await?
            .get(0);
        eprintln!(
            "b016 transaction pooling max_prepared_statements=0 distinct_update_backends={count}"
        );
        ensure!(count >= 2, "lease updates must span reassigned backends");
        Ok(())
    }
    .await;
    pool.rm().await?;
    result
}

async fn verify_pooled_completion(db: &Database) -> Result<()> {
    let mut status = std::process::Command::new(env!("CARGO_BIN_EXE_solo"));
    status
        .env("SOLO_DATABASE_URL", &db.dsn)
        .args(["status", "--key", "pooling"]);
    let mut status = db.start(status)?;
    ensure!(
        status.wait().await?.success(),
        "pooled status failed: {}",
        status.logs()?
    );
    let output = status.logs()?;
    ensure!(
        output.contains("State: free") && output.contains("Last outcome: success"),
        "pooled completion must be visible: {output}"
    );
    let marker = db.path("must-not-replay");
    let mut replay = db.spawn(
        "pooling",
        &format!("echo replayed > '{}'", marker.display()),
    )?;
    ensure!(
        replay.wait().await?.success()
            && replay.logs()?.contains("result=completed")
            && !marker.exists(),
        "pooled retry must skip completed interval: {}",
        replay.logs()?
    );
    eprintln!("b016 pooled status=free outcome=success completed_retry=skipped marker_absent=true");
    Ok(())
}

async fn serve_proxy(
    listener: TcpListener,
    host: String,
    port: u16,
    named: Arc<AtomicUsize>,
    parses: Arc<AtomicUsize>,
) -> Result<()> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, _) = accepted?;
                let server = TcpStream::connect((host.as_str(), port)).await?;
                client.set_nodelay(true)?;
                server.set_nodelay(true)?;
                connections.spawn(relay(client, server, Arc::clone(&named), Arc::clone(&parses)));
            }
            Some(done) = connections.join_next(), if !connections.is_empty() => { done??; }
        }
    }
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    named: Arc<AtomicUsize>,
    parses: Arc<AtomicUsize>,
) -> Result<()> {
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let upstream =
        async {
            let length = cr.read_u32().await?;
            ensure!((8..=1_048_576).contains(&length), "invalid startup length");
            let mut body = vec![
                0;
                usize::try_from(
                    length
                        .checked_sub(4)
                        .ok_or_else(|| anyhow::anyhow!("invalid protocol length"))?
                )?
            ];
            cr.read_exact(&mut body).await?;
            sw.write_u32(length).await?;
            sw.write_all(&body).await?;
            loop {
                let kind = match cr.read_u8().await {
                    Ok(kind) => kind,
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Ok::<(), anyhow::Error>(());
                    }
                    Err(error) => return Err(error.into()),
                };
                let length = cr.read_u32().await?;
                ensure!((4..=1_048_576).contains(&length), "invalid frame length");
                let mut body = vec![
                    0;
                    usize::try_from(length.checked_sub(4).ok_or_else(
                        || anyhow::anyhow!("invalid protocol length")
                    )?)?
                ];
                cr.read_exact(&mut body).await?;
                if kind == b'P' {
                    parses.fetch_add(1, Ordering::SeqCst);
                    if body.first() != Some(&0) {
                        named.fetch_add(1, Ordering::SeqCst);
                    }
                }
                sw.write_u8(kind).await?;
                sw.write_u32(length).await?;
                sw.write_all(&body).await?;
            }
        };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            result = upstream => result,
            result = tokio::io::copy(&mut sr, &mut cw) => { result?; Ok(()) }
        }
    })
    .await?
}
