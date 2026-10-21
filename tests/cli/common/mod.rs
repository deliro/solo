use anyhow::{Context, Result, ensure};
use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::Pid,
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, NoTls};

pub(super) struct Harness {
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) base: String,
    pub(super) backend_host: String,
}
impl Harness {
    pub(super) async fn database(self: &Arc<Self>) -> Result<Database> {
        let name = format!("s4_{}", uuid::Uuid::new_v4().simple());
        let admin = connect(&format!("{} dbname=postgres", self.base)).await?;
        // Only the generated ASCII UUID is interpolated as an SQL identifier.
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await?;
        let dsn = format!("{} dbname={name}", self.base);
        Ok(Database {
            client: connect(&dsn).await?,
            dsn,
            directory: tempfile::tempdir()?,
        })
    }
}
pub(super) async fn connect(dsn: &str) -> Result<Client> {
    let (client, connection) =
        tokio::time::timeout(Duration::from_secs(4), tokio_postgres::connect(dsn, NoTls)).await??;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("s4 observer connection ended: {error}");
        }
    });
    client.batch_execute("SET statement_timeout='3s'").await?;
    Ok(client)
}
pub(super) struct Database {
    pub(super) client: Client,
    pub(super) dsn: String,
    pub(super) directory: tempfile::TempDir,
}
impl Database {
    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }
    pub(super) fn command(&self, key: &str, schedule: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_solo"));
        command
            .env_remove("SOLO_KEY")
            .env_remove("SOLO_SCHEDULE")
            .env_remove("SOLO_MAX_RUNTIME")
            .env_remove("SOLO_TTL")
            .env_remove("SOLO_HEARTBEAT")
            .env_remove("SOLO_HEARTBEAT_RETRY_INTERVAL")
            .env_remove("SOLO_DB_TIMEOUT")
            .env_remove("SOLO_GRACE")
            .env_remove("SOLO_MIGRATION_TIMEOUT")
            .env("SOLO_DATABASE_URL", &self.dsn)
            .args([
                "run",
                "--key",
                key,
                "--schedule",
                schedule,
                "--ttl",
                "2s",
                "--heartbeat",
                "200ms",
                "--db-timeout",
                "200ms",
                "--grace",
                "100ms",
                "--migration-timeout",
                "500ms",
            ]);
        command
    }
    pub(super) fn spawn(&self, key: &str, script: &str) -> Result<Process> {
        let mut command = self.command(key, "@yearly");
        command.args(["--", "/bin/sh", "-c", script]);
        self.start(command)
    }
    pub(super) fn start(&self, mut command: Command) -> Result<Process> {
        let log = self.path(&format!("log_{}", uuid::Uuid::new_v4().simple()));
        let file = fs::File::create(&log)?;
        command
            .stdout(Stdio::from(file.try_clone()?))
            .stderr(Stdio::from(file));
        Ok(Process {
            child: command.spawn()?,
            log,
            group: None,
        })
    }
    pub(super) async fn bootstrap(&self) -> Result<()> {
        ensure!(
            self.spawn("bootstrap", "exit 0")?.wait().await?.success(),
            "bootstrap failed"
        );
        Ok(())
    }
}
pub(super) struct Process {
    child: Child,
    pub(super) log: PathBuf,
    group: Option<Pid>,
}
impl Process {
    pub(super) fn signal(&self, signal: Signal) -> Result<()> {
        kill(Pid::from_raw(i32::try_from(self.child.id())?), signal)?;
        Ok(())
    }
    pub(super) async fn track_group(&mut self, path: &Path) -> Result<()> {
        wait_file(path).await?;
        self.group = Some(Pid::from_raw(fs::read_to_string(path)?.trim().parse()?));
        Ok(())
    }
    pub(super) async fn wait(&mut self) -> Result<ExitStatus> {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(8))
            .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            ensure!(
                Instant::now() < deadline,
                "CLI exceeded deadline; logs: {}",
                fs::read_to_string(&self.log)?
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    pub(super) fn logs(&self) -> Result<String> {
        Ok(fs::read_to_string(&self.log)?)
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(group) = self.group {
            let _ = killpg(group, Signal::SIGKILL);
        }
        let _ = self.child.kill();
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            match self.child.try_wait() {
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Ok(Some(_)) | Err(_) => return,
            }
        }
        eprintln!("s4 cleanup: killed supervisor was not reaped before deadline");
    }
}
pub(super) async fn wait_file(path: &Path) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    while !path.exists() {
        ensure!(
            Instant::now() < deadline,
            "marker missing: {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}
pub(super) async fn stopped(pid: i32) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(3))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"));
        if stat
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            || stat
                .as_ref()
                .is_ok_and(|value| value.split_whitespace().nth(2) == Some("Z"))
        {
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "process {pid} remains alive");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
pub(super) fn pid(path: &Path) -> Result<i32> {
    fs::read_to_string(path)?
        .trim()
        .parse()
        .context("pid marker")
}
