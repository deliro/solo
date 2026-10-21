use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::Pid,
};
use std::{
    io::{PipeReader, PipeWriter, Write},
    os::fd::AsRawFd,
    process::{Child, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    HeartbeatFailure,
    Acquisition,
    ClosedOutput,
}

pub(super) fn trials(h: &Arc<Harness>) -> Vec<Trial> {
    [
        (
            "backpressure_heartbeat_stops_child",
            Scenario::HeartbeatFailure,
        ),
        (
            "backpressure_acquire_starts_promptly",
            Scenario::Acquisition,
        ),
        (
            "backpressure_closed_stderr_completes",
            Scenario::ClosedOutput,
        ),
    ]
    .into_iter()
    .map(|(name, case)| {
        let h = Arc::clone(h);
        Trial::test(name, move || {
            h.runtime
                .clone()
                .block_on(run(h, case))
                .map_err(|e| format!("{e:#}").into())
        })
    })
    .collect()
}

struct ManagedChild {
    child: Child,
    group: Option<Pid>,
}
impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(group) = self.group {
            let _ = killpg(group, Signal::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn fill_pipe(writer: PipeWriter) -> Result<()> {
    let fd = writer.as_raw_fd();
    // A duplicate shares file status flags: never set O_NONBLOCK here. This
    // bounded producer is released when the caller's reader is dropped, even
    // when a test assertion or database operation fails.
    std::thread::spawn(move || {
        let mut writer = writer;
        let _ = writer.write_all(&vec![b'x'; 1024 * 1024]);
    });
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let mut poll = libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: poll points to one initialized pollfd for this immediate call;
        // the producer retains its fd while the full pipe has a reader.
        let result = unsafe { libc::poll(&raw mut poll, 1, 0) };
        ensure!(result >= 0, "pipe readiness check failed");
        if result == 0 {
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "stderr pipe did not fill");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn exit_before(child: &mut Child, duration: Duration) -> Result<bool> {
    let deadline = Instant::now()
        .checked_add(duration)
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run(h: Arc<Harness>, case: Scenario) -> Result<()> {
    let db = h.database().await?;
    db.bootstrap().await?;
    let (reader, writer): (PipeReader, PipeWriter) = std::io::pipe()?;
    let ready = db.path("ready");
    let script = if case == Scenario::ClosedOutput {
        format!("touch '{}'", ready.display())
    } else {
        format!("echo $$ > '{}'; sleep 30", ready.display())
    };
    let mut command = db.command("backpressure", "@yearly");
    command
        .args(["--", "/bin/sh", "-c", &script])
        .stdout(Stdio::null())
        .stderr(writer.try_clone()?);
    if case == Scenario::Acquisition {
        fill_pipe(writer.try_clone()?).await?;
    }
    // Otherwise keep the unread pipe alive through all observations.
    if case == Scenario::ClosedOutput {
        drop(reader);
    }
    let mut child = ManagedChild {
        child: command.spawn()?,
        group: None,
    };
    if case == Scenario::ClosedOutput {
        ensure!(
            exit_before(&mut child.child, Duration::from_secs(1)).await?,
            "closed stderr blocked normal completion"
        );
        ensure!(ready.exists(), "closed stderr prevented handler execution");
        ensure!(
            child
                .child
                .try_wait()?
                .is_some_and(|status| status.success()),
            "closed stderr changed successful result"
        );
        return Ok(());
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(700))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    while !ready.exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    ensure!(
        ready.exists(),
        "full acquisition diagnostic prevented prompt handler spawn"
    );
    let pid = Pid::from_raw(common::pid(&ready)?);
    child.group = Some(pid);
    if case == Scenario::HeartbeatFailure {
        fill_pipe(writer).await?;
        // Ownership loss remains terminal; connection loss now retries.
        db.client.execute("UPDATE public.solo_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE key=$1", &[&"backpressure"]).await?;
        ensure!(
            exit_before(&mut child.child, Duration::from_millis(700)).await?,
            "supervisor blocked on full stderr after heartbeat failure"
        );
    } else {
        if child.child.try_wait()?.is_none() {
            kill(
                Pid::from_raw(i32::try_from(child.child.id())?),
                Signal::SIGTERM,
            )?;
        }
        ensure!(
            exit_before(&mut child.child, Duration::from_millis(700)).await?,
            "full stderr blocked SIGTERM shutdown"
        );
    }
    ensure!(kill(pid, None).is_err(), "handler survived shutdown");
    Ok(())
}
