use std::{io, process::Stdio, time::Duration};

use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use tokio::{
    process::{Child, Command},
    signal::unix::{SignalKind, signal},
    time::{Instant, sleep, sleep_until},
};

use crate::{
    config::Config,
    diagnostics::Diagnostics,
    store::{Acquire, Completion, Lease, Store, StoreError},
};

pub(super) async fn run(config: Config, diagnostics: &Diagnostics) -> Result<(), &'static str> {
    // Close the unhandled-signal window before taking ownership or starting a child.
    let mut terminate = signal(SignalKind::terminate()).map_err(|_| "signal_setup")?;
    let mut interrupt = signal(SignalKind::interrupt()).map_err(|_| "signal_setup")?;
    let (mut store, acquired) = tokio::select! {
        biased;
        _ = terminate.recv() => return Err("sigterm"),
        _ = interrupt.recv() => return Err("sigint"),
        result = acquire(&config, diagnostics) => result?,
    };
    let Some(lease) = acquired_lease(acquired, diagnostics) else {
        return Ok(());
    };
    if deadline(deadline(Instant::now(), config.grace)?, config.db_timeout)? >= lease.valid_until()
    {
        return Err("lease_budget");
    }
    diagnostics.emit(format!(
        "solo phase=acquire result=acquired scheduled={}",
        lease.scheduled()
    ));
    let started = Instant::now();
    let maximum = config
        .max_runtime
        .map(|limit| deadline(started, limit))
        .transpose()?;
    let mut next_heartbeat = deadline(Instant::now(), config.heartbeat)?;
    let mut child = start(&config, &mut store, &lease, diagnostics).await?;
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or("child_pid")?;
    diagnostics.emit("solo phase=handler result=started".to_owned());
    let mut valid_until = lease.valid_until();
    let mut renewal = Renewal { delay: None };
    let (completion, reason) = loop {
        // Keep the shutdown reserve; each renewal is clipped to the remaining lease.
        let Some(reserve) = valid_until.checked_sub(config.grace) else {
            break (Completion::Failure, Some("lease_deadline"));
        };
        let stop_at = maximum.unwrap_or(valid_until).min(reserve);
        tokio::select! {
            biased;
            _ = terminate.recv() => break (Completion::Failure, Some("sigterm")),
            _ = interrupt.recv() => break (Completion::Failure, Some("sigint")),
            () = sleep_until(stop_at) => break (Completion::Failure, Some(if maximum.is_some_and(|deadline| Instant::now() >= deadline) { "max_runtime" } else { "lease_deadline" })),
            status = child.wait() => {
                break match status {
                    Ok(status) if status.success() => (Completion::Success, None),
                    _ => (Completion::Failure, Some("handler_failure")),
                };
            }
            () = sleep_until(next_heartbeat) => {
                let heartbeat_started = Instant::now();
                // Do not let a slow DB operation consume the shutdown reserve.
                let (Ok(budget), Ok(renewed), Ok(next)) = (
                    deadline(heartbeat_started, config.db_timeout),
                    deadline(heartbeat_started, config.ttl),
                    deadline(heartbeat_started, config.heartbeat),
                ) else {
                    break (Completion::Failure, Some("heartbeat_deadline"));
                };
                let deadline = reserve.min(budget);
                tokio::select! {
                    biased;
                    _ = terminate.recv() => break (Completion::Failure, Some("sigterm")),
                    _ = interrupt.recv() => break (Completion::Failure, Some("sigint")),
                    () = sleep_until(stop_at) => break (Completion::Failure, Some("runtime_or_lease_deadline")),
                    heartbeat = renewal.attempt(&mut store, &config, &lease, deadline) => {
                        match heartbeat {
                            Ok(()) => {
                                if renewal.delay.take().is_some() {
                                    diagnostics.emit("solo phase=heartbeat result=recovered".to_owned());
                                }
                                valid_until = renewed;
                                next_heartbeat = next;
                            }
                            Err(error) => {
                                let Ok(next) = renewal.retry(&error, &config, reserve, diagnostics) else {
                                    break (Completion::Failure, Some("heartbeat"));
                                };
                                next_heartbeat = next;
                            }
                        }
                    }
                }
            }
        }
    };
    // Never free ownership while a handler or remaining group member may run.
    stop_group(&mut child, pid, config.grace).await?;
    if let Err(error) = store.finish(&lease, completion).await {
        // Keep the initiating failure visible; a successful child still needs
        // a confirmed commit before the attempt can succeed.
        diagnostics.emit(format!(
            "solo phase=cleanup reason=finish detail=\"{error}\""
        ));
        return Err(reason.unwrap_or("finish"));
    }
    if let Some(reason) = reason {
        return Err(reason);
    }
    diagnostics.emit("solo phase=finish result=success".to_owned());
    Ok(())
}

struct Renewal {
    delay: Option<Duration>,
}

impl Renewal {
    async fn attempt(
        &self,
        store: &mut Store,
        config: &Config,
        lease: &Lease,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let result = tokio::time::timeout_at(deadline, async {
            if self.delay.is_some() {
                // A failed or cancelled transaction cannot be reused. Reconnect
                // without acquiring a new owner or running migrations.
                *store = Store::connect(&config.database_url, config.db_timeout).await?;
            }
            store.heartbeat(lease, config.ttl).await
        })
        .await
        .unwrap_or(Err(StoreError::Timeout(config.db_timeout)));
        if result.is_err() {
            store.disconnect();
        }
        result
    }

    fn retry(
        &mut self,
        error: &StoreError,
        config: &Config,
        stop_at: Instant,
        diagnostics: &Diagnostics,
    ) -> Result<Instant, &'static str> {
        diagnostics.emit(format!("solo phase=heartbeat detail=\"{error}\""));
        if !error.is_retryable() {
            return Err("heartbeat");
        }
        let (delay, next) = retry_schedule(
            Instant::now(),
            self.delay,
            config.heartbeat_retry_interval,
            stop_at,
        )
        .ok_or("heartbeat")?;
        self.delay = Some(delay);
        diagnostics.emit(format!(
            "solo phase=heartbeat result=retry delay={}",
            humantime::format_duration(delay)
        ));
        Ok(next)
    }
}

fn acquired_lease(acquired: Acquire, diagnostics: &Diagnostics) -> Option<crate::store::Lease> {
    match acquired {
        Acquire::Acquired(lease) => Some(lease),
        Acquire::Busy => {
            diagnostics.emit("solo phase=acquire result=busy".to_owned());
            None
        }
        Acquire::Completed => {
            diagnostics.emit("solo phase=acquire result=completed".to_owned());
            None
        }
    }
}

fn deadline(start: Instant, duration: Duration) -> Result<Instant, &'static str> {
    start.checked_add(duration).ok_or("deadline_overflow")
}

fn retry_schedule(
    now: Instant,
    previous: Option<Duration>,
    initial: Duration,
    stop_at: Instant,
) -> Option<(Duration, Instant)> {
    let delay = previous.map_or(initial, |previous| {
        previous
            .saturating_mul(2)
            .min(initial.max(Duration::from_secs(2)))
    });
    now.checked_add(delay)
        .filter(|next| *next < stop_at)
        .map(|next| (delay, next))
}

async fn start(
    config: &Config,
    store: &mut Store,
    lease: &crate::store::Lease,
    diagnostics: &Diagnostics,
) -> Result<Child, &'static str> {
    if let Ok(child) = spawn(config) {
        Ok(child)
    } else {
        if let Err(error) = store.finish(lease, Completion::Failure).await {
            diagnostics.emit(format!(
                "solo phase=cleanup reason=finish detail=\"{error}\""
            ));
        }
        Err("spawn")
    }
}

async fn acquire(
    config: &Config,
    diagnostics: &Diagnostics,
) -> Result<(Store, Acquire), &'static str> {
    let mut store = Store::connect(&config.database_url, config.db_timeout)
        .await
        .map_err(|error| {
            diagnostics.emit(format!("solo phase=connect detail=\"{error}\""));
            "connect"
        })?;
    store
        .migrate(config.migration_timeout)
        .await
        .map_err(|error| {
            diagnostics.emit(format!("solo phase=migration detail=\"{error}\""));
            "migration"
        })?;
    let acquired = store
        .acquire(&config.key, &config.schedule, config.ttl)
        .await
        .map_err(|error| {
            diagnostics.emit(format!("solo phase=acquire detail=\"{error}\""));
            "acquire"
        })?;
    Ok((store, acquired))
}

fn spawn(config: &Config) -> io::Result<Child> {
    let (program, arguments) = config
        .command
        .split_first()
        .ok_or_else(|| io::Error::other("missing command"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .process_group(0)
        .kill_on_drop(true);
    for (name, _) in std::env::vars_os() {
        if name == "SOLO_DATABASE_URL" || name.as_encoded_bytes().starts_with(b"PG") {
            command.env_remove(name);
        }
    }
    #[cfg(target_os = "linux")]
    {
        let parent = i32::try_from(std::process::id())
            .map_err(|_| io::Error::other("invalid supervisor pid"))?;
        // SAFETY: only async-signal-safe libc operations run between fork and exec.
        // PDEATHSIG covers the direct child, not arbitrary descendants.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    return Err(io::Error::other("supervisor exited"));
                }
                Ok(())
            });
        }
    }
    command.spawn()
}

async fn stop_group(child: &mut Child, pid: i32, grace: Duration) -> Result<(), &'static str> {
    match killpg(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) => {
            sleep(grace).await;
            match killpg(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) | Err(Errno::ESRCH) => {}
                Err(_) => return Err("kill_group"),
            }
        }
        Err(Errno::ESRCH) => {}
        Err(_) => return Err("terminate_group"),
    }
    tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .map_err(|_| "reap_timeout")?
        .map_err(|_| "reap")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::retry_schedule;
    use std::time::Duration;
    use tokio::time::Instant;

    #[rstest::rstest]
    #[case::first(None, 200, 5000, Some(200))]
    #[case::double(Some(200), 200, 5000, Some(400))]
    #[case::cap(Some(1600), 200, 5000, Some(2000))]
    #[case::configured_above_cap(Some(3000), 3000, 5000, Some(3000))]
    #[case::exact_deadline(None, 200, 200, None)]
    #[case::no_budget(None, 200, 100, None)]
    #[case::expired(None, 200, 0, None)]
    fn retry_respects_backoff_and_deadline(
        #[case] previous: Option<u64>,
        #[case] initial: u64,
        #[case] budget: u64,
        #[case] expected: Option<u64>,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        let stop_at = now
            .checked_add(Duration::from_millis(budget))
            .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
        assert_eq!(
            retry_schedule(
                now,
                previous.map(Duration::from_millis),
                Duration::from_millis(initial),
                stop_at
            )
            .map(|(delay, next)| (delay, next.duration_since(now))),
            expected.map(|millis| (Duration::from_millis(millis), Duration::from_millis(millis)))
        );
        Ok(())
    }
}
