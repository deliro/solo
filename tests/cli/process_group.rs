use crate::common::{self, Harness};
use anyhow::{Result, ensure};
use libtest_mimic::Trial;
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use std::{process::Command, sync::Arc, time::Duration};

pub(super) fn trials(harness: &Arc<Harness>) -> Vec<Trial> {
    let harness = Arc::clone(harness);
    vec![Trial::test(
        "process_group_sigterm_after_child_exit",
        move || {
            harness
                .runtime
                .clone()
                .block_on(sigterm_after_child_exit(harness))
                .map_err(|error| format!("{error:#}").into())
        },
    )]
}

async fn sigterm_after_child_exit(harness: Arc<Harness>) -> Result<()> {
    let db = harness.database().await?;
    let ready = db.path("pid");
    let script = format!("echo $$ > '{}'; exec sleep 30", ready.display());
    let mut command = db.command("zombie", "@yearly");
    command.args([
        "--ttl",
        "10s",
        "--heartbeat",
        "2s",
        "--",
        "/bin/sh",
        "-c",
        &script,
    ]);
    let mut cli = db.start(command)?;
    cli.track_group(&ready).await?;
    // Keep the supervisor from reaping its child, then queue shutdown while
    // the process group consists only of the unreaped leader.
    cli.signal(Signal::SIGSTOP)?;
    let supervisor: i64 = db
        .client
        .query_one(
            "SELECT owner_pid FROM public.solo_leases WHERE key='zombie'",
            &[],
        )
        .await?
        .try_get(0)?;
    wait_state(i32::try_from(supervisor)?, 'T').await?;
    let pid = common::pid(&ready)?;
    kill(Pid::from_raw(pid), Signal::SIGKILL)?;
    wait_state(pid, 'Z').await?;
    cli.signal(Signal::SIGTERM)?;
    cli.signal(Signal::SIGCONT)?;
    ensure!(!cli.wait().await?.success(), "signal shutdown succeeded");
    ensure!(
        cli.logs()?.contains("reason=sigterm"),
        "shutdown lost signal reason: {}",
        cli.logs()?
    );
    let released: bool = db.client.query_one(
        "SELECT owner IS NULL AND last_success IS NULL FROM public.solo_leases WHERE key='zombie'",
        &[],
    ).await?.try_get(0)?;
    ensure!(released, "shutdown did not release ownership");
    Ok(())
}

async fn wait_state(pid: i32, state: char) -> Result<()> {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(3))
        .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
    loop {
        let output = Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()?;
        if String::from_utf8(output.stdout)?
            .trim_start()
            .starts_with(state)
        {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "process {pid} did not reach state {state}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}
