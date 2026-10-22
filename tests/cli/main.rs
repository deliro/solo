mod advanced;
mod backpressure;
mod common;
mod deadlines;
mod diagnostics;
mod finish_reconnect;
mod heartbeat_budget;
mod heartbeat_completion;
mod history;
mod migration_fast_path;
mod pgbouncer;
mod process_group;
mod scenarios;
mod sql_plans;
mod status;
mod tls;
mod windows;

use anyhow::Result;
use std::sync::Arc;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

fn main() -> Result<()> {
    let password = "s4_test_secret";
    let mut arguments = libtest_mimic::Arguments::from_args();
    // Timing-sensitive lease tests share a database server and spawn real processes.
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(2);
    }
    let runtime = tokio::runtime::Runtime::new()?;
    eprintln!("s4 container_start image=postgres:17 count=1");
    let container = runtime.block_on(
        GenericImage::new("postgres", "17")
            .with_entrypoint("/bin/sh")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
                    .with_times(2),
            ))
            .with_cmd(["-ec", "chown postgres:postgres /tmp/solo.key; chmod 600 /tmp/solo.key; exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/solo.crt -c ssl_key_file=/tmp/solo.key"])
            .with_copy_to("/tmp/solo.crt", include_bytes!("../fixtures/tls/server.crt").to_vec())
            .with_copy_to("/tmp/solo.key", include_bytes!("../fixtures/tls/server.key").to_vec())
            .with_env_var("POSTGRES_USER", "solo")
            .with_env_var("POSTGRES_PASSWORD", password)
            .with_env_var("POSTGRES_DB", "postgres")
            .start(),
    )?;
    eprintln!("s4 container_started id={}", container.id());
    let setup = runtime.block_on(async {
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432.tcp()).await?;
        anyhow::Ok(Arc::new(common::Harness {
            runtime: runtime.handle().clone(),
            base: format!("host={host} port={port} user=solo password={password} sslmode=disable connect_timeout=2"),
            backend_host: container.get_bridge_ip_address().await?.to_string(),
        }))
    });
    let outcome = setup.map(|harness| {
        libtest_mimic::run(&arguments, {
            let mut trials = scenarios::trials(&harness);
            trials.extend(backpressure::trials(&harness));
            trials.extend(heartbeat_budget::trials(&harness));
            trials.extend(heartbeat_completion::trials(&harness));
            trials.extend(migration_fast_path::trials(&harness));
            trials.extend(pgbouncer::trials(&harness));
            trials.extend(process_group::trials(&harness));
            trials.extend(advanced::trials(&harness));
            trials.extend(deadlines::trials(&harness));
            trials.extend(diagnostics::trials(&harness));
            trials.extend(finish_reconnect::trials(&harness));
            trials.extend(history::trials(&harness));
            trials.extend(windows::trials(&harness));
            trials.extend(sql_plans::trials(&harness));
            trials.extend(status::trials(&harness));
            trials.extend(tls::trials(&harness));
            trials
        })
    });
    eprintln!("s4 container_remove begin");
    runtime.block_on(container.rm())?;
    eprintln!("s4 container_remove complete");
    outcome?.exit();
}
