mod config;
mod diagnostics;
mod schedule;
mod status;
mod store;
mod supervisor;

use std::io::Write as _;

fn main() -> std::process::ExitCode {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_or_else(
            |_| {
                let _ = writeln!(std::io::stderr().lock(), "solo phase=start reason=runtime");
                std::process::ExitCode::FAILURE
            },
            |runtime| {
                let code = runtime.block_on(run());
                // DNS and CA loading may still be blocked after their caller times out.
                runtime.shutdown_timeout(std::time::Duration::from_millis(100));
                code
            },
        )
}

async fn run() -> std::process::ExitCode {
    let config = match config::Invocation::parse() {
        Ok(config) => config,
        Err(config::ConfigError::Cli(error)) => {
            let code = if error.exit_code() == 0 {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::FAILURE
            };
            if error.print().is_err() {
                return std::process::ExitCode::FAILURE;
            }
            return code;
        }
        Err(error) => {
            let _ = writeln!(std::io::stderr().lock(), "{error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let config = match config {
        config::Invocation::Run(config) => config,
        config::Invocation::Status(config) => {
            return match status::run(config).await {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(error) => {
                    let _ = writeln!(std::io::stderr().lock(), "solo phase=status reason={error}");
                    std::process::ExitCode::FAILURE
                }
            };
        }
    };
    let diagnostics = diagnostics::Diagnostics::new();
    match supervisor::run(config, &diagnostics).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(reason) => {
            diagnostics.emit(format!("solo phase=run reason={reason}"));
            std::process::ExitCode::FAILURE
        }
    }
}
