use crate::{
    config::StatusConfig,
    store::{Completion, Store, StoreError, TaskSnapshot, TaskStatus},
};
use chrono::{DateTime, Utc};
use std::{borrow::Cow, fmt::Write as _, io::Write as _};

#[derive(Debug, thiserror::Error)]
pub(super) enum StatusError {
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("could not write status output; check stdout")]
    Output(#[from] std::io::Error),
}

pub(super) async fn run(config: StatusConfig) -> Result<(), StatusError> {
    let mut store = Store::connect(&config.database_url, config.db_timeout).await?;
    let status = store.status(&config.key).await?;
    std::io::stdout()
        .lock()
        .write_all(render(&config.key, &status).as_bytes())?;
    Ok(())
}

fn timestamp(value: Option<DateTime<Utc>>) -> Cow<'static, str> {
    value.map_or_else(
        || Cow::Borrowed("-- (not recorded)"),
        |value| value.to_rfc3339().into(),
    )
}

fn elapsed(start: Option<DateTime<Utc>>, end: DateTime<Utc>) -> Cow<'static, str> {
    start.map_or_else(
        || Cow::Borrowed("-- (not recorded)"),
        |start| {
            end.signed_duration_since(start).to_std().map_or_else(
                |_| Cow::Borrowed("-- (timestamp is after database time)"),
                |duration| humantime::format_duration(duration).to_string().into(),
            )
        },
    )
}

fn render(key: &str, status: &TaskStatus) -> String {
    let mut output = format!("Key: {key:?}\n");
    // Writing to String is infallible; keep the formatting result local.
    let _ = match status {
        TaskStatus::Missing => writeln!(output, "State: unknown (key not recorded)"),
        TaskStatus::Found(snapshot) => render_snapshot(&mut output, snapshot),
    };
    output
}

fn render_snapshot(output: &mut String, snapshot: &TaskSnapshot) -> std::fmt::Result {
    let TaskSnapshot {
        observed_at,
        owner,
        owner_host,
        owner_pid,
        scheduled_at,
        expires_at,
        acquired_at,
        last_completed_at,
        last_outcome,
        last_success_at,
        last_success,
    } = snapshot;
    let state = if owner.is_none() {
        "free"
    } else if expires_at.is_some_and(|expires| expires > *observed_at) {
        "held (lease only; child liveness is not verified)"
    } else {
        "expired (ownership is stale)"
    };
    writeln!(output, "State: {state}")?;
    writeln!(output, "Database time: {}", observed_at.to_rfc3339())?;
    writeln!(
        output,
        "Owner UUID: {}",
        owner.map_or_else(
            || Cow::Borrowed("-- (not recorded)"),
            |owner| owner.to_string().into()
        )
    )?;
    writeln!(
        output,
        "Owner host: {}",
        owner_host.as_ref().map_or_else(
            || Cow::Borrowed("-- (not recorded)"),
            |host| format!("{host:?}").into()
        )
    )?;
    writeln!(
        output,
        "Owner Solo PID: {}",
        owner_pid.map_or_else(
            || Cow::Borrowed("-- (not recorded)"),
            |pid| pid.to_string().into()
        )
    )?;
    writeln!(
        output,
        "Latest attempt / acquired at: {}",
        timestamp(*acquired_at)
    )?;
    writeln!(
        output,
        "Elapsed since acquisition (DB time): {}",
        elapsed(*acquired_at, *observed_at)
    )?;
    writeln!(output, "Lease expires at: {}", timestamp(*expires_at))?;
    let remaining = expires_at.map_or_else(
        || Cow::Borrowed("-- (not recorded)"),
        |expires| {
            if expires <= *observed_at {
                Cow::Borrowed("0s (expired)")
            } else {
                elapsed(Some(*observed_at), expires)
            }
        },
    );
    writeln!(output, "Lease remaining (DB time): {remaining}")?;
    writeln!(output, "Scheduled interval: {}", timestamp(*scheduled_at))?;
    writeln!(
        output,
        "Last completed at: {}",
        timestamp(*last_completed_at)
    )?;
    let outcome = match last_outcome {
        Some(Completion::Success) => "success",
        Some(Completion::Failure) => "failure",
        None => "-- (not recorded)",
    };
    writeln!(output, "Last outcome: {outcome}")?;
    writeln!(
        output,
        "Latest actual successful completion: {}",
        timestamp(*last_success_at)
    )?;
    writeln!(
        output,
        "Successful scheduled interval: {}",
        timestamp(*last_success)
    )
}

#[cfg(test)]
mod tests {
    use super::{TaskStatus, render};

    #[rstest::rstest]
    fn unknown_key_output_escapes_terminal_controls() {
        assert_eq!(
            render("task\n\u{1b}[31m", &TaskStatus::Missing),
            "Key: \"task\\n\\u{1b}[31m\"\nState: unknown (key not recorded)\n"
        );
    }
}
