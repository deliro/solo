use std::num::NonZeroU32;

use chrono::{DateTime, Datelike, Timelike, Utc};

#[derive(Debug, thiserror::Error)]
pub(super) enum ScheduleError {
    #[error("unsupported or invalid five-field UTC cron schedule")]
    Invalid,
    #[error("schedule has no previous occurrence within a Gregorian cycle")]
    NoOccurrence,
}

#[derive(Debug)]
pub(super) struct Schedule {
    minute: Field,
    hour: Field,
    day: Field,
    month: Field,
    weekday: Field,
}

#[derive(Debug)]
struct Field {
    bits: u64,
    wildcard: bool,
}

impl Field {
    const fn contains(&self, value: u32) -> bool {
        match 1_u64.checked_shl(value) {
            Some(bit) => self.bits & bit != 0,
            None => false,
        }
    }

    fn parse(text: &str, min: u32, max: u32, names: &[&str]) -> Result<Self, ScheduleError> {
        let mut bits = 0;
        let mut wildcard = false;
        for item in text.split(',') {
            let (range, step) = item
                .split_once('/')
                .map_or((item, None), |(range, step)| (range, Some(step)));
            let step = step
                .map(str::parse::<NonZeroU32>)
                .transpose()
                .map_err(|_| ScheduleError::Invalid)?;
            let number = |value: &str| -> Result<u32, ScheduleError> {
                names
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(value))
                    .map_or_else(
                        || value.parse::<u32>().map_err(|_| ScheduleError::Invalid),
                        |index| {
                            u32::try_from(index)
                                .ok()
                                .and_then(|index| index.checked_add(min))
                                .ok_or(ScheduleError::Invalid)
                        },
                    )
            };
            let (start, end, star) = match range {
                "*" | "?" => (min, max, step.is_none_or(|step| step.get() == 1)),
                range => {
                    if let Some((start, end)) = range.split_once('-') {
                        (number(start)?, number(end)?, false)
                    } else {
                        let start = number(range)?;
                        (start, step.map_or(start, |_| max), false)
                    }
                }
            };
            if start < min || end > max || start > end {
                return Err(ScheduleError::Invalid);
            }
            let stride = usize::try_from(step.map_or(1, NonZeroU32::get))
                .map_err(|_| ScheduleError::Invalid)?;
            for value in (start..=end).step_by(stride) {
                bits |= 1_u64.checked_shl(value).ok_or(ScheduleError::Invalid)?;
            }
            wildcard |= star;
        }
        Ok(Self { bits, wildcard })
    }
}

impl Schedule {
    pub(super) fn parse(expression: &str) -> Result<Self, ScheduleError> {
        let expression = match expression.trim() {
            "@hourly" => "0 * * * *",
            "@daily" | "@midnight" => "0 0 * * *",
            "@weekly" => "0 0 * * 0",
            "@monthly" => "0 0 1 * *",
            "@yearly" | "@annually" => "0 0 1 1 *",
            other => other,
        };
        let mut fields = expression.split_whitespace();
        let (Some(minute), Some(hour), Some(day), Some(month), Some(weekday), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return Err(ScheduleError::Invalid);
        };
        Ok(Self {
            minute: Field::parse(minute, 0, 59, &[])?,
            hour: Field::parse(hour, 0, 23, &[])?,
            day: Field::parse(day, 1, 31, &[])?,
            month: Field::parse(
                month,
                1,
                12,
                &[
                    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
                    "dec",
                ],
            )?,
            weekday: Field::parse(
                weekday,
                0,
                6,
                &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
            )?,
        })
    }

    pub(super) fn latest(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        let mut date = now.date_naive();
        // The Gregorian calendar, including weekdays, repeats every 146097 days.
        for _ in 0..=146_097 {
            let dom = self.day.contains(date.day());
            let dow = self.weekday.contains(date.weekday().num_days_from_sunday());
            let day_matches = if self.day.wildcard || self.weekday.wildcard {
                dom && dow
            } else {
                dom || dow
            };
            if self.month.contains(date.month()) && day_matches {
                let ceiling = if date == now.date_naive() {
                    now.hour()
                        .checked_mul(60)
                        .and_then(|hour| hour.checked_add(now.minute()))
                        .ok_or(ScheduleError::NoOccurrence)?
                } else {
                    1439
                };
                for hour in (0..=ceiling / 60)
                    .rev()
                    .filter(|hour| self.hour.contains(*hour))
                {
                    let last_minute = if hour == ceiling / 60 {
                        ceiling % 60
                    } else {
                        59
                    };
                    if let Some(minute) = (0..=last_minute)
                        .rev()
                        .find(|minute| self.minute.contains(*minute))
                    {
                        return date
                            .and_hms_opt(hour, minute, 0)
                            .map(|value| value.and_utc())
                            .ok_or(ScheduleError::NoOccurrence);
                    }
                }
            }
            date = date.pred_opt().ok_or(ScheduleError::NoOccurrence)?;
        }
        Err(ScheduleError::NoOccurrence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[rstest::rstest]
    #[case::at_boundary("2026-10-21T00:00:00Z", "2026-10-21T00:00:00Z")]
    #[case::one_second("2026-10-21T00:00:01Z", "2026-10-21T00:00:00Z")]
    #[case::thirty_seconds("2026-10-21T00:00:30Z", "2026-10-21T00:00:00Z")]
    #[case::before_boundary("2026-10-21T00:04:59Z", "2026-10-21T00:00:00Z")]
    #[case::next_boundary("2026-10-21T00:05:00Z", "2026-10-21T00:05:00Z")]
    fn five_minute_window_boundaries(
        #[case] now: &str,
        #[case] expected: &str,
    ) -> anyhow::Result<()> {
        assert_eq!(
            Schedule::parse("*/5 * * * *")?.latest(now.parse()?)?,
            expected.parse::<DateTime<Utc>>()?
        );
        Ok(())
    }

    #[rstest::rstest]
    fn hourly_uses_latest_minute() -> Result<(), Box<dyn std::error::Error>> {
        let now = "2026-10-21T10:43:51Z".parse()?;
        assert_eq!(
            Schedule::parse("@hourly")?.latest(now)?.to_rfc3339(),
            "2026-10-21T10:00:00+00:00"
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case(
        "*/15 9-17 * * MON-FRI",
        "2026-10-21T10:43:51Z",
        "2026-10-21T10:30:00+00:00"
    )]
    #[case("0 0 1 * MON", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 * * MON", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 1-31 * MON", "2026-10-21T10:43:51Z", "2026-10-21T00:00:00+00:00")]
    #[case("0 0 */2 * MON", "2026-10-20T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 ? * MON", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 29 FEB *", "2103-02-28T00:00:00Z", "2096-02-29T00:00:00+00:00")]
    #[case("@annually", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00")]
    #[case("@monthly", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00")]
    #[case("@weekly", "2026-10-21T10:43:51Z", "2026-10-18T00:00:00+00:00")]
    #[case("@midnight", "2026-10-21T10:43:51Z", "2026-10-21T00:00:00+00:00")]
    #[case("5,35 0 JAN * *", "2026-10-21T10:43:51Z", "invalid")]
    #[case("5,35 0 * JAN *", "2026-10-21T10:43:51Z", "2026-01-31T00:35:00+00:00")]
    fn kubernetes_calendar_corpus(
        #[case] expression: &str,
        #[case] now: &str,
        #[case] expected: &str,
    ) {
        let result = Schedule::parse(expression)
            .and_then(|schedule| schedule.latest(now.parse().map_err(|_| ScheduleError::Invalid)?));
        let actual = result.map_or_else(|_| "invalid".into(), |value| value.to_rfc3339());
        assert_eq!(actual, expected, "{expression}");
    }

    #[rstest::rstest]
    #[case("0 * * * * *")]
    #[case("@every 1h")]
    #[case("TZ=UTC * * * * *")]
    #[case("CRON_TZ=UTC * * * * *")]
    #[case("0 0 L * *")]
    #[case("0 0 * * 7")]
    #[case("0 0 * * MON#2")]
    #[case("*/0 * * * *")]
    #[case("0 0 * * FRI-MON")]
    fn unsupported_syntax_is_rejected(#[case] expression: &str) {
        assert!(
            matches!(Schedule::parse(expression), Err(ScheduleError::Invalid)),
            "{expression}"
        );
    }

    #[rstest::rstest]
    fn impossible_date_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            Schedule::parse("0 0 31 FEB *")?.latest("2026-10-21T10:43:51Z".parse()?),
            Err(ScheduleError::NoOccurrence)
        ));
        Ok(())
    }
}
