use std::num::NonZeroU32;

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char, one_of},
    combinator::{all_consuming, map, map_res, opt, value},
    sequence::{preceded, separated_pair},
};

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
    fn contains(&self, value: u32) -> bool {
        1_u64
            .checked_shl(value)
            .is_some_and(|bit| self.bits & bit != 0)
    }

    fn parse(text: &str, min: u32, max: u32, names: &[&str]) -> Result<Self, ScheduleError> {
        let spec = FieldSpec { min, max, names };
        text.split(',').try_fold(
            Self {
                bits: 0,
                wildcard: false,
            },
            |field, text| {
                let (_, item) = all_consuming(item)
                    .parse(text)
                    .map_err(|_| ScheduleError::Invalid)?;
                let parsed = spec.expand(item)?;
                Ok(Self {
                    bits: field.bits | parsed.bits,
                    wildcard: field.wildcard || parsed.wildcard,
                })
            },
        )
    }
}

#[derive(Clone, Copy)]
enum Selection<'a> {
    All,
    Single(&'a str),
    Range { start: &'a str, end: &'a str },
}

#[derive(Clone, Copy)]
struct Item<'a> {
    selection: Selection<'a>,
    step: Option<NonZeroU32>,
}

fn atom(input: &str) -> IResult<&str, &str> {
    take_while1(|character: char| character.is_ascii_alphanumeric() || character == '+')(input)
}

fn selection(input: &str) -> IResult<&str, Selection<'_>> {
    alt((
        value(Selection::All, one_of("*?")),
        map(separated_pair(atom, char('-'), atom), |(start, end)| {
            Selection::Range { start, end }
        }),
        map(atom, Selection::Single),
    ))
    .parse(input)
}

fn item(input: &str) -> IResult<&str, Item<'_>> {
    map(
        (
            selection,
            opt(preceded(char('/'), map_res(atom, str::parse::<NonZeroU32>))),
        ),
        |(selection, step)| Item { selection, step },
    )
    .parse(input)
}

struct FieldSpec<'a> {
    min: u32,
    max: u32,
    names: &'a [&'a str],
}

impl FieldSpec<'_> {
    fn number(&self, text: &str) -> Result<u32, ScheduleError> {
        self.names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(text))
            .map_or_else(
                || text.parse().map_err(|_| ScheduleError::Invalid),
                |index| {
                    u32::try_from(index)
                        .ok()
                        .and_then(|index| index.checked_add(self.min))
                        .ok_or(ScheduleError::Invalid)
                },
            )
    }

    fn expand(&self, item: Item<'_>) -> Result<Field, ScheduleError> {
        let Item { selection, step } = item;
        let (start, end) = match selection {
            Selection::All => (self.min, self.max),
            Selection::Single(text) => {
                let start = self.number(text)?;
                (start, step.map_or(start, |_| self.max))
            }
            Selection::Range { start, end } => (self.number(start)?, self.number(end)?),
        };
        if start < self.min || end > self.max || start > end {
            return Err(ScheduleError::Invalid);
        }
        let stride =
            usize::try_from(step.map_or(1, NonZeroU32::get)).map_err(|_| ScheduleError::Invalid)?;
        let bits = (start..=end).step_by(stride).try_fold(0, |bits, value| {
            1_u64
                .checked_shl(value)
                .map(|bit| bits | bit)
                .ok_or(ScheduleError::Invalid)
        })?;
        // A full explicit range is still restricted for cron's day-of-month/weekday OR rule.
        let wildcard =
            matches!(selection, Selection::All) && step.is_none_or(|step| step.get() == 1);
        Ok(Field { bits, wildcard })
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
        // Dates and weekdays repeat every 400 years. Include the current partial
        // month as well as the 4800 months needed to cover that entire cycle.
        const MONTH_SEARCH_LIMIT: usize = 4801;
        std::iter::successors(Some(now.date_naive()), |date| date.with_day(1)?.pred_opt())
            .take(MONTH_SEARCH_LIMIT)
            .filter(|date| self.month.contains(date.month()))
            .flat_map(|last| {
                (1..=last.day())
                    .rev()
                    .filter_map(move |day| last.with_day(day))
            })
            .filter(|date| self.matches_day(*date))
            .find_map(|date| self.latest_on_date(date, now))
            .ok_or(ScheduleError::NoOccurrence)
    }

    fn matches_day(&self, date: NaiveDate) -> bool {
        let dom = self.day.contains(date.day());
        let dow = self.weekday.contains(date.weekday().num_days_from_sunday());
        if self.day.wildcard || self.weekday.wildcard {
            dom && dow
        } else {
            dom || dow
        }
    }

    fn latest_on_date(&self, date: NaiveDate, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let today = date == now.date_naive();
        let last_hour = if today { now.hour() } else { 23 };
        (0..=last_hour)
            .rev()
            .filter(|hour| self.hour.contains(*hour))
            .find_map(|hour| {
                let last_minute = if today && hour == now.hour() {
                    now.minute()
                } else {
                    59
                };
                let minute = (0..=last_minute)
                    .rev()
                    .find(|minute| self.minute.contains(*minute))?;
                date.and_hms_opt(hour, minute, 0).map(|time| time.and_utc())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Weekday};

    #[rstest::rstest]
    #[case::before_today("30 10 * * *", "2026-10-21T10:29:59Z", "2026-10-20T10:30:00Z")]
    #[case::exact("30 10 * * *", "2026-10-21T10:30:00Z", "2026-10-21T10:30:00Z")]
    #[case::subsecond(
        "30 10 * * *",
        "2026-10-21T10:30:00.000000001Z",
        "2026-10-21T10:30:00Z"
    )]
    #[case::earlier_hour("5,35 9,17 * * *", "2026-10-21T17:04:59Z", "2026-10-21T09:35:00Z")]
    #[case::same_hour("5,35 9,17 * * *", "2026-10-21T17:34:59Z", "2026-10-21T17:05:00Z")]
    #[case::midnight("59 23 * * *", "2026-01-01T00:00:00Z", "2025-12-31T23:59:00Z")]
    #[case::next_month("0 0 31 * *", "2026-05-01T00:00:00Z", "2026-03-31T00:00:00Z")]
    #[case::next_year("0 0 1 JAN *", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")]
    #[case::previous_year("0 12 1 JAN *", "2026-01-01T11:59:59Z", "2025-01-01T12:00:00Z")]
    #[case::leap_day("0 0 29 FEB *", "2000-02-29T00:00:00Z", "2000-02-29T00:00:00Z")]
    #[case::leap_day_before_time("0 12 29 FEB *", "2000-02-29T11:59:59Z", "1996-02-29T12:00:00Z")]
    #[case::nonleap_century("0 0 29 FEB *", "2100-03-01T00:00:00Z", "2096-02-29T00:00:00Z")]
    #[case::eight_year_gap("0 0 29 FEB *", "2104-02-28T23:59:59Z", "2096-02-29T00:00:00Z")]
    #[case::leap_century("0 0 29 FEB *", "2400-03-01T00:00:00Z", "2400-02-29T00:00:00Z")]
    #[case::impossible_day_with_weekday(
        "0 0 31 FEB MON",
        "2026-03-01T00:00:00Z",
        "2026-02-23T00:00:00Z"
    )]
    #[case::year_zero("0 0 29 FEB *", "0000-03-01T00:00:00Z", "0000-02-29T00:00:00Z")]
    #[case::bce_year("0 0 1 JAN *", "-0001-01-01T00:00:00Z", "-0001-01-01T00:00:00Z")]
    #[case::leap_second("* * * * *", "2016-12-31T23:59:60Z", "2016-12-31T23:59:00Z")]
    fn latest_boundaries(
        #[case] expression: &str,
        #[case] now: &str,
        #[case] expected: &str,
    ) -> anyhow::Result<()> {
        assert_eq!(
            Schedule::parse(expression)?.latest(now.parse()?)?,
            expected.parse::<DateTime<Utc>>()?
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::min_exact(DateTime::<Utc>::MIN_UTC, "* * * * *", Some(DateTime::<Utc>::MIN_UTC))]
    #[case::min_no_previous(DateTime::<Utc>::MIN_UTC, "1 * * * *", None)]
    #[case::max(DateTime::<Utc>::MAX_UTC, "* * * * *", DateTime::<Utc>::MAX_UTC.date_naive().and_hms_opt(23, 59, 0).map(|date| date.and_utc()))]
    fn latest_chrono_limits(
        #[case] now: DateTime<Utc>,
        #[case] expression: &str,
        #[case] expected: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let actual = Schedule::parse(expression)?.latest(now);
        match expected {
            Some(expected) => assert_eq!(actual?, expected),
            None => assert!(matches!(actual, Err(ScheduleError::NoOccurrence))),
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::february_31("0 0 31 FEB *")]
    #[case::february_30("0 0 30 FEB *")]
    #[case::short_months("0 0 31 FEB,APR,JUN,SEP,NOV *")]
    #[case::short_months_at_noon("30 12 31 APR,JUN,SEP,NOV *")]
    fn latest_impossible_dates(#[case] expression: &str) -> anyhow::Result<()> {
        assert!(matches!(
            Schedule::parse(expression)?.latest("2026-10-21T10:43:51Z".parse()?),
            Err(ScheduleError::NoOccurrence)
        ));
        Ok(())
    }

    // Walk forward and remember each matching midnight, independently of the
    // production backward search. Cover every day in a complete calendar cycle.
    #[rstest::rstest]
    #[case::yearly("@yearly", |date: NaiveDate| date.month() == 1 && date.day() == 1)]
    #[case::leap_days("0 0 29 FEB *", |date: NaiveDate| date.month() == 2 && date.day() == 29)]
    #[case::month_ends("0 0 31 * *", |date: NaiveDate| date.day() == 31)]
    #[case::day_or_weekday("0 0 13 * FRI", |date: NaiveDate| date.day() == 13 || date.weekday() == Weekday::Fri)]
    #[case::month_and_weekday("0 0 * FEB MON", |date: NaiveDate| date.month() == 2 && date.weekday() == Weekday::Mon)]
    #[case::impossible_day_or_weekday("0 0 31 FEB MON", |date: NaiveDate| date.month() == 2 && date.weekday() == Weekday::Mon)]
    #[case::stepped_wildcard("0 0 */2 * MON", |date: NaiveDate| date.day() % 2 == 1 || date.weekday() == Weekday::Mon)]
    #[case::unstepped_wildcard("0 0 */1 * MON", |date: NaiveDate| date.weekday() == Weekday::Mon)]
    fn calendar_cycle_matches_forward_oracle(
        #[case] expression: &str,
        #[case] matches: fn(NaiveDate) -> bool,
    ) -> anyhow::Result<()> {
        let schedule = Schedule::parse(expression)?;
        let start = "1996-01-01".parse::<NaiveDate>()?;
        let mut expected = None;
        for date in std::iter::successors(Some(start), NaiveDate::succ_opt)
            .take_while(|date| date.year() < 2400)
        {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow::anyhow!("invalid test midnight"))?
                .and_utc();
            if matches(date) {
                expected = Some(midnight);
            }
            if date.year() >= 2000 {
                let expected =
                    expected.ok_or_else(|| anyhow::anyhow!("oracle has no prior occurrence"))?;
                assert_eq!(
                    schedule.latest(midnight)?,
                    expected,
                    "{expression} at {date}"
                );
            }
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::every_minute("* * * * *", |_: u32, _: u32| true)]
    #[case::stepped_workday("*/7 9-17 * * *", |hour: u32, minute: u32| (9..=17).contains(&hour) && minute.is_multiple_of(7))]
    #[case::scattered_times("5,35 0,9,17,23 * * *", |hour: u32, minute: u32| [0, 9, 17, 23].contains(&hour) && [5, 35].contains(&minute))]
    #[case::midnight("0 0 * * *", |hour: u32, minute: u32| hour == 0 && minute == 0)]
    #[case::last_minute("59 23 * * *", |hour: u32, minute: u32| hour == 23 && minute == 59)]
    fn time_of_day_matches_forward_oracle(
        #[case] expression: &str,
        #[case] matches: fn(u32, u32) -> bool,
    ) -> anyhow::Result<()> {
        let schedule = Schedule::parse(expression)?;
        let mut expected = None;
        for day in [20, 21] {
            let date = NaiveDate::from_ymd_opt(2026, 10, day)
                .ok_or_else(|| anyhow::anyhow!("invalid test date"))?;
            for hour in 0..24 {
                for minute in 0..60 {
                    if matches(hour, minute) {
                        expected = date.and_hms_opt(hour, minute, 0).map(|time| time.and_utc());
                    }
                    if day == 21 {
                        let now = date
                            .and_hms_opt(hour, minute, 59)
                            .ok_or_else(|| anyhow::anyhow!("invalid test time"))?
                            .and_utc();
                        let expected = expected
                            .ok_or_else(|| anyhow::anyhow!("oracle has no prior occurrence"))?;
                        assert_eq!(schedule.latest(now)?, expected, "{expression} at {now}");
                    }
                }
            }
        }
        Ok(())
    }

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
    #[case("5/20 * * * *", "2026-10-21T10:43:51Z", "2026-10-21T10:25:00+00:00")]
    #[case("+5/+20 * * * *", "2026-10-21T10:43:51Z", "2026-10-21T10:25:00+00:00")]
    #[case("5-35/15 * * * *", "2026-10-21T10:43:51Z", "2026-10-21T10:35:00+00:00")]
    #[case("0 0 */1 * mon", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 */+1 * MON", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 *,1 * MON", "2026-10-21T10:43:51Z", "2026-10-19T00:00:00+00:00")]
    #[case("0 0 */2,1 * MON", "2026-10-21T10:43:51Z", "2026-10-21T00:00:00+00:00")]
    #[case(
        "0 0 * JAN-MAR/2 *",
        "2026-10-21T10:43:51Z",
        "2026-03-31T00:00:00+00:00"
    )]
    #[case(
        "0 0 * * MON-FRI/2",
        "2026-10-22T10:43:51Z",
        "2026-10-21T00:00:00+00:00"
    )]
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
    #[case("1,,2 * * * *")]
    #[case("1, * * * *")]
    #[case(",1 * * * *")]
    #[case("1- * * * *")]
    #[case("-1 * * * *")]
    #[case("1/ * * * *")]
    #[case("/1 * * * *")]
    #[case("*/1/2 * * * *")]
    #[case("1/0 * * * *")]
    #[case("1/4294967296 * * * *")]
    #[case("1foo * * * *")]
    #[case("0 0 * * MONfoo")]
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
