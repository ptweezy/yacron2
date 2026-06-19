//! Crontab parsing and matching, faithful to the `parse-crontab` library that
//! the original Python yacron2 used (josiahcarlson/parse-crontab).
//!
//! Notable semantics reproduced here (some differ from Vixie cron):
//!
//! * Fields are `[second] minute hour day-of-month month day-of-week [year]`.
//!   A 5-field spec gets an implicit `second = 0` and `year = *`; a 6-field
//!   spec keeps the 6th field as the **year** (not seconds) with an implicit
//!   `second = 0`; a 7-field spec is taken verbatim.
//! * Day-of-month and day-of-week are combined with **AND**, not the Vixie
//!   "OR when both are restricted" rule.
//! * Sunday may be written as `0` or `7`; month/weekday names are accepted.
//! * `*/step`, `a-b`, `a-b/step`, `a/step`, comma lists, `L` (last day of
//!   month) and `L<weekday>` / `L<a-b>` (last weekday-of-month) are supported.
//! * Ranges must be ascending (the library runs with `loop=False`); a
//!   descending range is a configuration error.
//! * The `@yearly`/`@annually`/`@monthly`/`@weekly`/`@daily`/`@hourly` aliases
//!   expand to their classic 5-field forms.

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashSet;

use crate::error::{cfg_err, ConfigResult};

/// Inclusive `(start, end)` value ranges for each of the seven fields.
const RANGES: [(u32, u32); 7] = [
    (0, 59),      // 0 second
    (0, 59),      // 1 minute
    (0, 23),      // 2 hour
    (1, 31),      // 3 day of month
    (1, 12),      // 4 month
    (0, 6),       // 5 day of week (Sunday = 0)
    (1970, 2099), // 6 year
];

const SECOND: usize = 0;
const DAY: usize = 3;
const MONTH: usize = 4;
const WEEKDAY: usize = 5;

const MONTH_NAMES: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const WEEKDAY_NAMES: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// A parsed schedule: either the special `@reboot`, or a crontab expression.
// `Cron` is much larger than `Reboot`, but a Schedule is stored once per job,
// never in a hot collection, so boxing would only add indirection.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Runs once, when yacron2 itself starts up.
    Reboot,
    /// A recurring crontab expression.
    Cron(CronExpr),
}

impl Schedule {
    /// Parse a string schedule (`"@reboot"`, an `@`-alias, or a crontab line).
    pub fn parse_string(s: &str) -> ConfigResult<Schedule> {
        let trimmed = s.trim();
        if trimmed == "@reboot" {
            return Ok(Schedule::Reboot);
        }
        Ok(Schedule::Cron(CronExpr::parse(trimmed)?))
    }

    /// Build a schedule from the object form, where each field defaults to `*`.
    ///
    /// Unlike the original Python (which dropped `year` when assembling the
    /// crontab string), the `year` field is honoured here, matching what the
    /// documentation has always advertised.
    pub fn from_fields(
        minute: &str,
        hour: &str,
        day_of_month: &str,
        month: &str,
        day_of_week: &str,
        year: &str,
    ) -> ConfigResult<Schedule> {
        let tab = format!("{minute} {hour} {day_of_month} {month} {day_of_week} {year}");
        Ok(Schedule::Cron(CronExpr::parse(&tab)?))
    }

    /// The human-readable form used for status output and the
    /// `YACRON2_JOB_SCHEDULE` reporter variable.
    pub fn display(&self) -> &str {
        match self {
            Schedule::Reboot => "@reboot",
            Schedule::Cron(expr) => &expr.display,
        }
    }

    /// Whether this schedule fires at the given wall-clock instant.
    #[allow(dead_code)] // convenience wrapper; the scheduler matches on CronExpr
    pub fn matches(&self, dt: &NaiveDateTime) -> bool {
        match self {
            Schedule::Reboot => false,
            Schedule::Cron(expr) => expr.matches(dt),
        }
    }
}

/// A compiled crontab expression: seven independent field matchers.
#[derive(Debug, Clone)]
pub struct CronExpr {
    fields: [Field; 7],
    display: String,
}

const ALIASES: &[(&str, &str)] = &[
    ("@yearly", "0 0 1 1 *"),
    ("@annually", "0 0 1 1 *"),
    ("@monthly", "0 0 1 * *"),
    ("@weekly", "0 0 * * 0"),
    ("@daily", "0 0 * * *"),
    ("@hourly", "0 * * * *"),
];

impl CronExpr {
    pub fn parse(spec: &str) -> ConfigResult<CronExpr> {
        let display = spec.to_string();
        let resolved = ALIASES
            .iter()
            .find(|(a, _)| *a == spec)
            .map(|(_, v)| *v)
            .unwrap_or(spec);

        let mut parts: Vec<String> = resolved.split_whitespace().map(|s| s.to_string()).collect();

        match parts.len() {
            5 => {
                parts.insert(0, "0".to_string());
                parts.push("*".to_string());
            }
            6 => {
                parts.insert(0, "0".to_string());
            }
            7 => {}
            n => {
                return Err(cfg_err!(
                    "improper number of cron entries specified; \
                     got {n} need 5 to 7"
                ))
            }
        }

        let mut fields = Vec::with_capacity(7);
        for (which, entry) in parts.iter().enumerate() {
            fields.push(Field::parse(which, entry)?);
        }
        let fields: [Field; 7] = fields.try_into().expect("exactly 7 fields");
        Ok(CronExpr { fields, display })
    }

    pub fn matches(&self, dt: &NaiveDateTime) -> bool {
        let values = field_values(dt);
        self.fields
            .iter()
            .zip(values)
            .all(|(field, value)| field.matches(value, dt))
    }

    /// The next minute-aligned instant strictly after `now` at which this
    /// expression fires, or `None` if none occurs within a sane horizon
    /// (e.g. a `year` field whose values are all in the past).
    ///
    /// This drives only the informational `GET /status` endpoint, so it is
    /// computed in wall-clock terms and ignores DST transitions.
    pub fn next_after(&self, now: NaiveDateTime) -> Option<NaiveDateTime> {
        // Start at the next whole minute.
        let mut t = now
            .with_second(0)?
            .with_nanosecond(0)?
            .checked_add_signed(Duration::minutes(1))?;

        // Generous cap: enough to scan to the end of the year range without
        // looping forever on an impossible spec.
        for _ in 0..(366 * 24 * 60 * 150) {
            if t.year() > RANGES[6].1 as i32 {
                return None;
            }
            if !self.fields[6].matches(t.year() as u32, &t) {
                // advance to 1 Jan of next year
                t = NaiveDate::from_ymd_opt(t.year() + 1, 1, 1)?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.fields[MONTH].matches(t.month(), &t) {
                t = first_of_next_month(&t)?;
                continue;
            }
            let day_ok = self.fields[DAY].matches(t.day(), &t);
            let dow_ok = self.fields[WEEKDAY].matches(t.weekday().num_days_from_sunday(), &t);
            if !(day_ok && dow_ok) {
                t = next_day_midnight(&t)?;
                continue;
            }
            if !self.fields[2].matches(t.hour(), &t) {
                t = next_hour(&t)?;
                continue;
            }
            if !self.fields[1].matches(t.minute(), &t) {
                t = t.checked_add_signed(Duration::minutes(1))?;
                continue;
            }
            if !self.fields[SECOND].matches(0, &t) {
                // A 7-field spec whose seconds never include 0 can never fire
                // on a minute boundary; bail rather than spin forever.
                return None;
            }
            return Some(t);
        }
        None
    }
}

/// Extract the seven field values from a datetime, with weekday as Sunday=0.
fn field_values(dt: &NaiveDateTime) -> [u32; 7] {
    [
        dt.second(),
        dt.minute(),
        dt.hour(),
        dt.day(),
        dt.month(),
        dt.weekday().num_days_from_sunday(),
        dt.year() as u32,
    ]
}

fn first_of_next_month(dt: &NaiveDateTime) -> Option<NaiveDateTime> {
    let (y, m) = (dt.year(), dt.month());
    let date = if m == 12 {
        NaiveDate::from_ymd_opt(y + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(y, m + 1, 1)
    }?;
    date.and_hms_opt(0, 0, 0)
}

fn next_day_midnight(dt: &NaiveDateTime) -> Option<NaiveDateTime> {
    let date = dt.date().checked_add_signed(Duration::days(1))?;
    date.and_hms_opt(0, 0, 0)
}

fn next_hour(dt: &NaiveDateTime) -> Option<NaiveDateTime> {
    dt.with_minute(0)?
        .with_second(0)?
        .checked_add_signed(Duration::hours(1))
}

/// The last calendar day number of `dt`'s month (28–31).
fn end_of_month_day(dt: &NaiveDateTime) -> u32 {
    let (y, m) = (dt.year(), dt.month());
    let first_next = if m == 12 {
        NaiveDate::from_ymd_opt(y + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(y, m + 1, 1)
    }
    .expect("valid month boundary");
    (first_next - Duration::days(1)).day()
}

/// One special "last-of-month" piece (`L` forms) that needs the live datetime.
#[allow(clippy::enum_variant_names)] // the shared "Last" prefix is meaningful
#[derive(Debug, Clone)]
enum Special {
    /// `L` in the day field: the last day of the month.
    LastDayOfMonth,
    /// `L<weekday>`: the last occurrence of that weekday in the month.
    LastWeekday(u32),
    /// `L<a-b>`: the last occurrence of any weekday in `a..=b` in the month.
    LastWeekdayRange(u32, u32),
}

/// A single compiled crontab field.
#[derive(Debug, Clone)]
struct Field {
    any: bool,
    allowed: HashSet<u32>,
    specials: Vec<Special>,
}

impl Field {
    fn parse(which: usize, entry: &str) -> ConfigResult<Field> {
        let lowered = entry.to_lowercase();
        let pieces: Vec<&str> = lowered.split(',').collect();
        let any = pieces.iter().any(|p| *p == "*" || *p == "?");

        let mut allowed = HashSet::new();
        let mut specials = Vec::new();
        for piece in &pieces {
            let (set, special) = parse_piece(which, piece)?;
            if let Some(set) = set {
                allowed.extend(set);
            }
            if let Some(special) = special {
                specials.push(special);
            }
        }
        Ok(Field {
            any,
            allowed,
            specials,
        })
    }

    fn matches(&self, value: u32, dt: &NaiveDateTime) -> bool {
        for special in &self.specials {
            match special {
                Special::LastDayOfMonth => {
                    if value == end_of_month_day(dt) {
                        return true;
                    }
                }
                Special::LastWeekday(target) => {
                    if !is_last_week_of_month(dt) {
                        continue;
                    }
                    if value == *target {
                        return true;
                    }
                }
                Special::LastWeekdayRange(start, end) => {
                    if !is_last_week_of_month(dt) {
                        continue;
                    }
                    let mut set: HashSet<u32> = (*start..=*end).collect();
                    if set.contains(&7) {
                        set.insert(0);
                    }
                    if set.contains(&value) {
                        return true;
                    }
                }
            }
        }
        self.any || self.allowed.contains(&value)
    }
}

fn is_last_week_of_month(dt: &NaiveDateTime) -> bool {
    let plus_week = dt.date() + Duration::days(7);
    plus_week.month() != dt.month()
}

/// Resolve a month/weekday name or a numeric token to its value, with range
/// validation. `end_limit` is 7 for the weekday field (Sunday-as-7), else the
/// field's natural maximum.
fn fix(which: usize, token: &str, start: u32, end_limit: u32) -> ConfigResult<u32> {
    if which == MONTH {
        if let Some(idx) = MONTH_NAMES.iter().position(|n| *n == token) {
            return Ok(idx as u32 + 1);
        }
    } else if which == WEEKDAY {
        if let Some(idx) = WEEKDAY_NAMES.iter().position(|n| *n == token) {
            return Ok(idx as u32);
        }
    }
    let value: u32 = token
        .parse()
        .map_err(|_| cfg_err!("invalid range specifier: {token:?}"))?;
    if value < start || value > end_limit {
        return Err(cfg_err!(
            "item value {value} out of range [{start}, {end_limit}]"
        ));
    }
    Ok(value)
}

/// Parse a single comma-separated piece of a field, returning the set of
/// matched values it contributes and/or a "last-of-month" special.
fn parse_piece(which: usize, entry: &str) -> ConfigResult<(Option<HashSet<u32>>, Option<Special>)> {
    let (start_range, end_range) = RANGES[which];
    let end_limit = if which == WEEKDAY { 7 } else { end_range };

    // wildcards
    if entry == "*" || entry == "?" {
        if entry == "?" && which != DAY && which != WEEKDAY {
            return Err(cfg_err!("cannot use '?' outside the day fields"));
        }
        return Ok((None, None));
    }

    // last day of month
    if entry == "l" {
        if which != DAY {
            return Err(cfg_err!(
                "you can only specify a bare 'L' in the 'day' field"
            ));
        }
        return Ok((None, Some(Special::LastDayOfMonth)));
    }

    // last <weekday> of month, e.g. "l5" or "l1-5"
    if let Some(rest) = entry.strip_prefix('l') {
        if which != WEEKDAY {
            return Err(cfg_err!(
                "you can only specify a leading 'L' in the 'weekday' field"
            ));
        }
        if let Some((a, b)) = rest.split_once('-') {
            let (sa, sb): (u32, u32) = (
                a.parse()
                    .map_err(|_| cfg_err!("invalid 'L' weekday range: {entry:?}"))?,
                b.parse()
                    .map_err(|_| cfg_err!("invalid 'L' weekday range: {entry:?}"))?,
            );
            if sa > 7 || sb > 7 {
                return Err(cfg_err!("'L' weekday range out of range: {entry:?}"));
            }
            return Ok((None, Some(Special::LastWeekdayRange(sa, sb))));
        }
        let day: u32 = rest
            .parse()
            .map_err(|_| cfg_err!("invalid 'L' weekday: {entry:?}"))?;
        if day > 7 {
            return Err(cfg_err!("'L' weekday out of range: {entry:?}"));
        }
        return Ok((
            None,
            Some(Special::LastWeekday(if day == 7 { 0 } else { day })),
        ));
    }

    // optional /step increment
    let (body, increment): (&str, Option<u32>) = match entry.split_once('/') {
        Some((b, inc)) => {
            let step: u32 = inc
                .parse()
                .map_err(|_| cfg_err!("invalid increment: {entry:?}"))?;
            if step == 0 {
                return Err(cfg_err!("increment must be positive: {entry:?}"));
            }
            if step > end_limit {
                return Err(cfg_err!(
                    "increment {step} must be <= {end_limit}: {entry:?}"
                ));
            }
            (b, Some(step))
        }
        None => (entry, None),
    };

    // a range, a wildcard-with-step, or a single value
    let (lo, hi): (u32, u32) = if let Some((a, b)) = body.split_once('-') {
        let mut hi = fix(which, b, start_range, end_limit)?;
        let lo = fix(which, a, start_range, end_limit)?;
        if (which == DAY || which == WEEKDAY) && hi == 0 {
            hi = 7; // "sat-sun"
        }
        (lo, hi)
    } else if body == "*" {
        (start_range, end_range)
    } else {
        let single = fix(which, body, start_range, end_limit)?;
        if increment.is_none() {
            let mut set = HashSet::new();
            set.insert(normalize_weekday(which, single));
            return Ok((Some(set), None));
        }
        (single, end_range)
    };

    if lo > hi {
        return Err(cfg_err!(
            "range start {lo} is greater than end {hi}: {entry:?}"
        ));
    }
    if let Some(step) = increment {
        if lo + step > end_limit {
            return Err(cfg_err!(
                "first stepped value {} is out of range: {entry:?}",
                lo + step
            ));
        }
    }

    let step = increment.unwrap_or(1);
    let mut set = HashSet::new();
    let mut v = lo;
    while v <= hi {
        set.insert(normalize_weekday(which, v));
        v += step;
    }
    Ok((Some(set), None))
}

/// Fold Sunday-as-7 down to 0 in the weekday field.
fn normalize_weekday(which: usize, value: u32) -> u32 {
    if which == WEEKDAY && value == 7 {
        0
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn dt(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap()
    }

    #[test]
    fn every_minute() {
        let s = Schedule::parse_string("* * * * *").unwrap();
        assert!(s.matches(&dt(2020, 7, 20, 14, 59)));
    }

    #[test]
    fn specific_time() {
        let s = Schedule::parse_string("59 14 * * *").unwrap();
        assert!(s.matches(&dt(2020, 7, 20, 14, 59)));
        assert!(!s.matches(&dt(2020, 7, 20, 14, 49)));
    }

    #[test]
    fn step_and_list() {
        let s = Schedule::parse_string("*/15 * * * *").unwrap();
        assert!(s.matches(&dt(2020, 1, 1, 0, 0)));
        assert!(s.matches(&dt(2020, 1, 1, 0, 15)));
        assert!(!s.matches(&dt(2020, 1, 1, 0, 16)));

        let s = Schedule::parse_string("0,30 * * * *").unwrap();
        assert!(s.matches(&dt(2020, 1, 1, 0, 30)));
        assert!(!s.matches(&dt(2020, 1, 1, 0, 31)));
    }

    #[test]
    fn weekday_names_and_sunday_seven() {
        // Sunday as 0 and 7 both work; names are case-insensitive.
        let s0 = Schedule::parse_string("0 0 * * 0").unwrap();
        let s7 = Schedule::parse_string("0 0 * * 7").unwrap();
        let sname = Schedule::parse_string("0 0 * * SUN").unwrap();
        let sunday = dt(2020, 7, 19, 0, 0); // a Sunday
        assert!(s0.matches(&sunday));
        assert!(s7.matches(&sunday));
        assert!(sname.matches(&sunday));
    }

    #[test]
    fn dom_and_dow_are_anded() {
        // parse-crontab ANDs day-of-month and day-of-week.
        let s = Schedule::parse_string("0 0 15 * MON").unwrap();
        // 15 Jun 2020 is a Monday -> matches.
        assert!(s.matches(&dt(2020, 6, 15, 0, 0)));
        // 15 Jul 2020 is a Wednesday -> does not match (AND, not OR).
        assert!(!s.matches(&dt(2020, 7, 15, 0, 0)));
    }

    #[test]
    fn month_names_and_year_field() {
        let s = Schedule::from_fields("*/5", "*", "19", "7", "*", "2017").unwrap();
        assert!(s.matches(&dt(2017, 7, 19, 0, 5)));
        assert!(!s.matches(&dt(2018, 7, 19, 0, 5)));
    }

    #[test]
    fn last_day_of_month() {
        let s = Schedule::parse_string("0 0 L * *").unwrap();
        assert!(s.matches(&dt(2020, 2, 29, 0, 0))); // leap year
        assert!(!s.matches(&dt(2020, 2, 28, 0, 0)));
    }

    #[test]
    fn descending_range_is_error() {
        assert!(Schedule::parse_string("0 0 * * 5-1").is_err());
    }

    #[test]
    fn wrong_field_count_is_error() {
        assert!(Schedule::parse_string("* * *").is_err());
    }

    #[test]
    fn next_after_basic() {
        let s = match Schedule::parse_string("30 4 * * *").unwrap() {
            Schedule::Cron(e) => e,
            _ => unreachable!(),
        };
        let next = s.next_after(dt(2020, 7, 20, 4, 0)).unwrap();
        assert_eq!(next, dt(2020, 7, 20, 4, 30));
    }
}
