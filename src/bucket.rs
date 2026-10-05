//! Time buckets: `snouttime.bucket(width, time [, origin | timezone])`.
//!
//! A bucket is the start of the width-sized interval, counted from an origin, that holds a
//! time. The semantics, which the regression test `bucket` holds to, are in README.md; in
//! short:
//!
//! * Without a time zone a `timestamptz` is bucketed in UTC, whatever the session's
//!   `TimeZone`, which is also how series partitions are aligned. So the same query gives the
//!   same buckets in every session.
//! * A width is either months (and years) or days and time, never both: "one month and a
//!   day" has no fixed length and no obvious meaning, so it is refused, as it is for
//!   partitions.
//! * Month widths are calendar arithmetic. Bucket k starts at origin + k * width months with
//!   the day clamped to the month's length, counted from the origin every time rather than
//!   from the previous bucket, so an origin on the 31st gives Jan 31, Feb 29, Mar 31 and never
//!   drifts to the 28th.
//! * With a time zone, a width of whole days or months starts at LOCAL midnight (a day bucket
//!   across a DST change is 23 or 25 hours long, which is the point); any other width is
//!   measured on the local clock from the local origin but anchored to the instant, so a
//!   bucket can never start after the time it holds. See `in_zone`.
//! * The default origin is Monday 2000-01-03 for day and time widths, so week buckets start
//!   on Mondays, and 2000-01-01 for month widths.
//!
//! The arithmetic is plain Rust on microseconds since 2000-01-01, Postgres's own
//! representation, with the calendar done by the days-from-civil algorithm; only the time
//! zone conversions call into Postgres, so the zone database is Postgres's.

use pgrx::fcinfo::direct_function_call;
use pgrx::prelude::*;

/// Microseconds in a day.
const USECS_PER_DAY: i64 = 86_400_000_000;
/// Days from 1970-01-01 (the civil algorithm's epoch) to 2000-01-01 (Postgres's).
const PG_EPOCH_DAYS: i64 = 10_957;
/// Monday 2000-01-03, the default origin for widths in days and time.
const MONDAY_ORIGIN: i64 = 2 * USECS_PER_DAY;
/// 2000-01-01, the default origin for widths in months.
const MONTH_ORIGIN: i64 = 0;
/// The range of a Postgres timestamp, 4714-11-24 BC to 294277-01-01 (MIN_TIMESTAMP and
/// END_TIMESTAMP in Postgres's datatype/timestamp.h, which bindgen does not carry over).
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;

/// A bucket width, validated once: months, or days and time, never both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Width {
	Months(i64),
	Micros(i64),
}

impl Width {
	pub(crate) fn of(width: &Interval) -> Width {
		let (months, days, micros) = (width.months() as i64, width.days() as i64, width.micros());
		if months != 0 && (days != 0 || micros != 0) {
			error!("a bucket width cannot mix months with days or time: {} months, {} days, {} microseconds",
				months, days, micros);
		}
		if months != 0 {
			if months < 0 {
				error!("a bucket width must be positive");
			}
			return Width::Months(months);
		}
		let total = days
			.checked_mul(USECS_PER_DAY)
			.and_then(|d| d.checked_add(micros))
			.unwrap_or_else(|| error!("a bucket width that long is out of range: a width in days and time must be under about 292,000 years"));
		if total <= 0 {
			error!("a bucket width must be positive");
		}
		Width::Micros(total)
	}

	/// Whether buckets of this width start at midnight (whole days, or months), which is
	/// what decides how a time zone is applied.
	fn is_calendar(self) -> bool {
		match self {
			Width::Months(_) => true,
			Width::Micros(us) => us % USECS_PER_DAY == 0,
		}
	}

	pub(crate) fn default_origin(self) -> i64 {
		match self {
			Width::Months(_) => MONTH_ORIGIN,
			Width::Micros(_) => MONDAY_ORIGIN,
		}
	}
}

/// The calendar, on microseconds since 2000-01-01. Pure, so it is tested without a server.
mod calendar {
	use super::{PG_EPOCH_DAYS, USECS_PER_DAY};

	pub fn floor_div(a: i64, b: i64) -> i64 {
		let q = a / b;
		if (a % b != 0) && ((a < 0) != (b < 0)) { q - 1 } else { q }
	}

	pub fn floor_mod(a: i64, b: i64) -> i64 {
		a - floor_div(a, b) * b
	}

	/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
	/// days_from_civil, public domain).
	pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
		let y = if m <= 2 { y - 1 } else { y };
		let era = floor_div(y, 400);
		let yoe = y - era * 400;
		let mp = (m + 9) % 12;
		let doy = (153 * mp + 2) / 5 + d - 1;
		let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
		era * 146_097 + doe - 719_468
	}

	/// The inverse: (year, month 1-12, day 1-31) of days since 1970-01-01.
	pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
		let z = z + 719_468;
		let era = floor_div(z, 146_097);
		let doe = z - era * 146_097;
		let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
		let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
		let mp = (5 * doy + 2) / 153;
		let d = doy - (153 * mp + 2) / 5 + 1;
		let m = if mp < 10 { mp + 3 } else { mp - 9 };
		(if m <= 2 { yoe + era * 400 + 1 } else { yoe + era * 400 }, m, d)
	}

	pub fn days_in_month(y: i64, m: i64) -> i64 {
		match m {
			2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
			2 => 28,
			4 | 6 | 9 | 11 => 30,
			_ => 31,
		}
	}

	/// (year, month, day, microseconds into the day) of a time.
	pub fn split(t: i64) -> (i64, i64, i64, i64) {
		let days = floor_div(t, USECS_PER_DAY);
		let (y, m, d) = civil_from_days(days + PG_EPOCH_DAYS);
		(y, m, d, t - days * USECS_PER_DAY)
	}

	/// `origin + months`, with the day clamped to the length of the month it lands in, as
	/// Postgres's own `timestamp + interval` does.
	pub fn add_months(origin: i64, months: i64) -> i64 {
		let (y, m, d, tod) = split(origin);
		let total = y * 12 + (m - 1) + months;
		let (y, m) = (floor_div(total, 12), floor_mod(total, 12) + 1);
		let d = d.min(days_in_month(y, m));
		(days_from_civil(y, m, d) - PG_EPOCH_DAYS) * USECS_PER_DAY + tod
	}

	/// The start of the `width`-month bucket counted from `origin` that holds `t`.
	pub fn month_bucket(t: i64, width: i64, origin: i64) -> i64 {
		let (ty, tm, _, _) = split(t);
		let (oy, om, _, _) = split(origin);
		let mut k = floor_div((ty * 12 + tm) - (oy * 12 + om), width) * width;
		let mut start = add_months(origin, k);
		// Same month as `t` but a later day or time than it (or a clamped day): the bucket
		// is the one before. Only ever one step, but a loop says what it means.
		while start > t {
			k -= width;
			start = add_months(origin, k);
		}
		start
	}

	/// The start of the fixed-width bucket counted from `origin` that holds `t`, or None if
	/// the arithmetic leaves the range of an i64.
	pub fn fixed_bucket(t: i64, width: i64, origin: i64) -> Option<i64> {
		let since = (t as i128) - (origin as i128);
		let back = since.rem_euclid(width as i128);
		i64::try_from(t as i128 - back).ok()
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		const DAY: i64 = USECS_PER_DAY;

		fn at(y: i64, m: i64, d: i64) -> i64 {
			(days_from_civil(y, m, d) - PG_EPOCH_DAYS) * DAY
		}

		#[test]
		fn civil_round_trips_across_four_centuries() {
			for z in -200_000..200_000 {
				let (y, m, d) = civil_from_days(z);
				assert_eq!(days_from_civil(y, m, d), z);
			}
		}

		#[test]
		fn the_postgres_epoch_is_2000_01_01() {
			assert_eq!(split(0), (2000, 1, 1, 0));
			assert_eq!(split(-1), (1999, 12, 31, DAY - 1));
		}

		#[test]
		fn months_clamp_and_do_not_drift() {
			let jan31 = at(2024, 1, 31);
			assert_eq!(add_months(jan31, 1), at(2024, 2, 29));
			assert_eq!(add_months(jan31, 2), at(2024, 3, 31));
			assert_eq!(add_months(jan31, 13), at(2025, 2, 28));
			assert_eq!(add_months(jan31, -1), at(2023, 12, 31));
		}

		#[test]
		fn a_month_bucket_from_a_month_end_origin() {
			let origin = at(2000, 1, 31);
			// 2024-02-29 is the start of February's bucket; the 28th is still January's.
			assert_eq!(month_bucket(at(2024, 2, 29), 1, origin), at(2024, 2, 29));
			assert_eq!(month_bucket(at(2024, 2, 28), 1, origin), at(2024, 1, 31));
			assert_eq!(month_bucket(at(2024, 3, 30), 1, origin), at(2024, 2, 29));
			assert_eq!(month_bucket(at(2024, 3, 31), 1, origin), at(2024, 3, 31));
		}

		#[test]
		fn quarters_and_years() {
			assert_eq!(month_bucket(at(2026, 9, 22) + 5, 3, 0), at(2026, 7, 1));
			assert_eq!(month_bucket(at(2026, 9, 22), 12, 0), at(2026, 1, 1));
			assert_eq!(month_bucket(at(1999, 12, 31), 12, 0), at(1999, 1, 1));
			assert_eq!(month_bucket(at(1600, 2, 29), 12, 0), at(1600, 1, 1));
		}

		#[test]
		fn fixed_buckets_before_the_origin_round_down() {
			assert_eq!(fixed_bucket(-1, DAY, 0), Some(-DAY));
			assert_eq!(fixed_bucket(0, DAY, 0), Some(0));
			assert_eq!(fixed_bucket(DAY - 1, DAY, 0), Some(0));
			assert_eq!(fixed_bucket(i64::MIN, DAY, 0), None);
		}
	}
}

/// A bucket start that is still a valid Postgres time, or an ERROR naming the width.
fn checked(result: Option<i64>) -> i64 {
	match result {
		// Postgres's valid range is well inside an i64; anything past it is an overflow.
		Some(t) if (MIN_TIMESTAMP..END_TIMESTAMP).contains(&t) => t,
		_ => error!("the bucket of this time is outside the range of a timestamp: use an origin nearer the time, or a smaller width"),
	}
}

/// The bucket of `t` in the time's own frame (UTC for a timestamptz, as written for a
/// timestamp).
pub(crate) fn plain(width: Width, t: i64, origin: i64) -> i64 {
	checked(match width {
		Width::Months(m) => Some(calendar::month_bucket(t, m, origin)),
		Width::Micros(us) => calendar::fixed_bucket(t, us, origin),
	})
}

fn zone_datum(zone: &str) -> Option<pg_sys::Datum> {
	zone.into_datum()
}

/// A timestamptz's local time in `zone`: `ts AT TIME ZONE zone`.
fn to_local(ts: i64, zone: &str) -> i64 {
	let local: Timestamp = unsafe {
		direct_function_call(pg_sys::timestamptz_zone, &[zone_datum(zone), ts.into_datum()])
	}
	.unwrap_or_else(|| error!("time zone conversion returned NULL"));
	local.into_inner()
}

/// The instant a local time in `zone` names: `local AT TIME ZONE zone`. Postgres's rules
/// decide what a local time that happens twice, or not at all, means.
fn from_local(local: i64, zone: &str) -> i64 {
	let ts: TimestampWithTimeZone = unsafe {
		direct_function_call(pg_sys::timestamp_zone, &[zone_datum(zone), local.into_datum()])
	}
	.unwrap_or_else(|| error!("time zone conversion returned NULL"));
	ts.into_inner()
}

/// The bucket of the instant `ts` in `zone`, with `origin` a local time.
///
/// Whole days and months start at local midnight: the local time is bucketed and the local
/// start converted back. That start can be ambiguous or missing on a DST change; if the
/// instant Postgres picks for it is AFTER `ts`, the start is read with `ts`'s own offset
/// instead, which cannot be (the local start is not after the local time).
///
/// Any other width: the LATEST instant at or before `ts` whose local clock reads a grid time
/// (origin + k * width, local). That is monotone (a later time can only have more grid
/// instants behind it), never after `ts`, and always on the local grid, so an hourly bucket
/// in the hour that happens twice is two buckets both labelled 01:00, and the hour that never
/// happens has none. Around a change a bucket is longer or shorter by the shift. The obvious
/// alternative, `ts` minus how far its local time is into its local bucket, is not monotone
/// for a width that does not line up with the change (90 minutes in New York: a later time
/// got an earlier bucket), which the regression test checks.
fn in_zone(width: Width, ts: i64, zone: &str, origin: i64) -> i64 {
	let local = to_local(ts, zone);
	if width.is_calendar() {
		let start = plain(width, local, origin);
		let at = from_local(start, zone);
		return if at <= ts { at } else { checked(start.checked_sub(local - ts)) };
	}
	let Width::Micros(us) = width else { unreachable!() };
	// The instants whose local clock reads a grid time, under a fixed offset `o`, are
	// `grid - o`. The answer is the latest of them that is not after `ts` and really HAS
	// offset `o`. It lies within two widths of `ts` (the latest grid instant before a change is
	// less than a width before the change), and a change in that window is between `ts - us`
	// and `ts`, so the offsets at those two instants are the only ones to try.
	let offset_at = |t: i64| to_local(t, zone) - t;
	let latest_with = |o: i64, steps: i64| -> Option<i64> {
		let top = calendar::fixed_bucket(ts.checked_add(o)?, us, origin)?.checked_sub(o)?;
		(0..steps)
			.filter_map(|k| top.checked_sub(k.checked_mul(us)?))
			.find(|&c| c <= ts && offset_at(c) == o)
	};
	let here = local - ts;
	// The usual case, one extra conversion: no change between the grid time and `ts`.
	if let Some(b) = latest_with(here, 1) {
		return b;
	}
	let before = offset_at(checked(ts.checked_sub(us)));
	[latest_with(here, 3), latest_with(before, 3)]
		.into_iter()
		.flatten()
		.max()
		.unwrap_or_else(|| error!("no bucket found for this time in time zone \"{zone}\""))
}

fn tstz(t: i64) -> TimestampWithTimeZone {
	TimestampWithTimeZone::try_from(t).unwrap_or_else(|_| error!("the bucket of this time is outside the range of a timestamp: use an origin nearer the time, or a smaller width"))
}

fn ts(t: i64) -> Timestamp {
	Timestamp::try_from(t).unwrap_or_else(|_| error!("the bucket of this time is outside the range of a timestamp: use an origin nearer the time, or a smaller width"))
}

// ---- timestamptz ----

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_tstz(width: Interval, ts: TimestampWithTimeZone) -> TimestampWithTimeZone {
	if !ts.is_finite() {
		return ts;
	}
	let w = Width::of(&width);
	tstz(plain(w, ts.into_inner(), w.default_origin()))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_tstz_origin(
	width: Interval,
	ts: TimestampWithTimeZone,
	origin: TimestampWithTimeZone,
) -> TimestampWithTimeZone {
	if !ts.is_finite() {
		return ts;
	}
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	tstz(plain(Width::of(&width), ts.into_inner(), origin.into_inner()))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_tstz_zone(width: Interval, ts: TimestampWithTimeZone, timezone: &str) -> TimestampWithTimeZone {
	if !ts.is_finite() {
		return ts;
	}
	let w = Width::of(&width);
	tstz(in_zone(w, ts.into_inner(), timezone, w.default_origin()))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_tstz_zone_origin(
	width: Interval,
	ts: TimestampWithTimeZone,
	timezone: &str,
	origin: Timestamp,
) -> TimestampWithTimeZone {
	if !ts.is_finite() {
		return ts;
	}
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	tstz(in_zone(Width::of(&width), ts.into_inner(), timezone, origin.into_inner()))
}

// ---- timestamp (no zone: bucketed as written) ----

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_ts(width: Interval, ts: Timestamp) -> Timestamp {
	if !ts.is_finite() {
		return ts;
	}
	let w = Width::of(&width);
	self::ts(plain(w, ts.into_inner(), w.default_origin()))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_ts_origin(width: Interval, ts: Timestamp, origin: Timestamp) -> Timestamp {
	if !ts.is_finite() {
		return ts;
	}
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	self::ts(plain(Width::of(&width), ts.into_inner(), origin.into_inner()))
}

// ---- date (whole days or months only) ----

fn date_width(width: &Interval) -> Width {
	let w = Width::of(width);
	if !w.is_calendar() {
		error!("a bucket width for a date must be whole days or months");
	}
	w
}

fn date_of(t: i64) -> Date {
	Date::try_from(calendar::floor_div(t, USECS_PER_DAY) as i32)
		.unwrap_or_else(|_| error!("the bucket of this date is outside the range of a date: use an origin nearer the date, or a smaller width"))
}

fn micros_of(d: Date) -> i64 {
	d.to_pg_epoch_days() as i64 * USECS_PER_DAY
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_date(width: Interval, day: Date) -> Date {
	if !day.is_finite() {
		return day;
	}
	let w = date_width(&width);
	date_of(plain(w, micros_of(day), w.default_origin()))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_date_origin(width: Interval, day: Date, origin: Date) -> Date {
	if !day.is_finite() {
		return day;
	}
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	date_of(plain(date_width(&width), micros_of(day), micros_of(origin)))
}

// ---- integers (a width and an offset in the column's own units) ----

fn int_bucket(width: i64, value: i64, offset: i64) -> i64 {
	if width <= 0 {
		error!("a bucket width must be positive");
	}
	calendar::fixed_bucket(value, width, offset).unwrap_or_else(|| error!("the bucket of this value is outside the range of its type: use an offset nearer the value, or a smaller width"))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_int8(width: i64, value: i64) -> i64 {
	int_bucket(width, value, 0)
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_int8_offset(width: i64, value: i64, offset: i64) -> i64 {
	int_bucket(width, value, offset)
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_int4(width: i32, value: i32) -> i32 {
	i32::try_from(int_bucket(width as i64, value as i64, 0)).unwrap_or_else(|_| error!("the bucket of this value is outside the range of an integer: use a bigint, or a smaller width"))
}

#[pg_extern(immutable, parallel_safe, name = "bucket")]
fn bucket_int4_offset(width: i32, value: i32, offset: i32) -> i32 {
	i32::try_from(int_bucket(width as i64, value as i64, offset as i64))
		.unwrap_or_else(|_| error!("the bucket of this value is outside the range of an integer: use a bigint, or an offset nearer the value"))
}

// ---- gapfill: every bucket between two times ----

/// The bucket after `b`, with `f` the bucket function. Jump ahead by about a bucket, then
/// walk back until the bucket before the one landed in is `b`: that finds the IMMEDIATE
/// successor for any width, zone and origin, including beside a DST change where two
/// buckets can be closer together than one width (the hour that happens twice).
fn successor(f: &dyn Fn(i64) -> i64, b: i64, jump: i64) -> i64 {
	let mut x = checked(b.checked_add(jump));
	let mut n = f(x);
	while n <= b {
		x = checked(x.checked_add(jump));
		n = f(x);
	}
	loop {
		let before = f(n - 1);
		if before <= b {
			return n;
		}
		n = before;
	}
}

fn jump_of(width: Width) -> i64 {
	match width {
		// More than one bucket's shortest length and less than two: 31 days a month.
		Width::Months(m) => m.saturating_mul(31 * USECS_PER_DAY),
		Width::Micros(us) => us,
	}
}

fn buckets_between(
	start: TimestampWithTimeZone,
	finish: TimestampWithTimeZone,
	width: Width,
	f: Box<dyn Fn(i64) -> i64>,
) -> SetOfIterator<'static, TimestampWithTimeZone> {
	if !start.is_finite() || !finish.is_finite() {
		error!("gapfill needs a finite start and finish");
	}
	let finish = finish.into_inner();
	let jump = jump_of(width);
	let mut next = Some(f(start.into_inner()));
	SetOfIterator::new(std::iter::from_fn(move || {
		let b = next.filter(|&b| b < finish)?;
		next = Some(successor(&*f, b, jump));
		Some(tstz(b))
	}))
}

/// Every bucket that overlaps [start, finish), in order: the first is the bucket `start` is
/// in (which can begin before it), and each one after is the next bucket, however long, so a
/// LEFT JOIN onto it gives a row for every bucket whether or not any data fell in it.
#[pg_extern(immutable, parallel_safe, name = "gapfill")]
fn gapfill_tstz(
	width: Interval,
	start: TimestampWithTimeZone,
	finish: TimestampWithTimeZone,
) -> SetOfIterator<'static, TimestampWithTimeZone> {
	let w = Width::of(&width);
	let origin = w.default_origin();
	buckets_between(start, finish, w, Box::new(move |t| plain(w, t, origin)))
}

#[pg_extern(immutable, parallel_safe, name = "gapfill")]
fn gapfill_tstz_origin(
	width: Interval,
	start: TimestampWithTimeZone,
	finish: TimestampWithTimeZone,
	origin: TimestampWithTimeZone,
) -> SetOfIterator<'static, TimestampWithTimeZone> {
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	let w = Width::of(&width);
	let origin = origin.into_inner();
	buckets_between(start, finish, w, Box::new(move |t| plain(w, t, origin)))
}

#[pg_extern(immutable, parallel_safe, name = "gapfill")]
fn gapfill_tstz_zone(
	width: Interval,
	start: TimestampWithTimeZone,
	finish: TimestampWithTimeZone,
	timezone: &str,
) -> SetOfIterator<'static, TimestampWithTimeZone> {
	let w = Width::of(&width);
	let (zone, origin) = (timezone.to_string(), w.default_origin());
	buckets_between(start, finish, w, Box::new(move |t| in_zone(w, t, &zone, origin)))
}

#[pg_extern(immutable, parallel_safe, name = "gapfill")]
fn gapfill_tstz_zone_origin(
	width: Interval,
	start: TimestampWithTimeZone,
	finish: TimestampWithTimeZone,
	timezone: &str,
	origin: Timestamp,
) -> SetOfIterator<'static, TimestampWithTimeZone> {
	if !origin.is_finite() {
		error!("a bucket origin must be finite");
	}
	let w = Width::of(&width);
	let (zone, origin) = (timezone.to_string(), origin.into_inner());
	buckets_between(start, finish, w, Box::new(move |t| in_zone(w, t, &zone, origin)))
}
