//! Date manipulation functions
/*

Copyright (c) 2020 John Goerzen

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use chrono::{offset::TimeZone, Date, NaiveDate, Utc, Datelike};
use julianday::JulianDay;

/// Convert a day to a [`JulianDay`]
#[allow(dead_code)]
pub fn day_to_jd(day: i32) -> JulianDay {
   JulianDay::new(day)
}

/// Convert a day to a [`NaiveDate`].
#[allow(dead_code)]
pub fn day_to_nd(day: i32) -> NaiveDate {
    jd_to_nd(&day_to_jd(day))
}

/// Convert a day to a [`Date<Utc>`].
#[allow(dead_code)]
pub fn day_to_dateutc(day: i32) -> Date<Utc> {
    jd_to_dateutc(&day_to_jd(day))
}

/** Convert a Julian day to a (year, month, day) tuple */
#[allow(dead_code)]
pub fn day_to_ymd(day: i32) -> (i32, u32, u32) {
    nd_to_ymd(&day_to_nd(day))
}

/// Convert a [`JulianDay`] to a i32-based day
#[allow(dead_code)]
pub fn jd_to_day(jd: &JulianDay) -> i32 {
    jd.clone().inner()
}

/// Convert a [`JulianDay`] to a [`NaiveDate`]
#[allow(dead_code)]
pub fn jd_to_nd(jd: &JulianDay) -> NaiveDate {
    jd.clone().to_date()
}

/// Convert a [`JulianDay`] to a UTC-based [`Date`]
#[allow(dead_code)]
pub fn jd_to_dateutc(jd: &JulianDay) -> Date<Utc> {
    nd_to_dateutc(&jd_to_nd(jd))
}

/// Convert a [`JulianDay`] to a (year, month, day) tuple
#[allow(dead_code)]
pub fn jd_to_ymd(jd: &JulianDay) -> (i32, u32, u32) {
    nd_to_ymd(&jd_to_nd(jd))
}

/** Convert a [`NaiveDate`] to a `JulianDay` */
#[allow(dead_code)]
pub fn nd_to_jd(nd: &NaiveDate) -> JulianDay {
    JulianDay::from(*nd)
}
/// Convert a [`NaiveDate`] to a UTC-based [`Date`]
#[allow(dead_code)]
pub fn nd_to_dateutc(nd: &NaiveDate) -> Date<Utc> {
    Utc.from_utc_date(nd)
}

/** Convert a [`NaiveDate`] to a i32-based Julian day */
#[allow(dead_code)]
pub fn nd_to_day(nd: &NaiveDate) -> i32 {
    jd_to_day(&nd_to_jd(nd))
}

/** Convert a [`NaiveDate`] to a (year, month, day) tuple */
#[allow(dead_code)]
pub fn nd_to_ymd(nd: &NaiveDate) -> (i32, u32, u32) {
    (nd.year(), nd.month(), nd.day())
}

/// Convert a year, month, day to a [`NaiveDate`]
#[allow(dead_code)]
pub fn ymd_to_nd(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd(year, month, day)
}

/// Convert a year, month, day to an i32-based day
#[allow(dead_code)]
pub fn ymd_to_day(year: i32, month: u32, day: u32) -> i32 {
    nd_to_day(&ymd_to_nd(year, month, day))
}

/** Convert a year, month, day to a [`JulianDay`] */
#[allow(dead_code)]
pub fn ymd_to_jd(year: i32, month: u32, day: u32) -> JulianDay {
    nd_to_jd(&ymd_to_nd(year, month, day))
}

/** Convert a year, month, day to a [`Date<Utc>`] */
#[allow(dead_code)]
pub fn ymd_to_dateutc(year: i32, month: u32, day: u32) -> Date<Utc> {
    nd_to_dateutc(&ymd_to_nd(year, month, day))
}

/** Convert a `Date<Utc>` to a NaiveDate */
#[allow(dead_code)]
pub fn dateutc_to_nd(dateutc: &Date<Utc>) -> NaiveDate {
    dateutc.naive_utc()
}

/** Convert a `Date<Utc>` to an i32-based day */
#[allow(dead_code)]
pub fn dateutc_to_day(dateutc: &Date<Utc>) -> i32 {
    nd_to_day(&dateutc_to_nd(dateutc))
}

/** Convert a `Date<Utc>` to a `JulianDay` */
#[allow(dead_code)]
pub fn dateutc_to_jd(dateutc: &Date<Utc>) -> JulianDay {
    nd_to_jd(&dateutc_to_nd(dateutc))
}

/** Convert a `Date<Utc>` to (year, month, day) */
#[allow(dead_code)]
pub fn dateutc_to_ymd(dateutc: &Date<Utc>) -> (i32, u32, u32) {
    nd_to_ymd(&dateutc_to_nd(dateutc))
}
