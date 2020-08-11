/* Parser

Copyright (c) 2019-2020 John Goerzen

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

pub use crate::loader::parseutil::*;
pub use crate::dbschema::*;
pub use crate::dateutil::*;
use csv;
use serde::Deserialize;
use sqlx::Transaction;
use chrono::NaiveDate;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct RTLiveRecord {
    pub date: String,
pub state: String,
pub rtindex: i64,
pub mean: f64,
pub median: f64,
pub lower_80: f64,
pub upper_80: f64,
pub infections: f64,
pub test_adjusted_positive: f64,
pub test_adjusted_positive_raw: f64,
    pub positive: f64,
pub tests: f64,
pub new_tests: Option<f64>,
pub new_cases: Option<f64>,
pub new_deaths: Option<f64>,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = RTLiveRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

/** Parse the CSV, loading it into the database.
 * Will panic on parse error.  */
pub async fn load<'a, A: std::io::Read>(
    rdr: &'a mut csv::Reader<A>,
    mut transaction: Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
) {
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    for rec in finaliter {
        let nd = NaiveDate::parse_from_str(rec.date.as_str(), "%Y-%m-%d").unwrap();
        let (y, m, d) = nd_to_ymd(&nd);
        let dbrec = RTLive {
            date: rec.date,
            date_julian: nd_to_day(&nd),
            date_year: y,
            date_month: m,
            date_day: d,
            state: rec.state,
            rtindex: rec.rtindex,
            mean: rec.mean,
            median: rec.median,
            lower_80: rec.lower_80,
            upper_80: rec.upper_80,
            infections: rec.infections,
            test_adjusted_positive: rec.test_adjusted_positive,
            test_adjusted_positive_raw: rec.test_adjusted_positive_raw,
            positive: rec.positive.round() as i64,
            tests: rec.tests.round() as i64,
            new_tests: rec.new_tests.map(|x| x.round() as i64),
            new_cases: rec.new_cases.map(|x| x.round() as i64),
            new_deaths: rec.new_deaths.map(|x| x.round() as i64),

        };
        let query =
            sqlx::query(RTLive::insert_str());
        dbrec.bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}
