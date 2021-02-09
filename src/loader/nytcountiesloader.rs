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

pub use crate::dateutil::*;
pub use crate::dbschema::*;
pub use crate::loader::parseutil::*;
use chrono::NaiveDate;
use csv;
use serde::Deserialize;
use sqlx::{Query, Transaction};

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct NYTCountyRecord {
    pub date: String,
    pub county: String,
    pub state: String,
    pub fips: i64,
    pub cases: i64,
    pub deaths: i64,
}

impl NYTCountyRecord {
    pub fn bind_query<'q>(self, query: Query<'q, sqlx::Sqlite>) -> Query<'q, sqlx::Sqlite> {
        query
            .bind(nd_to_day(
                &NaiveDate::parse_from_str(self.date.as_str(), "%Y-%m-%d").unwrap(),
            ))
            .bind(self.county)
            .bind(self.state)
            .bind(self.fips)
            .bind(self.cases)
            .bind(self.deaths)
    }

    pub fn insert_str() -> &'static str {
        "INSERT INTO nytcounties_raw VALUES (?, ?, ?, ?, ?, ?)"
    }
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = NYTCountyRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

/** Parse the CSV, loading it into the database.
 * Will panic on parse error.  */
pub async fn load<'a, A: std::io::Read>(
    rdr: &'a mut csv::Reader<A>,
    mut transaction: Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
) {
    assert_eq!(
        vec![
            "date",
            "county",
            "state",
            "fips",
            "cases",
            "deaths",
        ],
        rdr.headers().unwrap().iter().collect::<Vec<&str>>()
    );
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    for rec in finaliter {
        let query = sqlx::query(NYTCountyRecord::insert_str());
        rec.bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}
