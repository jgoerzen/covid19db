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
pub struct HarveyCountyRecord {
    pub date: String,
    pub kdhe_neg_results: Option<i64>,
    pub kdhe_pos_results: Option<i64>,
    pub harveyco_tot_results: Option<i64>,
    pub harveyco_pos_results: Option<i64>,
    pub harveyco_confirmed: Option<i64>,
    pub harveyco_recovered: Option<i64>,
}

impl HarveyCountyRecord {
    pub fn bind_query<'q>(self, query: Query<'q, sqlx::Sqlite>) -> Query<'q, sqlx::Sqlite> {
        query
            .bind(nd_to_day(
                &NaiveDate::parse_from_str(self.date.as_str(), "%Y-%m-%d").unwrap(),
            ))
            .bind(self.kdhe_neg_results)
            .bind(self.kdhe_pos_results)
            .bind(self.harveyco_tot_results)
            .bind(self.harveyco_pos_results)
            .bind(self.harveyco_confirmed)
            .bind(self.harveyco_recovered)
    }

    pub fn insert_str() -> &'static str {
        "INSERT INTO harveycodata_raw VALUES (?, ?, ?, ?, ?, ?, ?)"
    }
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = HarveyCountyRecord> {
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
            "kdhe_neg_results",
            "kdhe_pos_results",
            "harveyco_tot_results",
            "harveyco_pos_results",
            "harveyco_confirmed",
            "harveyco_recovered",
        ],
        rdr.headers().unwrap().iter().collect::<Vec<&str>>()
    );
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    for rec in finaliter {
        let query = sqlx::query(HarveyCountyRecord::insert_str());
        rec.bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}
