/* Parser - JHU records

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
use csv;
use serde::Deserialize;
use sqlx::Transaction;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct LocRecord {
    pub uid: u32,
    pub iso2: String,
    pub iso3: String,
    pub code3: Option<u32>,
    pub fips: Option<u32>,
    pub admin2: String,
    pub province_state: String,
    pub country_region: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub combined_key: String,
    pub population: Option<u64>,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = LocRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

/** Parse the CSV, loading it into the database, and returning a hashmap of fips to population.
 * Will panic on parse error.  */
pub async fn load<'a, A: std::io::Read>(
    rdr: &'a mut csv::Reader<A>,
    mut transaction: Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
) -> HashMap<u32, u64> {
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    let mut hm = HashMap::new();
    for rec in finaliter {
        match (rec.fips, rec.population) {
            (Some(fipsi), Some(popi)) => {
                hm.insert(fipsi, popi);
            }
            _ => (),
        }
        let query =
            sqlx::query("INSERT INTO loc_lookup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        query
            .bind(i64::from(rec.uid))
            .bind(rec.iso2)
            .bind(rec.iso3)
            .bind(rec.code3.map(i64::from))
            .bind(rec.fips.map(i64::from))
            .bind(if rec.admin2.len() == 0 {
                None
            } else {
                Some(rec.admin2)
            })
            .bind(if rec.province_state.len() == 0 {
                None
            } else {
                Some(rec.province_state)
            })
            .bind(rec.country_region)
            .bind(rec.lat)
            .bind(rec.lon)
            .bind(rec.combined_key)
            .bind(
                rec.population
                    .map(|x| i64::try_from(x).expect("population range")),
            )
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
    hm
}
