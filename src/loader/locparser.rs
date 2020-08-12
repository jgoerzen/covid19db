/* Parser - covidtracking data

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
use std::error::Error;
use std::fs::File;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct LocRecord {
    pub key: String,
    pub key_original: String,
    pub xtype: String,
    pub label: String,
    pub country_code: String,
    pub country_different: String,
    pub country_normalized: String,
    pub country_original: String,
    pub province_different: String,
    pub province_normalized: String,
    pub province_original: String,
    pub administrative_different: String,
    pub administrative_normalized: String,
    pub administrative_original: String,
    pub region: String,
    pub subregion: String,
    pub us_state_code: String,
    pub us_state_name: String,
    pub us_county_fips: Option<u32>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct LocRec {
    pub fips: Option<u32>,
    pub population: Option<u64>,
    pub locid: u32,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = LocRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

pub fn parse_init_file(file: File) -> Result<csv::Reader<File>, Box<dyn Error>> {
    let rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .double_quote(false)
        .flexible(true)
        .from_reader(file);
    Ok(rdr)
}

/* Will panic on parse error.  */
pub async fn parse<'a, A: std::io::Read>(
    mut transaction: Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
    fipshm: &HashMap<u32, u64>,
    rdr: &'a mut csv::Reader<A>,
) -> HashMap<String, LocRec> {
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    let mut hm = HashMap::new();
    let mut counter = 1;
    for rec in finaliter {
        let fips = rec.us_county_fips;
        hm.insert(
            rec.key,
            LocRec {
                locid: counter,
                fips,
                population: fips.and_then(|f| fipshm.get(&f).map(|x| *x)),
            },
        );
        let query =
            sqlx::query("INSERT INTO cdataset_loc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        query
            .bind(i64::from(counter))
            .bind(rec.xtype)
            .bind(rec.label)
            .bind(rec.country_code)
            .bind(rec.country_normalized)
            .bind(rec.province_normalized)
            .bind(rec.administrative_normalized)
            .bind(rec.region)
            .bind(rec.subregion)
            .bind(rec.us_state_code)
            .bind(rec.us_state_name)
            .bind(rec.us_county_fips.map(i64::from))
            .execute(&mut transaction)
            .await
            .unwrap();

        counter += 1;
    }
    transaction.commit().await.unwrap();
    hm
}
