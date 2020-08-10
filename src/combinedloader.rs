/* Combined loader

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

pub use crate::parseutil::*;
use std::collections::HashMap;
use sqlx::prelude::*;
use sqlx::Transaction;
use std::convert::TryFrom;
use mem::drop;

/** Parse the CSV, loading it into the database, and returning a hashmap of fips to population.
 * Will panic on parse error.  */
pub async fn load(
    inputpool: &mut Pool,
    outputpool: &mut Pool) {

    // Speed things up a bit.
    let conn = outputpool.acquire().await.unwrap();
    conn.execute("PRAGMA auto_vacuum = 0").await.unwrap();
    conn.execute("PRAGMA synchronous = 0").await.unwrap();
    drop(conn);

    let transaction = outputpool.begin().await.unwrap();


    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    let mut hm = HashMap::new();
    for rec in finaliter {
        match (rec.fips, rec.population) {
            (Some(fipsi), Some(popi)) => {
                hm.insert(fipsi, popi);
            },
            _ => ()
        }
        let query = sqlx::query("INSERT INTO loc_lookup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        query.bind(i64::from(rec.uid))
             .bind(rec.iso2)
             .bind(rec.iso3)
             .bind(rec.code3.map(i64::from))
             .bind(rec.fips.map(i64::from))
             .bind(if rec.admin2.len() == 0 { None  } else {Some(rec.admin2)})
             .bind(if rec.province_state.len() == 0 { None } else {Some(rec.province_state)})
             .bind(rec.country_region)
             .bind(rec.lat)
             .bind(rec.lon)
             .bind(rec.combined_key)
             .bind(rec.population.map(|x| i64::try_from(x).expect("population range")))
             .execute(&mut transaction).await.unwrap();
    }
    transaction.commit().await.unwrap();
    hm

}
