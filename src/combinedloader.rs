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
use std::mem::drop;
use crate::locparser::LocRec;
use chrono::NaiveDate;
use julianday::JulianDay;

fn set_per_pop(row: &sqlx::sqlite::SqliteRow, colname: &str, population: Option<i64>) -> Option<f64> {
    match row.get::<Option<f64>, &str>(format!("absolute_pop100k_{}", colname).as_str()) {
        Some(x) => Some(x),
        None => match population {
            None => None,
            Some(pop) => {
                Some((row.get::<i64, &str>(format!("absolute_{}", colname).as_str()) as f64)
                     * 100000.0 / (pop as f64))
            }
        }
    }
}

/** Parse the CSV, loading it into the database, and returning a hashmap of fips to population.
 * Will panic on parse error.  */
pub async fn load(
    inputpool: &mut sqlx::SqlitePool,
    outputpool: &mut sqlx::SqlitePool,
    lochm: &HashMap<String, LocRec>,
fipshm: &HashMap<u32, u64>) {

    // Speed things up a bit.
    let mut conn = outputpool.acquire().await.unwrap();
    conn.execute("PRAGMA auto_vacuum = 0").await.unwrap();
    conn.execute("PRAGMA synchronous = 0").await.unwrap();
    drop(conn);

    let mut transaction = outputpool.begin().await.unwrap();

    let mut iconn = inputpool.acquire().await.unwrap();
    let mut cursor = sqlx::query("SELECT * from dataset ORDER BY dataset, location_key, date")
        .fetch(&mut iconn);

    let mut lastrow = None;

    while let Some(row) = cursor.next().await.unwrap() {
        let locrec = lochm.get(&row.get::<String, &str>("location_key"));
        let fips = if let Some(loc) = locrec {
            Some(loc.fips)
        } else {
            None
        };

        let julian = JulianDay::from(NaiveDate::from_ymd(row.get("date_year"),
                                                         u32::try_from(row.get::<i32, &str>("date_month")).unwrap(),
                                                         u32::try_from(row.get::<i32, &str>("date_day")).unwrap())).inner();

        let population: Option<i64> = match row.get("factbook_population") {
            Some(pop) => Some(pop),
            None => fips.and_then(|x| fipshm.get(&x).and_then(|y| Some(i64::try_from(*y).unwrap())))
        };


       
        let query = sqlx::query("INSERT INTO cdataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        query.bind(row.get::<String, &str>("dataset"))
             .bind(row.get::<String, &str>("data_key"))
             .bind(row.get::<String, &str>("location_key"))
             .bind(row.get::<String, &str>("location_type"))
             .bind(row.get::<String, &str>("location_label"))
             .bind(row.get::<Option<String>, &str>("country_code"))
             .bind(row.get::<Option<String>, &str>("country"))
             .bind(row.get::<Option<String>, &str>("province"))
             .bind(row.get::<Option<String>, &str>("administrative"))
             .bind(row.get::<Option<String>, &str>("region"))
             .bind(row.get::<Option<String>, &str>("subregion"))
             .bind(row.get::<Option<f64>, &str>("location_lat"))
             .bind(row.get::<Option<f64>, &str>("location_long"))
             .bind(fips.map(i64::from))
             .bind(row.get::<String, &str>("date"))
             .bind(julian)
             .bind(row.get::<i64, &str>("date_year"))
             .bind(row.get::<i64, &str>("date_month"))
             .bind(row.get::<i64, &str>("date_day"))
             .bind(row.get::<i64, &str>("day_index_0"))
             .bind(row.get::<i64, &str>("day_index_1"))
             .bind(row.get::<Option<i64>, &str>("day_index_10"))
             .bind(row.get::<Option<i64>, &str>("day_index_100"))
             .bind(row.get::<Option<i64>, &str>("day_index_1k"))
             .bind(row.get::<Option<i64>, &str>("day_index_10k"))
             .bind(row.get::<Option<i64>, &str>("day_index_peak"))
             .bind(row.get::<Option<i64>, &str>("day_index_peak_confirmed"))
             .bind(row.get::<Option<i64>, &str>("day_index_peak_deaths"))
             .bind(row.get::<Option<i64>, &str>("absolute_confirmed").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("absolute_deaths").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("absolute_recovered").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("absolute_infected").unwrap_or(0))
             .bind(set_per_pop(&row, "confirmed", population))
             .bind(set_per_pop(&row, "deaths", population))
             .bind(set_per_pop(&row, "recovered", population))
             .bind(set_per_pop(&row, "infected", population))
             .bind(row.get::<i64, &str>("relative_deaths"))
             .bind(row.get::<i64, &str>("relative_recovered"))
             .bind(row.get::<i64, &str>("relative_infected"))
             .bind(row.get::<Option<i64>, &str>("delta_confirmed").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("delta_deaths").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("delta_recovered").unwrap_or(0))
             .bind(row.get::<Option<i64>, &str>("delta_infected").unwrap_or(0))
             .bind(row.get::<i64, &str>("delta_pct_confirmed"))
             .bind(row.get::<i64, &str>("delta_pct_deaths"))
             .bind(row.get::<i64, &str>("delta_pct_recovered"))
             .bind(row.get::<i64, &str>("delta_pct_infected"))
             .bind(row.get::<i64, &str>("delta_pop100k_confirmed"))
             .bind(row.get::<i64, &str>("delta_pop100k_deaths"))
             .bind(row.get::<i64, &str>("delta_pop100k_recovered"))
             .bind(row.get::<i64, &str>("delta_pop100k_infected"))
             .bind(row.get::<i64, &str>("peak_pct_confirmed"))
             .bind(row.get::<i64, &str>("peak_pct_deaths"))
             .bind(row.get::<i64, &str>("peak_pct_recovered"))
             .bind(row.get::<i64, &str>("peak_pct_infected"))
             .bind(row.get::<i64, &str>("factbook_area"))
             .bind(population)
             .bind(row.get::<i64, &str>("factbook_death_rate"))
             .bind(row.get::<i64, &str>("factbook_median_age"))
            .execute(&mut transaction).await.unwrap();
        lastrow = Some(row);
    }
    transaction.commit().await.unwrap();
}
