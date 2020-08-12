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

use crate::dateutil::*;
use crate::dbschema::*;
use crate::loader::locparser::LocRec;
pub use crate::loader::parseutil::*;
use chrono::NaiveDate;
use sqlx::prelude::*;
use sqlx::Transaction;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::io;
use std::io::Write;
use std::mem::drop;

/// Calculates the absolute rate per 100k population in case it's not there
fn set_per_pop(
    row: &sqlx::sqlite::SqliteRow,
    colprefix: &str,
    colname: &str,
    population: Option<i64>,
) -> Option<f64> {
    match row.get::<Option<f64>, &str>(format!("{}_pop100k_{}", colprefix, colname).as_str()) {
        Some(x) => Some(x),
        None => match population {
            None => None,
            Some(pop) => Some(
                (row.get::<i64, &str>(format!("{}_{}", colprefix, colname).as_str()) as f64)
                    * 100000.0
                    / (pop as f64),
            ),
        },
    }
}

/// Fill up rows in which no changes occurred.
async fn fillup(
    mut transaction: &mut Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
    lastrow: &Option<CDataSet>,
    nextrow: Option<&CDataSet>,
    prevdate: &NaiveDate,
    maxdate: &NaiveDate,
) {
    // If there was no last row, nothing to do.
    if let Some(lastrow) = lastrow {
        let targetmaxdate = if let Some(nextrow) = nextrow {
            if lastrow.dataset == nextrow.dataset && lastrow.location_key == nextrow.location_key {
                // If the previous row was for the same location as this one, use the date
                // immediately prior to this row's.
                prevdate
            } else {
                // Different location - fill the previous row up to the maxdate.
                maxdate
            }
        } else {
            // when run at the very end, there will be no nextrow, so it'll just be maxdate then.
            maxdate
        };

        // The +1 because we want to start AFTER the last row.
        let mut thisjulian = lastrow.date_julian + 1;
        let maxjulian = nd_to_day(targetmaxdate);
        let mut add_days: i32 = 1;
        while thisjulian <= maxjulian {
            let query = sqlx::query(CDataSet::insert_str());
            let mut cds = lastrow.clone().dup_day();
            cds.set_date(thisjulian);
            cds.day_index_0 += add_days;
            cds.day_index_1 += add_days;
            cds.day_index_10 = cds.day_index_10.map(|x| x + add_days);
            cds.day_index_100 = cds.day_index_100.map(|x| x + add_days);
            cds.day_index_1k = cds.day_index_1k.map(|x| x + add_days);
            cds.day_index_10k = cds.day_index_10k.map(|x| x + add_days);
            cds.day_index_peak = cds.day_index_peak.map(|x| x + add_days);
            cds.day_index_peak_confirmed = cds.day_index_peak_confirmed.map(|x| x + add_days);
            cds.day_index_peak_deaths = cds.day_index_peak_deaths.map(|x| x + add_days);

            /*
            println!("fillup lastrow = {}, prevdate = {} {}, maxdate = {} {}, targetmaxdate = {}, maxjulian = {}: adding {}",
                     lastrow.date_julian,
                     prevdate, JulianDay::from(*prevdate).inner(), maxdate, JulianDay::from(*maxdate).inner(), targetmaxdate,
                     maxjulian, cds.date_julian);
            println!("{:?}", cds);
            */
            cds.bind_query(query)
                .execute(&mut transaction)
                .await
                .unwrap();
            thisjulian += 1;
            add_days += 1;
        }
    }
}

/** Parse the CSV, loading it into the database, and returning a hashmap of fips to population.
 * Will panic on parse error.  */
pub async fn load(
    inputpool: &mut sqlx::SqlitePool,
    outputpool: &mut sqlx::SqlitePool,
    lochm: &HashMap<String, LocRec>,
    fipshm: &HashMap<u32, u64>,
) {
    // Speed things up a bit.
    let mut conn = outputpool.acquire().await.unwrap();
    conn.execute("PRAGMA auto_vacuum = 0").await.unwrap();
    conn.execute("PRAGMA synchronous = 0").await.unwrap();
    drop(conn);

    let mut transaction = outputpool.begin().await.unwrap();

    let mut iconn = inputpool.acquire().await.unwrap();
    let totalrecs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dataset")
        .fetch_one(&mut iconn)
        .await
        .unwrap();
    let mut processedrecs: i64 = 0;

    let maxdate_str: (String,) = sqlx::query_as("SELECT MAX(date) FROM dataset")
        .fetch_one(&mut iconn)
        .await
        .unwrap();
    let maxdate = NaiveDate::parse_from_str(maxdate_str.0.as_str(), "%Y-%m-%d").unwrap();

    let mut cursor =
        sqlx::query("SELECT * from dataset ORDER BY dataset, location_key, date").fetch(&mut iconn);

    let mut lastrow = None;

    while let Some(row) = cursor.next().await.unwrap() {
        let locrec = lochm.get(&row.get::<String, &str>("location_key"));
        let fips = if let Some(loc) = locrec {
            Some(loc.fips)
        } else {
            None
        };

        let nd = NaiveDate::from_ymd(
            row.get("date_year"),
            u32::try_from(row.get::<i32, &str>("date_month")).unwrap(),
            u32::try_from(row.get::<i32, &str>("date_day")).unwrap(),
        );
        let julian = nd_to_day(&nd);

        let population: Option<i64> = match row.get("factbook_population") {
            Some(pop) => Some(pop),
            None => fips.and_then(|x| {
                fipshm
                    .get(&x)
                    .and_then(|y| Some(i64::try_from(*y).unwrap()))
            }),
        };

        let query = sqlx::query(CDataSet::insert_str());
        let cds = CDataSet {
            dataset: row.get("dataset"),
            location_key: row.get("location_key"),
            location_type: row.get("location_type"),
            location_label: row.get("location_label"),
            country_code: row.get("country_code"),
            country: row.get("country"),
            province: row.get("province"),
            administrative: row.get("administrative"),
            region: row.get("region"),
            subregion: row.get("subregion"),
            location_lat: row.get("location_lat"),
            location_long: row.get("location_long"),
            us_county_fips: fips.map(i64::from),
            date: row.get("date"),
            date_julian: julian,
            date_year: row.get("date_year"),
            date_month: row.get::<i32, &str>("date_month").try_into().unwrap(),
            date_day: row.get::<i32, &str>("date_day").try_into().unwrap(),
            day_index_0: row.get("day_index_0"),
            day_index_1: row.get("day_index_1"),
            day_index_10: row.get("day_index_10"),
            day_index_100: row.get("day_index_100"),
            day_index_1k: row.get("day_index_1k"),
            day_index_10k: row.get("day_index_10k"),
            day_index_peak: row.get("day_index_peak"),
            day_index_peak_confirmed: row.get("day_index_peak_confirmed"),
            day_index_peak_deaths: row.get("day_index_peak_deaths"),
            absolute_confirmed: row
                .get::<Option<i64>, &str>("absolute_confirmed")
                .unwrap_or(0),
            absolute_deaths: row.get::<Option<i64>, &str>("absolute_deaths").unwrap_or(0),
            absolute_recovered: row
                .get::<Option<i64>, &str>("absolute_recovered")
                .unwrap_or(0),
            absolute_infected: row
                .get::<Option<i64>, &str>("absolute_infected")
                .unwrap_or(0),
            absolute_pop100k_confirmed: set_per_pop(&row, "absolute", "confirmed", population),
            absolute_pop100k_deaths: set_per_pop(&row, "absolute", "deaths", population),
            absolute_pop100k_recovered: set_per_pop(&row, "absolute", "recovered", population),
            absolute_pop100k_infected: set_per_pop(&row, "absolute", "infected", population),
            relative_deaths: row.get("relative_deaths"),
            relative_recovered: row.get("relative_recovered"),
            relative_infected: row.get("relative_infected"),
            delta_confirmed: row.get::<Option<i64>, &str>("delta_confirmed").unwrap_or(0),
            delta_deaths: row.get::<Option<i64>, &str>("delta_deaths").unwrap_or(0),
            delta_recovered: row.get::<Option<i64>, &str>("delta_recovered").unwrap_or(0),
            delta_infected: row.get::<Option<i64>, &str>("delta_infected").unwrap_or(0),
            delta_pct_confirmed: row.get("delta_pct_confirmed"),
            delta_pct_deaths: row.get("delta_pct_deaths"),
            delta_pct_recovered: row.get("delta_pct_recovered"),
            delta_pct_infected: row.get("delta_pct_infected"),
            delta_pop100k_confirmed: set_per_pop(&row, "delta", "confirmed", population),
            delta_pop100k_deaths: set_per_pop(&row, "delta", "deaths", population),
            delta_pop100k_recovered: set_per_pop(&row, "delta", "recovered", population),
            delta_pop100k_infected: set_per_pop(&row, "delta", "infected", population),
            peak_pct_confirmed: row.get("peak_pct_confirmed"),
            peak_pct_deaths: row.get("peak_pct_deaths"),
            peak_pct_recovered: row.get("peak_pct_recovered"),
            peak_pct_infected: row.get("peak_pct_infected"),
            factbook_area: row.get("factbook_area"),
            factbook_population: population,
            factbook_death_rate: row.get("factbook_death_rate"),
            factbook_median_age: row.get("factbook_median_age"),
        };

        fillup(&mut transaction, &lastrow, Some(&cds), &nd.pred(), &maxdate).await;
        // println!("Adding {}", cds.date_julian);
        // println!("{:?}", cds);
        cds.clone()
            .bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
        lastrow = Some(cds);
        processedrecs += 1;
        if processedrecs % 10000 == 0 {
            print!(
                "Processed {} of {} input records\r",
                processedrecs, totalrecs.0
            );
            io::stdout().flush().unwrap();
        }
    }
    fillup(&mut transaction, &lastrow, None, &maxdate, &maxdate).await;
    println!(
        "Processed {} of {} input records",
        processedrecs, totalrecs.0
    );
    io::stdout().flush().unwrap();
    println!("Committing...");
    transaction.commit().await.unwrap();
}
