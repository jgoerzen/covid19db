/* Database schema

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

use chrono::{Datelike, NaiveDate};
use julianday::JulianDay;
use sqlx::prelude::*;
use sqlx::Query;
use std::convert::TryInto;
use crate::dateutil::*;

/** The `CDataSet` struct represents a row in the `cdataset` table.  It is an instance
of `sqlx::FromRow` for the benefit of users of `sqlx::query_as`. */
#[derive(PartialEq, Clone, Debug, sqlx::FromRow)]
pub struct CDataSet {
    // from the schema
    // sed -e 's/ *\([^ ]*\)/pub \1:/' -e 's/integer not null/i64/' -e "s/text not null/String/" -e "s/text,/Option<String>,/" -e 's/real,/Option<f64>,/' -e 's/integer,/Option<i64>,/'
    //
    pub dataset: String,
    pub data_key: String,
    pub location_key: String,
    pub location_type: String,
    pub location_label: String,
    pub country_code: Option<String>,
    pub country: Option<String>,
    pub province: Option<String>,
    pub administrative: Option<String>,
    pub region: Option<String>,
    pub subregion: Option<String>,
    pub location_lat: Option<f64>,
    pub location_long: Option<f64>,
    pub us_county_fips: Option<i64>,
    pub date: String,
    pub date_julian: i32,
    pub date_year: i32,
    pub date_month: i32,
    pub date_day: i32,
    pub day_index_0: i32,
    pub day_index_1: i32,
    pub day_index_10: Option<i32>,
    pub day_index_100: Option<i32>,
    pub day_index_1k: Option<i32>,
    pub day_index_10k: Option<i32>,
    pub day_index_peak: Option<i32>,
    pub day_index_peak_confirmed: Option<i32>,
    pub day_index_peak_deaths: Option<i32>,
    pub absolute_confirmed: i64,
    pub absolute_deaths: i64,
    pub absolute_recovered: i64,
    pub absolute_infected: i64,
    pub absolute_pop100k_confirmed: Option<f64>,
    pub absolute_pop100k_deaths: Option<f64>,
    pub absolute_pop100k_recovered: Option<f64>,
    pub absolute_pop100k_infected: Option<f64>,
    pub relative_deaths: Option<f64>,
    pub relative_recovered: Option<f64>,
    pub relative_infected: Option<f64>,
    pub delta_confirmed: i64,
    pub delta_deaths: i64,
    pub delta_recovered: i64,
    pub delta_infected: i64,
    pub delta_pct_confirmed: Option<f64>,
    pub delta_pct_deaths: Option<f64>,
    pub delta_pct_recovered: Option<f64>,
    pub delta_pct_infected: Option<f64>,
    pub delta_pop100k_confirmed: Option<f64>,
    pub delta_pop100k_deaths: Option<f64>,
    pub delta_pop100k_recovered: Option<f64>,
    pub delta_pop100k_infected: Option<f64>,
    pub peak_pct_confirmed: Option<f64>,
    pub peak_pct_deaths: Option<f64>,
    pub peak_pct_recovered: Option<f64>,
    pub peak_pct_infected: Option<f64>,
    pub factbook_area: Option<f64>,
    pub factbook_population: Option<i64>,
    pub factbook_death_rate: Option<f64>,
    pub factbook_median_age: Option<f64>,
}

impl CDataSet {
    /// Load from a row from a `select(*)`.  Probably want to use the `FromRow` derivation instead, really.
    #[allow(dead_code)]
    pub fn from_row<'c>(row: &sqlx::sqlite::SqliteRow<'c>) -> Self {
        // From the schema
        CDataSet {
            dataset: row.get("dataset"),
            data_key: row.get("data_key"),
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
            us_county_fips: row.get("us_county_fips"),
            date: row.get("date"),
            date_julian: row.get("date_julian"),
            date_year: row.get("date_year"),
            date_month: row.get("date_month"),
            date_day: row.get("date_day"),
            day_index_0: row.get("day_index_0"),
            day_index_1: row.get("day_index_1"),
            day_index_10: row.get("day_index_10"),
            day_index_100: row.get("day_index_100"),
            day_index_1k: row.get("day_index_1k"),
            day_index_10k: row.get("day_index_10k"),
            day_index_peak: row.get("day_index_peak"),
            day_index_peak_confirmed: row.get("day_index_peak_confirmed"),
            day_index_peak_deaths: row.get("day_index_peak_deaths"),
            absolute_confirmed: row.get("absolute_confirmed"),
            absolute_deaths: row.get("absolute_deaths"),
            absolute_recovered: row.get("absolute_recovered"),
            absolute_infected: row.get("absolute_infected"),
            absolute_pop100k_confirmed: row.get("absolute_pop100k_confirmed"),
            absolute_pop100k_deaths: row.get("absolute_pop100k_deaths"),
            absolute_pop100k_recovered: row.get("absolute_pop100k_recovered"),
            absolute_pop100k_infected: row.get("absolute_pop100k_infected"),
            relative_deaths: row.get("relative_deaths"),
            relative_recovered: row.get("relative_recovered"),
            relative_infected: row.get("relative_infected"),
            delta_confirmed: row.get("delta_confirmed"),
            delta_deaths: row.get("delta_deaths"),
            delta_recovered: row.get("delta_recovered"),
            delta_infected: row.get("delta_infected"),
            delta_pct_confirmed: row.get("delta_pct_confirmed"),
            delta_pct_deaths: row.get("delta_pct_deaths"),
            delta_pct_recovered: row.get("delta_pct_recovered"),
            delta_pct_infected: row.get("delta_pct_infected"),
            delta_pop100k_confirmed: row.get("delta_pop100k_confirmed"),
            delta_pop100k_deaths: row.get("delta_pop100k_deaths"),
            delta_pop100k_recovered: row.get("delta_pop100k_recovered"),
            delta_pop100k_infected: row.get("delta_pop100k_infected"),
            peak_pct_confirmed: row.get("peak_pct_confirmed"),
            peak_pct_deaths: row.get("peak_pct_deaths"),
            peak_pct_recovered: row.get("peak_pct_recovered"),
            peak_pct_infected: row.get("peak_pct_infected"),
            factbook_area: row.get("factbook_area"),
            factbook_population: row.get("factbook_population"),
            factbook_death_rate: row.get("factbook_death_rate"),
            factbook_median_age: row.get("factbook_median_age"),
        }
    }

    /// Bind all the parameters to a query, perhaps as generated by [`insert_str`].
    pub fn bind_query<'q>(self, query: Query<'q, sqlx::Sqlite>) -> Query<'q, sqlx::Sqlite> {
        // from schema
        // sed -e 's/ *\([^ ]*\).*/.bind(self.\1)/'
        query
            .bind(self.dataset)
            .bind(self.data_key)
            .bind(self.location_key)
            .bind(self.location_type)
            .bind(self.location_label)
            .bind(self.country_code)
            .bind(self.country)
            .bind(self.province)
            .bind(self.administrative)
            .bind(self.region)
            .bind(self.subregion)
            .bind(self.location_lat)
            .bind(self.location_long)
            .bind(self.us_county_fips)
            .bind(self.date)
            .bind(self.date_julian)
            .bind(self.date_year)
            .bind(self.date_month)
            .bind(self.date_day)
            .bind(self.day_index_0)
            .bind(self.day_index_1)
            .bind(self.day_index_10)
            .bind(self.day_index_100)
            .bind(self.day_index_1k)
            .bind(self.day_index_10k)
            .bind(self.day_index_peak)
            .bind(self.day_index_peak_confirmed)
            .bind(self.day_index_peak_deaths)
            .bind(self.absolute_confirmed)
            .bind(self.absolute_deaths)
            .bind(self.absolute_recovered)
            .bind(self.absolute_infected)
            .bind(self.absolute_pop100k_confirmed)
            .bind(self.absolute_pop100k_deaths)
            .bind(self.absolute_pop100k_recovered)
            .bind(self.absolute_pop100k_infected)
            .bind(self.relative_deaths)
            .bind(self.relative_recovered)
            .bind(self.relative_infected)
            .bind(self.delta_confirmed)
            .bind(self.delta_deaths)
            .bind(self.delta_recovered)
            .bind(self.delta_infected)
            .bind(self.delta_pct_confirmed)
            .bind(self.delta_pct_deaths)
            .bind(self.delta_pct_recovered)
            .bind(self.delta_pct_infected)
            .bind(self.delta_pop100k_confirmed)
            .bind(self.delta_pop100k_deaths)
            .bind(self.delta_pop100k_recovered)
            .bind(self.delta_pop100k_infected)
            .bind(self.peak_pct_confirmed)
            .bind(self.peak_pct_deaths)
            .bind(self.peak_pct_recovered)
            .bind(self.peak_pct_infected)
            .bind(self.factbook_area)
            .bind(self.factbook_population)
            .bind(self.factbook_death_rate)
            .bind(self.factbook_median_age)
    }

    /// Gets an INSERT INTO string representing all the values in the table.
    pub fn insert_str() -> &'static str {
        "INSERT INTO cdataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    }

    /// Zeroes out the delta parameters so that this can reflect a duplicate day
    pub fn dup_day(self) -> Self {
        CDataSet {
            delta_confirmed: 0,
            delta_deaths: 0,
            delta_recovered: 0,
            delta_infected: 0,
            delta_pct_confirmed: None,
            delta_pct_deaths: None,
            delta_pct_recovered: None,
            delta_pct_infected: None,
            delta_pop100k_confirmed: None,
            delta_pop100k_deaths: None,
            delta_pop100k_recovered: None,
            delta_pop100k_infected: None,
            ..self
        }
    }

    /// Sets all date fields in the struct to appropriate representations of the
    /// given Julian date.
    pub fn set_date(&mut self, julian: i32) {
        let jd = JulianDay::new(julian);
        let nd = jd.to_date();
        self.date_julian = julian;
        self.date = format!("{}", nd.format("%Y-%m-%d"));
        self.date_year = nd.year();
        self.date_month = nd.month().try_into().unwrap();
        self.date_day = nd.day().try_into().unwrap();
        self.data_key = format!("{}-{}", self.data_key, julian);
    }

    #[allow(dead_code)]
    /// Sets all date fields in the struct to the appropriate representation of
    /// the given `JulianDay`.
    pub fn set_date_julianday(&mut self, jd: &JulianDay) {
        self.set_date(jd_to_day(jd));
    }

    #[allow(dead_code)]
    /// Sets all date fields in the struct to the appropriate representation of
    /// the given `NaiveDate` from the `chrono` package.
    pub fn set_date_naivedate(&mut self, nd: &NaiveDate) {
        self.set_date(nd_to_day(nd));
    }
}
