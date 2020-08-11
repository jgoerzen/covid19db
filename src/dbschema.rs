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
use std::convert::TryFrom;

/** Initialize a database.  This will drop all indices and tables related to
this project, then re-create them, thus emptying them and readying them to
receive data. */
pub async fn initdb<E: Executor>(db: &mut E) -> () {
    let statements = vec![
        "drop index if exists cdataset_uniq_idx",
        "drop table if exists cdataset",
        "drop index if exists loc_lookup_fips",
        "drop table if exists loc_lookup",
        "drop table if exists covid19schema",
        "create table covid19schema (version integer not null, minorversion integer not null)",
        "insert into covid19schema values (1, 0)",
        // From Johns Hopkins UID_ISO_FIPS_LookUp_Table.csv
        // https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv
        "create table loc_lookup (
         uid integer not null primary key,
         iso2 text not null,
         iso3 text not null,
         code3 integer,
         fips integer,
         admin2 text,
         province_state text,
         country_region text not null,
         latitude real,
         longitude real,
         combined_key text not null,
         population integer)",
        "create index loc_lookup_fips on loc_lookup (fips)",
        // From https://github.com/cipriancraciun/covid19-datasets/blob/master/exports/combined/v1/values-sqlite.db.gz
        "create table cdataset (
        dataset text not null,
        data_key text not null,
        location_key text not null,
        location_type text not null,
        location_label text not null,
        country_code text,
        country text,
        province text,
        administrative text,
        region text,
        subregion text,
        location_lat real,
        location_long real,
        us_county_fips integer,
        date text not null,
        date_julian integer not null,
        date_year integer not null,
        date_month integer not null,
        date_day integer not null,
        day_index_0 integer not null,
        day_index_1 integer not null,
        day_index_10 integer,
        day_index_100 integer,
        day_index_1k integer,
        day_index_10k integer,
        day_index_peak integer,
        day_index_peak_confirmed integer,
        day_index_peak_deaths integer,
        absolute_confirmed integer not null,
        absolute_deaths integer not null,
        absolute_recovered integer not null,
        absolute_infected integer not null,
        absolute_pop100k_confirmed real,
        absolute_pop100k_deaths real,
        absolute_pop100k_recovered real,
        absolute_pop100k_infected real,
        relative_deaths real,
        relative_recovered real,
        relative_infected real,
        delta_confirmed integer not null,
        delta_deaths integer not null,
        delta_recovered integer not null,
        delta_infected integer not null,
        delta_pct_confirmed real,
        delta_pct_deaths real,
        delta_pct_recovered real,
        delta_pct_infected real,
        delta_pop100k_confirmed real,
        delta_pop100k_deaths real,
        delta_pop100k_recovered real,
        delta_pop100k_infected real,
        peak_pct_confirmed real,
        peak_pct_deaths real,
        peak_pct_recovered real,
        peak_pct_infected real,
        factbook_area real,
        factbook_population integer,
        factbook_death_rate real,
        factbook_median_age real,
        primary key (data_key))",
        "CREATE UNIQUE INDEX cdataset_uniq_idx ON cdataset (dataset, location_key, date_julian)",
    ];

    for statement in statements {
        db.execute(statement)
            .await
            .expect("Error executing statement");
    }
}

/** The `CDataSet` struct represents a row in the `cdataset` table.  It is an instance
 * of `sqlx::FromRow` for the benefit of users of `sqlx::query_as`. */
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
    pub date_julian: i64,
    pub date_year: i64,
    pub date_month: i64,
    pub date_day: i64,
    pub day_index_0: i64,
    pub day_index_1: i64,
    pub day_index_10: Option<i64>,
    pub day_index_100: Option<i64>,
    pub day_index_1k: Option<i64>,
    pub day_index_10k: Option<i64>,
    pub day_index_peak: Option<i64>,
    pub day_index_peak_confirmed: Option<i64>,
    pub day_index_peak_deaths: Option<i64>,
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
    pub fn set_date(&mut self, julian: i64) {
        let jd = JulianDay::new(i32::try_from(julian).unwrap());
        let nd = jd.to_date();
        self.date_julian = i64::from(julian);
        self.date = format!("{}", nd.format("%Y-%m-%d"));
        self.date_year = i64::from(nd.year());
        self.date_month = i64::from(nd.month());
        self.date_day = i64::from(nd.day());
        self.data_key = format!("{}-{}", self.data_key, julian);
    }

    /// Sets all date fields in the struct to the appropriate representation of
    /// the given `JulianDay`.
    pub fn set_date_julianday(&mut self, jd: &JulianDay) {
        self.set_date(i64::from(jd.clone().inner()));
    }

    /// Sets all date fields in the struct to the appropriate representation of
    /// the given `NaiveDate` from the `chrono` package.
    pub fn set_date_naivedate(&mut self, nd: &NaiveDate) {
        self.set_date_julianday(&JulianDay::from(*nd));
    }
}
