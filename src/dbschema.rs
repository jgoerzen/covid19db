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

use sqlx::prelude::*;

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
        us_county_fips text,
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
        "CREATE UNIQUE INDEX cdataset_uniq_idx ON cdataset (dataset, location_key, date_julian)"
            ];

    for statement in statements {
        db.execute(statement).await.expect("Error executing statement");
    }
}
