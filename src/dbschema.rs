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

mod cdataset;
mod covidtracking;
mod owid;
mod rtlive;

pub use crate::dbschema::{cdataset::*, covidtracking::*, owid::*, rtlive::*};

/** Initialize a database.  This will drop all indices and tables related to
this project, then re-create them, thus emptying them and readying them to
receive data. */
pub async fn initdb<E: Executor>(db: &mut E) -> () {
    let statements = vec![
        "drop index if exists cdataset_raw_uniq_idx",
        "drop table if exists cdataset_raw",
        "drop table if exists cdataset_loc",
        "drop index if exists harveycotests_raw_uniq_idx",
        "drop view if exists harveycotests",
        "drop table if exists harveycotests_raw",
        "drop view if exists cdataset",
        "drop index if exists loc_lookup_fips",
        "drop table if exists loc_lookup",
        "drop table if exists covid19db_meta",
        "drop view if exists rtlive",
        "drop index if exists rtlive_raw_uniq_idx",
        "drop table if exists rtlive_raw",
        "drop index if exists covidtracking_uniq_idx",
        "drop view if exists covidtracking",
        "drop view if exists covidtracking_us",
        "drop table if exists covidtracking_raw",
        "drop view if exists owid",
        "drop index if exists owid_raw_uniq_idx",
        "drop table if exists owid_raw",
        "create table covid19db_meta (field text not null, value text not null)",
        "insert into covid19db_meta values ('schemaver', '2')",
        //
        // From Johns Hopkins UID_ISO_FIPS_LookUp_Table.csv
        // https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv
        //
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
        //
        // rt.live data
        // https://d14wlfuexuxgcm.cloudfront.net/covid/rt.csv
        //
        "create table rtlive_raw(
         date_julian integer not null,
         state text not null,
         rtindex integer not null,
         mean real not null,
         median real not null,
         lower_80 real not null,
         upper_80 real not null,
         infections real not null,
         test_adjusted_positive real not null,
         test_adjusted_positive_raw real not null,
         positive integer not null,
         tests integer not null,
         new_tests integer,
         new_cases integer,
         new_deaths integer)",
        "create index rtlive_raw_uniq_idx on rtlive_raw (state, date_julian)",
        // Harvey County data
        "create table harveycotests_raw(
         date_julian integer not null primary key,
         kdhe_neg_results,
         kdhe_pos_results,
         harveyco_neg_results,
         harveyco_pos_results)",
        //
        // From https://covidtracking.com/api/v1/states/daily.csv
        //
        "create table covidtracking_raw (
         date_julian integer not null,
         state text not null,
         positive integer,
         negative integer,
         pending integer,
         hospitalizedCurrently integer,
         hospitalizedCumulative integer,
         incluCurrently integer,
         incluCumulative integer,
         onVentilatorCurrently integer,
         onVentilatorCumulative integer,
         recovered integer,
         dataQualityGrade text,
         lastUpdateEt text,
         dateModified text,
         checkTimeEt text,
         death integer,
         hospitalized integer,
         dateChecked text,
         totalTestsViral integer,
         positiveTestsViral integer,
         negativeTestsViral integer,
         positiveCasesViral integer,
         deathConfirmed integer,
         deathProbable integer,
         totalTestEncountersViral integer,
         totalTestsPeopleViral integer,
         totalTestsAntibody integer,
         positiveTestsAntibody integer,
         negativeTestsAntibody integer,
         totalTestsPeopleAntibody integer,
         positiveTestsPeopleAntibody integer,
         negativeTestsPeopleAntibody integer,
         totalTestsPeopleAntigen integer,
         positiveTestsPeopleAntigen integer,
         totalTestsAntigen integer,
         positiveTestsAntigen integer,
         fips integer not null,
         positiveIncrease integer,
         negativeIncrease integer,
         total integer,
         totalTestResults integer,
         totalTestResultsIncrease integer,
         posNeg integer,
         deathIncrease integer,
         hospitalizedIncrease integer,
         commercialScore integer,
         negativeRegularScore integer,
         negativeScore integer,
         positiveScore integer,
         score integer,
         grade text
         )",
        "create unique index covidtracking_raw_uniq_idx on covidtracking_raw (date_julian, state)",
        //
        // From covid19-datasets
        //
        "create table cdataset_loc (
         locid integer not null primary key,
         xtype text not null,
         label text not null,
         country_code text not null,
         country_normalized text not null,
         province_normalized text not null,
         administrative_normalized text not null,
         region text not null,
         subregion text not null,
         us_state_code text,
         us_state_name text,
         us_county_fips integer
         )",
        //
        // From https://github.com/cipriancraciun/covid19-datasets/blob/master/exports/combined/v1/values-sqlite.db.gz
        //
        "create table cdataset_raw (
        dataset text not null,
        locid integer not null,
        location_lat real,
        location_long real,
        date_julian integer not null,
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
        factbook_median_age real
        )",
        "CREATE UNIQUE INDEX cdataset_raw_uniq_idx ON cdataset_raw (dataset, locid, date_julian)",
        //
        // Our World In Data set
        //
        "CREATE TABLE owid_raw (
         iso_code text,
         continent text,
         location text not null,
         date_julian integer not null,
         total_cases real,
         new_cases real,
         new_cases_smoothed real,
         total_deaths real,
         new_deaths real,
         new_deaths_smoothed real,
        total_cases_per_million real,
        new_cases_per_million real,
         new_cases_smoothed_per_million real,
        total_deaths_per_million real,
        new_deaths_per_million real,
         new_deaths_smoothed_per_million real,
        total_tests real,
        new_tests real,
        new_tests_smoothed real,
        total_tests_per_thousand real,
        new_tests_per_thousand real,
        new_tests_smoothed_per_thousand real,
        tests_per_case real,
        positive_rate real,
        tests_units text,
        stringency_index real,
        population real,
        population_density real,
        median_age real,
        aged_65_older real,
        aged_70_older real,
        gdp_per_capita real,
        extreme_poverty real,
        cardiovasc_death_rate real,
        diabetes_prevalence real,
        female_smokers real,
        male_smokers real,
        handwashing_facilities real,
        hospital_beds_per_thousand real,
        life_expectancy real
)",
        "CREATE UNIQUE INDEX owid_raw_uniq_idx ON owid_raw (date_julian, iso_code)",
    ];

    let views = vec![
        format!("CREATE VIEW cdataset AS select {} AS date, {} as date_year, {} as date_month, {} as date_day,
                 cdataset_loc.xtype AS location_type,
                 cdataset_loc.label AS location_label,
                 cdataset_loc.country_code AS country_code,
                 cdataset_loc.country_normalized AS country,
                 cdataset_loc.province_normalized AS province,
                 cdataset_loc.administrative_normalized AS administrative,
                 cdataset_loc.region AS region,
                 cdataset_loc.subregion AS subregion,
                 cdataset_loc.us_county_fips AS us_county_fips,
                 cdataset_raw.* FROM cdataset_raw, cdataset_loc WHERE cdataset_raw.locid = cdataset_loc.locid",
                querystr_jd_to_datestr("cdataset_raw.date_julian"),
                querystr_jd_to_year("cdataset_raw.date_julian"),
                querystr_jd_to_month("cdataset_raw.date_julian"),
                querystr_jd_to_day("cdataset_raw.date_julian"),
        ),
        format!("CREATE VIEW harveycotests AS select {} as date, {} as date_year, {} as date_month, {} as date_day,
                 34429 as population, kdhe_neg_results + kdhe_pos_results AS kdhe_tot_results,
                 harveyco_neg_results + harveyco_pos_results AS harveyco_tot_results,
                 harveycotests_raw.* FROM harveycotests_raw",
                querystr_jd_to_datestr("harveycotests_raw.date_julian"),
                querystr_jd_to_year("harveycotests_raw.date_julian"),
                querystr_jd_to_month("harveycotests_raw.date_julian"),
                querystr_jd_to_day("harveycotests_raw.date_julian"),
        ),
        format!("CREATE VIEW rtlive AS select {} as date, {} as date_year, {} as date_month, {} as date_day,
                 rtlive_raw.* FROM rtlive_raw",
                querystr_jd_to_datestr("rtlive_raw.date_julian"),
                querystr_jd_to_year("rtlive_raw.date_julian"),
                querystr_jd_to_month("rtlive_raw.date_julian"),
                querystr_jd_to_day("rtlive_raw.date_julian"),
        ),
        format!("CREATE VIEW covidtracking AS select {} as date, {} as date_year, {} as date_month, {} as date_day,
                 covidtracking_raw.* from covidtracking_raw",
                querystr_jd_to_datestr("covidtracking_raw.date_julian"),
                querystr_jd_to_year("covidtracking_raw.date_julian"),
                querystr_jd_to_month("covidtracking_raw.date_julian"),
                querystr_jd_to_day("covidtracking_raw.date_julian"),
        ),
        String::from("create view covidtracking_us as select date, date_julian, date_year, date_month, date_day,
        sum(positive) as positive, sum(negative) as negative, sum(pending) as pending,
        sum(hospitalizedCurrently) as hospitalizedCurrently, sum(hospitalizedCumulative) as hospitalizedCumulative,
        sum(incluCurrently) as inclueCurrently, sum(incluCumulative) as incluCumulative,
        sum(onVentilatorCurrently) as onVentilatorCurrently, sum(onVentilatorCumulative) as onVentilatorCumulative,
        sum(recovered) as recovered, sum(death) as death, sum(hospitalized) as hospitalized,
        sum(totalTestsViral) as totalTestsViral, sum(positiveTestsViral) as positiveTestsViral,
        sum(negativeTestsViral) as negativeTestsViral, sum(positiveCasesViral) as positiveCasesViral,
        sum(deathConfirmed) as deathConfirmed, sum(deathProbable) as deathProbable,
        sum(totalTestEncountersViral) as totalTestEncountersViral,
        sum(totalTestsPeopleViral) as totalTestsPeopleViral,
        sum(totalTestsAntibody) as totalTestsAntibody,
        sum(positiveTestsAntibody) as positiveTestsAntibody,
        sum(negativeTestsAntibody) as negativeTestsAntibody,
        sum(totalTestsPeopleAntibody) as totalTestsPeopleAntibody,
        sum(positiveTestsPeopleAntibody) as positiveTestsPeopleAntibody,
        sum(negativeTestsPeopleAntibody) as negativeTestsPeopleAntibody,
        sum(totalTestsPeopleAntigen) as totalTestsPeopleAntigen,
        sum(positiveTestsPeopleAntigen) as positiveTestsPeopleAntigen,
        sum(totalTestsAntigen) as totalTestsAntigen,
        sum(positiveTestsAntigen) as positiveTestsAntigen,
        sum(positiveIncrease) as positiveIncrease, sum(negativeIncrease) as negativeIncrease,
        sum(total) as total, sum(totalTestResults) as totalTestResults,
        sum(totalTestResultsIncrease) as totalTestResultsIncrease, sum(posNeg) as posNeg,
        sum(deathIncrease) as deathIncrease, sum(hospitalizedIncrease) as hospitalizedIncrease from covidtracking group by(date)"),
        format!("CREATE VIEW owid AS select {} as date, {} as date_year, {} as date_month, {} as date_day,
                 total_cases_per_million / 10.0 AS total_cases_per_100k,
                 new_cases_per_million / 10.0 AS new_cases_per_100k,
                 total_deaths_per_million / 10.0 AS total_deaths_per_100k,
                 new_deaths_per_million / 10.0 AS new_deaths_per_100k,
                 total_tests_per_thousand * 100.0 AS total_tests_per_100k,
                 new_tests_per_thousand * 100.0 AS new_tests_per_100k,
                 new_tests_smoothed_per_thousand * 100.0 AS new_tests_smoothed_per_100k, owid_raw.* FROM owid_raw",
                querystr_jd_to_datestr("owid_raw.date_julian"),
                querystr_jd_to_year("owid_raw.date_julian"),
                querystr_jd_to_month("owid_raw.date_julian"),
                querystr_jd_to_day("owid_raw.date_julian"),
        ),
    ];

    let mut queries: Vec<String> = statements.into_iter().map(String::from).collect();
    queries.extend(views);

    for query in queries {
        println!("PREP: executing {}", query);
        db.execute(query.as_str())
            .await
            .expect("Error executing statement");
    }
}

/// Returns a SQLite query string converting the Julian date to a date string for the given column
pub fn querystr_jd_to_datestr(col: &str) -> String {
    format!("DATE({})", col)
}

/// Returns a SQLite query string converting the Julian date to a year
pub fn querystr_jd_to_year(col: &str) -> String {
    format!("strftime('%Y', {})", col)
}

/// Returns a SQLite query string converting the Julian date to a month
pub fn querystr_jd_to_month(col: &str) -> String {
    format!("strftime('%m', {})", col)
}

/// Returns a SQLite query string converting the Julian date to a day
pub fn querystr_jd_to_day(col: &str) -> String {
    format!("strftime('%d', {})", col)
}
