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

use crate::dateutil::*;
use chrono::NaiveDate;
use julianday::JulianDay;
use sqlx::Query;

/** The `OWID` struct represents a row in the `rtlive` table.  It is an instance
of `sqlx::FromRow` for the benefit of users of `sqlx::query_as`. */
#[derive(PartialEq, Clone, Debug, sqlx::FromRow)]
pub struct OWID {
    pub iso_code: Option<String>,
pub continent: Option<String>,
pub location: String,
pub date_julian: i32,
pub total_cases: Option<f64>,
pub new_cases: Option<f64>,
    pub total_deaths: Option<f64>,
    pub new_deaths: Option<f64>,
pub total_cases_per_million: Option<f64>,
pub new_cases_per_million: Option<f64>,
pub total_deaths_per_million: Option<f64>,
pub new_deaths_per_million: Option<f64>,
pub total_tests: Option<f64>,
pub new_tests: Option<f64>,
pub new_tests_smoothed: Option<f64>,
pub total_tests_per_thousand: Option<f64>,
pub new_tests_per_thousand: Option<f64>,
pub new_tests_smoothed_per_thousand: Option<f64>,
pub tests_per_case: Option<f64>,
pub positive_rate: Option<f64>,
pub tests_units: Option<String>,
pub stringency_index: Option<f64>,
pub population: Option<f64>,
pub population_density: Option<f64>,
pub median_age: Option<f64>,
pub aged_65_older: Option<f64>,
pub aged_70_older: Option<f64>,
pub gdp_per_capita: Option<f64>,
pub extreme_poverty: Option<f64>,
pub cardiovasc_death_rate: Option<f64>,
pub diabetes_prevalence: Option<f64>,
pub female_smokers: Option<f64>,
pub male_smokers: Option<f64>,
pub handwashing_facilities: Option<f64>,
pub hospital_beds_per_thousand: Option<f64>,
pub life_expectancy: Option<f64>,
}

impl OWID {
    /// Bind all the parameters to a query, perhaps as generated by [`insert_str`].
    pub fn bind_query<'q>(self, query: Query<'q, sqlx::Sqlite>) -> Query<'q, sqlx::Sqlite> {
        // from schema
        // sed -e 's/ *\([^ ]*\).*/.bind(self.\1)/'
        query
            .bind(self.iso_code)
.bind(self.continent)
.bind(self.location)
.bind(self.date_julian)
.bind(self.total_cases)
.bind(self.new_cases)
.bind(self.total_deaths)
            .bind(self.new_deaths)
.bind(self.total_cases_per_million)
.bind(self.new_cases_per_million)
.bind(self.total_deaths_per_million)
.bind(self.new_deaths_per_million)
.bind(self.total_tests)
.bind(self.new_tests)
.bind(self.new_tests_smoothed)
.bind(self.total_tests_per_thousand)
.bind(self.new_tests_per_thousand)
.bind(self.new_tests_smoothed_per_thousand)
.bind(self.tests_per_case)
.bind(self.positive_rate)
.bind(self.tests_units)
.bind(self.stringency_index)
.bind(self.population)
.bind(self.population_density)
.bind(self.median_age)
.bind(self.aged_65_older)
.bind(self.aged_70_older)
.bind(self.gdp_per_capita)
.bind(self.extreme_poverty)
.bind(self.cardiovasc_death_rate)
.bind(self.diabetes_prevalence)
.bind(self.female_smokers)
.bind(self.male_smokers)
.bind(self.handwashing_facilities)
.bind(self.hospital_beds_per_thousand)
.bind(self.life_expectancy)

    }

    /// Gets an INSERT INTO string representing all the values in the table.
    pub fn insert_str() -> &'static str {
        "INSERT INTO owid_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    }

    /// Sets all date fields in the struct to appropriate representations of the
    /// given Julian date.
    pub fn set_date(&mut self, julian: i32) {
        self.date_julian = julian;
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
