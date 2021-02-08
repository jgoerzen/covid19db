/* OWID loader

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

pub use crate::dateutil::*;
pub use crate::dbschema::*;
pub use crate::loader::parseutil::*;
use chrono::NaiveDate;
use csv;
use serde::Deserialize;
use sqlx::Transaction;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct OWIDRecord {
    pub iso_code: Option<String>,
    pub continent: Option<String>,
    pub location: String,
    pub date: String,
    pub total_cases: Option<f64>,
    pub new_cases: Option<f64>,
    pub new_cases_smoothed: Option<f64>,
    pub total_deaths: Option<f64>,
    pub new_deaths: Option<f64>,
    pub new_deaths_smoothed: Option<f64>,
    pub total_cases_per_million: Option<f64>,
    pub new_cases_per_million: Option<f64>,
    pub new_cases_smoothed_per_million: Option<f64>,
    pub total_deaths_per_million: Option<f64>,
    pub new_deaths_per_million: Option<f64>,
    pub new_deaths_smoothed_per_million: Option<f64>,
    pub reproduction_rate: Option<f64>,
    pub icu_patients: Option<f64>,
    pub icu_patients_per_million: Option<f64>,
    pub hosp_patients: Option<f64>,
    pub hosp_patients_per_million: Option<f64>,
    pub weekly_icu_admissions: Option<f64>,
    pub weekly_icu_admissions_per_million: Option<f64>,
    pub weekly_hosp_admissions: Option<f64>,
    pub weekly_hosp_admissions_per_million: Option<f64>,
    pub new_tests: Option<f64>,
    pub total_tests: Option<f64>,
    pub total_tests_per_thousand: Option<f64>,
    pub new_tests_per_thousand: Option<f64>,
    pub new_tests_smoothed: Option<f64>,
    pub new_tests_smoothed_per_thousand: Option<f64>,
    pub positive_rate: Option<f64>,
    pub tests_per_case: Option<f64>,
    pub tests_units: Option<String>,
    pub total_vaccinations: Option<f64>,
    pub people_vaccinated: Option<f64>,
    pub people_fully_vaccinated: Option<f64>,
    pub new_vaccinations: Option<f64>,
    pub new_vaccinations_smoothed: Option<f64>,
    pub total_vaccinations_per_hundred: Option<f64>,
    pub people_vaccinated_per_hundred: Option<f64>,
    pub people_fully_vaccinated_per_hundred: Option<f64>,
    pub new_vaccinations_smoothed_per_million: Option<f64>,
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
    pub human_development_index: Option<f64>,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = OWIDRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

/** Parse the CSV, loading it into the database.
 * Will panic on parse error.  */
pub async fn load<'a, A: std::io::Read>(
    rdr: &'a mut csv::Reader<A>,
    mut transaction: Transaction<sqlx::pool::PoolConnection<sqlx::SqliteConnection>>,
) {
    assert_eq!(
        vec![
            "iso_code",
            "continent",
            "location",
            "date",
            "total_cases",
            "new_cases",
            "new_cases_smoothed",
            "total_deaths",
            "new_deaths",
            "new_deaths_smoothed",
            "total_cases_per_million",
            "new_cases_per_million",
            "new_cases_smoothed_per_million",
            "total_deaths_per_million",
            "new_deaths_per_million",
            "new_deaths_smoothed_per_million",
            "reproduction_rate",
            "icu_patients",
            "icu_patients_per_million",
            "hosp_patients",
            "hosp_patients_per_million",
            "weekly_icu_admissions",
            "weekly_icu_admissions_per_million",
            "weekly_hosp_admissions",
            "weekly_hosp_admissions_per_million",
            "new_tests",
            "total_tests_per_thousand",
            "total_tests",
            "new_tests_per_thousand",
            "new_tests_smoothed",
            "new_tests_smoothed_per_thousand",
            "positive_rate",
            "tests_per_case",
            "tests_units",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "new_vaccinations",
            "new_vaccinations_smoothed",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "new_vaccinations_smoothed_per_million",
            "stringency_index",
            "population",
            "population_density",
            "median_age",
            "aged_65_older",
            "aged_70_older",
            "gdp_per_capita",
            "extreme_poverty",
            "cardiovasc_death_rate",
            "diabetes_prevalence",
            "female_smokers",
            "male_smokers",
            "handwashing_facilities",
            "hospital_beds_per_thousand",
            "life_expectancy",
            "human_development_index"
        ],
        rdr.headers().unwrap().iter().collect::<Vec<&str>>()
    );
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    for rec in finaliter {
        let nd = NaiveDate::parse_from_str(rec.date.as_str(), "%Y-%m-%d").unwrap();
        let dbrec = OWID {
            date_julian: nd_to_day(&nd),
            iso_code: rec.iso_code,
            continent: rec.continent,
            location: rec.location,
            total_cases: rec.total_cases,
            new_cases: rec.new_cases,
            new_cases_smoothed: rec.new_cases_smoothed,
            total_deaths: rec.total_deaths,
            new_deaths: rec.new_deaths,
            new_deaths_smoothed: rec.new_deaths_smoothed,
            total_cases_per_million: rec.total_cases_per_million,
            new_cases_per_million: rec.new_cases_per_million,
            new_cases_smoothed_per_million: rec.new_cases_smoothed_per_million,
            total_deaths_per_million: rec.total_deaths_per_million,
            new_deaths_per_million: rec.new_deaths_per_million,
            new_deaths_smoothed_per_million: rec.new_deaths_smoothed_per_million,
            reproduction_rate: rec.reproduction_rate,
            icu_patients: rec.icu_patients,
            icu_patients_per_million: rec.icu_patients_per_million,
            hosp_patients: rec.hosp_patients,
            hosp_patients_per_million: rec.hosp_patients_per_million,
            weekly_icu_admissions: rec.weekly_icu_admissions,
            weekly_icu_admissions_per_million: rec.weekly_icu_admissions_per_million,
            weekly_hosp_admissions: rec.weekly_hosp_admissions,
            weekly_hosp_admissions_per_million: rec.weekly_hosp_admissions_per_million,
            new_tests_smoothed: rec.new_tests_smoothed,
            new_tests: rec.new_tests,
            total_tests: rec.total_tests,
            total_tests_per_thousand: rec.total_tests_per_thousand,
            new_tests_per_thousand: rec.new_tests_per_thousand,
            new_tests_smoothed_per_thousand: rec.new_tests_smoothed_per_thousand,
            tests_per_case: rec.tests_per_case,
            positive_rate: rec.positive_rate,
            tests_units: rec.tests_units,
            total_vaccinations: rec.total_vaccinations,
            people_vaccinated: rec.people_vaccinated,
            people_fully_vaccinated: rec.people_fully_vaccinated,
            new_vaccinations: rec.new_vaccinations,
            new_vaccinations_smoothed: rec.new_vaccinations_smoothed,
            total_vaccinations_per_hundred: rec.total_vaccinations_per_hundred,
            people_vaccinated_per_hundred: rec.people_vaccinated_per_hundred,
            people_fully_vaccinated_per_hundred: rec.people_fully_vaccinated_per_hundred,
            new_vaccinations_smoothed_per_million: rec.new_vaccinations_smoothed_per_million,
            stringency_index: rec.stringency_index,
            population: rec.population,
            population_density: rec.population_density,
            median_age: rec.median_age,
            aged_65_older: rec.aged_65_older,
            aged_70_older: rec.aged_70_older,
            gdp_per_capita: rec.gdp_per_capita,
            extreme_poverty: rec.extreme_poverty,
            cardiovasc_death_rate: rec.cardiovasc_death_rate,
            diabetes_prevalence: rec.diabetes_prevalence,
            female_smokers: rec.female_smokers,
            male_smokers: rec.male_smokers,
            handwashing_facilities: rec.handwashing_facilities,
            hospital_beds_per_thousand: rec.hospital_beds_per_thousand,
            life_expectancy: rec.life_expectancy,
            human_development_index: rec.human_development_index,
        };
        let query = sqlx::query(OWID::insert_str());
        dbrec
            .bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}
