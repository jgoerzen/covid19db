/* Parser

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
#![allow(non_snake_case)]

pub use crate::dateutil::*;
pub use crate::dbschema::*;
pub use crate::loader::parseutil::*;
use chrono::NaiveDate;
use csv;
use serde::Deserialize;
use sqlx::Transaction;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct CsvRec {
    pub date: String,
    pub state: String,
    pub positive: Option<i64>,
    pub probableCases: Option<i64>,
    pub negative: Option<i64>,
    pub pending: Option<i64>,
    pub totalTestResultsSource: String,
    pub totalTestResults: Option<i64>,
    pub hospitalizedCurrently: Option<i64>,
    pub hospitalizedCumulative: Option<i64>,
    pub incluCurrently: Option<i64>,
    pub incluCumulative: Option<i64>,
    pub onVentilatorCurrently: Option<i64>,
    pub onVentilatorCumulative: Option<i64>,
    pub recovered: Option<i64>,
    pub lastUpdateEt: Option<String>,
    pub dateModified: Option<String>,
    pub checkTimeEt: Option<String>,
    pub death: Option<i64>,
    pub hospitalized: Option<i64>,
    pub hospitalizedDischarged: Option<i64>,
    pub dateChecked: Option<String>,
    pub totalTestsViral: Option<i64>,
    pub positiveTestsViral: Option<i64>,
    pub negativeTestsViral: Option<i64>,
    pub positiveCasesViral: Option<i64>,
    pub deathConfirmed: Option<i64>,
    pub deathProbable: Option<i64>,
    pub totalTestEncountersViral: Option<i64>,
    pub totalTestsPeopleViral: Option<i64>,
    pub totalTestsAntibody: Option<i64>,
    pub positiveTestsAntibody: Option<i64>,
    pub negativeTestsAntibody: Option<i64>,
    pub totalTestsPeopleAntibody: Option<i64>,
    pub positiveTestsPeopleAntibody: Option<i64>,
    pub negativeTestsPeopleAntibody: Option<i64>,
    pub totalTestsPeopleAntigen: Option<i64>,
    pub positiveTestsPeopleAntigen: Option<i64>,
    pub totalTestsAntigen: Option<i64>,
    pub positiveTestsAntigen: Option<i64>,
    pub fips: i64,
    pub positiveIncrease: Option<i64>,
    pub negativeIncrease: Option<i64>,
    pub total: Option<i64>,
    pub totalTestResultsIncrease: Option<i64>,
    pub posNeg: Option<i64>,
    pub dataQualityGrade: Option<String>,
    pub deathIncrease: Option<i64>,
    pub hospitalizedIncrease: Option<i64>,
    pub hash: Option<String>,
    pub commercialScore: Option<i64>,
    pub negativeRegularScore: Option<i64>,
    pub negativeScore: Option<i64>,
    pub positiveScore: Option<i64>,
    pub score: Option<i64>,
    pub grade: Option<String>,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = CsvRec> {
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
            "date",
            "state",
            "positive",
            "probableCases",
            "negative",
            "pending",
            "totalTestResultsSource",
            "totalTestResults",
            "hospitalizedCurrently",
            "hospitalizedCumulative",
            "inIcuCurrently",
            "inIcuCumulative",
            "onVentilatorCurrently",
            "onVentilatorCumulative",
            "recovered",
            "lastUpdateEt",
            "dateModified",
            "checkTimeEt",
            "death",
            "hospitalized",
            "hospitalizedDischarged",
            "dateChecked",
            "totalTestsViral",
            "positiveTestsViral",
            "negativeTestsViral",
            "positiveCasesViral",
            "deathConfirmed",
            "deathProbable",
            "totalTestEncountersViral",
            "totalTestsPeopleViral",
            "totalTestsAntibody",
            "positiveTestsAntibody",
            "negativeTestsAntibody",
            "totalTestsPeopleAntibody",
            "positiveTestsPeopleAntibody",
            "negativeTestsPeopleAntibody",
            "totalTestsPeopleAntigen",
            "positiveTestsPeopleAntigen",
            "totalTestsAntigen",
            "positiveTestsAntigen",
            "fips",
            "positiveIncrease",
            "negativeIncrease",
            "total",
            "totalTestResultsIncrease",
            "posNeg",
            "dataQualityGrade",
            "deathIncrease",
            "hospitalizedIncrease",
            "hash",
            "commercialScore",
            "negativeRegularScore",
            "negativeScore",
            "positiveScore",
            "score",
            "grade"
        ],
        rdr.headers().unwrap().iter().collect::<Vec<&str>>()
    );
    let recs = parse_records(rdr.byte_records());
    let finaliter = parse_to_final(recs);
    for rec in finaliter {
        let nd = NaiveDate::parse_from_str(rec.date.as_str(), "%Y%m%d").unwrap();
        // from the schema: sed -e 's/ *\([^ ]*\).*/\1: rec.\1,/'
        let dbrec = CovidTracking {
            date_julian: nd_to_day(&nd),
            state: rec.state,
            positive: rec.positive,
            probableCases: rec.probableCases,
            negative: rec.negative,
            pending: rec.pending,
            totalTestResults: rec.totalTestResults,
            hospitalizedCurrently: rec.hospitalizedCurrently,
            hospitalizedCumulative: rec.hospitalizedCumulative,
            incluCurrently: rec.incluCurrently,
            incluCumulative: rec.incluCumulative,
            onVentilatorCurrently: rec.onVentilatorCurrently,
            onVentilatorCumulative: rec.onVentilatorCumulative,
            recovered: rec.recovered,
            dataQualityGrade: rec.dataQualityGrade,
            lastUpdateEt: rec.lastUpdateEt,
            dateModified: rec.dateModified,
            checkTimeEt: rec.checkTimeEt,
            death: rec.death,
            hospitalized: rec.hospitalized,
            hospitalizedDischarged: rec.hospitalizedDischarged,
            dateChecked: rec.dateChecked,
            totalTestsViral: rec.totalTestsViral,
            positiveTestsViral: rec.positiveTestsViral,
            negativeTestsViral: rec.negativeTestsViral,
            positiveCasesViral: rec.positiveCasesViral,
            deathConfirmed: rec.deathConfirmed,
            deathProbable: rec.deathProbable,
            totalTestEncountersViral: rec.totalTestEncountersViral,
            totalTestsPeopleViral: rec.totalTestsPeopleViral,
            totalTestsAntibody: rec.totalTestsAntibody,
            positiveTestsAntibody: rec.positiveTestsAntibody,
            negativeTestsAntibody: rec.negativeTestsAntibody,
            totalTestsPeopleAntibody: rec.totalTestsPeopleAntibody,
            positiveTestsPeopleAntibody: rec.positiveTestsPeopleAntibody,
            negativeTestsPeopleAntibody: rec.negativeTestsPeopleAntibody,
            totalTestsPeopleAntigen: rec.totalTestsPeopleAntigen,
            positiveTestsPeopleAntigen: rec.positiveTestsPeopleAntigen,
            totalTestsAntigen: rec.totalTestsAntigen,
            positiveTestsAntigen: rec.positiveTestsAntigen,
            fips: rec.fips,
            positiveIncrease: rec.positiveIncrease,
            negativeIncrease: rec.negativeIncrease,
            total: rec.total,
            totalTestResultsSource: rec.totalTestResultsSource,
            totalTestResultsIncrease: rec.totalTestResultsIncrease,
            posNeg: rec.posNeg,
            deathIncrease: rec.deathIncrease,
            hospitalizedIncrease: rec.hospitalizedIncrease,
            commercialScore: rec.commercialScore,
            negativeRegularScore: rec.negativeRegularScore,
            negativeScore: rec.negativeScore,
            positiveScore: rec.positiveScore,
            score: rec.score,
            grade: rec.grade,
        };
        let query = sqlx::query(CovidTracking::insert_str());
        dbrec
            .bind_query(query)
            .execute(&mut transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}
