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

pub use crate::parseutil::*;
use csv;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct FipsRecord {
    pub uid: String,
    pub iso2: String,
    pub iso3: String,
    pub code3: String,
    pub fips: Option<u32>,
    pub admin2: String,
    pub province_state: String,
    pub country_region: String,
    pub lat: String,
    pub lon: String,
    pub combined_key: String,
    pub population: Option<u64>,
}

pub fn parse_to_final<A: Iterator<Item = csv::StringRecord>>(
    striter: A,
) -> impl Iterator<Item = FipsRecord> {
    striter.filter_map(|x| rec_to_struct(&x).expect("rec_to_struct"))
}

/* Will panic on parse error.  */
pub fn parse<'a, A: std::io::Read>(
    rdr: &'a mut csv::Reader<A>,
) -> HashMap<u32, u64> {
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
    }
    hm

}
