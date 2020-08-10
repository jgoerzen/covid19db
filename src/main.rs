/*

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

use std::env;
use std::error::Error;
use std::ffi::OsString;
use tokio::prelude::*;
use sqlx::sqlite::SqlitePool;

mod locparser;
mod fipsparser;
mod parseutil;
mod dbschema;

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(arg: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(arg) {
        None => Err(From::from("expected argument, but got none; syntax: covid19db-loader path-to-csse_covid_19_data_UID_ISO_FIPS_LookUp_Table.csv path-to-locations-diff.tsv path-to-input-values-sqlite.db")),
        Some(file_path) => Ok(file_path),
    }
}

#[tokio::main]
async fn main() {
    println!("Initializing output database");
    let outputpool = SqlitePool::builder()
        .max_size(5)
        .build("sqlite::covid19.db").await.expect("Error building output sqlite");
    dbschema::initdb(&mut outputpool.acquire().await.unwrap()).await;

    println!("Processing FIPS map");
    let file_path = get_nth_arg(1)
        .expect("need args: path-to-fips.csv");
    let mut rdr = parseutil::parse_init_file(file_path).expect("Couldn't init parser");
    let fipshm = fipsparser::parse(&mut rdr);

    println!("Processing location data");
    let file_path = get_nth_arg(2)
        .expect("need args: path-to-locations-diff.tsv");
    let mut rdr = locparser::parse_init_file(file_path).expect("Couldn't init parser");
    let lochm = locparser::parse(fipshm, &mut rdr);

    println!("Processing SQLITE data");
    let input_path = get_nth_arg(3)
        .expect("Need args: path to sqlite.db");
    let inputpool = SqlitePool::builder()
        .max_size(5)
        .build(format!("sqlite::{}", input_path.into_string().unwrap()).as_ref()).await.expect("Error building");
}
