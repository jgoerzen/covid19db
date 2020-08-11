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

use sqlx::prelude::*;
use sqlx::sqlite::SqlitePool;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use tempfile::tempdir;
use std::io::{Seek, SeekFrom};
use reqwest::blocking;
use libflate::gzip::Decoder;
use std::mem::drop;

mod combinedloader;
mod dbschema;
mod loclookuploader;
mod locparser;
mod parseutil;

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(arg: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(arg) {
        None => Err(From::from("expected argument, but got none; syntax: covid19db-loader path-to-csse_covid_19_data_UID_ISO_FIPS_LookUp_Table.csv path-to-locations-diff.tsv path-to-input-values-sqlite.db")),
        Some(file_path) => Ok(file_path),
    }
}

fn downloadto(url: &str, file: &mut File) {
    let mut result = blocking::get(url).unwrap();
    result.copy_to(file).unwrap();
}

#[tokio::main]
async fn main() {
    let tmp_dir = tempdir().unwrap();
    let tmp_path = tmp_dir.path().to_owned();
    let mut stdoptions = &mut OpenOptions::new();
    stdoptions = stdoptions.read(true).write(true).create_new(true);

    // OUTPUT DB INIT

    println!("Initializing output database");
    let mut outputpool = SqlitePool::builder()
        .max_size(5)
        .build("sqlite::covid19.db")
        .await
        .expect("Error building output sqlite");
    dbschema::initdb(&mut outputpool.acquire().await.unwrap()).await;

    // CSSE FIPS

    let csse_fips_path = tmp_path.join("UID_ISO_FIPS_LookUp_Table.csv");
    let mut csse_fips_file = stdoptions.open(&csse_fips_path).unwrap();
    println!("Downloading {:#?}", csse_fips_path);
    downloadto("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv",
               &mut csse_fips_file);
    csse_fips_file.seek(SeekFrom::Start(0));
    println!("Processing {:#?}", csse_fips_path);
    let mut rdr = parseutil::parse_init_file(csse_fips_file).expect("Couldn't init parser");
    let fipshm = loclookuploader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

    // Location map

    let loc_path = tmp_path.join("locations-diff.tsv");
    let mut loc_file = stdoptions.open(&loc_path).unwrap();
    println!("Downloading {:#?}", loc_path);
    downloadto("https://github.com/cipriancraciun/covid19-datasets/raw/master/exports/combined/v1/locations-diff.tsv",
               &mut loc_file);
    loc_file.seek(SeekFrom::Start(0));
    println!("Processing {:#?}", loc_path);
    let mut rdr = locparser::parse_init_file(loc_file).expect("Couldn't init parser");
    let lochm = locparser::parse(&fipshm, &mut rdr);

    // Sqlite Combined

    let combined_path = tmp_path.join("values-sqlite.db");
    let mut combined_file = stdoptions.open(&combined_path).unwrap();
    println!("Downloading and decompressing {:#?}", combined_path);
    let mut result = blocking::get("https://github.com/cipriancraciun/covid19-datasets/raw/master/exports/combined/v1/values-sqlite.db.gz").unwrap();
    let mut decoder = Decoder::new(&mut result).unwrap();
    std::io::copy(&mut decoder, &mut combined_file).unwrap();
    drop(combined_file);
    println!("Processing {:#?}...  This one will take a little while...", combined_path);

    let mut inputpool = SqlitePool::builder()
        .max_size(5)
        .build(format!("sqlite::{}", combined_path.to_str().unwrap()).as_ref())
        .await
        .expect("Error building");
    combinedloader::load(&mut inputpool, &mut outputpool, &lochm, &fipshm).await;

    let mut conn = outputpool.acquire().await.unwrap();
    println!("Vacuuming");
    conn.execute("VACUUM").await.unwrap();
    println!("Optimizing");
    conn.execute("PRAGMA OPTIMIZE").await.unwrap();
}
