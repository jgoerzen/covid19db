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

use flate2::write::GzDecoder;
use reqwest;
use sqlx::prelude::*;
use sqlx::sqlite::SqlitePool;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::mem::drop;
use tempfile::tempdir;

use crate::dbschema;
mod combinedloader;
mod covid19trackingloader;
mod loclookuploader;
mod locparser;
mod parseutil;
mod rtliveloader;

pub async fn downloadto<W: Write>(url: &str, file: &mut W) {
    let mut result = reqwest::get(url).await.unwrap();
    while let Some(chunk) = result.chunk().await.unwrap() {
        file.write_all(chunk.as_ref()).unwrap();
    }
}

/** Downloads the data and puts it in `covid19.db` in the current working directory. */
pub async fn load() {
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
               &mut csse_fips_file).await;
    csse_fips_file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", csse_fips_path);
    let mut rdr = parseutil::parse_init_file(csse_fips_file).expect("Couldn't init parser");
    let fipshm = loclookuploader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

    // covid19tracking

    let path = tmp_path.join("covid19tracking.csv");
    let mut file = stdoptions.open(&path).unwrap();
    println!("Downloading {:#?}", path);
    downloadto(
        "https://covidtracking.com/api/v1/states/daily.csv",
        &mut file,
    )
    .await;
    file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", path);
    let mut rdr = parseutil::parse_init_file(file).expect("Couldn't init parser");
    covid19trackingloader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

    // rt.live

    let path = tmp_path.join("rt.csv");
    let mut file = stdoptions.open(&path).unwrap();
    println!("Downloading {:#?}", path);
    downloadto(
        "https://d14wlfuexuxgcm.cloudfront.net/covid/rt.csv",
        &mut file,
    )
    .await;
    file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", path);
    let mut rdr = parseutil::parse_init_file(file).expect("Couldn't init parser");
    rtliveloader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

    // Location map

    let loc_path = tmp_path.join("locations-diff.tsv");
    let mut loc_file = stdoptions.open(&loc_path).unwrap();
    println!("Downloading {:#?}", loc_path);
    downloadto("https://github.com/cipriancraciun/covid19-datasets/raw/master/exports/combined/v1/locations-diff.tsv",
               &mut loc_file).await;
    loc_file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", loc_path);
    let mut rdr = locparser::parse_init_file(loc_file).expect("Couldn't init parser");
    let lochm = locparser::parse(&fipshm, &mut rdr);

    // Sqlite Combined

    let combined_path = tmp_path.join("values-sqlite.db");
    let combined_file = stdoptions.open(&combined_path).unwrap();
    println!("Downloading and decompressing {:#?}", combined_path);
    let mut gzdecoder = GzDecoder::new(combined_file);
    downloadto("https://github.com/cipriancraciun/covid19-datasets/raw/master/exports/combined/v1/values-sqlite.db.gz",
               &mut gzdecoder).await;
    drop(gzdecoder);
    println!(
        "Processing {:#?}...  This one will take a little while...",
        combined_path
    );

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
    println!("Finished successfully!");
}
