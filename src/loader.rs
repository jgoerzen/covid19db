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
use crate::dbutil::*;
mod combinedloader;
mod combinedlocloader;
mod covidtrackingloader;
mod harveycodataloader;
mod loclookuploader;
mod owidloader;
mod parseutil;
mod rtliveloader;

pub async fn downloadto<W: Write>(url: &str, file: &mut W) {
    let mut result = reqwest::get(url).await.unwrap();
    // let mut counter: usize = 0;
    while let Some(chunk) = result.chunk().await.unwrap() {
        // counter += chunk.len();
        file.write_all(chunk.as_ref()).unwrap();
        // println!("{}", counter);
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
        .max_size(1)
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

    // Harvey County
    //
    // https://github.com/jgoerzen/covid19-data/raw/master/harveycodata.csv
    let path = tmp_path.join("harveycodata.csv");
    let mut file = stdoptions.open(&path).unwrap();
    println!("Downloading {:#?}", path);
    downloadto(
        "https://github.com/jgoerzen/covid19-data/raw/master/harveycodata.csv",
        &mut file,
    )
    .await;
    file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", path);
    let mut rdr = parseutil::parse_init_file(file).expect("Couldn't init parser");
    harveycodataloader::load(&mut rdr, outputpool.begin().await.unwrap()).await;
    let mut conn = outputpool.acquire().await.unwrap();
    assert_one_opti64(
        Some(51),
        "SELECT kdhe_neg_results FROM harveycodata WHERE date = '2020-07-19'",
        &mut conn,
    )
    .await;
    assert_one_opti64(
        Some(1),
        "SELECT kdhe_pos_results FROM harveycodata WHERE date = '2020-07-19'",
        &mut conn,
    )
    .await;
    assert_one_opti64(
        None,
        "SELECT harveyco_neg_results FROM harveycodata WHERE date = '2020-07-19'",
        &mut conn,
    )
    .await;
    assert_one_opti64(
        None,
        "SELECT harveyco_pos_results FROM harveycodata WHERE date = '2020-07-19'",
        &mut conn,
    )
    .await;
    assert_one_i64(
        50,
        "SELECT kdhe_neg_results FROM harveycodata WHERE date = '2020-08-15'",
        &mut conn,
    )
    .await;
    assert_one_i64(
        22,
        "SELECT kdhe_pos_results FROM harveycodata WHERE date = '2020-08-15'",
        &mut conn,
    )
    .await;
    assert_one_i64(
        32,
        "SELECT harveyco_neg_results FROM harveycodata WHERE date = '2020-08-15'",
        &mut conn,
    )
    .await;
    assert_one_i64(
        4,
        "SELECT harveyco_pos_results FROM harveycodata WHERE date = '2020-08-15'",
        &mut conn,
    )
    .await;
    assert_one_i64(
        20,
        "SELECT harveyco_recovered FROM harveycodata WHERE date = '2020-06-30'",
        &mut conn,
    ).await;
    assert_one_i64(
        41,
        "SELECT harveyco_confirmed FROM harveycodata WHERE date = '2020-06-30'",
        &mut conn,
    ).await;
    drop(conn);

    // covidtracking

    let path = tmp_path.join("covidtracking.csv");
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
    covidtrackingloader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

    // Our World in Data

    let path = tmp_path.join("owid.csv");
    let mut file = stdoptions.open(&path).unwrap();
    println!("Downloading {:#?}", path);
    downloadto(
        "https://covid.ourworldindata.org/data/owid-covid-data.csv",
        &mut file,
    )
    .await;
    file.seek(SeekFrom::Start(0)).unwrap();
    println!("Processing {:#?}", path);
    let mut rdr = parseutil::parse_init_file(file).expect("Couldn't init parser");
    owidloader::load(&mut rdr, outputpool.begin().await.unwrap()).await;

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
    let mut rdr = combinedlocloader::parse_init_file(loc_file).expect("Couldn't init parser");
    let mut lochm =
        combinedlocloader::load(outputpool.begin().await.unwrap(), &fipshm, &mut rdr).await;

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
    combinedloader::load(&mut inputpool, &mut outputpool, &mut lochm, &fipshm).await;

    // Started getting errors at VACUUM about statements in progress.  Drop and re-connect.
    outputpool.close().await;
    let outputpool = SqlitePool::builder()
        .max_size(5)
        .build("sqlite::covid19.db")
        .await
        .expect("Error building output sqlite");
    let mut conn = outputpool.acquire().await.unwrap();
    println!("Vacuuming");
    conn.execute("VACUUM").await.unwrap();
    println!("Optimizing");
    conn.execute("PRAGMA OPTIMIZE").await.unwrap();
    println!(" *** All data loaded; row counts follow:");
    for (tablename, minrows) in vec![
        ("cdataset", 1250000),
        ("covidtracking", 9000),
        ("loc_lookup", 4000),
        ("rtlive", 8000),
        ("harveycodata", 80),
    ] {
        let rows: (i64,) = sqlx::query_as(format!("SELECT COUNT(*) FROM {}", tablename).as_str())
            .fetch_one(&mut conn)
            .await
            .unwrap();
        println!("{}: {}", tablename, rows.0);
        assert!(rows.0 >= minrows);
    }
    println!("Finished successfully!");
}
