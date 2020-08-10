/* Combined loader

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

pub use crate::parseutil::*;
use std::collections::HashMap;
use sqlx::prelude::*;
use sqlx::Transaction;
use std::convert::TryFrom;
use std::mem::drop;

/** Parse the CSV, loading it into the database, and returning a hashmap of fips to population.
 * Will panic on parse error.  */
pub async fn load(
    inputpool: &mut sqlx::SqlitePool,
    outputpool: &mut sqlx::SqlitePool) {

    // Speed things up a bit.
    let mut conn = outputpool.acquire().await.unwrap();
    conn.execute("PRAGMA auto_vacuum = 0").await.unwrap();
    conn.execute("PRAGMA synchronous = 0").await.unwrap();
    drop(conn);

    let mut transaction = outputpool.begin().await.unwrap();

    let mut iconn = inputpool.acquire().await.unwrap();
    let mut cursor = sqlx::query("SELECT * from dataset ORDER BY dataset, location_key, date")
        .fetch(&mut iconn);

    while let Some(row) = cursor.next().await.unwrap() {
        let query = sqlx::query("INSERT INTO cdataset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        query.bind(row.get::<i64, &str>("uid"))
            .execute(&mut transaction).await.unwrap();
    }
    transaction.commit().await.unwrap();
}
