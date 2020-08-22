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
use sqlx::sqlite::SqliteRow;

pub async fn assert_one_i64(expected: i64, query: &str, conn: &mut sqlx::pool::PoolConnection<sqlx::SqliteConnection>)
{
    let val: (i64, ) = sqlx::query_as(query)
        .fetch_one(conn)
        .await
        .unwrap();
    assert_eq!(expected, val.0);
}

pub async fn assert_one_opti64(expected: Option<i64>, query: &str, conn: &mut sqlx::pool::PoolConnection<sqlx::SqliteConnection>)
{
    let val: (Option<i64>, ) = sqlx::query_as(query)
        .fetch_one(conn)
        .await
        .unwrap();
    assert_eq!(expected, val.0);
}
