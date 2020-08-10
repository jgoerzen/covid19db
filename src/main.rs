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

mod locparser;
mod parseutil;

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(arg: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(arg) {
        None => Err(From::from("expected argument, but got none; syntax: covid19db path-to-locations-diff.tsv")),
        Some(file_path) => Ok(file_path),
    }
}
fn main() {
    let loc_file_path = get_nth_arg(1)
        .expect("need args: path-to-locations-diff.tsv")
        .into_string()
        .expect("conversion issue");
    let mut locrdr = parseutil::parse_init_file(loc_file_path).expect("Couldn't init parser");
    let lochm = locparser::parse(&mut locrdr);

}
