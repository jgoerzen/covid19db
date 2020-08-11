# COVID-19 Database for Research and Analysis

![build](https://github.com/jgoerzen/covid19db/workflows/build/badge.svg)

This repository contains tools to generate a COVID-19 database for research and analysis, and links to a pre-generated database.  The database is a self-contained [Sqlite](https://www.sqlite.org/) database which can be used on any platform.

The program in this library can be run on your machine to download data from the Internet and assemble your own database.  The process takes approximately two minutes and you can run it however often you like to obtain the latest data.  Alternatively, a database is generated daily that you can download as well.

# Download the database

You can download a compressed database for yourself here: [covid19.zip](https://github.com/jgoerzen/covid19db/releases/download/v0.1.0/covid19db.zip).

This file is automatically regenerated daily.

# Included data and sources

You can find a complete database schema in [dbschema.rs](src/dbschema.rs).  A Rust API for `sqlx` is also provided for select tables.  Direct source data download URLs are in [main.rs](src/main.rs).

Here are the sources:

- `cdataset` is from the [COVID-19 derived datasets](https://github.com/cipriancraciun/covid19-datasets) project, which includes data from Johns Hopkins University, the New York Times, and ECDC.  This integrates the "combined" set, so you will almost certainly want to use a `WHERE dataset='foo'` in every query so that you use only a single dataset.  `select distinct dataset from cdataset order by dataset;` will show you the available datasets.  Please see the derived datasets link above for a description of the sources and the augmentation done there.  Additional augmentation is done on reading in to this system:
  - Counties are cross-referenced with their FIPS code, which is added to the cdataset table.
  - A [Julian date](https://en.wikipedia.org/wiki/Julian_day) field is added for ease of computation.  It simply increases by 1 for each day, and makes date-based arithmetic simpler in many cases.
  - County populations were not previously populated, and are now done so in the `factbook_population` column using the Johns Hopkins data (see below).
  - Counties that did not previously have a population present have the per-100k people calculations performed and added.
  - The source data eliminated rows for a given dataset and location on days on which there were no new cases/deaths (all the delta values would be zero).  For ease of tabulation, those rows are added back in so a given dataseries for a given location should have a row present for every day.
  - The source data used NULL instead of 0 for deltas.  This has been corrected to 0 in these tables.
- `loc_lookup` is from the [Johns Hopkins dataset](https://github.com/CSSEGISandData/COVID-19), the bulk of which it already included above in `cdataset`.  This table represents the [`UID_ISO_FIPS_LookUp_Table.csv`](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv) file, which contains county-level population data that is integrated into `cdataset` or can be queried separately.

# Additional Resources

 - https://source.opennews.org/articles/comparison-four-major-covid-19-data-sources/ has an overview of sources.
 
These are potential future integrations:

- https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
- https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
- https://catalog.data.gov/dataset/covid-19-cases-summarized-by-age-group-and-gender
- https://aws.amazon.com/data-exchange/covid-19/?cards.sort-by=item.additionalFields.order&cards.sort-order=asc
- https://duckduckgo.com/?t=ffab&q=covid-19+data+set+by+age&ia=web
- https://rt.live/
- https://covidtracking.com/data/download

# Building your own database

A command like this should do it

``` sh
git clone https://github.com/jgoerzen/covid19db
cd covid19db
cargo run --release
```

You will then get a file named `covid19.db` in the working directory.  Just use this with Sqlite.

With these commands, you can verify these results for yourself.  If you don't already have Rust installed, see the [Rust installation](https://www.rust-lang.org/tools/install) page.

# Copyright

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

