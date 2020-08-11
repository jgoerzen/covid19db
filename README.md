# COVID-19 Database for Research and Analysis

![build](https://github.com/jgoerzen/covid19db/workflows/build/badge.svg) ![tests](https://github.com/jgoerzen/covid19db/workflows/tests/badge.svg)

This repository contains tools to generate a COVID-19 database for research and analysis, and links to a pre-generated database.  The database is a self-contained [Sqlite](https://www.sqlite.org/) database which can be used on any platform.

The program in this library can be run on your machine to download data from the Internet and assemble your own database.  The process takes approximately two minutes and you can run it however often you like to obtain the latest data.  Alternatively, a database is generated daily that you can download as well.

# Download the database

You can download a compressed database for yourself here: [covid19db.zip](https://github.com/jgoerzen/covid19db/releases/download/v0.1.0/covid19db.zip).

This file is automatically regenerated daily.

# Using the data

Besides the [Sqlite](https://www.sqlite.org/) command-line tools, here are some other tips on using the data:

- The [DB Browser for SQLite](https://sqlitebrowser.org) is a nice graphical explorer for SQLite.
- You can use SQLite in Microsoft Excel and LibreOffice.  Search for information on doing so.

Please note that various included data requests or requires attribution.  Please give credit to original sources of data (eg, The New York Times) and aggregators in your work.

# Included data and sources

You can find a complete database schema in [dbschema.rs](src/dbschema.rs).  A Rust API for `sqlx` is also provided for select tables.  Direct source data download URLs are in [loader.rs](src/loader.rs).

Here are the sources:

- `cdataset` is from the [COVID-19 derived datasets](https://github.com/cipriancraciun/covid19-datasets) project, which includes data from Johns Hopkins University, the New York Times, and ECDC.  This integrates the "combined" set, so you will almost certainly want to use a `WHERE dataset='foo'` in every query so that you use only a single dataset.  `select distinct dataset from cdataset order by dataset;` will show you the available datasets.  Please see the derived datasets link above for a description of the sources and the augmentation done there.  Additional augmentation is done on reading in to this system:
  - Counties are cross-referenced with their FIPS code, which is added to the cdataset table.
  - A [Julian date](https://en.wikipedia.org/wiki/Julian_day) field is added for ease of computation.  It simply increases by 1 for each day, and makes date-based arithmetic simpler in many cases.
  - County populations were not previously populated, and are now done so in the `factbook_population` column using the Johns Hopkins data (see below).
  - Counties that did not previously have a population present have the per-100k people calculations performed and added.
  - The source data eliminated rows for a given dataset and location on days on which there were no new cases/deaths (all the delta values would be zero).  For ease of tabulation, those rows are added back in so a given dataseries for a given location should have a row present for every day.
  - The source data used NULL instead of 0 for deltas.  This has been corrected to 0 in these tables.
- `loc_lookup` is from the [Johns Hopkins dataset](https://github.com/CSSEGISandData/COVID-19), the bulk of which it already included above in `cdataset`.  This table represents the [`UID_ISO_FIPS_LookUp_Table.csv`](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv) file, which contains county-level population data that is integrated into `cdataset` or can be queried separately.
- `rtlive` is from [rt.live](https://rt.live).  Julian dates and YYYY-MM-DD dates are added to the CSV source; no other changes were made. 
- `covid19tracking` is from the [COVID-19 Tracking Project data downloads](https://covidtracking.com/data/download).  Julian dates and Y/M/D dates are added to the CSV source; no other changes were made.
  - The COVID Tracking Project makes a separate US file available, which aggregates data to have one row per day across the entire USA.  Instead of parsing another file, there is a view `covid19tracking_us` that uses the data in `covid19tracking` to present the same kind of view.

# Additional Resources

 - https://source.opennews.org/articles/comparison-four-major-covid-19-data-sources/ has an overview of sources.
 
These are potential future integrations:

- https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
- https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
- https://catalog.data.gov/dataset/covid-19-cases-summarized-by-age-group-and-gender
- https://aws.amazon.com/data-exchange/covid-19/?cards.sort-by=item.additionalFields.order&cards.sort-order=asc
- https://duckduckgo.com/?t=ffab&q=covid-19+data+set+by+age&ia=web

# Building your own database

A command like this should do it

``` sh
git clone https://github.com/jgoerzen/covid19db
cd covid19db
cargo run --release
```

You will then get a file named `covid19.db` in the working directory.  Just use this with Sqlite.

With these commands, you can verify these results for yourself.  If you don't already have Rust installed, see the [Rust installation](https://www.rust-lang.org/tools/install) page.

# The Rust library

It is pretty skeletal at the moment, but you can [browse the docs](https://docs.rs/covid19db/latest/covid19db/).

# Users

This data is used by the [Kansas COVID-19 Charts project](https://github.com/jgoerzen/covid19ks) and perhaps others.

# Copyright and Acknowledgments

    This code is Copyright (c) 2019-2020 John Goerzen

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

This repository contains only tools for obtaining data and no data itself, though the data itself may be available elsewhere on Github.  If you use the data herein, or download it, you may be required to acknowledge the source.  Here are some details:

## cdataset - New York Times

In general, we are making this data publicly available for broad, noncommercial public use including by medical and public health researchers, policymakers, analysts and local news media.

If you use this data, you must attribute it to “The New York Times” in any publication. If you would like a more expanded description of the data, you could say “Data from The New York Times, based on reports from state and local health agencies.”

If you use it in an online presentation, we would appreciate it if you would link to our U.S. tracking page at https://www.nytimes.com/interactive/2020/us/coronavirus-us-cases.html.

If you use this data, please let us know at covid-data@nytimes.com.

See our LICENSE for the full terms of use for this data.

This license is co-extensive with the Creative Commons Attribution-NonCommercial 4.0 International license, and licensees should refer to that license (CC BY-NC) if they have questions about the scope of the license.

[source](https://github.com/nytimes/covid-19-data)

## cdataset and loc_lookup - Johns Hopkins

1.    This data set is licensed under the Creative Commons Attribution 4.0 International (CC BY 4.0) by the Johns Hopkins University on behalf of its Center for Systems Science in Engineering. Copyright Johns Hopkins University 2020.
2.    Attribute the data as the "COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University" or "JHU CSSE COVID-19 Data" for short, and the url: https://github.com/CSSEGISandData/COVID-19.
3.    For publications that use the data, please cite the following publication: "Dong E, Du H, Gardner L. An interactive web-based dashboard to track COVID-19 in real time. Lancet Inf Dis. 20(5):533-534. doi: 10.1016/S1473-3099(20)30120-1"

[source](https://github.com/CSSEGISandData/COVID-19)

## rtlive - rt.live

We just ask that you cite Rt.live as the source and link where appropriate.

[source](https://rt.live/faq)

## covid19tracking - COVID-19 Tracking Project

You are welcome to copy, distribute, and develop data and website content from The COVID Tracking Project at The Atlantic for all healthcare, medical, journalistic and non-commercial uses, including any personal, editorial, academic, or research purposes.

The COVID Tracking Project at The Atlantic data and website content is published under a Creative Commons CC BY-NC-4.0 license, which requires users to attribute the source and license type (CC BY-NC-4.0) when sharing our data or website content. The COVID Tracking Project at The Atlantic also grants permission for any derivative use of this data and website content that supports healthcare or medical research (including institutional use by public health and for-profit organizations), or journalistic usage (by nonprofit or for-profit organizations). All other commercial uses are not permitted under the Creative Commons license, and will require permission from The COVID Tracking Project at The Atlantic.

[source](https://covidtracking.com/about-data/license)
