# COVID-19 Database for Research and Analysis

This repository contains tools to generate a COVID-19 database for research and analysis, and links to a pre-generated database.  The database is a self-contained [Sqlite](https://www.sqlite.org/) database which can be used on any platform.

The program in this library can be run on your machine to download data from the Internet and assemble your own database.  The process takes approximately two minutes and you can run it however often you like to obtain the latest data.  Alternatively, a database is generated daily that you can download as well.

# Included data and sources

You can find a complete database schema in [dbschema.rs](src/dbschema.rs).  

You can find det

https://source.opennews.org/articles/comparison-four-major-covid-19-data-sources/
https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
https://www.cdc.gov/nchs/nvss/vsrr/covid_weekly/index.htm
https://catalog.data.gov/dataset/covid-19-cases-summarized-by-age-group-and-gender
https://aws.amazon.com/data-exchange/covid-19/?cards.sort-by=item.additionalFields.order&cards.sort-order=asc
https://duckduckgo.com/?t=ffab&q=covid-19+data+set+by+age&ia=web
https://rt.live/
https://covidtracking.com/data/download

In 2020, some counties in Kansas adopted a mask requirement while others didn't.  The Kansas Department of Health and Environment published a [chart](kdhe-chart.pdf) illustrating COVID-19 changes in masks vs. without counties.

Some people questioned whether this chart was misleading due to its use of different Y-axis.  This repository produces a similar chart with a unified Y-axis and shows that the result was not misleading.  Here is the chart I generated:

![](main.png)

The source data is us-counties.csv from the [covid-19-data set](https://github.com/nytimes/covid-19-data), collected by the New York Times based on government sources.  The [Johns Hopkins CSSE data set](https://github.com/CSSEGISandData/COVID-19) is also used to plot alternative versions of these charts.

I have [many more graphs](images/README.md) also available.

# Invocation

To run, you must pass it the path to the NYT `us-counties.csv` file and the John Hopkins `csse_covid_19_daily_reports` directory.  For instance:

``` sh
git clone https://github.com/nytimes/covid-19-data
git clone https://github.com/CSSEGISandData/COVID-19
git clone https://github.com/jgoerzen/covid19ks
cd covid19ks
cargo run --release -- ../covid-19-data/us-counties.csv ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports
```

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

