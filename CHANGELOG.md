# covid19db changelog

Major changes only; minor changes are in git commit notes.

# v2.0.0 - 2020-08-12

- Renamed covid19tracking -> covidtracking
- Set up views over optimized tables, halving the size of the database and improving performance
- Added this changelog
- An effort to reduce the size of the DB
  - Starting size: 544M
  - After removing just data_key from cdataset: 447M 
  - After removing extra date bits from cdataset: 425M
  - After moving cdataset locations to cdataset_loc: 235M
  - After removing extra date bits from rtlive: no change
  - After removing extra data bits from covid19tracking: no change
- Removed extraneous CDataSet::fromrow

