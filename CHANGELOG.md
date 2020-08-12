# covid19db changelog

Major changes only; minor changes are in git commit notes.

# v1.2.0 - 2020-08-12

- An effort to reduce the size of the DB
  - Starting size: 544M
  - After removing just data_key: 447M 
  - After removing extra date bits: 425M
  - After moving cdataset locations to cdataset_loc: 235M
- Removed extraneous CDataSet::fromrow

