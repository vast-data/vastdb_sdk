# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.0.5] (2024-02-25)
[0.0.5]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.4.0...v0.0.5

### Added
- Created a CHANGELOG

### Fixed
- Wait until all files are processed during import
- Sync with internal `api.py` code


## [0.0.4.0] (2024-02-14)
[0.0.4.0]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.2...v0.0.4.0

### Added
- Allow creating a table using the schema of an existing Parquet file via `create_table_from_parquet_schema()`
- Allow inserting RecordBatch into DB via `insert()`

### Changed
- Rename `vastdb_api` module to `api`

### Fixed
- Fix `concurrent.futures` import error
