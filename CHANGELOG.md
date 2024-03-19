# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.0.5.2] (2024-03-19)
[0.0.5.2]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.5.1...v0.0.5.2

### Added
- Support retrieving row IDs via `VastdbApi.query()` [#31]

### Fixed
- Fixed several functions that didn't use the correct txid [#30]
- Fix graceful cancellation for `VastdbApi.query_iterator()` [#33]

### Removed
- Remove `vast_protobuf` package and `protobuf` dependency [#29]


## [0.0.5.1] (2024-03-01)
[0.0.5.1]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.5.0...v0.0.5.1

### Fixed
- Support latest Python (3.11+) by not requiring too old dependencies. [#16]


## [0.0.5.0] (2024-02-25)
[0.0.5.0]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.4.0...v0.0.5.0

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


[#16]: https://github.com/vast-data/vastdb_sdk/pull/16
[#29]: https://github.com/vast-data/vastdb_sdk/pull/29
[#30]: https://github.com/vast-data/vastdb_sdk/pull/30
[#31]: https://github.com/vast-data/vastdb_sdk/pull/31
[#33]: https://github.com/vast-data/vastdb_sdk/pull/33
