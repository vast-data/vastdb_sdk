# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.3.6] (2024-12-23)
[1.3.2]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.1...v1.3.2

### Fixed
 - Fix missing api call.

## [1.3.5] (2024-12-16)
[1.3.5]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.4...v1.3.5

### Added
 - Separated Kafka Topic specific APIs

## [1.3.4] (2024-12-11)
[1.3.4]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.3...v1.3.4

### Added
 - Support predicate pushdown on unsigned integers


## [1.3.3] (2024-11-18)
[1.3.3]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.2...v1.3.3

### Added
 - Detect non-VAST DB server responses

### Fixed
 - Fetch table statistics when constructing a `Table` objects and accessing `Table.stats`


## [1.3.2] (2024-10-07)
[1.3.2]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.1...v1.3.2

### Fixed
 - Fix broken links in https://vastdb-sdk.readthedocs.io
 - Document `vastdb_rowid` column
 - Document prefix match operator


## [1.3.1] (2024-09-10)
[1.3.1]: https://github.com/vast-data/vastdb_sdk/compare/v1.3.0...v1.3.1

### Fixed
 - Retry in case `HEAD` request fails


## [1.3.0] (2024-09-01)
[1.3.0]: https://github.com/vast-data/vastdb_sdk/compare/v1.2.0...v1.3.0

### Added
 - Support `startswith` pushdown over string columns


## [1.2.0] (2024-08-19)
[1.2.0]: https://github.com/vast-data/vastdb_sdk/compare/v1.1.2...v1.2.0

### Added
 - Allow listing tables' names without fetching columns


## [1.1.2] (2024-08-19)
[1.1.2]: https://github.com/vast-data/vastdb_sdk/compare/v1.1.1...v1.1.2

### Fixed
 - Fix `Table.update()` when column names are not specified [#66]


## [1.1.1] (2024-08-11)
[1.1.1]: https://github.com/vast-data/vastdb_sdk/compare/v1.1.0...v1.1.1

### Fixed
 - Combine record batches when inserting `pyarrow.Table`

### Changed
 - Don't require `boto3` for checking bucket existence
 - Improve documentation


## [1.1.0] (2024-07-25)
[1.1.0]: https://github.com/vast-data/vastdb_sdk/compare/v1.0.0...v1.1.0

## Fixed
 - Close internal sessions in `select` and `import`

## Added
 - VastDB/Parquet performance benchmark harness
 - Support predicate pushdown over subfields

## [1.0.0] (2024-07-21)
[1.0.0]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.11...v1.0.0

## Added
 - Add CLA (`CONTRIBUTING.md`)
 - Support setting HTTP timeouts

## [0.1.11] (2024-07-17)
[0.1.11]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.10...v0.1.11

## Fixed
 - Revert timezone check to keep previous SDK behaviour

## Added
 - Use Sphinx for building documentation

## Changed
 - Defer imports to optimize initial import latency

## [0.1.10] (2024-07-09)
[0.1.10]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.9...v0.1.10

## Fixed
 - Support responses with no record batches
 - Document supported Arrow data types

## [0.1.9] (2024-07-01)
[0.1.9]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.8...v0.1.9

## Fixed
 - Don't drop timezone information when creating a table
 - Clarify the catalog example

## [0.1.8] (2024-06-26)
[0.1.8]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.7...v0.1.8

## Fixed
 - Allow retries also during query response parsing
 - Don't retrieve stats during Table.select() if not needed
 - Log successful server-side file import

## [0.1.7] (2024-06-03)
[0.1.7]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.6...v0.1.7

## Added
 - Retry idempotent requests in case of retriable errors
 - Allow setting `Table.select()` RPC queue priority
 - Send SDK version via Tabular RPC
 - Support external row IDs allocation
 - Document required VAST release
 - Add a helper function for defining a range of VIPs

## Changed
 - Update `ibis-framework` dependency to 9.0.0
 - Refactor internal RPC client to support retries
 - Refactor request invocation and result handling

## Fixed
 - Sort record batch according to internal row ID column for updates/deletes
 - Don't use `ListBuckets` RPC to probe for server's version
 - Don't fail on an empty batch
 - Don't use 'us-east-1' region name

## Removed
 - Remove unused internal methods


## [0.1.6] (2024-05-23)
[0.1.6]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.5...v0.1.6

### Added
 - Allow listing and querying Catalog snapshots
 - Support nested schemas
 - Use automatic split estimation

### Fixed
 - Allow using specific semi-sorted projection (needs VAST 5.1+ release)
 - Add a client-side check for too large requests

## [0.1.5] (2024-05-16)
[0.1.5]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.4...v0.1.5

### Added
 - Allow passing `ssl_verify` via `Session` c-tor
 - Retry `requests.exceptions.ConnectionError`
 - Document `QueryConfig` properties
 - Support "deferred" `ibis` predicates using [Underscore (`_`) API](https://github.com/ibis-project/ibis/pull/3804)

### Fixed
 - Fix predicate pushdown when nested columns are present
 - Attach original traceback to `MissingBucket` exception

## [0.1.4] (2024-05-13)
[0.1.4]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.3...v0.1.4

### Added
 - Resume `Table.select()` on HTTP 503 code
 - Allow pushing down True/False boolean literals
 - Support `between` predicate pushdown
 - Support inserting wide rows (larger than 5MB)

## [0.1.3] (2024-05-05)
[0.1.3]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.2...v0.1.3

### Added
 - Document predicate pushdown support
 - Access imports' table (for VAST 5.2+)
 - Support `is_in` predicate pushdown
 - Document `table.py`

### Fixed
 - Freeze `ibis` dependency at 8.0.0
 - Support snapshot-based access
 - Optimize RecordBatch slicing

## [0.1.2] (2024-04-25)
[0.1.2]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.1...v0.1.2

### Added
 - Allow querying [VAST Catalog](https://www.vastdata.com/blog/vast-catalog-treat-your-file-system-like-a-database)
 - Use [mypy](https://mypy-lang.org/) for type annotation checking

### Fixed
 - Fix handling HTTPS endpoints [#50]
 - Optimize `Table.select()` performance
 - Use `GET` request to retrieve cluster version
 - Enable `ruff` preview mode
 - Make sure DuckDB integration fails gracefully when transaction is closed prematurely

### Removed
 - Don't return row IDs from `Table.insert()`

## [0.1.1] (2024-04-15)
[0.1.1]: https://github.com/vast-data/vastdb_sdk/compare/v0.1.0...v0.1.1

### Added
 - Support date & timestamp predicate pushdown [#25]

### Fixed
 - Add missing `requirements.txt` file to released tarball [#47]

## [0.1.0] (2024-04-11)
[0.1.0]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.5.3...v0.1.0

### Added
- Transactions are implemented using [context managers](https://docs.python.org/3/library/stdtypes.html#typecontextmanager)
- Database entities are derived from a specific transaction
- Predicate pushdown is using [ibis](https://ibis-project.org/)-like expressions
- Public SDK [test suite](vastdb/tests/)
- Internal CI with Python 3.9 - 3.12
- Using [`ruff`](https://github.com/astral-sh/ruff) for linting and code formatting
- Improved [`README.md`](README.md) with new API examples

### Removed

- Old SDK classes and methods are deprecated

## [0.0.5.3] (2024-03-24)
[0.0.5.3]: https://github.com/vast-data/vastdb_sdk/compare/v0.0.5.2...v0.0.5.3

### Added
- Allow changing default max list_columns page size [#43]

### Fixed
- Fixing insert function to use the correct tx while listing columns [#42]

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
[#25]: https://github.com/vast-data/vastdb_sdk/pull/25
[#29]: https://github.com/vast-data/vastdb_sdk/pull/29
[#30]: https://github.com/vast-data/vastdb_sdk/pull/30
[#31]: https://github.com/vast-data/vastdb_sdk/pull/31
[#33]: https://github.com/vast-data/vastdb_sdk/pull/33
[#42]: https://github.com/vast-data/vastdb_sdk/pull/42
[#43]: https://github.com/vast-data/vastdb_sdk/pull/43
[#47]: https://github.com/vast-data/vastdb_sdk/pull/47
[#50]: https://github.com/vast-data/vastdb_sdk/pull/50
[#66]: https://github.com/vast-data/vastdb_sdk/issues/66
