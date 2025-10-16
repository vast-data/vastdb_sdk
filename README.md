[![version](https://img.shields.io/pypi/v/vastdb.svg)](https://pypi.org/project/vastdb)

# VAST DB Python SDK

A Python SDK for seamless interaction with [VAST Database](https://vastdata.com/database) and [VAST Catalog](https://vastdata.com/blog/vast-catalog-treat-your-file-system-like-a-database). Enables powerful data operations including:

- Schema and table management
- Efficient data ingest and querying
- Advanced filtering with predicate pushdown
- Direct integration with PyArrow and DuckDB
- File system querying through VAST Catalog

[![vastdb](docs/vastdb.png)](https://vastdata.com/database)

For technical details about VAST Database architecture, see the [whitepaper](https://vastdata.com/whitepaper/#TheVASTDataBase).

## Getting Started

### Requirements

- Linux client with Python 3.10 - 3.13, and network access to the VAST Cluster
- VAST Cluster release `5.0.0-sp10` or later
  - If your VAST Cluster is running an older release, please contact customer.support@vastdata.com.
- [Virtual IP pool configured with DNS service](https://support.vastdata.com/s/topic/0TOV40000000FThOAM/configuring-network-access-v50)
- [S3 access & secret keys on the VAST cluster](https://support.vastdata.com/s/article/UUID-4d2e7e23-b2fb-7900-d98f-96c31a499626)
- [Tabular identity policy with the proper permissions](https://support.vastdata.com/s/article/UUID-14322b60-d6a2-89ac-3df0-3dfbb6974182)

### Installation

```bash
pip install vastdb
```

See the [Release Notes](CHANGELOG.md) for the SDK.

### Quick Start

Create schemas and tables, basic inserts, and selects:

```python
import pyarrow as pa
import vastdb

session = vastdb.connect(
    endpoint='http://vip-pool.v123-xy.VastENG.lab',
    access=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY)

with session.transaction() as tx:
    bucket = tx.bucket("bucket-name")

    schema = bucket.create_schema("schema-name")
    print(bucket.schemas())

    columns = pa.schema([
        ('c1', pa.int16()),
        ('c2', pa.float32()),
        ('c3', pa.utf8()),
    ])
    table = schema.create_table("table-name", columns)
    print(schema.tables())
    print(table.columns())

    arrow_table = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    table.insert(arrow_table)

    # run `SELECT * FROM t`
    reader = table.select()  # return a `pyarrow.RecordBatchReader`
    result = reader.read_all()  # build an PyArrow Table from the `pyarrow.RecordBatch` objects read from VAST
    assert result == arrow_table

    # the transaction is automatically committed when exiting the context
```

Note: the transaction must be remain open while the returned [pyarrow.RecordBatchReader](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html) generator is being used.

The list of supported data types can be found [here](docs/types.md).

## Features

### Select Performance

The `Table.select()` method accepts a [QueryConfig](vastdb/config.py) object that modifies how the select is fulfilled.

The most important setting is the `data_endpoints` parameter that, when set, will allow the SDK to parallelize the select across multiple CNodes. Without this, only the CNode specified in the `connect()` will service the select.

```python
from vastdb.config import QueryConfig

# load default configuration values
cfg = QueryConfig()

# set data_endpoints to CNode VIPs
cfg.data_endpoints = [
    "http://172.19.196.1",
    "http://172.19.196.2",
    "http://172.19.196.3",
    "http://172.19.196.4",
]

table.select(columns=['c1'], predicate=(_.c2 > 2), config=cfg)
```

If using DNS with either TTL=0 or multi-response per the [Best Practice on Load Balancing CNodes](https://support.vastdata.com/s/document-item?bundleId=z-kb-articles-publications-prod&topicId=6058049537.html&_LANG=enus), passing in the same DNS name equal to the number of VIPs is a decent proxy.

```python
cfg.data_endpoints = ["http://vip-pool.v123-xy.VastENG.lab"] * 16  # assuming 16 VIPs in the pool
```

### Filters and Projections

The SDK supports predicate and projection pushdown using Ibis:

```python
from ibis import _

# SELECT c1 FROM t WHERE (c2 > 2) AND (c3 IS NULL)
table.select(columns=['c1'],
             predicate=(_.c2 > 2) & _.c3.isnull())

# SELECT c2, c3 FROM t WHERE (c2 BETWEEN 0 AND 1) OR (c2 > 10)
table.select(columns=['c2', 'c3'],
             predicate=(_.c2.between(0, 1) | (_.c2 > 10))

# SELECT * FROM t WHERE c3 LIKE '%substring%'
table.select(predicate=_.c3.contains('substring'))
```

See the [Predicate pushdown support document](docs/predicate.md) for more information on constructing predicates using Ibis.

### Import Parquet files via S3 protocol

You can efficiently create tables from Parquet files that already exist in an S3 bucket on VAST without copying them via the client. If more than one file is included in `parquet_files` they will be loaded concurrently.

```python
with session.transaction() as tx:
    schema = tx.bucket('database-name').schema('schema-name')
    table = util.create_table_from_files(
        schema=schema, table_name='imported-table',
        parquet_files=['/bucket-name/staging/file.parquet'])
```

If the table already exists, you can use the `table.import_files()` method to add more data to the table from Parquet files that already exist in an S3 bucket on VAST.

```python
with session.transaction() as tx:
    table = tx.bucket('database-name').schema('schema-name').table('table-name')
    table.import_files(["/bucket-name/staging/file2.parquet"])
```

### Semi-sorted Projections

Create, list and delete [available semi-sorted projections](https://support.vastdata.com/s/article/UUID-e4ca42ab-d15b-6b72-bd6b-f3c77b455de4):

```python
p = table.create_projection('proj', sorted=['c3'], unsorted=['c1'])
print(table.projections())
print(p.get_stats())
p.drop()
```

### Snapshots

You can access the VAST Database using [snapshots](https://vastdata.com/blog/bringing-snapshots-to-vasts-element-store):

```python
snaps = bucket.list_snapshots()
batches = snaps[0].schema('schema-name').table('table-name').select()
```

## Interactive and Non-Interactive Workflows

A `Table` created via the `Schema` object (`tx.bucket('..').schema('..').table('..')`) loads metadata and stats eagerly allowing for interactive development. Each object (bucket, schema, table) requires one or more round-trips to the server and `.table()` will fetch the full table schema.

It's generally more efficient to use the `TableMetadata` interface that allows for both lazy loading of the schemas as it's needed, as well as allowing reusing the metadata across transactions.

```python
# load the table schema & stats into an object we can use across transactions
table_md = TableMetadata(TableRef("bucket-name", "schema-name", "table-name"))
with session.transaction() as tx:
    table_md.load(tx)

# you may init the TableMetadata with the schema in advanced (to save that round-trip)
table_md = TableMetadata(TableRef("bucket-name", "schema-name", "table-name"),
                         arrow_schema=<some-arrow-schema>)

# now we can reuse it without the overhead of reloading the schema and stats,
# such as for inserts:
with session.transaction() as tx:
    table = tx.table_from_metadata(table_md)
    rows = [{"id": 1, "name": "row1"}]
    py_records = pa.RecordBatch.from_pylist(rows, schema=table_md.arrow_schema)
    table.insert(py_records)

# and selects:
with session.transaction() as tx:
    table = tx.table_from_metadata(table_md)
    reader = table.select()
    results = reader.read_all()
    print(results)
```

Some table operations, like `table.import_files()`, does not require the client to know the table schema, and using the `TableMetadata` interface will bypass fetching the schema entirely.

```python
table_md = TableMetadata(TableRef("bucket-name", "schema-name", "table-name"))

with session.transaction() as tx:
    table = tx.table_from_metadata(table_md)
    table.import_files(["/bucket-name/staging/file2.parquet"])
```

## Post-processing

### Export

`Table.select()` returns a stream of [PyArrow record batches](https://arrow.apache.org/docs/python/data.html#record-batches), which can be directly exported into a Parquet file:

```python
batches = table.select()
with contextlib.closing(pa.parquet.ParquetWriter('/path/to/file.parquet', batches.schema)) as writer:
    for batch in table_batches:
        writer.write_batch(batch)
```

### DuckDB Integration

Use [DuckDB](https://duckdb.org/docs/guides/python/sql_on_arrow.html) to post-process the resulting stream of [PyArrow record batches](https://arrow.apache.org/docs/python/data.html#record-batches):

```python
from ibis import _

import duckdb
conn = duckdb.connect()

with session.transaction() as tx:
    table = tx.bucket("bucket-name").schema("schema-name").table("table-name")
    batches = table.select(columns=['c1'], predicate=(_.c2 > 2))
    print(conn.execute("SELECT sum(c1) FROM batches").arrow())
```

Note: the transaction must be active while the DuckDB query is executing and fetching results using the Python SDK.

## VAST Catalog

The [VAST Catalog](https://vastdata.com/blog/vast-catalog-treat-your-file-system-like-a-database) can be queried as a regular table:

```python
import pyarrow as pa
import vastdb

session = vastdb.connect(
    endpoint='http://vip-pool.v123-xy.VastENG.lab',
    access=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY)

with session.transaction() as tx:
    table = tx.catalog().select(['element_type']).read_all()
    df = table.to_pandas()

    total_elements = len(df)
    print(f"Total elements in the catalog: {total_elements}")

    file_count = (df['element_type'] == 'FILE').sum()
    print(f"Number of files/objects: {file_count}")

    distinct_elements = df['element_type'].unique()
    print("Distinct element types on the system:")
    print(distinct_elements)
```

## More Information

See these blog posts for more examples:

- https://vastdata.com/blog/the-vast-catalog-in-action-part-1
- https://vastdata.com/blog/the-vast-catalog-in-action-part-2

See also the [full Vast DB Python SDK documentation](https://vastdb-sdk.readthedocs.io/en/latest/)
