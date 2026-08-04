#!/usr/bin/env python3
"""VAST DB Python SDK - Parquet import/export examples."""
from __future__ import annotations

import contextlib
import os
import tempfile

import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

import vastdb
from vastdb import TableRef
from vastdb import util as vastdb_util
from vastdb.table_metadata import TableMetadata

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
session = vastdb.connect()
"""You can efficiently create tables from Parquet files that already exist in an S3 bucket on VAST without copying them via the client. Paths must start with `/`."""
# stage sample parquet objects in the bucket over S3
s3fs = pa.fs.S3FileSystem(
    access_key=os.environ["AWS_ACCESS_KEY_ID"],
    secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    endpoint_override=os.environ["AWS_S3_ENDPOINT_URL"],
)

# create sample parquet files and upload them to the bucket
sample = pa.table({"c1": [1, 2, 3], "c2": ["a", "b", "c"]})
files = []
for name in ("qs_staging/a.parquet", "qs_staging/b.parquet"):
    with s3fs.open_output_stream(f"{BUCKET}/{name}") as out:
        pq.write_table(sample, out)
    files.append(f"/{BUCKET}/{name}")
print(f"staged parquet files: {files}")

first, rest = files[0], files[1:]

# drop leftovers from previous runs
with session.transaction() as tx:
    existing = tx.bucket(BUCKET).schema("parquet_schema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()

# create schema, import first file we can have all files in one go.
    schema = tx.bucket(BUCKET).create_schema("parquet_schema")
    table = vastdb_util.create_table_from_files(
        schema=schema,
        table_name="imported_table",
        parquet_files=[first],
    )
    print(f"created imported_table from {first}")
    print(f"imported columns: {table.columns()}")

# append more files via TableMetadata (no schema fetch)
if rest:
    table_md = TableMetadata(TableRef(BUCKET, "parquet_schema", "imported_table"))
    with session.transaction() as tx:
        table = tx.table_from_metadata(table_md)
        table.import_files(rest)
        print(f"imported additional files via TableMetadata: {rest}")
# export select() batches to a local parquet file using standard python parquet library
with session.transaction() as tx:
    table = tx.bucket(BUCKET).schema("parquet_schema").table("imported_table")
    batches: pyarrow.RecordBatch = table.select()
    out_path = os.path.join(tempfile.gettempdir(), "vastdb_quickstart_export.parquet")
    with contextlib.closing(pq.ParquetWriter(out_path, batches.schema)) as writer:
        for batch in batches:
            writer.write_batch(batch)
    print(f"wrote {out_path}")
    exported = pq.read_table(out_path)
    print(f"exported rows: {exported.num_rows}")
