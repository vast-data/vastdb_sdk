#!/usr/bin/env python3
"""VAST DB Python SDK - basic examples."""
from __future__ import annotations

import os

import pyarrow as pa
from ibis import _
from pyarrow import RecordBatch, RecordBatchReader

import vastdb
from vastdb import TableRef
from vastdb.config import SessionConfig
from vastdb.table_metadata import TableMetadata, TableType

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
# create a connection
session = vastdb.connect()

myarrowschema = pa.schema([
    ("c1", pa.int16()),
    ("c2", pa.float32()),
    ("c3", pa.utf8()),
])
# create transaction
with session.transaction() as tx:
    # drop leftovers from a previous run (tables first, then schema)
    existing = tx.bucket(BUCKET).schema("myschema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()

    # create a schema
    myschema = tx.bucket(BUCKET).create_schema("myschema")

    # create a table
    mytable = myschema.create_table("mytable", myarrowschema)
    mytable.insert(RecordBatch.from_pylist(
            [{"c1": 1, "c2": 1.5, "c3": "aaaa"},
             {"c1": 2, "c2": 2.5, "c3": "bbbb"},
             {"c1": 3, "c2": 3.5, "c3": "cccc"},
             {"c1": 4, "c2": 4.5, "c3": "dddd"},
             ],
            schema=myarrowschema,
        ))

    mytable.drop_column(pa.schema([myarrowschema.field("c3")]))

    # SELECT * reader must be fully consumed inside this transaction.
    reader: RecordBatchReader = mytable.select()
    result: RecordBatch = reader.read_all()
    print(result)

    # SELECT rows WHERE (c2 > 4) using Ibis, then delete by row id
    r1 = mytable.select(
        predicate=(_.c2 > 4),
        internal_row_id=True,
    ).read_all()
    mytable.delete(r1)

    # Transaction auto commits on context exit.

# Interactive and Non-Interactive Workflows
table_ref = TableRef(BUCKET, "myschema", "mytable")

# Option A: fetch schema/stats from the server (one round-trip).
table_md = TableMetadata(table_ref)
with session.transaction() as tx:
    table_md.load(tx)

# Option B: supply Arrow schema + table_type up front to skip the schema round-trip.
# Inserts work with that alone; select() also needs load_stats() (or QueryConfig.num_splits).
known_schema = pa.schema([
    ("c1", pa.int16()),
    ("c2", pa.float32()),
])
table_md = TableMetadata(table_ref, arrow_schema=known_schema, table_type=TableType.Regular)

with session.transaction() as tx:
    table_md.load_stats(tx)
    mytable = tx.table_from_metadata(table_md)
    result = mytable.select(columns=["c1"], predicate=(_.c2 > 2)).read_all()
    assert result == pa.table({"c1": pa.array([2, 3], type=pa.int16())})


data_endpoints = [
    e.strip()
    for e in os.environ.get("VASTDB_DATA_ENDPOINTS", "").split(",")
    if e.strip()
]

# Session Config to have a set of endpoints always set for query and import
session_with_data_endpoints = vastdb.connect(
    config=SessionConfig(data_endpoints=data_endpoints or None)
)

with session_with_data_endpoints.transaction() as tx:
    mytable = tx.bucket(BUCKET).schema("myschema").table("mytable")
    result = mytable.select(
        columns=["c1"],
        predicate=(_.c2 > 2),
    ).read_all()
    print(f"filtered select with SessionConfig data_endpoints ->\n{result}")
