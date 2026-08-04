#!/usr/bin/env python3
"""VAST DB Python SDK - post-processing examples."""
from __future__ import annotations

import os

import duckdb
import pyarrow as pa
from ibis import _
from pyarrow import RecordBatch

import vastdb
from vastdb import QueryConfig

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")

session = vastdb.connect()

myarrowschema = pa.schema([
    ("c1", pa.int16()),
    ("c2", pa.float32()),
])

# drop leftovers from previous runs
with session.transaction() as tx:
    existing = tx.bucket(BUCKET).schema("postprocess_schema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()
# create new schma and table for this example run.
    myschema = tx.bucket(BUCKET).create_schema("postprocess_schema")
    mytable = myschema.create_table("mytable", myarrowschema)
    mytable.insert(RecordBatch.from_pylist(
        [{"c1": 1, "c2": 1.5},
         {"c1": 2, "c2": 2.5},
         {"c1": 3, "c2": 3.5},
         {"c1": 4, "c2": 4.5}],
        schema=myarrowschema,
    ))

"""
The `Table.select()` method accepts a `QueryConfig` object that modifies how the select is fulfilled.

The most important setting is the `data_endpoints` parameter that, when set, will allow the SDK to parallelize the select across multiple CNodes. Without this, only the CNode specified in the `connect()` will service the select.
If using DNS with either TTL=0 or multi-response per the Best Practice on Load Balancing CNodes -> (https://kb.vastdata.com/docs/best-practice-on-load-balancing-cnodes), passing in the same DNS name equal to the number of VIPs is a decent proxy.
"""

print("QueryConfig".center(80, "="))
# QueryConfig — per-select endpoints (fallback to connect endpoint when unset)
data_endpoints = [
    e.strip()
    for e in os.environ.get("VASTDB_DATA_ENDPOINTS", "").split(",")
    if e.strip()
]
config = QueryConfig(data_endpoints=data_endpoints or None)
with session.transaction() as tx:
    mytable = tx.bucket(BUCKET).schema("postprocess_schema").table("mytable")
    result = mytable.select(
        columns=["c1"],
        predicate=(_.c2 > 2),
        config=config,
    ).read_all()
    print(f"filtered select with QueryConfig ->\n{result}")

print("DuckDB post-processing".center(80, "="))
# DuckDB post-processing - must keep the transaction open while DuckDB reads the stream
conn = duckdb.connect()
with session.transaction() as tx:
    mytable = tx.bucket(BUCKET).schema("postprocess_schema").table("mytable")
    batches = mytable.select(columns=["c1"], predicate=(_.c2 > 2))
    result = conn.execute("SELECT sum(c1) AS total FROM batches").arrow()
    print(f"sum(c1) where c2 > 2 ->\n{result}")
