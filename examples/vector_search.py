#!/usr/bin/env python3
"""VAST DB Python SDK - vector search examples.

Requirements (this example fails without them):
follow the instructions at https://kb.vastdata.com/documentation/docs/vast-query-engine
"""
from __future__ import annotations

import os

import pyarrow as pa

import vastdb
from vastdb import VectorIndexSpec

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
# resolve ADBC driver path (env or packaged driver)
driver_path = os.environ.get("VASTDB_ADBC_DRIVER_PATH", "")
if not driver_path:
    import adbc_driver_vastdb
    driver_path = adbc_driver_vastdb.get_driver_path()

# create a connection with ADBC
session = vastdb.connect(adbc_driver_path=driver_path)

dim = 8
vector_type = pa.list_(pa.field("item", pa.float32(), nullable=False), dim)
vector_schema = pa.schema([
    ("id", pa.int32()),
    ("vector_column", vector_type),
])
vector_index = VectorIndexSpec(column="vector_column", distance_metric="l2sq")
query = [1.0] * dim

# create schema, table with vector index, insert rows
with session.transaction() as tx:
    # drop leftovers from previous runs
    existing = tx.bucket(BUCKET).schema("vector_schema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()

    schema = tx.bucket(BUCKET).create_schema("vector_schema")
    table = schema.create_table(
        "vector_table",
        vector_schema,
        vector_index=vector_index,
    )
    rows = pa.table(schema=vector_schema, data=[
        [10, 20, 30, 60],
        [
            [1.1] * dim,
            [1.2] * dim,
            [1.3] * dim,
            [5.0] * dim,
        ],
    ])
    table.insert(rows)
    print(f"inserted {rows.num_rows} vector rows")

# reload schema, then nearest-neighbor search
with session.transaction() as tx:
    table = tx.bucket(BUCKET).schema("vector_schema").table("vector_table")
    table.reload_schema()
    result = table.vector_search(
        vec=query,
        columns=["id", "vector_column"],
        limit=2,
        predicate=table["id"] > 15,
    ).read_all()
    print(f"vector_search (limit=2, id>15) ->\n{result}")
