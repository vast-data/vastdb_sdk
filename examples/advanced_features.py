#!/usr/bin/env python3
"""VAST DB Python SDK - advanced features examples."""
from __future__ import annotations

import os

import pyarrow as pa
from pyarrow import RecordBatch

import vastdb

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
session = vastdb.connect()
myarrowschema = pa.schema([
    ("c1", pa.int16()),
    ("c2", pa.float32()),
    ("c3", pa.utf8()),
])
# drop leftovers from previous runs, then create schema and table
with session.transaction() as tx:
    existing = tx.bucket(BUCKET).schema("advanced_schema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()

    myschema = tx.bucket(BUCKET).create_schema("advanced_schema")
    mytable = myschema.create_table("mytable", myarrowschema)
    mytable.insert(RecordBatch.from_pylist(
        [{"c1": 1, "c2": 1.5, "c3": "aaaa"},
         {"c1": 2, "c2": 2.5, "c3": "bbbb"},
         {"c1": 3, "c2": 3.5, "c3": "cccc"},
         {"c1": 4, "c2": 4.5, "c3": "dddd"}],
        schema=myarrowschema,
    ))

    """Create, list and delete available semi-sorted projections ->(https://kb.vastdata.com/documentation/docs/managing-table-projections)"""
    print("Semi-sorted projections".center(80, "="))
    # create, list, and drop a semi-sorted projection
    projection = mytable.create_projection(
        "qs_proj",
        sorted_columns=["c3"],
        unsorted_columns=["c1"],
    )
    print(f"projections: {[p.name for p in mytable.projections()]}")
    print(f"projection stats: {projection.stats}")
    projection.drop()

"""You can access the VAST Database using (https://vastdata.com/blog/bringing-snapshots-to-vasts-element-store). Creating snapshots is done via VMS / `vcli`;
the SDK lists and reads them. The snapshot must contain the schema/table you query."""
print("Snapshots".center(80, "="))
# Snapshots - you can look at a list of snapshots and read from there
with session.transaction() as tx:
    snaps = list(tx.bucket(BUCKET).snapshots())
    if snaps:
        print(f"Snapshots: {len(snaps)}")
        print(f"example snapshot: {snaps[0]}")

print("Audit Log".center(80, "="))
# Audit Log you can query as a regular table
with session.transaction() as tx:
    audit = tx.audit_log(fail_if_missing=False)
    if audit is None:
        print("audit log not available on this cluster")
    else:
        print("limiting audit log select to 5 rows; the table can be very large")
        rows = audit.select(limit_rows=5).read_all()
        print(rows)

print("VAST Catalog".center(80, "="))
# VAST Catalog — query as a regular table for pandas post-processing (https://vastdata.com/blog/vast-catalog-treat-your-file-system-like-a-database)
with session.transaction() as tx:
    catalog_table = tx.catalog()
    arrow = catalog_table.select(["element_type"]).read_all()
    df = arrow.to_pandas()
    print(f"Total elements in the catalog: {len(df)}")
    file_count = (df["element_type"] == "FILE").sum()
    print(f"Number of files/objects: {file_count}")
    print("Distinct element types:", df["element_type"].unique())
