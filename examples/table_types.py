#!/usr/bin/env python3
"""VAST DB Python SDK - partitioning and sorting examples.

A `Table` created via the `Schema` object (`tx.bucket('..').schema('..').table('..')`) loads metadata and stats eagerly allowing for interactive development. Each object (bucket, schema, table) requires one or more round-trips to the server and `.table()` will fetch the full table schema.
It's generally more efficient to use the `TableMetadata` interface that allows for both lazy loading of the schemas as it's needed, as well as allowing reusing the metadata across transactions.
Some table operations, like `table.import_files()`, do not require the client to know the table schema, and using the `TableMetadata` interface will bypass fetching the schema entirely.

Partitioning groups rows so filters can prune; a sorting key keeps rows ordered for efficient range scans. Specify either or both when creating a table
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.transforms import BucketTransform

import vastdb
from vastdb import PartitionKey, PartitionSpec

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
# create a connection
session = vastdb.connect()

part_schema = pa.schema([
    ("id", pa.int64()),
    ("category", pa.utf8()),
    ("timestamp", pa.timestamp("us")),
    ("value", pa.float64()),
])
partition_spec = PartitionSpec([
    PartitionKey("category", BucketTransform(4)),
])
sorting_key = ["timestamp"]
now = datetime.now(timezone.utc).replace(tzinfo=None)
rows = pa.table(schema=part_schema, data=[
    [1, 2, 3],
    ["alpha", "beta", "alpha"],
    [now, now, now],
    [1.0, 2.0, 3.0],
])

with session.transaction() as tx:
    # drop leftovers from previous runs
    existing = tx.bucket(BUCKET).schema("partition_schema", fail_if_missing=False)
    if existing is not None:
        for t in existing.tables():
            t.drop()
        existing.drop()

    schema = tx.bucket(BUCKET).create_schema("partition_schema")
    # partitioned only — PartitionSpec for filter pruning
    t_part = schema.create_table(
        "partitioned_table",
        part_schema,
        partition_spec=partition_spec,
    )
    t_part.insert(rows)
    print(f"partitioned_table sorted={t_part.sorted_table}")

    # sorted only — sorting_key for range scans
    t_sorted = schema.create_table(
        "sorted_table",
        part_schema,
        sorting_key=sorting_key,
    )
    t_sorted.insert(rows)
    print(f"sorted_table sorted={t_sorted.sorted_table}")

    # partitioned + sorted
    t_both = schema.create_table(
        "sorted_partitioned_table",
        part_schema,
        sorting_key=sorting_key,
        partition_spec=partition_spec,
    )
    t_both.insert(rows)
    print(f"sorted_partitioned_table sorted={t_both.sorted_table}")
