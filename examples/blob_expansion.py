#!/usr/bin/env python3
"""VAST DB Python SDK - blob expansion examples.

Expand a blob column of an existing Kafka topic into columns on a target table.
https://kafka.apache.org/quickstart/ -> quickstart for kafka topic creation.
point VASTDB_BLOB_EXPANSION_SCHEMA / VASTDB_BLOB_EXPANSION_TOPIC
at it, otherwise the example is skipped.
Requires VAST 5.5+. Mandatory field metadata and error topic requires 5.5.3+.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa

import vastdb
from vastdb import errors
from vastdb.table import BlobExpansionConfig, ExpansionFormat

BUCKET = os.environ.get("VASTDB_BUCKET", "vastdb")
# The expansion source must be an existing Kafka topic.
TOPIC_SCHEMA = os.environ.get("VASTDB_BLOB_EXPANSION_SCHEMA", "")
TOPIC = os.environ.get("VASTDB_BLOB_EXPANSION_TOPIC", "")
SOURCE_COLUMN = os.environ.get("VASTDB_BLOB_EXPANSION_COLUMN", "value")
TARGET_TABLE = f"{TOPIC}_expanded"

session = vastdb.connect()

try:
    session.features.check_blob_expansion()
except errors.NotSupportedVersion as exc:
    print(f"blob expansion not available: {exc}")
    sys.exit(0)

if not TOPIC_SCHEMA or not TOPIC:
    print("no topic configured: set VASTDB_BLOB_EXPANSION_SCHEMA and VASTDB_BLOB_EXPANSION_TOPIC")
    sys.exit(0)

# Target columns the expansion will fill from the topic's payload column.
initial_expansion = pa.schema([
    ("name", pa.string()),
    ("score", pa.int32()),
])
# Columns added later via add_columns().
extra_columns = pa.schema([
    ("active", pa.bool_()),
    ("tags", pa.list_(pa.string())),
])

# Look up the topic first, missing topic will be result in a skip
with session.transaction() as tx:
    schema = tx.bucket(BUCKET).schema(TOPIC_SCHEMA, fail_if_missing=False)
    topic = schema.table(TOPIC, fail_if_missing=False) if schema is not None else None
    topic_columns = topic.columns().names if topic is not None else []

if topic is None:
    print(f"topic {TOPIC_SCHEMA}/{TOPIC} not found in bucket {BUCKET}; skipping")
    sys.exit(0)
if SOURCE_COLUMN not in topic_columns:
    print(f"topic {TOPIC_SCHEMA}/{TOPIC} has no {SOURCE_COLUMN!r} column: {topic_columns}; skipping")
    sys.exit(0)

with session.transaction() as tx:
    schema = tx.bucket(BUCKET).schema(TOPIC_SCHEMA)
    source = schema.table(TOPIC)
    # Clean leftovers of a previous run: the expansion and its target table only
    try:
        source.blob_expansion(SOURCE_COLUMN).drop()
    except errors.MissingBlobExpansion:
        pass
    stale_target = schema.table(TARGET_TABLE, fail_if_missing=False)
    if stale_target is not None:
        stale_target.drop()
    stale_errors = schema.table(f"{TOPIC}_blob_expansion_errors", fail_if_missing=False)
    if stale_errors is not None:
        stale_errors.drop()

    # Target table must already exist with (at least) the expansion columns.
    target = schema.create_table(
        TARGET_TABLE,
        pa.schema(list(initial_expansion) + list(extra_columns)),
    )

    print("Create blob expansion".center(80, "="))
    # added error topic is done only in create_blob_expansion, and It is set in BlobExpansionConfig
    blob_expansion = source.create_blob_expansion(
        source_column_name=SOURCE_COLUMN,
        expansion_schema=initial_expansion,
        target_table_name=target.name,
        config=BlobExpansionConfig(
            expansion_format=ExpansionFormat.JSON,
            create_errors_topic=True,
        ),
    )
    print(f"source column: {blob_expansion.source_column_name}")
    print(f"target table:  {blob_expansion.target_table_name}")
    print(f"schema:        {blob_expansion.schema}")
    print(f"errors topic name: {blob_expansion.config.errors_topic_name}")

    print("Get blob expansion".center(80, "="))
    retrieved = source.blob_expansion(SOURCE_COLUMN)
    print(f"retrieved schema names: {retrieved.schema.names}")

    print("Add columns".center(80, "="))
    # Mark selected fields mandatory when the cluster supports it (5.5.3+).
    try:
        session.features.check_blob_expansion_mandatory_fields_error_topic()
        mandatory_fields = {"active"}
        print("marked mandatory: active. for mandatory fields use a dot as a separator example : outer.inner.must")
    except errors.NotSupportedVersion:
        mandatory_fields = set()
        print("mandatory fields not supported; adding without Vast:mandatory metadata")

    blob_expansion.add_columns(
        columns_to_add=extra_columns,
        mandatory_fields=mandatory_fields,
    )
    after_add = source.blob_expansion(SOURCE_COLUMN)
    print(f"schema after add: {after_add.schema.names}")

    print("Drop columns".center(80, "="))
    blob_expansion.drop_columns(columns_to_remove=pa.schema([("tags", pa.list_(pa.string()))]))
    after_drop = source.blob_expansion(SOURCE_COLUMN)
    print(f"schema after remove: {after_drop.schema.names}")

    print("Drop blob expansion".center(80, "="))
    blob_expansion.drop()
    target.drop()  # the topic itself is left untouched
    print("dropped")
