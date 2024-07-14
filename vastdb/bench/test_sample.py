#!/usr/bin/env python3

import functools
import itertools
import logging
import os
import random
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pyarrow as pa

import vastdb.errors
from vastdb.table import INTERNAL_ROW_ID
from vastdb.tests import metrics

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)-10s %(process)d/%(thread)d %(filename)s:%(lineno)d %(message)s")

log = logging.getLogger()

sdk_version = vastdb.version()
log.info("Python SDK version: %s", sdk_version)

NUM_COLUMNS = 10_000
COLUMNS_BATCH = 10

NUM_ROW_GROUPS = 100
ROW_GROUP_SIZE = 100_000


INTERNAL_ROWID_FIELD = pa.field(INTERNAL_ROW_ID, pa.uint64())  # used for UPDATE
EXTERNAL_ROWID_FIELD = pa.field("vastdb_rowid", pa.int64())  # used for INSERT & SELECT

SCHEMA = "perf"
TABLE = "sample"

SCHEMA_ARROW = pa.schema(
    [pa.field(f'c{i}', pa.float32()) for i in range(NUM_COLUMNS)]
)


def load_batch(bucket, session_kwargs, offset, limit):
    log.info('loading into [%d..%d)', offset, limit)

    # Iterate over all row-groups in this file
    rowids_range = range(offset, limit)
    rowids = pa.array(rowids_range, INTERNAL_ROWID_FIELD.type)

    session = vastdb.connect(**session_kwargs)
    metrics_rows = []

    with session.transaction() as tx:
        table = tx.bucket(bucket).schema(SCHEMA).table(TABLE)

        col = table[EXTERNAL_ROWID_FIELD.name]
        pred = (col >= rowids_range[0]) & (col <= rowids_range[-1])
        count = sum(len(rb) for rb in table.select(columns=[], predicate=pred))
        log.info("%d rows exist at %s", count, rowids_range)
        if count == len(rowids_range):
            # skip already loaded rows
            log.info('skipping [%d..%d)', offset, limit)

        pid = os.getpid()
        tid = threading.get_native_id()
        total_nbytes = 0
        calls = 0
        t0 = time.time()
        # Insert/update every chunk of columns in this rowgroup
        for j in range(0, len(SCHEMA_ARROW), COLUMNS_BATCH):
            cols_batch = list(SCHEMA_ARROW)[j:j + COLUMNS_BATCH]
            arrays = [
                pa.array(np.float32(np.random.uniform(size=[ROW_GROUP_SIZE])))
                for _ in cols_batch
            ]
            chunk = pa.table(data=arrays, schema=pa.schema(cols_batch))
            nbytes = chunk.get_total_buffer_size()
            start = time.perf_counter()
            if j == 0:
                chunk = chunk.add_column(0, EXTERNAL_ROWID_FIELD, rowids.cast(EXTERNAL_ROWID_FIELD.type))
                op = 'insert'
                table.insert(chunk)
            else:
                chunk = chunk.add_column(0, INTERNAL_ROWID_FIELD, rowids)
                op = 'update'
                table.update(chunk)
            finish = time.perf_counter()

            metrics_rows.append(metrics.Row(
                start=start, finish=finish, table_path=table.path, op=op,
                nbytes=nbytes, rows=len(chunk), cols=len(cols_batch),
                pid=pid, tid=tid, sdk_version=sdk_version))

            total_nbytes += nbytes
            calls += 1
            log.debug("%s into %s: %d rows x %d cols, %.3f MB",
                op, rowids_range, len(chunk), len(chunk.schema),
                chunk.get_total_buffer_size() / 1e6)

        dt = time.time() - t0

    log.info('loaded into [%d..%d): %d rows x %d cols, %.3f MB, %d RPCs, %.3f seconds',
             offset, limit, limit - offset, NUM_COLUMNS, total_nbytes / 1e6, calls, dt)
    return metrics_rows


def test_ingest(bucket_name, session_kwargs, tabular_endpoint_urls, num_workers, perf_metrics_db):
    session = vastdb.connect(**session_kwargs)
    metrics_table = metrics.Table(perf_metrics_db, "ingest")

    with session.transaction() as tx:
        b = tx.bucket(bucket_name)
        try:
            s = b.schema(SCHEMA)
        except vastdb.errors.MissingSchema:
            s = b.create_schema(SCHEMA)

        try:
            s.table(TABLE)
        except vastdb.errors.MissingTable:
            s.create_table(TABLE, pa.schema([EXTERNAL_ROWID_FIELD] + list(SCHEMA_ARROW)))

    ranges = [
        (i * ROW_GROUP_SIZE, (i + 1) * ROW_GROUP_SIZE)
        for i in range(NUM_ROW_GROUPS)
    ]

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(load_batch, bucket_name, session_kwargs | {'endpoint': url}, offset, limit)
            for (offset, limit), url in zip(ranges, itertools.cycle(tabular_endpoint_urls))
        ]
        log.info("spawned %d futures", len(futures))
        for future in as_completed(futures):
            metrics_table.insert(future.result())

    with session.transaction() as tx:
        t = tx.bucket(bucket_name).schema(SCHEMA).table(TABLE)
        count = sum(len(rb) for rb in t.select([]))
        log.info("%s has %d rows: %s", t, count, t.stats)


def run_query(session_kwargs, i, bucket_name, endpoint_url):
    num_columns = 2000
    row_groups_per_query = 10

    config = vastdb.table.QueryConfig(
        num_sub_splits=1,
        num_splits=1,
        limit_rows_per_sub_split=ROW_GROUP_SIZE,
        num_row_groups_per_sub_split=1)

    row_group_indices = list(range(NUM_ROW_GROUPS))
    r = random.Random(i)
    r.shuffle(row_group_indices)

    pid = os.getpid()
    tid = threading.get_native_id()
    metrics_rows = []

    session = vastdb.connect(**(session_kwargs | {"endpoint": endpoint_url}))
    with session.transaction() as tx:
        t = tx.bucket(bucket_name).schema(SCHEMA).table(TABLE)

        fields = list(t.arrow_schema)[1:]
        r.shuffle(fields)
        cols = [f.name for f in fields[:num_columns]]

        vastdb_rowid = t['vastdb_rowid']
        preds = []
        for offset in range(0, len(row_group_indices), row_groups_per_query):
            rowid_ranges = (
                vastdb_rowid.between(j * ROW_GROUP_SIZE, (j + 1) * ROW_GROUP_SIZE - 1)
                for j in row_group_indices[offset:offset + row_groups_per_query]
            )
            pred = functools.reduce((lambda x, y: x | y), rowid_ranges)
            preds.append(pred)

        for j, pred in enumerate(preds):
            log.info("%d) starting query #%d on %s", i, j, endpoint_url)

            start = time.perf_counter()
            res = t.select(columns=cols, predicate=pred, config=config)
            rows = 0
            data = 0
            for rb in res:
                rows += len(rb)
                data += rb.nbytes
                dt = time.perf_counter() - start
                log.info("%d) got query #%d batch %.3f[s], %.3f[GB] %.3f[MB/s], %.3f[Mrows]", i, j, dt, data / 1e9, data / 1e6 / dt, rows / 1e6)

            finish = time.perf_counter()
            dt = finish - start
            log.info("%d) finished query #%d %.3f[s], %.3f[GB], %.3f[MB/s], %.3f[Mrows]", i, j, dt, data / 1e9, data / 1e6 / dt, rows / 1e6)

            metrics_rows.append(metrics.Row(
                start=start, finish=finish, table_path=t.path, op="select",
                nbytes=data, rows=rows, cols=len(cols),
                pid=pid, tid=tid, sdk_version=sdk_version))


def test_scan(test_bucket_name, session, num_workers, session_kwargs, tabular_endpoint_urls, perf_metrics_db):
    metrics_table = metrics.Table(perf_metrics_db, "query")

    log.info("starting %d workers, endpoints=%s", num_workers, tabular_endpoint_urls)
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(run_query, session_kwargs, i, test_bucket_name, url)
            for i, url in zip(range(num_workers), itertools.cycle(tabular_endpoint_urls))
        ]
        for future in as_completed(futures):
            metrics_table.insert(future.result())

    log.info("finished %d workers", num_workers)
