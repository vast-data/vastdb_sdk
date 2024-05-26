import logging
import re
from typing import TYPE_CHECKING, Callable, List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import InvalidArgument, TooWideRow

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .schema import Schema
    from .table import ImportConfig, Table


def create_table_from_files(
        schema: "Schema", table_name: str, parquet_files: List[str],
        schema_merge_func: Optional[Callable] = None,
        config: Optional["ImportConfig"] = None) -> "Table":
    if not schema_merge_func:
        schema_merge_func = default_schema_merge
    else:
        assert schema_merge_func in [default_schema_merge, strict_schema_merge, union_schema_merge]
    tx = schema.tx
    current_schema = pa.schema([])
    s3fs = pa.fs.S3FileSystem(
        access_key=tx._rpc.api.access_key, secret_key=tx._rpc.api.secret_key, endpoint_override=tx._rpc.api.url)
    for prq_file in parquet_files:
        if not prq_file.startswith('/'):
            raise InvalidArgument(f"Path {prq_file} must start with a '/'")
        parquet_ds = pq.ParquetDataset(prq_file.lstrip('/'), filesystem=s3fs)
        current_schema = schema_merge_func(current_schema, parquet_ds.schema)

    log.info("Creating table %s from %d Parquet files, with columns: %s",
             table_name, len(parquet_files), list(current_schema))
    table = schema.create_table(table_name, current_schema)

    log.info("Starting import of %d files to table: %s", len(parquet_files), table)
    table.import_files(parquet_files, config=config)
    log.info("Finished import of %d files to table: %s", len(parquet_files), table)
    return table


def default_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function validates a schema is contained in another schema
    Raises an InvalidArgument if a certain field does not exist in the target schema
    """
    if not current_schema.names:
        return new_schema
    s1 = set(current_schema)
    s2 = set(new_schema)

    if len(s1) > len(s2):
        s1, s2 = s2, s1
        result = current_schema  # We need this variable in order to preserve the original fields order
    else:
        result = new_schema

    if not s1.issubset(s2):
        log.error("Schema mismatch. schema: %s isn't contained in schema: %s.", s1, s2)
        raise InvalidArgument("Found mismatch in parquet files schemas.")
    return result


def strict_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function validates two Schemas are identical.
    Raises an InvalidArgument if schemas aren't identical.
    """
    if current_schema.names and current_schema != new_schema:
        raise InvalidArgument(f"Schemas are not identical. \n {current_schema} \n vs \n {new_schema}")

    return new_schema


def union_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function returns a unified schema from potentially two different schemas.
    """
    return pa.unify_schemas([current_schema, new_schema])


MAX_TABULAR_REQUEST_SIZE = 5 << 20  # in bytes
MAX_RECORD_BATCH_SLICE_SIZE = int(0.9 * MAX_TABULAR_REQUEST_SIZE)
MAX_QUERY_DATA_REQUEST_SIZE = int(0.9 * MAX_TABULAR_REQUEST_SIZE)


def iter_serialized_slices(batch: Union[pa.RecordBatch, pa.Table], max_rows_per_slice=None):
    """Iterate over a list of record batch slices."""

    rows_per_slice = int(0.9 * len(batch) * MAX_RECORD_BATCH_SLICE_SIZE / batch.nbytes)
    if max_rows_per_slice is not None:
        rows_per_slice = min(rows_per_slice, max_rows_per_slice)

    offset = 0
    while offset < len(batch):
        if rows_per_slice < 1:
            raise TooWideRow(batch)

        batch_slice = batch.slice(offset, rows_per_slice)
        serialized_slice_batch = serialize_record_batch(batch_slice)
        if len(serialized_slice_batch) <= MAX_RECORD_BATCH_SLICE_SIZE:
            yield serialized_slice_batch
            offset += rows_per_slice
        else:
            rows_per_slice = rows_per_slice // 2


def serialize_record_batch(batch: Union[pa.RecordBatch, pa.Table]):
    """Serialize a RecordBatch using Arrow IPC format."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write(batch)
    return sink.getvalue()


def expand_ip_ranges(endpoints):
    """Expands endpoint strings that include an IP range in the format 'http://172.19.101.1-16'."""
    expanded_endpoints = []
    pattern = re.compile(r"(http://\d+\.\d+\.\d+)\.(\d+)-(\d+)")

    for endpoint in endpoints:
        match = pattern.match(endpoint)
        if match:
            base_url = match.group(1)
            start_ip = int(match.group(2))
            end_ip = int(match.group(3))
            if start_ip > end_ip:
                raise ValueError("Start IP cannot be greater than end IP in the range.")
            expanded_endpoints.extend(f"{base_url}.{ip}" for ip in range(start_ip, end_ip + 1))
        else:
            expanded_endpoints.append(endpoint)
    return expanded_endpoints
