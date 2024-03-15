import logging
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from vastdb.v2 import InvalidArgumentError, Table, Schema


log = logging.getLogger(__name__)


def create_table_from_files(
        schema: Schema, table_name: str, parquet_files: [str], schema_merge_func: Callable = None) -> Table:
    if not schema_merge_func:
        schema_merge_func = _default_schema_merge
    else:
        assert schema_merge_func in [_default_schema_merge, _strict_schema_merge, _union_schema_merge]
    tx = schema.tx
    current_schema = pa.schema([])
    s3fs = pa.fs.S3FileSystem(
        access_key=tx._rpc.api.access_key, secret_key=tx._rpc.api.secret_key, endpoint_override=tx._rpc.api.url)
    for prq_file in parquet_files:
        if not prq_file.startswith('/'):
            raise InvalidArgumentError(f"Path {prq_file} must start with a '/'")
        parquet_ds = pq.ParquetDataset(prq_file.lstrip('/'), filesystem=s3fs)
        current_schema = schema_merge_func(current_schema, parquet_ds.schema)

    table = schema.create_table(table_name, current_schema)

    log.info("Starting import of %d files to table %s with columns %s", len(parquet_files), table, table.arrow_schema)
    table.import_files(parquet_files)
    return table


def _default_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function validates a schema is contained in another schema
    Raises an InvalidArgumentError if a certain field does not exist in the target schema
    """
    if not current_schema.names:
        return new_schema
    schema_a, schema_b = (current_schema, new_schema) \
        if len(new_schema.names) >= len(current_schema.names) else (new_schema, current_schema)
    for field in schema_a:
        if field not in schema_b:
            log.error("field %s doesn't exist in schema %s", field, schema_b)
            raise InvalidArgumentError("Found mismatch in parquet files schemas")
    return schema_b


def _strict_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function validates two Schemas are identical.
    Raises an InvalidArgumentError if schemas aren't identical.
    """
    if current_schema.names and current_schema != new_schema:
        raise InvalidArgumentError(f"Schemas are not identical. \n {current_schema} \n vs \n {new_schema}")

    return new_schema


def _union_schema_merge(current_schema: pa.Schema, new_schema: pa.Schema) -> pa.Schema:
    """
    This function returns a unified schema from potentially two different schemas.
    """
    return pa.unify_schemas([current_schema, new_schema])
