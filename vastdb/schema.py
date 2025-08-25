"""VAST Database schema (a container of tables).

VAST S3 buckets can be used to create Database schemas and tables.
It is possible to list and access VAST snapshots generated over a bucket.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional

import pyarrow as pa

from . import bucket, errors, schema, table
from ._ibis_support import validate_ibis_support_schema

if TYPE_CHECKING:
    from .table import Table


log = logging.getLogger(__name__)


@dataclass
class Schema:
    """VAST Schema."""

    name: str
    bucket: "bucket.Bucket"

    @property
    def tx(self):
        """VAST transaction used for this schema."""
        return self.bucket.tx

    def _subschema_full_name(self, name: str) -> str:
        return f"{self.name}/{name}" if self.name else name

    def create_schema(self, name: str, fail_if_exists=True) -> "Schema":
        """Create a new schema (a container of tables) under this schema."""
        if current := self.schema(name, fail_if_missing=False):
            if fail_if_exists:
                raise errors.SchemaExists(self.bucket.name, name)
            else:
                return current
        full_name = self._subschema_full_name(name)
        self.tx._rpc.api.create_schema(self.bucket.name, full_name, txid=self.tx.txid)
        log.info("Created schema: %s", full_name)
        return self.schema(name)  # type: ignore[return-value]

    def schema(self, name: str, fail_if_missing=True) -> Optional["Schema"]:
        """Get a specific schema (a container of tables) under this schema."""
        _bucket_name, schemas, _next_key, _is_truncated, _ = \
            self.tx._rpc.api.list_schemas(bucket=self.bucket.name, schema=self.name, next_key=0, txid=self.tx.txid,
                                          name_prefix=name, exact_match=True, max_keys=1)
        names = [name for name, *_ in schemas]
        log.debug("Found schemas: %s", names)
        if not names:
            if fail_if_missing:
                raise errors.MissingSchema(self.bucket.name, self._subschema_full_name(name))
            else:
                return None

        assert len(names) == 1, f"Expected to receive only a single schema, but got {len(schemas)}: ({schemas})"
        return schema.Schema(name=self._subschema_full_name(names[0]), bucket=self.bucket)

    def schemas(self, batch_size=None) -> Iterable["Schema"]:
        """List child schemas."""
        next_key = 0
        if not batch_size:
            batch_size = 1000
        result: List["Schema"] = []
        while True:
            _bucket_name, curr_schemas, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_schemas(bucket=self.bucket.name, schema=self.name, next_key=next_key, max_keys=batch_size, txid=self.tx.txid)
            result.extend(schema.Schema(name=self._subschema_full_name(name), bucket=self.bucket) for name, *_ in curr_schemas)
            if not is_truncated:
                break
        return result

    def create_table(self, table_name: str, columns: pa.Schema, fail_if_exists=True,
                     use_external_row_ids_allocation=False, sorting_key=[]) -> "Table":
        """Create a new table under this schema.

        A virtual `vastdb_rowid` column (of `int64` type) can be created to access and filter by internal VAST row IDs.
        See https://support.vastdata.com/s/article/UUID-48d0a8cf-5786-5ef3-3fa3-9c64e63a0967 for more details.
        """
        if current := self.table(table_name, fail_if_missing=False):
            if fail_if_exists:
                raise errors.TableExists(self.bucket.name, self.name, table_name)
            else:
                return current
        if use_external_row_ids_allocation:
            self.tx._rpc.features.check_external_row_ids_allocation()

        validate_ibis_support_schema(columns)
        self.tx._rpc.api.create_table(self.bucket.name, self.name, table_name, columns, txid=self.tx.txid,
                                      use_external_row_ids_allocation=use_external_row_ids_allocation,
                                      sorting_key=sorting_key)
        log.info("Created table: %s", table_name)
        return self.table(table_name)  # type: ignore[return-value]

    def table(self, name: str, fail_if_missing=True) -> Optional["table.Table"]:
        """Get a specific table under this schema."""
        t = self.tables(table_name=name)
        if not t:
            if fail_if_missing:
                raise errors.MissingTable(self.bucket.name, self.name, name)
            else:
                return None
        assert len(t) == 1, f"Expected to receive only a single table, but got: {len(t)}. tables: {t}"
        log.debug("Found table: %s", t[0])
        return t[0]

    def _iter_tables(self, table_name=None, page_size=1000):
        next_key = 0
        name_prefix = table_name if table_name else ""
        exact_match = bool(table_name)
        while True:
            _bucket_name, _schema_name, curr_tables, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_tables(
                    bucket=self.bucket.name, schema=self.name, next_key=next_key, max_keys=page_size, txid=self.tx.txid,
                    exact_match=exact_match, name_prefix=name_prefix, include_list_stats=exact_match)
            if not curr_tables:
                break
            yield from curr_tables
            if not is_truncated:
                break

    def tables(self, table_name: str = "", page_size=1000) -> List["Table"]:
        """List all tables under this schema if `table_name` is empty.

        Otherwise, list only the specific table (if exists).
        """
        return [
            _parse_table_info(table_info, self)
            for table_info in self._iter_tables(table_name=table_name, page_size=page_size)
        ]

    def tablenames(self, page_size=1000) -> List[str]:
        """List all table names under this schema."""
        return [table_info.name for table_info in self._iter_tables(page_size=page_size)]

    def drop(self) -> None:
        """Delete this schema."""
        self.tx._rpc.api.drop_schema(self.bucket.name, self.name, txid=self.tx.txid)
        log.info("Dropped schema: %s", self.name)

    def rename(self, new_name) -> None:
        """Rename this schema."""
        self.tx._rpc.api.alter_schema(self.bucket.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed schema: %s to %s", self.name, new_name)
        self.name = new_name


def _parse_table_info(table_info, schema: "schema.Schema"):
    return table.Table(name=table_info.name, schema=schema, handle=int(table_info.handle), _imports_table=False, sorted_table=table_info.sorting_key_enabled)
