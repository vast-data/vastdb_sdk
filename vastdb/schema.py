"""VAST Database schema (a container of tables).

VAST S3 buckets can be used to create Database schemas and tables.
It is possible to list and access VAST snapshots generated over a bucket.
"""

from . import bucket, errors, schema, table

import pyarrow as pa

from dataclasses import dataclass
import logging

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

    def create_table(self, table_name: str, columns: pa.Schema) -> "table.Table":
        """Create a new table under this schema."""
        self.tx._rpc.api.create_table(self.bucket.name, self.name, table_name, columns, txid=self.tx.txid)
        log.info("Created table: %s", table_name)
        return self.table(table_name)

    def table(self, name: str) -> "table.Table":
        """Get a specific table under this schema."""
        t = self.tables(table_name=name)
        if not t:
            raise errors.NotFoundError(f"Table '{name}' was not found under schema: {self.name}")
        assert len(t) == 1, f"Expected to receive only a single table, but got: {len(t)}. tables: {t}"
        log.debug("Found table: %s", t[0])
        return t[0]

    def tables(self, table_name=None) -> ["table.Table"]:
        """List all tables under this schema."""
        tables = []
        next_key = 0
        name_prefix = table_name if table_name else ""
        exact_match = bool(table_name)
        while True:
            bucket_name, schema_name, curr_tables, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_tables(
                    bucket=self.bucket.name, schema=self.name, next_key=next_key, txid=self.tx.txid,
                    exact_match=exact_match, name_prefix=name_prefix, include_list_stats=exact_match)
            if not curr_tables:
                break
            tables.extend(curr_tables)
            if not is_truncated:
                break

        return [_parse_table_info(table, self) for table in tables]

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
    stats = table.TableStats(num_rows=table_info.num_rows, size=table_info.size_in_bytes)
    return table.Table(name=table_info.name, schema=schema, handle=int(table_info.handle), stats=stats)
