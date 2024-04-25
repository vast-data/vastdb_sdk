"""VAST Database bucket.

VAST S3 buckets can be used to create Database schemas and tables.
It is possible to list and access VAST snapshots generated over a bucket.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from . import errors, schema, transaction

if TYPE_CHECKING:
    from .schema import Schema

log = logging.getLogger(__name__)


@dataclass
class Bucket:
    """VAST bucket."""

    name: str
    tx: "transaction.Transaction"

    def create_schema(self, path: str, fail_if_exists=True) -> "Schema":
        """Create a new schema (a container of tables) under this bucket."""
        if current := self.schema(path, fail_if_missing=False):
            if fail_if_exists:
                raise errors.SchemaExists(self.name, path)
            else:
                return current
        self.tx._rpc.api.create_schema(self.name, path, txid=self.tx.txid)
        log.info("Created schema: %s", path)
        return self.schema(path)  # type: ignore[return-value]

    def schema(self, path: str, fail_if_missing=True) -> Optional["Schema"]:
        """Get a specific schema (a container of tables) under this bucket."""
        s = self.schemas(path)
        log.debug("schema: %s", s)
        if not s:
            if fail_if_missing:
                raise errors.MissingSchema(self.name, path)
            else:
                return None
        assert len(s) == 1, f"Expected to receive only a single schema, but got: {len(s)}. ({s})"
        log.debug("Found schema: %s", s[0].name)
        return s[0]

    def schemas(self, name: Optional[str] = None) -> List["Schema"]:
        """List bucket's schemas."""
        schemas = []
        next_key = 0
        exact_match = bool(name)
        log.debug("list schemas param: schema=%s, exact_match=%s", name, exact_match)
        while True:
            _bucket_name, curr_schemas, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_schemas(bucket=self.name, next_key=next_key, txid=self.tx.txid,
                                               name_prefix=name, exact_match=exact_match)
            if not curr_schemas:
                break
            schemas.extend(curr_schemas)
            if not is_truncated:
                break

        return [schema.Schema(name=name, bucket=self) for name, *_ in schemas]

    def snapshots(self) -> List["Bucket"]:
        """List bucket's snapshots."""
        snapshots = []
        next_key = 0
        while True:
            curr_snapshots, is_truncated, next_key = \
                self.tx._rpc.api.list_snapshots(bucket=self.name, next_token=next_key)
            if not curr_snapshots:
                break
            snapshots.extend(curr_snapshots)
            if not is_truncated:
                break

        return [
            Bucket(name=f'{self.name}/{snapshot.strip("/")}', tx=self.tx)
            for snapshot in snapshots
        ]
