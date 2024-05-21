"""VAST Database bucket.

VAST S3 buckets can be used to create Database schemas and tables.
It is possible to list and access VAST snapshots generated over a bucket.
"""

import logging
from dataclasses import dataclass, field
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
    _root_schema: "Schema" = field(init=False, compare=False, repr=False)

    def __post_init__(self):
        """Root schema is empty."""
        self._root_schema = schema.Schema(name="", bucket=self)

    def create_schema(self, name: str, fail_if_exists=True) -> "Schema":
        """Create a new schema (a container of tables) under this bucket."""
        return self._root_schema.create_schema(name=name, fail_if_exists=fail_if_exists)

    def schema(self, name: str, fail_if_missing=True) -> Optional["Schema"]:
        """Get a specific schema (a container of tables) under this bucket."""
        return self._root_schema.schema(name=name, fail_if_missing=fail_if_missing)

    def schemas(self, batch_size=None):
        """List bucket's schemas."""
        return self._root_schema.schemas(batch_size=batch_size)

    def snapshot(self, name, fail_if_missing=True) -> Optional["Bucket"]:
        """Get snapshot by name (if exists)."""
        snapshots, _is_truncated, _next_key = \
            self.tx._rpc.api.list_snapshots(bucket=self.name, name_prefix=name, max_keys=1)

        expected_name = f".snapshot/{name}"
        exists = snapshots and snapshots[0] == expected_name + "/"
        if not exists:
            if fail_if_missing:
                raise errors.MissingSnapshot(self.name, expected_name)
            else:
                return None

        return Bucket(name=f'{self.name}/{expected_name}', tx=self.tx)

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
