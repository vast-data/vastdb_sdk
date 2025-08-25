"""VAST Database transaction.

A transcation is used as a context manager, since every Database-related operation in VAST requires a transaction.

    with session.transaction() as tx:
        tx.bucket("bucket").create_schema("schema")
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional

from . import bucket, errors, schema, session

if TYPE_CHECKING:
    from bucket import Bucket
    from table import Table


log = logging.getLogger(__name__)

VAST_CATALOG_BUCKET_NAME = "vast-big-catalog-bucket"
VAST_CATALOG_SCHEMA_NAME = 'vast_big_catalog_schema'
VAST_CATALOG_TABLE_NAME = 'vast_big_catalog_table'

AUDIT_LOG_BUCKET_NAME = "vast-audit-log-bucket"
AUDIT_LOG_SCHEMA_NAME = 'vast_audit_log_schema'
AUDIT_LOG_TABLE_NAME = 'vast_audit_log_table'


class TransactionNotActiveError(Exception):
    """Transaction is not active error."""

    pass


@dataclass
class Transaction:
    """A holder of a single VAST transaction."""

    _rpc: "session.Session"
    txid: Optional[int] = None

    def __enter__(self):
        """Create a transaction and store its ID."""
        response = self._rpc.api.begin_transaction()
        self.txid = int(response.headers['tabular-txid'])
        log.debug("opened txid=%016x", self.txid)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """On success, the transaction is committed. Otherwise, it is rolled back."""
        txid = self.txid
        self.txid = None
        if (exc_type, exc_value, exc_traceback) == (None, None, None):
            log.debug("committing txid=%016x", txid)
            self._rpc.api.commit_transaction(txid)
        else:
            log.exception("rolling back txid=%016x due to:", txid)
            self._rpc.api.rollback_transaction(txid)

    def __repr__(self):
        """Don't show the session details."""
        if self.txid is None:
            return 'InvalidTransaction'
        return f'Transaction(id=0x{self.txid:016x})'

    def bucket(self, name: str) -> "Bucket":
        """Return a VAST Bucket, if exists."""
        try:
            self._rpc.api.head_bucket(name)
        except errors.NotFound as e:
            raise errors.MissingBucket(name) from e

        return bucket.Bucket(name, self)

    def catalog_snapshots(self) -> Iterable["Bucket"]:
        """Return VAST Catalog bucket snapshots."""
        return bucket.Bucket(VAST_CATALOG_BUCKET_NAME, self).snapshots()

    def catalog(self, snapshot: Optional["Bucket"] = None, fail_if_missing=True) -> Optional["Table"]:
        """Return VAST Catalog table."""
        b = snapshot or bucket.Bucket(VAST_CATALOG_BUCKET_NAME, self)
        s = schema.Schema(VAST_CATALOG_SCHEMA_NAME, b)
        return s.table(name=VAST_CATALOG_TABLE_NAME, fail_if_missing=fail_if_missing)

    def audit_log(self, fail_if_missing=True) -> Optional["Table"]:
        """Return VAST Audit Log table."""
        b = bucket.Bucket(AUDIT_LOG_BUCKET_NAME, self)
        s = schema.Schema(AUDIT_LOG_SCHEMA_NAME, b)
        return s.table(name=AUDIT_LOG_TABLE_NAME, fail_if_missing=fail_if_missing)

    @property
    def is_active(self) -> bool:
        """Return whether transaction is active."""
        return self.txid is not None

    @property
    def active_txid(self) -> int:
        """Return active transaction ID."""
        if self.txid is None:
            raise TransactionNotActiveError()
        return self.txid
