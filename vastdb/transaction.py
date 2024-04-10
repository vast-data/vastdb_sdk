"""VAST Database transaction.

A transcation is used as a context manager, since every Database-related operation in VAST requires a transaction.

    with session.transaction() as tx:
        tx.bucket("bucket").create_schema("schema")
"""

from . import bucket, errors, session

import botocore

from dataclasses import dataclass
import logging


log = logging.getLogger(__name__)

@dataclass
class Transaction:
    """A holder of a single VAST transaction."""

    _rpc: "session.Session"
    txid: int = None

    def __enter__(self):
        """Create a transaction and store its ID."""
        response = self._rpc.api.begin_transaction()
        self.txid = int(response.headers['tabular-txid'])
        log.debug("opened txid=%016x", self.txid)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """On success, the transaction is committed. Otherwise, it is rolled back."""
        if (exc_type, exc_value, exc_traceback) == (None, None, None):
            log.debug("committing txid=%016x", self.txid)
            self._rpc.api.commit_transaction(self.txid)
        else:
            log.exception("rolling back txid=%016x due to:", self.txid)
            self._rpc.api.rollback_transaction(self.txid)

    def __repr__(self):
        """Don't show the session details."""
        return f'Transaction(id=0x{self.txid:016x})'

    def bucket(self, name: str) -> "bucket.Bucket":
        """Return a VAST Bucket, if exists."""
        try:
            self._rpc.s3.head_bucket(Bucket=name)
        except botocore.exceptions.ClientError as e:
            log.warning("res: %s", e.response)
            if e.response['Error']['Code'] == '404':
                raise errors.MissingBucket(name)
            raise
        return bucket.Bucket(name, self)
