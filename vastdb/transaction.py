from . import bucket, errors, session

import botocore

from dataclasses import dataclass
import logging


log = logging.getLogger(__name__)

@dataclass
class Transaction:
    _rpc: "session.Session"
    txid: int = None

    def __enter__(self):
        response = self._rpc.api.begin_transaction()
        self.txid = int(response.headers['tabular-txid'])
        log.debug("opened txid=%016x", self.txid)
        return self

    def __exit__(self, *args):
        if args == (None, None, None):
            log.debug("committing txid=%016x", self.txid)
            self._rpc.api.commit_transaction(self.txid)
        else:
            log.exception("rolling back txid=%016x", self.txid)
            self._rpc.api.rollback_transaction(self.txid)

    def __repr__(self):
        return f'Transaction(id=0x{self.txid:016x})'

    def bucket(self, name: str) -> "bucket.Bucket":
        try:
            self._rpc.s3.head_bucket(Bucket=name)
            return bucket.Bucket(name, self)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 403:
                raise errors.AccessDeniedError(f"Access is denied to bucket: {name}") from e
            else:
                raise errors.NotFoundError(f"Bucket {name} does not exist") from e
