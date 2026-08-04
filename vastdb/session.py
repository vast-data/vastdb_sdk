"""VAST database session.

It should be used to interact with a specific VAST cluster.
For more details see:
- [Virtual IP pool configured with DNS service](https://kb.vastdata.com/documentation/docs/configuring-the-vast-cluster-dns-service)
- [S3 access & secret keys on VAST cluster](https://kb.vastdata.com/documentation/docs/managing-s3-user-access-from-the-vast-web-ui)
- [Tabular identity policy with the proper permissions](https://kb.vastdata.com/documentation/docs/creating-identity-policies)
"""

import copy
import os
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional

from ._by_partition_insert import ByPartitionBufferedInserter as _BufferedInserter
from ._session_context import SessionContext
from .config import BufferedInsertConfig, SessionConfig
from .table_metadata import TableMetadata, TableRef
from .transaction import Transaction

if TYPE_CHECKING:
    from .config import BackoffConfig


class Session:
    """VAST database session."""

    def __init__(self, access: Optional[str] = None, secret: Optional[str] = None, endpoint: Optional[str] = None,
                 *,
                 max_entities_per_page: int = 10_000,
                 ssl_verify: bool = True,
                 timeout=None,
                 backoff_config: Optional["BackoffConfig"] = None,
                 adbc_driver_path: Optional[str] = None,
                 end_user: Optional[str] = None,
                 config: SessionConfig = SessionConfig()
    ):
        """Connect to a VAST Database endpoint, using specified credentials."""
        from . import _internal, features

        env_endpoint = os.environ.get('AWS_S3_ENDPOINT_URL')
        effective_endpoint: str
        if endpoint is not None:
            effective_endpoint = endpoint
        elif env_endpoint is not None:
            effective_endpoint = env_endpoint
        else:
            raise ValueError("One of endpoint and AWS_S3_ENDPOINT_URL env is required")

        if access is None:
            access = os.environ['AWS_ACCESS_KEY_ID']
        if secret is None:
            secret = os.environ['AWS_SECRET_ACCESS_KEY']

        self.access: str = access
        self.secret: str = secret

        self.api = _internal.VastdbApi(
            access_key=self.access,
            secret_key=self.secret,
            endpoints=[effective_endpoint],
            max_entities_per_page=max_entities_per_page,
            ssl_verify=ssl_verify,
            timeout=timeout,
            backoff_config=backoff_config,
            end_user=end_user)

        self._context = SessionContext(
            api=self.api,
            endpoint=effective_endpoint,
            config=config,
            end_user=end_user,
        )

        self.features: features.Features = features.Features(self.api.vast_version)
        self.adbc_driver_path: Optional[str] = adbc_driver_path

    def __repr__(self):
        """Don't show the secret key."""
        return f'{self.__class__.__name__}(endpoints={self.api.endpoints}, access={self.api.access_key})'

    def transaction(self) -> Transaction:
        """Create a non-initialized transaction object.

        It should be used as a context manager:

            with session.transaction() as tx:
                tx.bucket("bucket").create_schema("schema")
        """
        from . import transaction
        return transaction.Transaction(_rpc=self,
                                       _adbc_driver_path=self.adbc_driver_path,
                                       _session_context=self._context)

    def buffered_inserter(
        self,
        metadata: TableRef | TableMetadata,
        executor: ThreadPoolExecutor,
        flush_config: BufferedInsertConfig | None = None,
    ) -> "_BufferedInserter":
        """Buffered Insert."""
        self.features.check_partitioned()
        flush_config = flush_config or BufferedInsertConfig()

        if isinstance(metadata, TableRef):
            metadata = TableMetadata(ref=copy.copy(metadata))

            if flush_config.tx is None:
                with self.transaction() as tx:
                    metadata.load(tx)
            else:
                metadata.load(flush_config.tx)
        else:
            metadata = copy.deepcopy(metadata)

        api = (
            self.api.with_endpoints(flush_config.data_endpoints)
            if len(flush_config.data_endpoints) != 0
            else self.api
        )

        return _BufferedInserter(lambda: self.transaction(), api, metadata, executor, flush_config)
