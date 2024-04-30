"""VAST database session.

It should be used to interact with a specific VAST cluster.
For more details see:
- [Virtual IP pool configured with DNS service](https://support.vastdata.com/s/topic/0TOV40000000FThOAM/configuring-network-access-v50)
- [S3 access & secret keys on VAST cluster](https://support.vastdata.com/s/article/UUID-4d2e7e23-b2fb-7900-d98f-96c31a499626)
- [Tabular identity policy with the proper permissions](https://support.vastdata.com/s/article/UUID-14322b60-d6a2-89ac-3df0-3dfbb6974182)
"""

import os

import boto3

from . import internal_commands, transaction, errors

class Features:
    def __init__(self, vast_version):
        self.vast_version = vast_version

    def check_import_table(self):
        if self.vast_version < (5, 2):
            raise errors.NotSupportedVersion("import_table requires 5.2+", self.vast_version)

class Session:
    """VAST database session."""

    def __init__(self, access=None, secret=None, endpoint=None):
        """Connect to a VAST Database endpoint, using specified credentials."""
        if access is None:
            access = os.environ['AWS_ACCESS_KEY_ID']
        if secret is None:
            secret = os.environ['AWS_SECRET_ACCESS_KEY']
        if endpoint is None:
            endpoint = os.environ['AWS_S3_ENDPOINT_URL']

        self.api = internal_commands.VastdbApi(endpoint, access, secret)
        version_tuple = tuple(int(part) for part in self.api.vast_version.split('.'))
        self.features = Features(version_tuple)
        self.s3 = boto3.client('s3',
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            endpoint_url=endpoint)

    def __repr__(self):
        """Don't show the secret key."""
        return f'{self.__class__.__name__}(endpoint={self.api.url}, access={self.api.access_key})'

    def transaction(self):
        """Create a non-initialized transaction object.

        It should be used as a context manager:

            with session.transaction() as tx:
                tx.bucket("bucket").create_schema("schema")
        """
        return transaction.Transaction(self)
