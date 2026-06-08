"""VAST Database Python SDK."""

import functools

from . import session
from ._internal import DefaultQuantization, VectorIndexSpec
from .config import BackoffConfig, BufferedInsertConfig, ImportConfig, QueryConfig
from .partitioning import PartitionKey, PartitionSpec
from .table import BlobExpansionConfig, ExpansionFormat
from .table_metadata import TableRef, TableType


# A helper function, useful as a short-hand for Session c-tor: `session = vastdb.connect(...)`
@functools.wraps(session.Session)
def connect(*args, **kwargs):  # noqa: D103
    return session.Session(*args, **kwargs)


def version():
    """Return VAST DB SDK version."""
    from importlib import metadata
    assert __package__ is not None, "__package__ must not be none"
    return metadata.distribution(__package__).version


__all__ = [
    "connect",
    "version",
    "session",
    "PartitionSpec",
    "PartitionKey",
    "VectorIndexSpec",
    "DefaultQuantization",
    "QueryConfig",
    "ImportConfig",
    "BufferedInsertConfig",
    "BackoffConfig",
    "BlobExpansionConfig",
    "ExpansionFormat",
    "TableRef",
    "TableType",
]
