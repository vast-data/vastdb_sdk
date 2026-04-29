"""VAST Partitioning."""

from dataclasses import dataclass
from typing import Any

from pyiceberg.transforms import IdentityTransform, Transform


@dataclass
class PartitionKey:
    """A partition key defined by a transform on a single column."""

    column: str
    transform: Transform[Any, Any]

    @property
    def is_identity(self) -> bool:
        """Returns whether the transform of self is identity."""
        return isinstance(self.transform, IdentityTransform)


@dataclass
class PartitionSpec:
    """Partition Specification when creating a table."""

    partition_keys: list[PartitionKey]

    def __post_init__(self):
        """Validate after initialization."""
        if len(self.partition_keys) > 4:
            raise ValueError("A partitioned table can be partitioned on no more than 4 keys")


__all__ = ["PartitionKey", "PartitionSpec"]
