"""VAST Partitioning."""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pyarrow as pa
from pyiceberg.transforms import BucketTransform, IdentityTransform, Transform


@dataclass
class PartitionKey:
    """A partition key defined by a transform on a single column."""

    column: str
    transform: Transform[Any, Any]

    @staticmethod
    def _get_column_index_by_name(schema: pa.Schema, column_name: str) -> int:
        column_idx_list: List[int] = schema.get_all_field_indices(column_name)
        column_num_indices = len(column_idx_list)
        if column_num_indices != 1:
            raise RuntimeError(
                f"invalid column name {column_name} it appears {column_num_indices} times in the schema"
            )
        return column_idx_list[0]

    def serialize(self, schema: pa.Schema) -> str:
        """Serialize INTERNAL protocol serialization."""
        dict_repr: Dict[str, Union[int, str]] = {
            "column-index": self._get_column_index_by_name(schema, self.column)
        }

        if isinstance(self.transform, BucketTransform):
            dict_repr["transform"] = "bucket"
            dict_repr["transform-arg"] = self.transform.num_buckets
        else:
            dict_repr["transform"] = str(self.transform)

        return json.dumps(dict_repr)

    @property
    def is_identity(self) -> bool:
        """Returns whether the trasform of self is identity."""
        return isinstance(self.transform, IdentityTransform)

    @property
    def pre_transform_name(self) -> str:
        """Returns the name of the column prior to its transform."""
        if self.is_identity:
            return self.column

        return self.column.rsplit("_", 1)[0]


@dataclass
class PartitionSpec:
    """Partition Specification when creating a table."""

    partition_keys: list[PartitionKey]

    def __post_init__(self):
        """Validate after initialization."""
        assert len(self.partition_keys) <= 4, (
            "A partitioned table can be partitioned on no more than 4 keys"
        )

    def serialize(self, schema: pa.Schema) -> Dict[str, str]:
        """Serialize INTERNAL protocol serialization."""
        return {
            f"VAST:table:partition-key-{i}": key_part.serialize(schema)
            for i, key_part in enumerate(self.partition_keys)
        }


__all__ = ["PartitionKey", "PartitionSpec"]
