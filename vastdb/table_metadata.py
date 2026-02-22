"""VAST Database table metadata."""

import logging
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Optional

import ibis
import pyarrow as pa
import pydantic
from pyiceberg.transforms import parse_transform

from vastdb import errors
from vastdb._ibis_support import validate_ibis_support_schema
from vastdb._internal import TableStats, VectorIndex
from vastdb.partitioning import PartitionKey, PartitionSpec

if TYPE_CHECKING:
    from .transaction import Transaction


log = logging.getLogger(__name__)


class TableType(Enum):
    """Table Type."""

    Regular = 1
    Elysium = 2
    TableImports = 3
    Partitioned = 4  # Hydra
    SortedPartitions = 5  # Lissandra


class _ColumnMetadata(pydantic.BaseModel):
    column_id: Optional[int] = pydantic.Field(alias="VAST:column_id", default=None)
    partition_key_index: Optional[int] = pydantic.Field(alias="VAST:partition:key_index", default=None)
    partition_transform: Optional[str] = pydantic.Field(alias="VAST:partition:transform", default=None)
    partition_transform_arg: Optional[int] = pydantic.Field(
        alias="VAST:partition:transform-arg", default=None
    )

    class Config:
        populate_by_name = True

    @pydantic.field_validator("partition_transform")
    @classmethod
    def transform_to_lower(cls, v: str) -> str:
        return v.lower()

    @property
    def transform_name(self) -> Optional[str]:
        if self.partition_transform is None:
            return None

        if self.partition_transform_arg is not None:
            return f'{self.partition_transform}[{self.partition_transform_arg}]'

        return self.partition_transform


@dataclass
class TableRef:
    """Represents a table ref (table's full path)."""

    bucket: str
    schema: str
    table: str

    @property
    def full_path(self) -> str:
        """Table full path."""
        return f"{self.bucket}/{self.schema}/{self.table}"

    @property
    def query_engine_full_path(self) -> str:
        """Table full path for VastDB Query Engine."""
        return f'"{self.bucket}/{self.schema}".{self.table}'

    def __str__(self) -> str:
        """Table full path."""
        return self.full_path


class TableMetadata:
    """Table Metadata."""

    _ref: TableRef
    _arrow_schema: Optional[pa.Schema]
    _sorted_columns: Optional[list[pa.Field]]
    _ibis_table: ibis.Table
    _stats: Optional[TableStats]
    _vector_index: Optional[VectorIndex]
    _partitioning: Optional[PartitionSpec]

    def __init__(
        self,
        ref: TableRef,
        arrow_schema: Optional[pa.Schema] = None,
        table_type: Optional[TableType] = None,
        vector_index: Optional[VectorIndex] = None,
        partitioning: Optional[PartitionSpec] = None
    ):
        """Table Metadata."""
        self._ref = deepcopy(ref)
        self._table_type = table_type
        self.arrow_schema = deepcopy(arrow_schema)
        self._sorted_columns = None
        self._stats = None
        self._vector_index = vector_index
        self._partitioning = partitioning

        if table_type not in {TableType.SortedPartitions, TableType.Partitioned, None} and partitioning is not None:
            raise ValueError(f"Only a partitioned table may have partitioning. Given TableType: {table_type} with partitioning: {partitioning}")

    def __eq__(self, other: object) -> bool:
        """TableMetadata Equal."""
        if not isinstance(other, TableMetadata):
            return False

        return self._ref == other._ref and self._table_type == other._table_type

    def rename_table(self, name: str) -> None:
        """Rename table metadata's table name."""
        self._ref.table = name

    def load(self, tx: "Transaction") -> None:
        """Load/Reload table metadata."""
        self.load_stats(tx)
        self.load_schema(tx)

        if self._table_type in {TableType.Elysium, TableType.Partitioned, TableType.SortedPartitions}:
            self.load_sorted_columns(tx)

    def load_schema(self, tx: "Transaction") -> None:
        """Load/Reload table schema."""
        self.arrow_schema = tx._rpc.api.list_all_columns(
            self.ref.bucket,
            self.ref.schema,
            self.ref.table,
            sorted_columns=False,
            txid=tx.active_txid,
            list_imports_table=self.is_imports_table
        )

        # This is because ibis doesn't support fixed binary
        if self._ref.table.endswith("___VAST_PARTITIONS"):
            fields = [f for f in self.arrow_schema if f.name.startswith("keys_") and f.name != "keys_hash"]
            self.arrow_schema = pa.schema(fields, metadata=self.arrow_schema.metadata)

    def load_sorted_columns(self, tx: "Transaction") -> None:
        """Return sorted columns' metadata."""
        try:
            schema = tx._rpc.api.list_all_columns(
                self.ref.bucket,
                self.ref.schema,
                self.ref.table,
                sorted_columns=True,
                txid=tx.active_txid,
                list_imports_table=self.is_imports_table
            )
            self._sorted_columns = [f for f in schema]
        except errors.InternalServerError as ise:
            log.warning(
                "Failed to get the sorted columns Elysium might not be supported: %s",
                ise,
            )
            raise
        except errors.NotSupportedVersion:
            log.warning("Failed to get the sorted columns, Elysium not supported")
            raise

        partition_keys: list[PartitionKey] = []

        for sorted_column in self._sorted_columns:
            decoded_metadata = {k.decode(): v.decode() for k, v in sorted_column.metadata.items()}
            column_metadata = _ColumnMetadata.model_validate(decoded_metadata)

            if column_metadata.transform_name is not None:
                partition_keys.append(
                    PartitionKey(
                        column=sorted_column.name,
                        transform=parse_transform(column_metadata.transform_name),
                    )
                )

        self._partitioning = PartitionSpec(partition_keys=partition_keys)

    def load_stats(self, tx: "Transaction") -> None:
        """Load/Reload table stats."""
        self._stats = tx._rpc.api.get_table_stats(
            bucket=self.ref.bucket,
            schema=self.ref.schema,
            name=self.ref.table,
            txid=tx.active_txid,
            imports_table_stats=self.is_imports_table,
        )

        is_sorted = self._stats.sorting_key_enabled
        is_partitioned = self._stats.partitioning_info is not None

        if self._table_type is None:
            if is_sorted and is_partitioned:
                self._set_sorted_partitions_table(tx)
            elif is_sorted:
                self._set_sorted_table(tx)
            elif is_partitioned:
                self._set_partitioned_table(tx)
            else:
                self._set_regular_table()
        else:
            if is_sorted and self.table_type not in {TableType.Elysium, TableType.SortedPartitions}:
                raise ValueError(
                    "Actual table is sorted (TableType.Elysium / TableType.SortedPartitions), was not inited as such"
                )

            if is_partitioned and self.table_type not in {TableType.Partitioned, TableType.SortedPartitions}:
                raise ValueError(
                    "Actual table is partitioned (TableType.Partitioned / TableType.SortedPartitions), was not inited as such"
                )

        self._parse_stats_vector_index()

    def _parse_stats_vector_index(self):
        vector_index_is_set = self._vector_index is not None

        if vector_index_is_set and self._stats.vector_index != self._vector_index:
            raise ValueError(
                f"Table has index {self._stats.vector_index}, but was initialized as {self._vector_index}"
                )
        else:
            self._vector_index = self._stats.vector_index

    def _set_sorted_partitions_table(self, tx: "Transaction") -> None:
        self._table_type = TableType.SortedPartitions
        tx._rpc.features.check_elysium()
        tx._rpc.features.check_partitioned()

    def _set_sorted_table(self, tx: "Transaction") -> None:
        self._table_type = TableType.Elysium
        tx._rpc.features.check_elysium()

    def _set_partitioned_table(self, tx: "Transaction") -> None:
        self._table_type = TableType.Partitioned
        tx._rpc.features.check_partitioned()

    def _set_regular_table(self) -> None:
        self._table_type = TableType.Regular

    @property
    def partitioning(self) -> Optional[PartitionSpec]:
        """Get table's partitioning."""
        return self._partitioning

    @property
    def stats(self) -> Optional[TableStats]:
        """Get table's stats."""
        return self._stats

    @property
    def arrow_schema(self) -> pa.Schema:
        """Table's arrow schema."""
        return self._arrow_schema

    @arrow_schema.setter
    def arrow_schema(self, arrow_schema: Optional[pa.Schema]):
        """Set arrow schema."""
        if arrow_schema:
            validate_ibis_support_schema(arrow_schema)
            self._arrow_schema = arrow_schema
            self._ibis_table = ibis.table(
                ibis.Schema.from_pyarrow(arrow_schema), self._ref.full_path
            )
        else:
            self._arrow_schema = None
            self._ibis_table = None

    @property
    def sorted_columns(self) -> list[pa.Field]:
        """Sorted columns."""
        if self._sorted_columns is None:
            raise ValueError("sorted columns not loaded")
        return self._sorted_columns

    @property
    def ibis_table(self) -> ibis.Table:
        """Ibis table."""
        return self._ibis_table

    @property
    def ref(self) -> TableRef:
        """Table's reference."""
        return self._ref

    @property
    def table_type(self) -> TableType:
        """Table's type."""
        if self._table_type is None:
            raise ValueError(
                "TableType was not loaded. load using TableMetadata.load_stats"
            )

        return self._table_type

    @property
    def is_imports_table(self) -> bool:
        """Is table an imports table."""
        return self._table_type is TableType.TableImports
