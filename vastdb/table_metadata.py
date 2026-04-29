"""VAST Database table metadata."""

import json
import logging
import warnings
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Optional

import ibis
import pyarrow as pa
import pydantic
from pyiceberg.transforms import Transform, parse_transform

from vastdb import errors
from vastdb._ibis_support import validate_ibis_support_schema
from vastdb._internal import TableStats, VectorIndex
from vastdb.partitioning import PartitionKey, PartitionSpec

from . import _internal

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


class _PartitionColumnMetadata(pydantic.BaseModel):
    column_name: str
    column_type: str
    source_column_name: str
    transform: str
    arg: Optional[int] = pydantic.Field(default=None)

    def as_iceberg_transform(self) -> Transform:
        name = self.transform.lower()

        if name in {_internal.VAST_BUCKET_PARTITION_TRANSFORM_NAME, _internal.VAST_TRUNCATE_PARTITION_TRANSFORM_NAME}:
            transform_name_in_iceberg_format = f"{name}[{self.arg}]"
        else:
            transform_name_in_iceberg_format = name

        return parse_transform(transform_name_in_iceberg_format)


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


@dataclass
class PartitionsMetadata:
    """Partitions Metadata.

    Used in order to keep the server as the source of truth for post transform column names
    """

    spec: PartitionSpec
    post_transform_names: list[str]

    def __post_init__(self):
        """Validate params."""
        if len(self.spec.partition_keys) != len(self.post_transform_names):
            raise ValueError("Length and order of post_transform_names must match the number of keys in spec")


class TableMetadata:
    """Table Metadata."""

    _ref: TableRef
    _arrow_schema: Optional[pa.Schema]
    _sorted_columns: Optional[list[pa.Field]]
    _ibis_table: ibis.Table
    _stats: Optional[TableStats]
    _vector_index: Optional[VectorIndex]
    _partitions_metadata: Optional[PartitionsMetadata]

    def __init__(
        self,
        ref: TableRef,
        arrow_schema: Optional[pa.Schema] = None,
        table_type: Optional[TableType] = None,
        vector_index: Optional[VectorIndex] = None,
        partitions_metadata: Optional[PartitionsMetadata] = None,
    ):
        """Table Metadata."""
        self._ref = deepcopy(ref)
        self._table_type = table_type
        self.arrow_schema = deepcopy(arrow_schema)
        self._sorted_columns = None
        self._stats = None
        self._vector_index = vector_index
        self._partitions_metadata = partitions_metadata

        if table_type not in {TableType.SortedPartitions, TableType.Partitioned, None} and partitions_metadata is not None:
            raise ValueError(f"Only a partitioned table may have partitions metadata. Given TableType: {table_type}")

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
        self.load_schema(tx)

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

        if self.is_imports_table:
            return

        self._vector_index = self._load_vector_index(tx)

        if self._vector_index is not None:
            tx._rpc.features.check_vector()
            self._table_type = TableType.Regular
            return

        self._sorted_columns = self._load_sorted_columns(self.arrow_schema, tx)
        self._partitions_metadata = TableMetadata._parse_partition_spec(self.arrow_schema)

        match (self._sorted_columns, self._partitions_metadata):
            case (None, None):
                self._table_type = TableType.Regular
            case (None, _):
                self._table_type = TableType.Partitioned
            case (_, None):
                self._table_type = TableType.Elysium
            case (_, _):
                self._table_type = TableType.SortedPartitions

    def _load_vector_index(self, tx: "Transaction") -> Optional[VectorIndex]:
        self._stats = tx._rpc.api.get_table_stats(
            bucket=self.ref.bucket,
            schema=self.ref.schema,
            name=self.ref.table,
            txid=tx.active_txid,
            imports_table_stats=self.is_imports_table,
        )
        vector_index_is_set = self._vector_index is not None

        if vector_index_is_set and self._stats.vector_index != self._vector_index:
            raise ValueError(
                f"Table has index {self._stats.vector_index}, but was initialized as {self._vector_index}"
                )

        return self._stats.vector_index

    def _load_sorted_columns(self, schema: pa.Schema, tx: "Transaction") -> Optional[list[pa.Field]]:
        if tx._rpc.features.vast_version >= (5, 5):
            sorted_columns = [f for f in schema if _internal.COLUMN_METADATA_SORTED == f.metadata.get(_internal.COLUMN_METADATA_SORT_TYPE_PROPERTY)]
            sorted_columns.sort(key=lambda f: f.metadata[_internal.COLUMN_METADATA_SORT_INDEX_PROPERTY])
            return sorted_columns if len(sorted_columns) > 0 else None

        try:
            schema = tx._rpc.api.list_all_columns(
                self.ref.bucket,
                self.ref.schema,
                self.ref.table,
                sorted_columns=True,
                txid=tx.active_txid,
                list_imports_table=self.is_imports_table
            )
            sorted_columns = [f for f in schema]
            return sorted_columns if len(sorted_columns) > 0 else None
        except errors.InternalServerError as e:
            log.debug("Returning no sorted columns after silencing: %s", e)
            return None
        except errors.InvalidArgument as e:
            log.debug("Returning no sorted columns after silencing: %s", e)
            return None
        except errors.BadRequest as e:
            log.debug("Returning no sorted columns after silencing: %s", e)
            return None
        except errors.NotSupportedVersion as e:
            log.debug("Returning no sorted columns after silencing: %s", e)
            return None

    @staticmethod
    def _parse_partition_spec(schema: pa.Schema) -> Optional[PartitionsMetadata]:
        if (partition_spec_json := schema.metadata.get(_internal.TABLE_METADATA_PARTITION_SPEC_PROPERTY)) is None:
            return None

        partition_keys: list[PartitionKey] = []
        post_transform_names = []
        partitioning: list[dict[str, str]] = json.loads(partition_spec_json)

        for partition_column in partitioning:
            column_metadata = _PartitionColumnMetadata.model_validate(partition_column)
            post_transform_names.append(column_metadata.column_name)
            partition_keys.append(
                PartitionKey(
                    column_metadata.source_column_name,
                    column_metadata.as_iceberg_transform(),
                )
            )

        if len(partition_keys) == 0:
            raise AssertionError("Server returned an empty partition spec")

        return PartitionsMetadata(PartitionSpec(partition_keys=partition_keys), post_transform_names)

    def load_sorted_columns(self, tx: "Transaction") -> None:
        """Return sorted columns' metadata."""
        warnings.warn(
            "load_sorted_columns is deprecated and will be removed in vastdb 3.0.0, use load_schema instead",
            DeprecationWarning,
            stacklevel=4
        )

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

            if self._table_type in {None, TableType.Regular}:
                self._table_type = TableType.Elysium
        except errors.InternalServerError as ise:
            log.warning(
                "Failed to get the sorted columns Elysium might not be supported: %s",
                ise,
            )
            raise
        except errors.NotSupportedVersion:
            log.warning("Failed to get the sorted columns, Elysium not supported")
            raise

    def load_stats(self, tx: "Transaction") -> None:
        """Load/Reload table stats."""
        self._stats = tx._rpc.api.get_table_stats(
            bucket=self.ref.bucket,
            schema=self.ref.schema,
            name=self.ref.table,
            txid=tx.active_txid,
            imports_table_stats=self.is_imports_table,
        )

    @property
    def partitioning(self) -> Optional[PartitionsMetadata]:
        """Get table's partitions metadata."""
        return self._partitions_metadata

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
