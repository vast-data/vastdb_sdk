"""VAST Database table metadata."""

import logging
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Optional, Tuple

import ibis
import pyarrow as pa

from vastdb import errors
from vastdb._ibis_support import validate_ibis_support_schema

if TYPE_CHECKING:
    from .transaction import Transaction

log = logging.getLogger(__name__)


class TableType(Enum):
    """Table Type."""

    Regular = 1
    Elysium = 2
    TableImports = 3


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

    def __str__(self) -> str:
        """Table full path."""
        return self.full_path


@dataclass
class TableStats:
    """Table-related information."""

    num_rows: int
    size_in_bytes: int
    sorting_score: int
    write_amplification: int
    acummulative_row_inserition_count: int
    is_external_rowid_alloc: bool = False
    sorting_key_enabled: bool = False
    sorting_done: bool = False
    endpoints: Tuple[str, ...] = ()


class TableMetadata:
    """Table Metadata."""

    _ref: TableRef
    _arrow_schema: Optional[pa.Schema]
    _sorted_columns: Optional[list[str]]
    _ibis_table: ibis.Table
    _stats: Optional[TableStats]

    def __init__(self,
                 ref: TableRef,
                 arrow_schema: Optional[pa.Schema] = None,
                 table_type: Optional[TableType] = None):
        """Table Metadata."""
        self._ref = deepcopy(ref)
        self._table_type = table_type
        self.arrow_schema = deepcopy(arrow_schema)
        self._sorted_columns = None
        self._stats = None

    def __eq__(self, other: object) -> bool:
        """TableMetadata Equal."""
        if not isinstance(other, TableMetadata):
            return False

        return (self._ref == other._ref and
                self._table_type == other._table_type)

    def rename_table(self, name: str) -> None:
        """Rename table metadata's table name."""
        self._ref.table = name

    def load(self, tx: "Transaction") -> None:
        """Load/Reload table metadata."""
        self.load_stats(tx)
        self.load_schema(tx)

        if self._table_type is TableType.Elysium:
            self.load_sorted_columns(tx)

    def load_schema(self, tx: "Transaction") -> None:
        """Load/Reload table schema."""
        fields = []
        next_key = 0
        while True:
            cur_columns, next_key, is_truncated, _count = tx._rpc.api.list_columns(
                bucket=self.ref.bucket,
                schema=self.ref.schema,
                table=self.ref.table,
                next_key=next_key,
                txid=tx.active_txid,
                list_imports_table=self.is_imports_table)
            fields.extend(cur_columns)
            if not is_truncated:
                break

        self.arrow_schema = pa.schema(fields)

    def load_sorted_columns(self, tx: "Transaction") -> None:
        """Return sorted columns' metadata."""
        fields = []
        try:
            next_key = 0
            while True:
                cur_columns, next_key, is_truncated, _count = tx._rpc.api.list_sorted_columns(
                    bucket=self.ref.bucket, schema=self.ref.schema, table=self.ref.table,
                    next_key=next_key, txid=tx.active_txid, list_imports_table=self.is_imports_table)
                fields.extend(cur_columns)
                if not is_truncated:
                    break
        except errors.BadRequest:
            raise
        except errors.InternalServerError as ise:
            log.warning(
                "Failed to get the sorted columns Elysium might not be supported: %s", ise)
            raise
        except errors.NotSupportedVersion:
            log.warning("Failed to get the sorted columns, Elysium not supported")
            raise
        finally:
            self._sorted_columns = fields

    def load_stats(self, tx: "Transaction") -> None:
        """Load/Reload table stats."""
        stats_tuple = tx._rpc.api.get_table_stats(
            bucket=self.ref.bucket, schema=self.ref.schema, name=self.ref.table, txid=tx.active_txid,
            imports_table_stats=self.is_imports_table)
        self._stats = TableStats(**stats_tuple._asdict())

        is_elysium_table = self._stats.sorting_key_enabled

        if self._table_type is None:
            if is_elysium_table:
                self._set_sorted_table(tx)
            else:
                self._set_regular_table()
        else:
            if is_elysium_table and self.table_type is not TableType.Elysium:
                raise ValueError(
                    "Actual table is sorted (TableType.Elysium), was not inited as TableType.Elysium"
                )

    def _set_sorted_table(self, tx: "Transaction"):
        self._table_type = TableType.Elysium
        tx._rpc.features.check_elysium()

    def _set_regular_table(self):
        self._table_type = TableType.Regular

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
            self._ibis_table = ibis.table(ibis.Schema.from_pyarrow(arrow_schema), self._ref.full_path)
        else:
            self._arrow_schema = None
            self._ibis_table = None

    @property
    def sorted_columns(self) -> list:
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
                "TableType was not loaded. load using TableMetadata.load_stats")

        return self._table_type

    @property
    def is_imports_table(self) -> bool:
        """Is table an imports table."""
        return self._table_type is TableType.TableImports
