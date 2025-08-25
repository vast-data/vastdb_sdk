from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Optional, Union

import ibis
import pyarrow as pa

from .config import ImportConfig, QueryConfig
from .table_metadata import TableRef

if TYPE_CHECKING:
    from .table import Projection


class ITable(ABC):
    """Interface for VAST Table operations."""

    @property
    @abstractmethod
    def ref(self) -> TableRef:
        """Return Table Ref."""
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        """Table __eq__."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Table name."""
        pass

    @property
    @abstractmethod
    def arrow_schema(self) -> pa.Schema:
        """Table arrow schema."""
        pass

    @property
    @abstractmethod
    def path(self) -> str:
        """Return table's path."""
        pass

    @abstractmethod
    def sorted_columns(self) -> list[str]:
        """Return sorted columns' names."""
        pass

    @abstractmethod
    def projection(self, name: str) -> "Projection":
        """Get a specific semi-sorted projection of this table."""
        pass

    @abstractmethod
    def projections(self, projection_name: str = "") -> Iterable["Projection"]:
        """List semi-sorted projections."""
        pass

    @abstractmethod
    def import_files(self, files_to_import: Iterable[str], config: Optional[ImportConfig] = None) -> None:
        """Import files into table."""
        pass

    @abstractmethod
    def import_partitioned_files(self, files_and_partitions: dict[str, pa.RecordBatch], config: Optional[ImportConfig] = None) -> None:
        """Import partitioned files."""
        pass

    @abstractmethod
    def select(self,
               columns: Optional[list[str]] = None,
               predicate: Union[ibis.expr.types.BooleanColumn,
                                ibis.common.deferred.Deferred] = None,
               config: Optional[QueryConfig] = None,
               *,
               internal_row_id: bool = False,
               limit_rows: Optional[int] = None) -> pa.RecordBatchReader:
        """Execute a query."""
        pass

    @abstractmethod
    def insert(self, rows: Union[pa.RecordBatch, pa.Table]) -> pa.ChunkedArray:
        """Insert rows into table."""
        pass

    @abstractmethod
    def update(self,
               rows: Union[pa.RecordBatch, pa.Table],
               columns: Optional[list[str]] = None) -> None:
        """Update rows in table."""
        pass

    @abstractmethod
    def delete(self, rows: Union[pa.RecordBatch, pa.Table]) -> None:
        """Delete rows from table."""
        pass

    @abstractmethod
    def imports_table(self) -> Optional["ITable"]:
        """Get imports table."""
        pass

    @abstractmethod
    def sorting_done(self) -> bool:
        """Check if sorting is done."""
        pass

    @abstractmethod
    def sorting_score(self) -> int:
        """Get sorting score."""
        pass

    @abstractmethod
    def reload_schema(self) -> None:
        """Reload Arrow Schema."""
        pass

    @abstractmethod
    def reload_stats(self) -> None:
        """Reload Table Stats."""
        pass

    @abstractmethod
    def reload_sorted_columns(self) -> None:
        """Reload Sorted Columns."""
        pass

    @abstractmethod
    def __getitem__(self, col_name: str) -> ibis.Column:
        """Allow constructing ibis-like column expressions from this table.

        It is useful for constructing expressions for predicate pushdown in `ITable.select()` method.
        """
        pass
