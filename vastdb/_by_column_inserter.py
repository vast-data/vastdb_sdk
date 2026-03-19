import itertools
import logging
from collections.abc import Callable, Iterable
from enum import Enum, auto
from typing import Optional

import pyarrow as pa

from . import _internal
from .errors import TooWideRow

log = logging.getLogger(__name__)

_ARROW_IPC_SCHEMA_ALIGNMENT_OVERHEAD = 8
_MAX_REQUEST_SIZE_SAFTEY_MARGIN = 0.98
_MAX_ROWS_PER_INSERT_SAFTEY_MARGIN = 0.95
# $row_id and another column
_MIN_NUM_COLUMNS_FOR_UPDATE = 2


class _SliceKind(Enum):
    HORIZONTAL = auto()
    VERTICAL = auto()


class Inserter:
    """Insert data by splitting columns between initial and subsequent UPDATEs.

    Priority:
    1. Insert entire batch in one rpc
    2. Insert at least the non updatable columns in one rpc to get row ids (split rows if needed to multiple rpcs)
    3. update the rest of the columns (using row ids). Prefer full columns if possible, otherwise split by rows
    """

    def __init__(
        self,
        insert_rows_rpc: Callable[[pa.RecordBatch], pa.Array],
        update_rows_rpc: Callable[[pa.RecordBatch], None],
        max_request_body_size: int,
        schema: pa.Schema,
        non_updatable_field_names: set[str],
    ):
        self._insert_rows_rpc = insert_rows_rpc
        self._update_rows_rpc = update_rows_rpc
        self._adjusted_max_request_body_size = int(_MAX_REQUEST_SIZE_SAFTEY_MARGIN * max_request_body_size)
        self._schema = pa.schema(
            itertools.chain(
                (field for field in schema if field.name in non_updatable_field_names),
                (field for field in schema if field.name not in non_updatable_field_names),
            )
        )
        self._internal_row_id_field = _internal_row_id_field(non_updatable_field_names)
        self._num_non_updatable_fields = max(len(non_updatable_field_names), 1)
        self._max_rows_per_insert = (
            _MAX_ROWS_PER_INSERT_SAFTEY_MARGIN
            * self._adjusted_max_request_body_size
            / self._internal_row_id_field.type.byte_width
        )
        log.debug(
            f"Inserter config: {self._adjusted_max_request_body_size=}, {self._max_rows_per_insert=}, {non_updatable_field_names=}"
        )

    def insert(self, batch: pa.RecordBatch) -> pa.ChunkedArray:
        log.debug(f"Inserting new batch: {batch.num_columns=}, {batch.num_rows=}")

        if batch.num_rows == 0 or batch.num_columns == 0:
            log.debug("Batch is empty returning empty row ids")
            return pa.chunked_array([], type=self._internal_row_id_field.type)

        if batch.num_rows <= self._max_rows_per_insert and self._can_fit_in_one_rpc(
            batch, self._schema.serialize().size
        ):
            log.debug("Batch fits in one rpc, inserting it as is")
            return pa.chunked_array([self._insert_rows_rpc(batch)])

        batch = batch.select(self._schema.names)
        row_ids, num_columns_inserted = self._insert_to_get_row_ids(batch)
        log.debug(f"Managed to insert {num_columns_inserted} columns while getting row ids")

        batch_for_updates = batch.select(range(num_columns_inserted, batch.num_columns)).add_column(
            0, _internal.INTERNAL_ROW_ID, row_ids
        )
        self._update_columns(batch_for_updates)
        log.debug("Insert of batch completed")
        return row_ids

    def _insert_to_get_row_ids(self, batch: pa.RecordBatch) -> tuple[pa.ChunkedArray, int]:
        batch_to_send = self._slice_that_fits_in_one_rpc(
            batch,
            _SliceKind.VERTICAL,
            min_to_slice=self._num_non_updatable_fields + 1,
        )
        row_ids_arrays = []

        if batch_to_send is not None and batch_to_send.num_rows <= self._max_rows_per_insert:
            log.debug(f"Inserting {batch_to_send.num_columns} columns without splitting rows")
            row_ids_arrays.append(self._insert_rows_rpc(batch_to_send))
        else:
            log.debug("Inserting only non updatable columns while splitting rows")
            batch_to_send = batch.select(range(self._num_non_updatable_fields))

            for batch_slice in self._split_by_rows(batch_to_send, self._max_rows_per_insert):
                row_ids_arrays.append(self._insert_rows_rpc(batch_slice))

        return pa.chunked_array(row_ids_arrays), batch_to_send.num_columns

    def _update_columns(self, batch: pa.RecordBatch) -> None:
        while batch.num_columns >= _MIN_NUM_COLUMNS_FOR_UPDATE:
            batch_to_send = self._slice_that_fits_in_one_rpc(
                batch, _SliceKind.VERTICAL, min_to_slice=_MIN_NUM_COLUMNS_FOR_UPDATE + 1
            )

            if batch_to_send is not None:
                log.debug(f"Updating {batch_to_send.num_columns} columns without splitting rows")
                self._update_rows_rpc(batch_to_send)
                next_column_offset = batch_to_send.num_columns
            else:
                log.debug("Updating the minimum of row ids and another column while splitting rows")
                batch_to_send = batch.select(range(_MIN_NUM_COLUMNS_FOR_UPDATE))
                next_column_offset = _MIN_NUM_COLUMNS_FOR_UPDATE

                for batch_slice in self._split_by_rows(batch_to_send):
                    self._update_rows_rpc(batch_slice)

            batch = batch.select(itertools.chain([0], range(next_column_offset, batch.num_columns)))

    def _split_by_rows(self, batch: pa.RecordBatch, max_rows_per_slice: Optional[int] = None) -> Iterable[bytes]:
        while batch.num_rows > 0:
            batch_to_send = self._slice_that_fits_in_one_rpc(
                batch, _SliceKind.HORIZONTAL, max_to_slice=max_rows_per_slice
            )

            if batch_to_send is None:
                raise TooWideRow("One of the rows in the given batch is too big for an rpc")

            batch = batch.slice(batch_to_send.num_rows, batch.num_rows)
            yield batch_to_send

    def _slice_that_fits_in_one_rpc(
        self,
        batch: pa.RecordBatch,
        slice_kind: _SliceKind,
        min_to_slice: int = 1,
        max_to_slice: Optional[int] = None,
    ) -> Optional[pa.RecordBatch]:
        estimated_schema_size = batch.schema.serialize().size if slice_kind is _SliceKind.HORIZONTAL else None
        batch_length = batch.num_columns if slice_kind is _SliceKind.VERTICAL else batch.num_rows

        if max_to_slice is None:
            amount_to_try = batch_length
        else:
            amount_to_try = min(max_to_slice, batch_length)

        while min_to_slice <= amount_to_try:
            batch_to_check = (
                batch.select(range(amount_to_try))
                if slice_kind is _SliceKind.VERTICAL
                else batch.slice(0, amount_to_try)
            )

            if self._can_fit_in_one_rpc(batch_to_check, estimated_schema_size):
                return batch_to_check

            if amount_to_try == min_to_slice:
                break

            amount_to_try = (min_to_slice + amount_to_try) // 2

        return None

    def _can_fit_in_one_rpc(
        self,
        batch: pa.RecordBatch,
        serialized_schema_size: Optional[int] = None,
    ) -> bool:
        if serialized_schema_size is None:
            serialized_schema_size = batch.schema.serialize().size

        estimated_batch_size = pa.ipc.get_record_batch_size(batch)
        estimated_size = serialized_schema_size + estimated_batch_size + _ARROW_IPC_SCHEMA_ALIGNMENT_OVERHEAD
        return estimated_size <= self._adjusted_max_request_body_size


def _internal_row_id_field(non_updatable_field_names: set[str]) -> pa.DataType:
    return (
        _internal.INTERNAL_ROW_ID_FIELD
        if len(non_updatable_field_names) == 0
        else _internal.INTERNAL_ROW_ID_SORTED_FIELD
    )
