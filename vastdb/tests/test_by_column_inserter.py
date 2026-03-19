import itertools
from typing import Any

import pyarrow as pa
import pytest

from vastdb import _by_column_inserter, _internal, errors, util
from vastdb._by_column_inserter import Inserter

_SENTINEL_FOR_EMPTY_CELL = object()


class MockClient:
    def __init__(self, max_request_body_size: int, row_ids_type: pa.DataType):
        self.max_request_body_size = max_request_body_size
        self._next_row_id = 0
        self._row_ids_type = row_ids_type
        self.insert_calls: list[pa.RecordBatch] = []
        self.update_calls: list[pa.RecordBatch] = []

    def _validate_request(self, batch: pa.RecordBatch) -> None:
        assert len(util.serialize_record_batch(batch)) <= self.max_request_body_size
        assert batch.num_rows >= 1

    def insert_rows(self, batch: pa.RecordBatch) -> pa.Array:
        self._validate_request(batch)
        self.insert_calls.append(batch)
        row_ids = pa.array(range(self._next_row_id, self._next_row_id + batch.num_rows), type=self._row_ids_type)
        self._next_row_id += batch.num_rows
        return row_ids

    def update_rows(self, batch: pa.RecordBatch) -> None:
        self._validate_request(batch)
        assert batch.num_columns >= _by_column_inserter._MIN_NUM_COLUMNS_FOR_UPDATE
        self.update_calls.append(batch)


def create_batch(num_rows: int, num_cols: int) -> pa.RecordBatch:
    field_types_pattern = (
        pa.int64(),
        pa.string(),
        pa.struct({"nested0": pa.int64(), "nested1": pa.int64()}),
        pa.list_(pa.int64()),
    )
    value_generators = (
        lambda cell_index: cell_index,
        lambda cell_index: f"str{cell_index}",
        lambda cell_index: {"nested0": cell_index, "nested1": cell_index + 1},
        lambda cell_index: [cell_index, cell_index + 1],
    )
    pattern_length = len(field_types_pattern)
    schema = pa.schema(pa.field(f"col{c}", field_types_pattern[c % pattern_length]) for c in range(num_cols))
    arrays = [
        pa.array(value_generators[c % pattern_length](c * num_rows + r) for r in range(num_rows))
        for c in range(num_cols)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def test_scenarios() -> list[tuple[pa.RecordBatch, set[str], float]]:
    batch_width_and_height_cases = (
        (0, 1),
        (256, 1),
        (1, 256),
        (256, 3),
        (3, 256),
        (257, 4),
        (4, 257),
        (10, 10),
        (50, 50),
        (100, 100),
        (101, 101),
        (200, 200),
        (200, 400),
        (400, 200),
        (401, 201),
    )
    non_updatable_names_cases: tuple[set[str], ...] = (set(), {"col0"}, {"col0", "col1"})
    buffer_size_factor_cases = (1.5, 1.0, 0.75, 0.5, 0.25, 0.1)
    all_cases = itertools.product(batch_width_and_height_cases, non_updatable_names_cases, buffer_size_factor_cases)
    valid_cases = []

    for (num_rows, num_columns), non_updatable_names, buffer_size_factor in all_cases:
        batch = create_batch(num_rows, num_columns)

        if any(batch.schema.get_field_index(name) == -1 for name in non_updatable_names):
            # Skip scenarios where non updatable names do not agree with schema
            continue

        valid_cases.append((batch, non_updatable_names, buffer_size_factor))

    return valid_cases


@pytest.mark.parametrize("batch, non_updatable_names, buffer_size_factor", list(test_scenarios()))
def test_insert(batch: pa.RecordBatch, non_updatable_names: set[str], buffer_size_factor: float):
    max_request_body_size = int(buffer_size_factor * len(util.serialize_record_batch(batch)))
    row_ids_field = _by_column_inserter._internal_row_id_field(non_updatable_names)
    mock = MockClient(max_request_body_size, row_ids_field.type)
    inserter = Inserter(
        mock.insert_rows,
        mock.update_rows,
        max_request_body_size,
        batch.schema,
        non_updatable_names,
    )

    if len(non_updatable_names) == 0:
        non_updatable_names = {batch.schema.field(0).name}

    serialized_batch_length = len(util.serialize_record_batch(batch))
    serialized_non_updatable_batch_length = len(util.serialize_record_batch(batch.select(non_updatable_names)))

    try:
        row_ids = inserter.insert(batch).combine_chunks()
        assert row_ids.to_pylist() == [i for i in range(batch.num_rows)]
        assert mock._next_row_id == batch.num_rows

        match (len(mock.insert_calls), len(mock.update_calls)):
            case (0, 0):
                assert batch.num_rows == 0 or batch.num_columns == 0
            case (1, 0):
                assert serialized_batch_length <= inserter._adjusted_max_request_body_size
            case (_, 0):
                assert batch.num_columns == len(non_updatable_names)
                assert (
                    serialized_non_updatable_batch_length > inserter._adjusted_max_request_body_size
                    or batch.num_rows > inserter._max_rows_per_insert
                )
            case (0, _):
                assert False, "Can't have updates without inserts"
            case (1, _):
                assert serialized_batch_length > inserter._adjusted_max_request_body_size
                assert serialized_non_updatable_batch_length <= inserter._adjusted_max_request_body_size
            case (_, 1):
                assert (
                    serialized_non_updatable_batch_length > inserter._adjusted_max_request_body_size
                    or batch.num_rows > inserter._max_rows_per_insert
                )
                assert (
                    serialized_batch_length - serialized_non_updatable_batch_length + row_ids.nbytes
                    <= inserter._adjusted_max_request_body_size
                )
            case (_, _):
                assert (
                    serialized_non_updatable_batch_length > inserter._adjusted_max_request_body_size
                    or batch.num_rows > inserter._max_rows_per_insert
                )

        received_batch = {f.name: [_SENTINEL_FOR_EMPTY_CELL for _ in range(batch.num_rows)] for f in batch.schema}
        rows_offset = 0

        for batch_for_insert in mock.insert_calls:
            assert batch_for_insert.num_rows <= inserter._max_rows_per_insert
            assert all(batch_for_insert.schema.get_field_index(name) != -1 for name in non_updatable_names)
            assert batch_for_insert.num_columns >= max(len(non_updatable_names), 1)
            _insert_batch_to(batch_for_insert, received_batch, rows_offset)
            rows_offset += batch_for_insert.num_rows

        for batch_for_update in mock.update_calls:
            _update_batch_in(batch_for_update, received_batch)

        _assert_all_the_data_was_received(batch, received_batch)
    except errors.TooWideRow:
        _assert_batch_can_not_be_inserted(
            batch, non_updatable_names, inserter._adjusted_max_request_body_size, row_ids_field
        )


def _assert_all_the_data_was_received(batch: pa.RecordBatch, received_batch: dict[str, list]) -> None:
    arrays = [pa.array(received_batch[f.name], type=f.type) for f in batch.schema]
    returned_batch = pa.RecordBatch.from_arrays(arrays, schema=batch.schema)
    assert returned_batch.equals(batch)


def _insert_batch_to(batch: pa.RecordBatch, received_batch: dict[str, list[Any]], row_offset: int) -> None:
    for field, column in zip(batch.schema, batch.columns):
        col = received_batch[field.name]
        assert all(val is _SENTINEL_FOR_EMPTY_CELL for val in col[row_offset : row_offset + batch.num_rows])
        col[row_offset : row_offset + batch.num_rows] = column.to_pylist()


def _update_batch_in(batch: pa.RecordBatch, received_batch: dict[str, list[Any]]) -> None:
    row_ids_field_index = batch.schema.get_field_index(_internal.INTERNAL_ROW_ID)
    assert row_ids_field_index != -1
    row_ids = batch.column(row_ids_field_index).to_pylist()
    updated_columns_batch = batch.select(i for i in range(batch.num_columns) if i != row_ids_field_index)

    for field, column in zip(updated_columns_batch.schema, updated_columns_batch.columns):
        col = received_batch[field.name]

        for row_id, value in zip(row_ids, column.to_pylist()):
            int_row_id = int(row_id)
            assert col[int_row_id] is _SENTINEL_FOR_EMPTY_CELL
            col[int_row_id] = value


def _assert_batch_can_not_be_inserted(
    batch: pa.RecordBatch, non_updatable_names: set[str], max_request_size: int, row_ids_field: pa.DataType
) -> None:
    can_not_insert = False
    can_not_update = False
    non_updatable_batch = batch.select(non_updatable_names)

    for i in range(non_updatable_batch.num_rows):
        min_batch_for_insert = non_updatable_batch.slice(i, i + 1)
        can_not_insert |= len(util.serialize_record_batch(min_batch_for_insert)) > max_request_size

    updatable_names = set(batch.schema.names) - non_updatable_names
    updatable_batch = batch.select(updatable_names).add_column(
        0, _internal.INTERNAL_ROW_ID, pa.array(range(batch.num_rows), type=row_ids_field.type)
    )

    for col in range(1, updatable_batch.num_columns):
        for row in range(updatable_batch.num_rows):
            min_batch_for_update = updatable_batch.select((0, col)).slice(row, row + 1)
            can_not_update |= len(util.serialize_record_batch(min_batch_for_update)) > max_request_size

    assert can_not_insert or can_not_update
