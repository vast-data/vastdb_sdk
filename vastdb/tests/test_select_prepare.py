"""Unit tests for TableInTransaction._select_prepare that don't need a cluster."""
from types import SimpleNamespace

import pyarrow as pa
import pytest

from vastdb.config import QueryConfig
from vastdb.table import INTERNAL_ROW_ID, TableInTransaction, _EmptyResultException

_SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.utf8())])


def _fake_table() -> SimpleNamespace:
    fake = SimpleNamespace(
        _internal_rowid_field=pa.field(INTERNAL_ROW_ID, pa.uint64()),
        _metadata=SimpleNamespace(ibis_table=None),
        _is_sorted_table=False,
        _tx=SimpleNamespace(_rpc=SimpleNamespace(features=None)),
    )
    fake._get_split_estimation = lambda *a, **k: TableInTransaction._get_split_estimation(fake, *a, **k)  # type: ignore[arg-type]
    return fake


def _config() -> QueryConfig:
    return QueryConfig(num_splits=1, data_endpoints=["http://localhost"])


def test_internal_row_id_does_not_modify_callers_columns():
    fake = _fake_table()
    columns = ["a"]

    for _ in range(2):
        TableInTransaction._select_prepare(fake, _SCHEMA, _config(), columns, None, internal_row_id=True)  # type: ignore[arg-type]

    assert columns == ["a"]


def test_zero_limit_returns_empty_result():
    fake = _fake_table()

    with pytest.raises(_EmptyResultException) as e:
        TableInTransaction._select_prepare(fake, _SCHEMA, _config(), ["a"], None, limit_rows=0)  # type: ignore[arg-type]

    assert e.value.response_schema == pa.schema([("a", pa.int64())])


def test_positive_limit_is_used_per_sub_split():
    fake = _fake_table()

    split_config, _, _ = TableInTransaction._select_prepare(fake, _SCHEMA, _config(), ["a"], None, limit_rows=5)  # type: ignore[arg-type]

    assert split_config.limit_rows_per_sub_split == 5
