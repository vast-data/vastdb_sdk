from dataclasses import dataclass
from typing import Generator, Optional

import ibis
import pyarrow as pa
import pytest

from vastdb.session import Session
from vastdb.table import INTERNAL_ROW_ID, ITable
from vastdb.table_metadata import TableMetadata, TableRef, TableType
from vastdb.transaction import Transaction

from .util import compare_pyarrow_tables, prepare_data_get_tx


def test_sanity(session: Session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data_get_tx(session, clean_bucket_name, 's', 't', expected) as tx:
        ref = TableRef(clean_bucket_name, 's', 't')
        table_md = TableMetadata(ref, columns, TableType.Regular)

        table_md.load_stats(tx)

        t = tx.table_from_metadata(table_md)

        actual = t.select(columns=['a', 'b', 's']).read_all()
        assert actual == expected


@dataclass
class SimpleDbSetup:
    tx: Transaction
    ref: TableRef
    table_type: TableType
    arrow_schema: Optional[pa.Schema] = None


@pytest.fixture(scope="function")
def simple_db_setup(session: Session, clean_bucket_name: str) -> Generator[SimpleDbSetup, None, None]:
    arrow_schema = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=arrow_schema, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data_get_tx(session, clean_bucket_name, 's', 't', expected) as tx:
        yield SimpleDbSetup(tx=tx,
                         arrow_schema=arrow_schema,
                         ref=TableRef(clean_bucket_name, 's', 't'),
                         table_type=TableType.Regular)


def test_schema_load_through_metadata(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(simple_db_setup.ref,
                             table_type=simple_db_setup.table_type)

    table = simple_db_setup.tx.table_from_metadata(table_md)
    assert table.arrow_schema is None
    table.reload_schema()
    assert table.arrow_schema is not None


def test_metadata_init_with_schema(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)

    table = simple_db_setup.tx.table_from_metadata(table_md)
    assert table.arrow_schema is not None


def test_path(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)

    table = simple_db_setup.tx.table_from_metadata(table_md)
    assert table.path == simple_db_setup.ref.full_path


def test_name(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)
    table = simple_db_setup.tx.table_from_metadata(table_md)
    assert table.name == simple_db_setup.ref.table


def test_arrow_schema(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)
    table = simple_db_setup.tx.table_from_metadata(table_md)
    assert table.arrow_schema == simple_db_setup.arrow_schema


def test_eq(simple_db_setup: SimpleDbSetup):
    table_md1 = TableMetadata(ref=simple_db_setup.ref, table_type=simple_db_setup.table_type)
    table1 = simple_db_setup.tx.table_from_metadata(table_md1)

    table_md2 = TableMetadata(ref=simple_db_setup.ref, table_type=simple_db_setup.table_type)
    table2 = simple_db_setup.tx.table_from_metadata(table_md2)

    assert table1 == table2

    other_ref = TableRef(simple_db_setup.ref.bucket, simple_db_setup.ref.schema, "other_table")
    table_md3 = TableMetadata(ref=other_ref, table_type=simple_db_setup.table_type)
    table3 = simple_db_setup.tx.table_from_metadata(table_md3)
    assert table1 != table3


def test_insert_and_select(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)
    table_md.load_stats(simple_db_setup.tx)  # the next select requires stats loaded

    table = simple_db_setup.tx.table_from_metadata(table_md)

    initial_data = table.select().read_all()

    assert initial_data.num_rows == 3

    new_rows = pa.table(schema=simple_db_setup.arrow_schema, data=[[444], [4.5], ["dddd"]])
    table.insert(new_rows)

    all_data = table.select().read_all()
    assert all_data.num_rows == 4

    t = ibis.table(table.arrow_schema, name=table.name)
    reader = table.select(predicate=t.a > 300)
    filtered_data = reader.read_all()
    assert filtered_data.num_rows == 2


def test_sorting_status(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref, table_type=simple_db_setup.table_type)
    table = simple_db_setup.tx.table_from_metadata(table_md)

    is_done = table.sorting_done()
    assert isinstance(is_done, bool)

    score = table.sorting_score()
    assert isinstance(score, int)


def test_projections(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref, table_type=simple_db_setup.table_type)
    table: ITable = simple_db_setup.tx.table_from_metadata(table_md)

    ref = simple_db_setup.ref
    legacy_table = simple_db_setup.tx.bucket(ref.bucket).schema(ref.schema).table(ref.table)

    initial_projections = list(table.projections())
    proj_name = "my_proj"
    proj = legacy_table.create_projection(
        projection_name=proj_name, sorted_columns=["a"], unsorted_columns=["s"]
    )
    assert proj.name == proj_name

    retrieved_proj = table.projection(proj_name)
    assert retrieved_proj == proj

    all_projections = list(table.projections())
    assert len(all_projections) == len(initial_projections) + 1


def test_update(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)
    table_md.load_stats(simple_db_setup.tx)
    table = simple_db_setup.tx.table_from_metadata(table_md)

    # 1. Select a row to update
    row_to_update = table.select(predicate=table['a'] == 222, internal_row_id=True).read_all()
    assert row_to_update.num_rows == 1

    # 2. Create a modified version in a new RecordBatch
    update_data = pa.table({
        INTERNAL_ROW_ID: row_to_update[INTERNAL_ROW_ID],
        's': ['updated_bb']
    })

    # 3. Call table.update()
    table.update(update_data)

    # 4. Select the row again and verify changes
    updated_row = table.select(predicate=table['a'] == 222).read_all()
    assert updated_row.to_pydict()['s'] == ['updated_bb']

    remaining_rows = table.select(predicate=table['a'] != 222).read_all()
    expected_remaining = pa.table({
        'a': pa.array([111, 333], type=pa.int64()),
        'b': pa.array([0.5, 2.5], type=pa.float32()),
        's': pa.array(['a', 'ccc'], type=pa.utf8()),
    })
    assert compare_pyarrow_tables(remaining_rows, expected_remaining)


def test_delete(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(ref=simple_db_setup.ref,
                             arrow_schema=simple_db_setup.arrow_schema,
                             table_type=simple_db_setup.table_type)
    table_md.load_stats(simple_db_setup.tx)
    table = simple_db_setup.tx.table_from_metadata(table_md)

    # 1. Identify a row to delete
    row_to_delete = table.select(predicate=table['a'] == 333, internal_row_id=True).read_all()
    assert row_to_delete.num_rows == 1

    # 2. Create a RecordBatch with the key of the row
    delete_data = pa.table({
        INTERNAL_ROW_ID: row_to_delete[INTERNAL_ROW_ID]
    })

    # 3. Call table.delete()
    table.delete(delete_data)

    # 4. Select to verify the row is gone
    all_data = table.select().read_all()
    assert all_data.num_rows == 2
    assert 333 not in all_data.to_pydict()['a']
    expected_remaining = pa.table({
        'a': pa.array([111, 222], type=pa.int64()),
        'b': pa.array([0.5, 1.5], type=pa.float32()),
        's': pa.array(['a', 'bb'], type=pa.utf8()),
    })
    assert compare_pyarrow_tables(all_data, expected_remaining)


def test_sanity_load(simple_db_setup: SimpleDbSetup):
    table_md = TableMetadata(TableRef(simple_db_setup.ref.bucket,
                                      simple_db_setup.ref.schema,
                                      simple_db_setup.ref.table))
    table_md.load(simple_db_setup.tx)
