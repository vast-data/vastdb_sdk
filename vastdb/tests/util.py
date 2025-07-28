import logging
from contextlib import contextmanager

import pyarrow as pa

from vastdb.session import Session

log = logging.getLogger(__name__)


def assert_row_ids_ascending_on_first_insertion_to_table(row_ids, expected_num_rows, sorted_table):
    adjusted_row_ids = [
        int(row_id) & 0xFFFFFFFFFFFFFF for row_id in row_ids
        ] if sorted_table else row_ids

    assert adjusted_row_ids == list(range(expected_num_rows))


@contextmanager
def prepare_data(session: Session, clean_bucket_name, schema_name, table_name, arrow_table, sorting_key=[]):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema, sorting_key=sorting_key)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert_row_ids_ascending_on_first_insertion_to_table(row_ids, arrow_table.num_rows, t.sorted_table)
        yield t
        t.drop()
        s.drop()


def compare_pyarrow_tables(t1, t2):

    def sort_table(table):
        return table.sort_by([(col, 'ascending') for col in table.schema.names])

    def compare_tables(table1, table2):
        if table1.schema != table2.schema:
            raise RuntimeError(f"Schema mismatch. {table1.schema} vs {table2.schema}")

        for t1_col, t2_col in zip(table1.columns, table2.columns):
            if not pa.compute.equal(t1_col, t2_col).to_pandas().all():
                raise RuntimeError(f"Data mismatch in column {t1_col} vs {t2_col}.")
        return True

    sorted_table1 = sort_table(t1)
    sorted_table2 = sort_table(t2)
    return compare_tables(sorted_table1, sorted_table2)
