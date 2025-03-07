import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def prepare_data(session, clean_bucket_name, schema_name, table_name, arrow_table, sorting_key=[]):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema, sorting_key=sorting_key)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert row_ids == list(range(arrow_table.num_rows))
        yield t
        t.drop()
        s.drop()
