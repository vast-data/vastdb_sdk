import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def prepare_data(session, clean_bucket_name, schema_name, table_name, arrow_table):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        t.insert(arrow_table)
        yield t
        t.drop()
        s.drop()
