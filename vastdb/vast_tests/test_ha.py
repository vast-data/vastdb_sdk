import logging
import time

import pyarrow as pa
import pytest

from vastdb.table import QueryConfig

logger = logging.getLogger(__name__)


@pytest.mark.ha
def test_ha_query(session, test_bucket_name, schema_name, table_name):
    # runs in parallel to ha scenario
    times_to_query, records_in_table = 50, 100_000_000
    arrow_array = pa.array(range(0, records_in_table), type=pa.int64())

    with session.transaction() as tx:
        config = QueryConfig(num_splits=1, num_sub_splits=1)
        start = time.time()
        for i in range(times_to_query):
            table = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
            s = time.time()
            res = table.select(config=config).read_all()
            e = time.time()
            logger.info(f'query number {i} took {e - s} sec')
            assert arrow_array == pa.concat_arrays(res.column('a').chunks)
        end = time.time()
        logger.info(f'SDK test took {end - start}')
