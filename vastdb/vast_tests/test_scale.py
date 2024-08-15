import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa

from vastdb.table import QueryConfig

logger = logging.getLogger(__name__)


def test_concurrent_query(session, test_bucket_name, schema_name, table_name):
    """
    This test runs several selective queries in parallel. It is used to check various internal VAST scenarios.
    """
    amount_of_queries_in_parallel = 10  # due to limit on requests connection-pool
    config = QueryConfig(num_splits=1, num_sub_splits=1)

    def _execute_single_query():
        with session.transaction() as tx:
            t = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
            pred = (t["a"] == 0)  # 0 is in the min-max range
            s = time.time()
            t.select(config=config, predicate=pred).read_all()
            e = time.time()
            logger.info(f"Query took {e - s}")

    logger.info(f"about to submit {amount_of_queries_in_parallel} queries in parallel")
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_execute_single_query) for _ in range(amount_of_queries_in_parallel)]
        for future in futures:
            future.result()
    logger.info(f"finished running {amount_of_queries_in_parallel} queries")


def test_table_stats(session, test_bucket_name, schema_name, table_name):
    """
    Testing stats integrity while altering table
    """
    NUM_TIMES_TO_INSERT = 1000
    seed = random.randint(0, 10)
    logger.info(f"random seed is {seed}")
    r = random.Random(seed)

    with session.transaction() as tx:
        t = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
        initial_stat = t.get_stats()
        table_fields = t.columns()

    rand_values = {}  # create a dict with a random value from each column
    with session.transaction() as tx:
        t = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
        for col in table_fields:
            res = t.select(columns=[col.name]).read_all().column(col.name)
            rand_values[col.name] = res[int(r.uniform(0, len(res)))].as_py()

    logger.info(f"rand row to insert to the table - {rand_values}, {NUM_TIMES_TO_INSERT} times")
    rb = pa.RecordBatch.from_pylist([rand_values] * NUM_TIMES_TO_INSERT)
    with session.transaction() as tx:
        t = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
        t.insert(rb)
        time.sleep(2)  # waiting for stats to get updated
        new_stat = t.get_stats()

    logger.info("inserted to table")
    assert new_stat.size_in_bytes != initial_stat.size_in_bytes
    assert new_stat.num_rows - NUM_TIMES_TO_INSERT == initial_stat.num_rows


def test_ndu_while_querying(session, test_bucket_name, schema_name, table_name):
    """
    Executing queries while a NDU takes place.
    """
    # TODO: Before merging run mypy and print query result

    config = QueryConfig(num_splits=1, num_sub_splits=1)

    logger.info(f'{test_bucket_name=}, {schema_name=}, {table_name=}')

    for query in range(300):
        with session.transaction() as tx:
            t = tx.bucket(test_bucket_name).schema(schema_name).table(table_name)
            s = time.time()
            if query == 0:
                res = t.select(config=config).read_all()
                logger.info(f'{res=}')
            else:
                assert res == t.select(config=config).read_all()
            e = time.time()
            logger.info(f'{query=} took {e - s}')
