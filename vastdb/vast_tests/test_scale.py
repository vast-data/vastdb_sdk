import logging
import time
from concurrent.futures import ThreadPoolExecutor

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
