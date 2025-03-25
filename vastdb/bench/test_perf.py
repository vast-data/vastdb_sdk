import datetime as dt
import logging
import time

import pytest

from vastdb import util
from vastdb.table import ImportConfig, QueryConfig
from vastdb.tests.util import compare_pyarrow_tables

log = logging.getLogger(__name__)


@pytest.mark.benchmark
def test_bench(session, test_bucket_name, parquets_path, crater_path):
    files = [str(parquets_path / f) for f in (parquets_path.glob('**/*.pq'))]
    stats = None

    with session.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        s = b.create_schema('s1')
        util.create_table_from_files(s, 't1', files, config=ImportConfig(import_concurrency=8))
        t2 = util.create_table_from_files(s, 't2', files, config=ImportConfig(import_concurrency=8))
        # Enabling Elysium with 4 sorting keys - ts, sid, ask_open, ask_close
        t2.add_sorting_key([2, 0, 3, 4])
        stats = t2.get_stats()
        log.info("Added sorting keys")

    assert stats
    # Waiting up to 2 hours for sorting to complete.
    start_time = time.time()
    while not stats.sorting_done:
        if time.time() - start_time > 7200:
            raise TimeoutError("Sorting did not complete after waiting for 2 hours.")
        time.sleep(30)
        with session.transaction() as tx:
            table = tx.bucket(test_bucket_name).schema('s1').table('t2')
            stats = table.get_stats()
    log.info("Sorting completed")

    queries = [
        {'query_str': "select sid from {t} where sid = 10033007".format, 'columns': ['sid'],
         'predicate': lambda t: t['sid'] == 10033007},
        {'query_str': "select last_trade_price from {t} where ts between "
                      "TIMESTAMP'2018-01-04 20:30:00' AND TIMESTAMP'2018-01-05 20:30:00'".format,
         'columns': ['last_trade_price'], 'predicate': lambda t: (t['ts'].between(
            dt.datetime(2018, 1, 4, 20, 30, 00, 00), dt.datetime(2018, 1, 5, 20, 30, 00, 00)))},
        {'query_str': "select ts,ask_close,ask_open from {t} where bid_qty = 684000 and ask_close > 1".format,
         'columns': ['ts', 'ask_close', 'ask_open'],
         'predicate': lambda t: ((t['bid_qty'] == 684000) & (t['ask_close'] > 1))},
        {'query_str': "select ts,ticker from {t} where "
                      "ask_open between 4374 and 4375 OR ask_open between 380 and 381".format,
         'columns': ['ts', 'ticker'],
         'predicate': lambda t: ((t['ask_open'].between(4374, 4375)) | (t['ask_open'].between(380, 381)))},
        {
         'query_str': "select trade_close, trade_high, trade_low, trade_open from {t} where ticker in ('BANR', 'KELYB')".format,
         'columns': ['trade_close', 'trade_high', 'trade_low', 'trade_open'],
         'predicate': lambda t: (t['ticker'].isin(['BANR', 'KELYB']))}
    ]

    log.info("Starting to run queries")
    with session.transaction() as tx:
        schema = tx.bucket(test_bucket_name).schema('s1')
        t1 = schema.table("t1")
        t2 = schema.table("t2")

        config = QueryConfig(num_splits=8, num_sub_splits=4)

        for q in queries:
            normal_table_res, els_table_res = None, None
            for table in [t1, t2]:
                log.info("Starting query: %s", q['query_str'](t=table.name))
                s = time.time()
                res = table.select(columns=q['columns'], predicate=q['predicate'](table), config=config).read_all()
                e = time.time()
                if table == t1:
                    normal_table_res = res
                else:
                    els_table_res = res
                log.info("Query %s returned in %s seconds.", q['query_str'](t=table.name), e - s)
                if crater_path:
                    with open(f'{crater_path}/bench_results', 'a') as f:
                        f.write(f"Query '{q['query_str'](t=table)}' returned in {e - s} seconds")

            assert normal_table_res, f"missing result for {t1} table"
            assert els_table_res, f"missing result for {t2} table"
            assert compare_pyarrow_tables(normal_table_res, els_table_res)
