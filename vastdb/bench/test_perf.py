import logging
import time

import pytest

from vastdb import util
from vastdb.table import ImportConfig, QueryConfig

log = logging.getLogger(__name__)


@pytest.mark.benchmark
def test_bench(session, test_bucket_name, parquets_path, crater_path):
    files = [str(parquets_path / f) for f in (parquets_path.glob('**/*.pq'))]

    with session.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        s = b.create_schema('s1')
        t = util.create_table_from_files(s, 't1', files, config=ImportConfig(import_concurrency=8))
        config = QueryConfig(num_splits=8, num_sub_splits=4)
        s = time.time()
        pa_table = t.select(columns=['sid'], predicate=t['sid'] == 10033007, config=config).read_all()
        e = time.time()
        log.info("'SELECT sid from TABLE WHERE sid = 10033007' returned in %s seconds.", e - s)
        if crater_path:
            with open(f'{crater_path}/bench_results', 'a') as f:
                f.write(f"'SELECT sid FROM TABLE WHERE sid = 10033007' returned in {e - s} seconds")
        assert pa_table.num_rows == 255_075
