import pytest
import os

import pyarrow as pa
import pyarrow.parquet as pq

from vastdb.v2 import InvalidArgumentError
from vastdb import util


def test_create_table_from_files(rpc, clean_bucket_name, s3):
    datasets = [
        {'num': [0],
         'varch': ['z']},
        {'num': [1, 2, 3, 4, 5],
         'varch': ['a', 'b', 'c', 'd', 'e']},
        {'num': [1, 2, 3, 4, 5],
         'bool': [True, False, None, None, False],
         'varch': ['a', 'b', 'c', 'd', 'e']},
        {'num': [1, 2],
         'bool': [True, True]},
        {'varch': ['a', 'b', 'c'],
         'mismatch': [1, 2, 3]}
    ]
    for i, ds in enumerate(datasets):
        table = pa.Table.from_pydict(ds)
        pq.write_table(table, f'prq{i}')
        with open(f'prq{i}', 'rb') as f:
            s3.put_object(Bucket=clean_bucket_name, Key=f'prq{i}', Body=f)
        os.remove(f'prq{i}')

    same_schema_files = [f'/{clean_bucket_name}/prq{i}' for i in range(2)]
    contained_schema_files = [f'/{clean_bucket_name}/prq{i}' for i in range(4)]
    different_schema_files = [f'/{clean_bucket_name}/prq{i}' for i in range(5)]

    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = util.create_table_from_files(s, 't1', contained_schema_files)
        assert len(t.arrow_schema) == 3
        assert t.arrow_schema == pa.schema([('num', pa.int64()), ('bool', pa.bool_()), ('varch', pa.string())])

        with pytest.raises(InvalidArgumentError):
            util.create_table_from_files(s, 't2', different_schema_files)

        with pytest.raises(InvalidArgumentError):
            util.create_table_from_files(s, 't2', contained_schema_files, schema_merge_func=util.strict_schema_merge)

        util.create_table_from_files(s, 't2', different_schema_files, schema_merge_func=util.union_schema_merge)
        util.create_table_from_files(s, 't3', same_schema_files, schema_merge_func=util.strict_schema_merge)
