import pytest

from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq

from vastdb.v2 import InvalidArgumentError
from vastdb.api import ImportFilesError
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
        with NamedTemporaryFile() as f:
            pq.write_table(table, f.name)
            s3.put_object(Bucket=clean_bucket_name, Key=f'prq{i}', Body=f)

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


def test_import_name_mismatch_error(rpc, clean_bucket_name, s3):
    ds = {'varch': ['a', 'b', 'c'],
          'invalid_column_name': [1, 2, 3]}
    prq_name = 'name_mismatch.parquet'
    table = pa.Table.from_pydict(ds)
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        s3.put_object(Bucket=clean_bucket_name, Key=prq_name, Body=f)

    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('varch', pa.string()), ('num', pa.int64())]))
        with pytest.raises(ImportFilesError) as exc:
            t.import_files([f'/{clean_bucket_name}/{prq_name}'])
        assert exc.value.error_dict['object_name'] == prq_name
        assert exc.value.error_dict['res'] == 'TabularMismatchColumnName'
        assert 'invalid_column_name' in exc.value.error_dict['err_msg']


def test_import_type_mismatch_error(rpc, clean_bucket_name, s3):
    ds = {'varch': ['a', 'b', 'c'],
          'num_type_mismatch': [1, 2, 3]}
    prq_name = 'type_mismatch.parquet'
    table = pa.Table.from_pydict(ds)
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        s3.put_object(Bucket=clean_bucket_name, Key=prq_name, Body=f)

    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('varch', pa.string()), ('num_type_mismatch', pa.bool_())]))
        with pytest.raises(ImportFilesError) as exc:
            t.import_files([f'/{clean_bucket_name}/{prq_name}'])
        assert exc.value.error_dict['object_name'] == prq_name
        assert exc.value.error_dict['res'] == 'TabularMismatchColumnType'
        assert 'num_type_mismatch' in exc.value.error_dict['err_msg']
