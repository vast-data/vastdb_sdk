import logging
from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vastdb import util
from vastdb.config import ImportConfig
from vastdb.errors import ImportFilesError, InternalServerError, InvalidArgument

log = logging.getLogger(__name__)


def test_parallel_imports(session, clean_bucket_name, s3):
    num_rows = 1000
    num_files = 53
    ds = {'num': [i for i in range(num_rows)]}
    files = []
    table = pa.Table.from_pydict(ds)
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        s3.put_object(Bucket=clean_bucket_name, Key='prq0', Body=f)
        files.append(f'/{clean_bucket_name}/prq0')

    for i in range(1, num_files):
        copy_source = {
            'Bucket': clean_bucket_name,
            'Key': 'prq0'
        }
        s3.copy(copy_source, clean_bucket_name, f'prq{i}')
        files.append(f'/{clean_bucket_name}/prq{i}')

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('num', pa.int64())]))
        with pytest.raises(InternalServerError):
            t.create_imports_table()
        log.info("Starting import of %d files", num_files)
        t.import_files(files)
        arrow_table = t.select(columns=['num']).read_all()
        assert arrow_table.num_rows == num_rows * num_files
        arrow_table = t.select(columns=['num'], predicate=t['num'] == 100).read_all()
        assert arrow_table.num_rows == num_files
        import_table = t.imports_table()
        # checking all imports are on the imports table:
        objects_name = import_table.select(columns=["ObjectName"]).read_all()
        objects_name = objects_name.to_pydict()
        object_names = set(objects_name['ObjectName'])
        prefix = 'prq'
        numbers = set(range(53))
        assert all(name.startswith(prefix) for name in object_names)
        numbers.issubset(int(name.replace(prefix, '')) for name in object_names)
        assert len(object_names) == len(objects_name['ObjectName'])


def test_zip_imports(session, clean_bucket_name, s3):
    num_rows = 10
    num_files = 5
    files = []
    ids = [i for i in range(num_rows)]
    symbols = [chr(c) for c in range(ord('a'), ord('a') + num_rows)]
    for i in range(num_files):
        ds = {'id': ids,
              'symbol': symbols,
              f'feature{i}': [i * 10 + k for k in range(num_rows)]}
        table = pa.Table.from_pydict(ds)
        with NamedTemporaryFile() as f:
            pq.write_table(table, f.name)
            pname = f'prq{i}'
            s3.put_object(Bucket=clean_bucket_name, Key=pname, Body=f)
            files.append(f'/{clean_bucket_name}/{pname}')

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('vastdb_rowid', pa.int64()), ('id', pa.int64()), ('symbol', pa.string())]))
        columns = pa.schema([
            ('vastdb_rowid', pa.int64()),
            ('id', pa.int64()),
            ('symbol', pa.string()),
        ])
        ext_row_ids = [10 + i for i in range(num_rows)]
        arrow_table = pa.table(schema=columns, data=[
            ext_row_ids,
            ids,
            symbols,
        ])
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert row_ids == ext_row_ids

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s1')
        t = s.table('t1')
        # with pytest.raises(InternalServerError):
        #     t.create_imports_table()
        log.info("Starting import of %d files", num_files)
        config = ImportConfig()
        config.key_names = ['id', 'symbol']
        t.import_files(files, config=config)

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s1')
        t = s.table('t1')
        arrow_table = t.select(columns=['feature0']).read_all()
        assert arrow_table.num_rows == num_rows
        log.debug(f"table schema={t.arrow_schema}")
        assert len(t.arrow_schema) == 8


def test_create_table_from_files(session, clean_bucket_name, s3):
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

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = util.create_table_from_files(s, 't1', contained_schema_files)
        assert len(t.arrow_schema) == 3
        assert t.arrow_schema == pa.schema([('num', pa.int64()), ('bool', pa.bool_()), ('varch', pa.string())])

        with pytest.raises(InvalidArgument):
            util.create_table_from_files(s, 't2', different_schema_files)

        with pytest.raises(InvalidArgument):
            util.create_table_from_files(s, 't2', contained_schema_files, schema_merge_func=util.strict_schema_merge)

        util.create_table_from_files(s, 't2', different_schema_files, schema_merge_func=util.union_schema_merge)
        util.create_table_from_files(s, 't3', same_schema_files, schema_merge_func=util.strict_schema_merge)


def test_import_name_mismatch_error(session, clean_bucket_name, s3):
    ds = {'varch': ['a', 'b', 'c'],
          'invalid_column_name': [1, 2, 3]}
    prq_name = 'name_mismatch.parquet'
    table = pa.Table.from_pydict(ds)
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        s3.put_object(Bucket=clean_bucket_name, Key=prq_name, Body=f)

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('varch', pa.string()), ('num', pa.int64())]))
        with pytest.raises(ImportFilesError) as exc:
            t.import_files([f'/{clean_bucket_name}/{prq_name}'])
        assert exc.value.error_dict['object_name'] == prq_name
        assert exc.value.error_dict['res'] == 'TabularMismatchColumnName'
        assert 'invalid_column_name' in exc.value.error_dict['err_msg']


def test_import_type_mismatch_error(session, clean_bucket_name, s3):
    ds = {'varch': ['a', 'b', 'c'],
          'num_type_mismatch': [1, 2, 3]}
    prq_name = 'type_mismatch.parquet'
    table = pa.Table.from_pydict(ds)
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        s3.put_object(Bucket=clean_bucket_name, Key=prq_name, Body=f)

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s1')
        t = s.create_table('t1', pa.schema([('varch', pa.string()), ('num_type_mismatch', pa.bool_())]))
        with pytest.raises(ImportFilesError) as exc:
            t.import_files([f'/{clean_bucket_name}/{prq_name}'])
        assert exc.value.error_dict['object_name'] == prq_name
        assert exc.value.error_dict['res'] == 'TabularMismatchColumnType'
        assert 'num_type_mismatch' in exc.value.error_dict['err_msg']
