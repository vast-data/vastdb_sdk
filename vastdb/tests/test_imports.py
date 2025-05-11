import logging
from datetime import datetime
from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vastdb import util
from vastdb.config import ImportConfig
from vastdb.errors import (
    ImportFilesError,
    InternalServerError,
    InvalidArgument,
    NotSupportedVersion,
)

log = logging.getLogger(__name__)


@pytest.fixture
def zip_import_session(session):
    with session.transaction() as tx:
        try:
            tx._rpc.features.check_zip_import()
            return session
        except NotSupportedVersion:
            pytest.skip("Skipped because this test requires version 5.3.1")


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


def create_parquet_file(s3, bucket_name, file_key, data):
    """Creates a Parquet file and uploads it to S3."""
    parquet_table = pa.Table.from_pydict(data)
    with NamedTemporaryFile(delete=False) as f:
        pq.write_table(parquet_table, f.name)
        with open(f.name, 'rb') as file_data:
            s3.put_object(Bucket=bucket_name, Key=file_key, Body=file_data)
    return f'/{bucket_name}/{file_key}'


def create_table_with_data(session, bucket_name, schema_name, table_name, schema, data=None):
    """Creates a table with the specified schema and optional initial data."""
    with session.transaction() as tx:
        b = tx.bucket(bucket_name)
        s = b.create_schema(schema_name)
        t = s.create_table(table_name, schema)
        if data:
            arrow_table = pa.table(schema=schema, data=data)
            t.insert(arrow_table)
        return t


def attempt_import(session, bucket_name, schema_name, table_name, files, key_names, expected_error=None):
    """Attempts to import files into a table and handles expected errors."""
    with session.transaction() as tx:
        t = tx.bucket(bucket_name).schema(schema_name).table(table_name)
        config = ImportConfig()
        config.key_names = key_names

        if expected_error:
            try:
                t.import_files(files, config=config)
            except Exception as e:
                log.info(f"Caught expected error: {e}")
                assert expected_error in str(e)
        else:
            t.import_files(files, config=config)


def test_zip_imports(zip_import_session, clean_bucket_name, s3):
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
    ])
    num_rows = 10
    num_files = 5

    # Step 1: Generate and upload Parquet files
    files = []
    for i in range(num_files):
        data = {
            'id': [k for k in range(num_rows)],
            'symbol': [chr(c) for c in range(ord('a'), ord('a') + num_rows)],
            f'feature{i}': [i * 10 + k for k in range(num_rows)],
        }
        file_key = f'prq{i}'
        files.append(create_parquet_file(s3, clean_bucket_name, file_key, data))

    # Step 2: Create table and insert initial data
    data = {
        'vastdb_rowid': [10 + i for i in range(num_rows)],
        'id': [i for i in range(num_rows)],
        'symbol': [chr(c) for c in range(ord('a'), ord('a') + num_rows)],
    }
    create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema, data)

    # Step 3: Import files into the table
    attempt_import(zip_import_session, clean_bucket_name, 's1', 't1', files, key_names=['id', 'symbol'])

    # Step 4: Construct expected rows
    expected_rows = []
    for i in range(num_rows):
        row = {
            'vastdb_rowid': 10 + i,  # Initial vastdb_rowid values (10-19)
            'id': i,  # ID values (0-9)
            'symbol': chr(ord('a') + i),  # Symbol values ('a' to 'j')
            'feature0': 0 * 10 + i,  # Values from file 1 (0-9)
            'feature1': 1 * 10 + i,  # Values from file 2 (10-19)
            'feature2': 2 * 10 + i,  # Values from file 3 (20-29)
            'feature3': 3 * 10 + i,  # Values from file 4 (30-39)
            'feature4': 4 * 10 + i,  # Values from file 5 (40-49)
        }
        expected_rows.append(row)

    # Step 5: Query the actual data from the table
    with zip_import_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema('s1').table('t1')
        arrow_table = t.select().read_all()
        actual_data = arrow_table.to_pydict()

    # Step 6: Compare expected and actual data
    num_actual_rows = len(next(iter(actual_data.values()), []))
    assert num_actual_rows == len(expected_rows), f"Expected {len(expected_rows)} rows but got {num_actual_rows}"

    # Convert expected_rows to a comparable format (pydict format)
    expected_data = {k: [] for k in expected_rows[0].keys()}
    for row in expected_rows:
        for k, v in row.items():
            expected_data[k].append(v)

    # Check that all expected columns exist in actual data
    for col in expected_data:
        assert col in actual_data, f"Expected column {col} not found in actual data"

    # Compare column values
    for col in expected_data:
        assert actual_data[col] == expected_data[col], f"Values in column {col} don't match expected values"


def test_zip_imports_scale(zip_import_session, clean_bucket_name, s3):
    """Verify that many key names, and large amounts of data of different kind work as expected."""
    # Step 1: Create and upload Parquet data
    log.info("Step 1: Creating and uploading Parquet data")
    num_rows = 1_000_000
    data = {
        'id': [i for i in range(num_rows)],
        'symbol': [chr((i % 26) + ord('a')) for i in range(num_rows)],
        'feature': [i * 10 for i in range(num_rows)],  # Extra column not in the initial table
        'col_0': [datetime.now() for _ in range(num_rows)],
        'col_1': [1 for _ in range(num_rows)],
        'col_2': [2 for _ in range(num_rows)],
        'col_3': [3 for _ in range(num_rows)],
        'col_4': [4 for _ in range(num_rows)],
        'col_5': [5 for _ in range(num_rows)],  # Extra column not in the initial table
    }
    file_key = 'large_data.parquet'
    file_path = create_parquet_file(s3, clean_bucket_name, file_key, data)

    # Step 2: Create table and insert initial data
    log.info("Step 2: Creating table and inserting initial data")
    table_data = {
        'vastdb_rowid': [10 + i for i in range(num_rows)],
        'id': data['id'],
        'symbol': data['symbol'],
        'col_0': data['col_0'],
        'col_1': data['col_1'],
        'col_2': data['col_2'],
        'col_3': data['col_3'],
        'col_4': data['col_4'],
    }
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
        ('col_0', pa.timestamp('s')),
        ('col_1', pa.int64()),
        ('col_2', pa.int64()),
        ('col_3', pa.int64()),
        ('col_4', pa.int64()),
    ])
    create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema, table_data)

    # Step 3: Import the Parquet file into the table
    log.info("Step 3: Importing Parquet file into the table")
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [file_path],
        key_names=['id', 'symbol', 'col_0', 'col_1', 'col_2', 'col_3', 'col_4']
    )

    # Step 4: Verify schema and row count
    log.info("Step 4: Verifying schema and row count")
    with (zip_import_session.transaction() as tx):
        table = tx.bucket(clean_bucket_name).schema('s1').table('t1')
        updated_schema = table.arrow_schema
        updated_data = table.select().read_all()

        # Verify schema
        expected_schema = pa.schema([
            ('vastdb_rowid', pa.int64()),
            ('id', pa.int64()),
            ('symbol', pa.string()),
            ('col_0', pa.timestamp('s')),
            ('col_1', pa.int64()),
            ('col_2', pa.int64()),
            ('col_3', pa.int64()),
            ('col_4', pa.int64()),
            ('feature', pa.int64()),  # Added during import
            ('col_5', pa.int64()),   # Added during import
        ])
        assert updated_schema == expected_schema, \
            "The table schema does not match the expected schema."

        assert updated_data.num_rows == num_rows, \
            f"Expected {num_rows} rows, but got {updated_data.num_rows}."

        assert len(updated_schema.names) == 10, \
            "The table should have exactly 10 columns"


def test_zip_imports_missing_columns(zip_import_session, clean_bucket_name, s3):
    """Verify that importing Parquet data with missing columns fails as expected."""
    # Step 1: Create and upload Parquet data missing key columns
    log.info("Step 1: Creating and uploading Parquet data without key columns")
    data = {
        'feature': [i * 10 for i in range(10)],  # Only feature column, no 'id' or 'symbol'
    }
    file_key = 'missing_keys.parquet'
    file_path = create_parquet_file(s3, clean_bucket_name, file_key, data)

    # Step 2: Create table with key columns
    log.info("Step 2: Creating table with key columns")
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
    ])
    create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema)

    # Step 3: Attempt to import Parquet data missing key columns
    log.info("Step 3: Attempting to import data without key columns")
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [file_path],
        key_names=['id', 'symbol'],
        expected_error="Failed to verify import keys"
    )


def test_zip_imports_missing_key_values(zip_import_session, clean_bucket_name, s3):
    """Verify that importing Parquet data with extra key values fails as expected
    and that importing a subset of key values fails as expected."""
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
    ])
    num_rows = 5

    # Step 1: Create Parquet data with keys 0-4
    data = {
        'id': [i for i in range(num_rows)],
        'symbol': [chr((i % 26) + ord('a')) for i in range(num_rows)],
        'feature': [i * 10 for i in range(num_rows)],
    }
    file_key = 'missing_key_values.parquet'
    file_path = create_parquet_file(s3, clean_bucket_name, file_key, data)

    # Step 2: Create a table with non-overlapping keys 3-7
    table_data = {
        'vastdb_rowid': [i + 3 for i in range(num_rows)],
        'id': [i + 3 for i in range(num_rows)],
        'symbol': [chr(((i + 3) % 26) + ord('k')) for i in range(num_rows)],
    }
    create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema, table_data)

    # Step 3: Attempt to import Parquet data with mismatched keys
    log.info("Step 3: Attempting to import Parquet data with keys that do not match the table")
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [file_path],
        key_names=['id', 'symbol'],
        expected_error="Failed to get row_ids to update on table"
    )

    # Step 4: Create and upload Parquet data with fewer rows but all key values present in the table
    log.info("Step 4: Creating and uploading Parquet data with fewer rows, but matching all table keys")
    smaller_data = {
        'id': [3, 4],  # Subset of the table keys
        'symbol': ['k', 'l'],  # Matching symbols for keys 3 and 4
        'feature': [300, 400],  # Example new feature data
    }
    smaller_file_key = 'subset_matching_keys.parquet'
    smaller_file_path = create_parquet_file(s3, clean_bucket_name, smaller_file_key, smaller_data)

    # Step 5: Attempt to import the Parquet data with fewer rows but all key values present
    log.info("Step 5: Attempting to import smaller Parquet data with all table keys")
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [smaller_file_path],
        key_names=['id', 'symbol'],
        expected_error='Failed to get row_ids to update on table'
    )


def test_zip_imports_nested_keys(zip_import_session, clean_bucket_name, s3):
    """Verify that importing Parquet data with nested key columns fails as expected."""
    # Step 1: Creating Parquet data with nested key columns
    log.info("Step 1: Creating Parquet data with nested key columns")
    num_rows = 10
    nested_keys = [{'id': i, 'symbol': chr(ord('a') + i)} for i in range(num_rows)]
    feature_column = [i * 10 for i in range(num_rows)]

    ds = {
        'nested_key': nested_keys,
        'feature': feature_column,
    }

    # Use create_parquet_file helper
    file_key = 'nested_keys.parquet'
    file_path = create_parquet_file(s3, clean_bucket_name, file_key, ds)

    # Step 2: Creating table with flat key columns
    log.info("Step 2: Creating table with flat key columns")
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
    ])

    # Use create_table_with_data helper
    create_table_with_data(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        schema
    )

    # Step 3: Attempt to import Parquet data with nested key columns
    log.info("Step 3: Attempting to import data with nested key columns")

    # Use attempt_import helper with expected error
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [file_path],
        ['id', 'symbol'],
        expected_error="Failed to verify import keys"
    )


def test_zip_imports_type_mismatch(zip_import_session, clean_bucket_name, s3):
    """Verify behavior when key column data types in the Parquet file do not match the table schema."""
    # Step 1: Define table schema with id as string
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.string()),  # Expecting strings here
        ('symbol', pa.string()),
    ])
    num_rows = 10

    # Step 2: Generate and upload a single Parquet file with mismatched id type (integers)
    log.info("Step 2: Creating a Parquet file with mismatched key column data types")
    data = {
        'id': [k for k in range(num_rows)],  # Integers, causing the type mismatch
        'symbol': [chr(c) for c in range(ord('a'), ord('a') + num_rows)],
        'feature': [k * 10 for k in range(num_rows)],
    }
    file_key = 'mismatched_data.parquet'
    file_path = create_parquet_file(s3, clean_bucket_name, file_key, data)

    # Step 3: Create table with string id column and insert valid initial data
    log.info("Step 3: Creating table with string key column and valid initial data")
    table_data = {
        'vastdb_rowid': [10 + i for i in range(num_rows)],
        'id': [str(i) for i in range(num_rows)],  # Strings to match schema
        'symbol': [chr(c) for c in range(ord('a'), ord('a') + num_rows)],
    }
    create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema, table_data)

    # Step 4: Attempt to import the file into the table
    log.info("Step 4: Attempting to import the Parquet file with mismatched key column data types")
    attempt_import(
        zip_import_session,
        clean_bucket_name,
        's1',
        't1',
        [file_path],
        key_names=['id', 'symbol'],
        expected_error="TabularMismatchColumnType"
    )


def test_zip_imports_duplicate_key_values(zip_import_session, clean_bucket_name):
    """Verify that creating a table with duplicate key values fails as expected,
    also show that it has to be in same order."""
    schema = pa.schema([
        ('vastdb_rowid', pa.int64()),
        ('id', pa.int64()),
        ('symbol', pa.string()),
    ])

    # Data with duplicate keys
    table_data = {
        'vastdb_rowid': [1, 2, 2, 4, 5],
        'id': [1, 2, 2, 4, 5],
        'symbol': ['a', 'b', 'b', 'd', 'e'],
    }

    try:
        # Attempt to create the table
        create_table_with_data(zip_import_session, clean_bucket_name, 's1', 't1', schema, table_data)
        assert False, "Expected an error due to duplicate keys, but the table was created successfully."
    except Exception as e:
        # Verify the exception is due to duplicate row IDs
        assert "Found duplicate row ids or not in ascending order" in str(e), f"Unexpected error: {e}"
