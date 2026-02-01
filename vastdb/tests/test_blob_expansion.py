"""Tests for blob expansion functionality."""

import logging

import pyarrow as pa
import pytest

from vastdb import errors
from vastdb.table import BlobExpansionConfig, ExpansionFormat

log = logging.getLogger(__name__)


def test_basic_blob_expansion(session, clean_bucket_name):
    """Test creating, retrieving, and dropping blob expansions."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('data', pa.string()),
            ('timestamp', pa.int64()),
        ])

        assert s.tables() == []
        t = s.create_table('t1', columns)
        assert s.tables() == [t]

        expansion_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
            ('field3', pa.float64()),
        ])

        s.create_table('t1_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='data',
            expansion_schema=expansion_schema,
            target_table_name='t1_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, copy_source_column=False)
        )

        assert be.source_column_name == 'data'
        assert be.target_table_name == '/vastdb/s1/t1_expanded'
        assert be.config.expansion_format == ExpansionFormat.JSON
        assert be.config.copy_source_column is False

        be_retrieved = t.blob_expansion('data')
        assert be_retrieved.source_column_name == be.source_column_name
        assert be_retrieved.target_table_name == be.target_table_name

        be.drop()

        with pytest.raises(errors.MissingBlobExpansion):
            t.blob_expansion('data')


def test_blob_expansion_add_columns(session, clean_bucket_name):
    """Test adding columns to an existing blob expansion."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        initial_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
        ])

        s.create_table('t1_json_expanded', initial_schema)

        be = t.create_blob_expansion(
            source_column_name='json_data',
            expansion_schema=initial_schema,
            target_table_name='t1_json_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        additional_schema = pa.schema([
            ('field3', pa.float64()),
            ('field4', pa.bool_()),
        ])

        be.add_columns(
            columns_to_add=additional_schema,
            add_copy_source_column=False
        )

        be_updated = t.blob_expansion('json_data')
        assert be_updated.source_column_name == 'json_data'

        be.drop()


def test_blob_expansion_add_already_added_columns(session, clean_bucket_name):
    """Test that adding already existing columns raises an error."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        initial_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
        ])

        s.create_table('t1_json_expanded', initial_schema)

        be = t.create_blob_expansion(
            source_column_name='json_data',
            expansion_schema=initial_schema,
            target_table_name='t1_json_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        # Try to add columns that already exist
        with pytest.raises(errors.Conflict):
            be.add_columns(columns_to_add=initial_schema)

        be.drop()


def test_blob_expansion_drop_already_dropped_columns(session, clean_bucket_name):
    """Test that dropping already dropped columns raises an error."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        expansion_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
            ('field3', pa.float64()),
        ])

        s.create_table('t1_json_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='json_data',
            expansion_schema=expansion_schema,
            target_table_name='t1_json_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        columns_to_drop = pa.schema([
            ('field3', pa.float64()),
        ])

        # First drop should succeed
        be.drop_columns(columns_to_remove=columns_to_drop)

        be.drop_columns(columns_to_remove=columns_to_drop)

        be.drop()


def test_blob_expansion_drop_non_existent_columns(session, clean_bucket_name):
    """Test that dropping non-existent columns raises an error."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        expansion_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
        ])

        s.create_table('t1_json_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='json_data',
            expansion_schema=expansion_schema,
            target_table_name='t1_json_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        non_existent_columns = pa.schema([
            ('non_existent_field', pa.string()),
        ])

        with pytest.raises(errors.NotFound):
            be.drop_columns(columns_to_remove=non_existent_columns)

        be.drop()


def test_blob_expansion_drop_columns(session, clean_bucket_name):
    """Test dropping columns from an existing blob expansion."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        expansion_schema = pa.schema([
            ('field1', pa.string()),
            ('field2', pa.int32()),
            ('field3', pa.float64()),
            ('field4', pa.bool_()),
        ])

        s.create_table('t1_json_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='json_data',
            expansion_schema=expansion_schema,
            target_table_name='t1_json_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        columns_to_drop = pa.schema([
            ('field3', pa.float64()),
            ('field4', pa.bool_()),
        ])

        be.drop_columns(
            columns_to_remove=columns_to_drop,
            remove_copy_source_column=False
        )

        be_updated = t.blob_expansion('json_data')
        assert be_updated.source_column_name == 'json_data'

        be.drop()


def test_blob_expansion_with_copy_source_column(session, clean_bucket_name):
    """Test blob expansion with copy_source_column option."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('blob_col', pa.string()),
        ])

        t = s.create_table('t1', columns)

        expansion_schema = pa.schema([
            ('extracted_field1', pa.string()),
            ('extracted_field2', pa.int32()),
        ])

        s.create_table('t1_blob_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='blob_col',
            expansion_schema=expansion_schema,
            target_table_name='t1_blob_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, copy_source_column=True)
        )

        assert be.config.copy_source_column is True

        be.drop()


def test_blob_expansion_missing_error(session, clean_bucket_name):
    """Test that accessing non-existent blob expansion raises appropriate error."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('data', pa.string()),
        ])

        t = s.create_table('t1', columns)

        with pytest.raises(errors.MissingBlobExpansion) as exc_info:
            t.blob_expansion('nonexistent_column')

        assert exc_info.value.table_ref.bucket == clean_bucket_name
        assert exc_info.value.table_ref.schema == 's1'
        assert exc_info.value.table_ref.table == 't1'
        assert exc_info.value.source_column == 'nonexistent_column'


def test_blob_expansion_nested_schema(session, clean_bucket_name):
    """Test blob expansion with nested/complex schema types."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('json_blob', pa.string()),
        ])

        t = s.create_table('t1', columns)

        expansion_schema = pa.schema([
            ('simple_field', pa.string()),
            ('nested_struct', pa.struct([
                ('sub_field1', pa.int32()),
                ('sub_field2', pa.string()),
            ])),
            ('list_field', pa.list_(pa.int32())),
        ])

        s.create_table('t1_complex_expanded', expansion_schema)

        be = t.create_blob_expansion(
            source_column_name='json_blob',
            expansion_schema=expansion_schema,
            target_table_name='t1_complex_expanded',
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON)
        )

        assert be.source_column_name == 'json_blob'
        assert be.target_table_name == '/vastdb/s1/t1_complex_expanded'

        be.drop()


def test_blob_expansions_not_implemented(session, clean_bucket_name):
    """Test that blob_expansions() list method is not implemented."""
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')

        columns = pa.schema([
            ('id', pa.int64()),
            ('data', pa.string()),
        ])

        s.create_table('t1', columns)
