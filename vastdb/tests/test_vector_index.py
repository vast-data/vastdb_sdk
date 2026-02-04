"""Tests for vector index functionality."""

import logging
from typing import Optional

import pyarrow as pa
import pytest

from vastdb import errors
from vastdb._internal import VectorIndexSpec
from vastdb.session import Session

log = logging.getLogger(__name__)


@pytest.mark.parametrize("table_name,vector_index", [
    # Test 1: Table without vector index
    ("table_without_index", None),
    # Test 2: Table with L2 vector index
    ("table_with_l2_index", VectorIndexSpec("embedding", "l2sq")),
    # Test 3: Table with inner product vector index
    ("table_with_ip_index", VectorIndexSpec("embedding", "ip")),
])
def test_create_table_with_vector_index_metadata(session: Session,
                                                 clean_bucket_name: str,
                                                 table_name: str,
                                                 vector_index: Optional[VectorIndexSpec]):
    """Test that table creation and stats retrieval work correctly with vector index metadata."""
    schema_name = "schema1"

    with session.transaction() as tx:
        log.info(f"Testing table '{table_name}' with {vector_index}")

        # Create schema
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        # Create the appropriate schema based on whether vector index is needed
        if vector_index is None:
            # Simple table without vector index
            arrow_schema = pa.schema([
                ('id', pa.int64()),
                ('data', pa.string())
            ])
        else:
            # Table with vector column
            vector_dimension = 128  # Fixed-size vector dimension
            vec_type = pa.list_(pa.field('', pa.float32(), False), vector_dimension)
            arrow_schema = pa.schema([
                ('id', pa.int64()),
                ('embedding', vec_type)  # Fixed-size vector column
            ])

        # Create table with or without vector index
        log.info(f"Creating table: {table_name}")
        table = schema.create_table(
            table_name=table_name,
            columns=arrow_schema,
            vector_index=vector_index
        )

        # Reload stats to ensure we get the vector index metadata
        table.reload_stats()

        # Get vector index metadata
        result_vector_index = table._metadata._vector_index

        log.info(f"Vector index metadata: {result_vector_index}")

        # Assert expected values (should match input parameters)
        result_vector_index_spec = (
            None
            if result_vector_index is None
            else result_vector_index.to_vector_index_spec()
        )
        assert result_vector_index_spec == vector_index

        log.info(f"✓ Test passed for table '{table_name}'")


@pytest.mark.parametrize("table_name,vector_index,expected_error", [
    # Test 1: Invalid column name (column doesn't exist in schema)
    ("table_invalid_column", VectorIndexSpec("nonexistent_column", "l2sq"), "invalid vector indexed column name nonexistent_column"),
    # Test 2: Invalid distance metric
    ("table_invalid_metric", VectorIndexSpec("embedding", "invalid_metric"), "invalid vector index distance metric invalid_metric, supported metrics: 'l2sq', 'ip'"),
])
def test_create_table_with_invalid_vector_index(session: Session,
                                                clean_bucket_name: str,
                                                table_name: str,
                                                vector_index: VectorIndexSpec,
                                                expected_error: str):
    """Test that table creation fails with appropriate error messages for invalid vector index parameters."""
    schema_name = "schema1"

    with session.transaction() as tx:
        log.info(f"Testing invalid table '{table_name}' with vector_index={vector_index}, expected_error={expected_error}")

        # Create schema
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        # Table with vector column
        vector_dimension = 128  # Fixed-size vector dimension
        vec_type = pa.list_(pa.field('', pa.float32(), False), vector_dimension)
        arrow_schema = pa.schema([
            ('id', pa.int64()),
            ('embedding', vec_type)  # Fixed-size vector column
        ])

        # Attempt to create table with invalid parameters - should raise an error
        log.info(f"Attempting to create invalid table: {table_name}")
        with pytest.raises((errors.BadRequest)) as exc_info:
            schema.create_table(
                table_name=table_name,
                columns=arrow_schema,
                vector_index=vector_index
            )

        # Verify the error message contains the expected error text
        assert expected_error in str(exc_info.value), \
            f"Expected error message to contain '{expected_error}', got '{str(exc_info.value)}'"

        log.info(f"✓ Test passed for invalid table '{table_name}'")


def test_vector_index_metadata_from_stats(session: Session, clean_bucket_name: str):
    """Test that vector index metadata is correctly retrieved from table stats."""
    schema_name = "schema1"
    table_name = "vector_table"

    with session.transaction() as tx:
        # Create schema
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        # Create table with vector index
        vector_dimension = 128
        vec_type = pa.list_(pa.field('', pa.float32(), False), vector_dimension)
        arrow_schema = pa.schema([
            ('id', pa.int64()),
            ('embedding', vec_type)
        ])

        table = schema.create_table(
            table_name=table_name,
            columns=arrow_schema,
            vector_index=VectorIndexSpec("embedding", "l2sq")
        )

        # Check stats object directly
        stats = table.stats
        assert stats is not None
        assert stats.vector_index is not None
        assert stats.vector_index.column == "embedding"
        assert stats.vector_index.distance_metric == "l2sq"

        # Check via the table methods
        assert table._metadata._vector_index is not None
        assert table._metadata._vector_index.column == "embedding"
        assert table._metadata._vector_index.distance_metric == "l2sq"

        log.info("✓ Vector index metadata correctly retrieved from stats")
