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


@pytest.fixture
def vector_index_test_tables(session: Session, clean_bucket_name: str):
    """
    Fixture that creates a schema with 4 test tables:
    - regular_table: no vector index
    - vector_table_l2: l2sq distance metric
    - vector_table_ip: ip distance metric
    - vector_table_cosine: cosine distance metric

    Returns a tuple of (schema, table_names_dict, expected_metrics) where:
    - table_names_dict contains the names of all created tables
    - expected_metrics is a list of (metric_key, column, distance_metric, sql_function) tuples
    """
    schema_name = "vector_list_schema"
    vector_dimension = 5

    with session.transaction() as tx:
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        # Create test tables
        vec_type = pa.list_(pa.field('', pa.float32(), False), vector_dimension)
        arrow_schema = pa.schema([
            ('id', pa.int64()),
            ('embedding', vec_type)
        ])

        table_names = {}
        expected_metrics = []

        # Regular table without vector index
        table_name = "regular_table"
        schema.create_table(table_name, arrow_schema)
        table_names["regular"] = table_name
        log.info(f"Created regular table: {table_name}")

        # Vector index table with l2sq metric
        table_name = "vector_table_l2"
        schema.create_table(table_name, arrow_schema, vector_index=VectorIndexSpec("embedding", "l2sq"))
        table_names["l2"] = table_name
        expected_metrics.append(("l2", "embedding", "l2sq", "array_distance"))
        log.info(f"Created vector index table (l2sq): {table_name}")

        # Vector index table with ip metric
        table_name = "vector_table_ip"
        schema.create_table(table_name, arrow_schema, vector_index=VectorIndexSpec("embedding", "ip"))
        table_names["ip"] = table_name
        expected_metrics.append(("ip", "embedding", "ip", "array_inner_product"))
        log.info(f"Created vector index table (ip): {table_name}")

        # Vector index table with cosine metric
        table_name = "vector_table_cosine"
        schema.create_table(table_name, arrow_schema, vector_index=VectorIndexSpec("embedding", "cosine"))
        table_names["cosine"] = table_name
        expected_metrics.append(("cosine", "embedding", "cosine", "array_cosine_distance"))
        log.info(f"Created vector index table (cosine): {table_name}")

        yield schema, table_names, expected_metrics


def _check_vector_index_metadata(actual_column, actual_metric, actual_sql_func,
                                 expected_column, expected_metric, expected_sql_func,
                                 expect_full_metadata: bool,
                                 table_name: str = "table"):
    """Pure checker helper to validate vector index metadata values.

    Args:
        actual_column: Actual column name from result
        actual_metric: Actual distance metric from result
        actual_sql_func: Actual SQL function name from result
        expected_column: Expected column name (when full metadata is present)
        expected_metric: Expected distance metric (when full metadata is present)
        expected_sql_func: Expected SQL function name (when full metadata is present)
        expect_full_metadata: Whether full metadata should be present
        table_name: Name of table for error messages
    """
    # Determine actual expected values based on metadata presence
    if expect_full_metadata:
        expected_col = expected_column
        expected_met = expected_metric
        expected_sql = expected_sql_func
    else:
        # When metadata is not loaded, both Table objects and TableInfo use empty strings
        expected_col = ""
        expected_met = ""
        expected_sql = ""

    assert actual_column == expected_col, \
        f"Expected column='{expected_col}', got '{actual_column}' for {table_name}"
    assert actual_metric == expected_met, \
        f"Expected metric='{expected_met}', got '{actual_metric}' for {table_name}"
    assert actual_sql_func == expected_sql, \
        f"Expected sql_func='{expected_sql}', got '{actual_sql_func}' for {table_name}"


@pytest.mark.parametrize("test_case,use_public_api,include_metadata,expect_full_metadata", [
    # Test 1: Public API schema.tables() - returns Table objects with empty metadata
    ("schema.tables() public API", True, None, False),
    # Test 2: Internal API with full metadata enabled
    ("_iter_tables(include_vector_index_metadata=True)", False, True, True),
    # Test 3: Internal API with metadata disabled - returns flag only
    ("_iter_tables(include_vector_index_metadata=False)", False, False, False),
])
def test_list_tables_vector_index_metadata(session: Session, vector_index_test_tables,
                                          test_case: str, use_public_api: bool,
                                          include_metadata: Optional[bool], expect_full_metadata: bool):
    """
    Test that list_tables APIs return correct vector index metadata based on parameters.

    Tests three scenarios:
    1. schema.tables() - public API returns Table objects with vector_index placeholder (empty strings)
    2. _iter_tables(include_vector_index_metadata=True) - returns full metadata
    3. _iter_tables(include_vector_index_metadata=False) - returns flag only, no expensive metadata
    """
    schema, table_names, expected_metrics = vector_index_test_tables

    log.info(f"Testing: {test_case}")

    # Call the appropriate API and extract vector index info into uniform structure
    if use_public_api:
        # Public API returns Table objects with _vector_index
        results = schema.tables()
        results_by_name = {t.name: (t, t._metadata._vector_index) for t in results}
    else:
        # Internal API returns table info objects with individual vector_index fields
        # Create a simple object to match _vector_index interface
        from types import SimpleNamespace
        results = list(schema._iter_tables(include_vector_index_metadata=include_metadata))
        results_by_name = {}
        for t in results:
            # Create object with same attributes as VectorIndex for uniform access
            vi = SimpleNamespace(
                column=t.vector_index_column_name,
                distance_metric=t.vector_index_distance_metric,
                sql_distance_function=t.vector_index_sql_function_name,
            ) if t.vector_index_enabled else None
            results_by_name[t.name] = (t, vi)

    assert len(results) == 4, f"Expected 4 tables, got {len(results)}"

    # Validate regular table (no vector index)
    _, regular_vi = results_by_name[table_names["regular"]]
    assert regular_vi is None, \
        "Expected vector_index=None for regular table"

    # Validate vector index tables
    for metric_key, expected_column, expected_metric, expected_sql_func in expected_metrics:
        _, vi = results_by_name[table_names[metric_key]]

        assert vi is not None, \
            f"Expected vector_index present for {metric_key}"

        _check_vector_index_metadata(
            vi.column, vi.distance_metric, vi.sql_distance_function,
            expected_column, expected_metric, expected_sql_func,
            expect_full_metadata,
            table_name=metric_key
        )

    log.info(f"{test_case} validated successfully")
