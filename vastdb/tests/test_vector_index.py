"""Tests for vector index functionality."""

import itertools
import logging
from typing import Optional

import pyarrow as pa
import pytest

from vastdb import errors
from vastdb._internal import DefaultQuantization, VectorIndexSpec
from vastdb.session import Session

from .util import pytest_skip_if_vector_index_not_supported

log = logging.getLogger(__name__)

_QUANTIZATION_VALUES = ["scalar_8bit", "none"]

_INVALID_QUANTIZATION_COMBINATIONS = {
    ("scalar_8bit", "none"),
}

_DEFAULT_VECTOR_QUANTIZATION = "scalar_8bit"


def _resolved_quantization(value: str | DefaultQuantization) -> str:
    return _DEFAULT_VECTOR_QUANTIZATION if isinstance(value, DefaultQuantization) else value


def _run_vector_index_test(session: Session,
                           clean_bucket_name: str,
                           schema_name: str,
                           input_spec: Optional[VectorIndexSpec],
                           expect_success: bool,
                           expected_spec: Optional[VectorIndexSpec]):
    if input_spec is not None:
        pytest_skip_if_vector_index_not_supported(session)
    with session.transaction() as tx:
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        if input_spec is None:
            arrow_schema = pa.schema([('id', pa.int64()), ('data', pa.string())])
        else:
            vector_dimension = 128
            vec_type = pa.list_(pa.field('', pa.float32(), False), vector_dimension)
            arrow_schema = pa.schema([('id', pa.int64()), ('embedding', vec_type)])

        if not expect_success:
            with pytest.raises(errors.BadRequest):
                schema.create_table(table_name="vec_table", columns=arrow_schema, vector_index=input_spec)
            return

        table = schema.create_table(table_name="vec_table", columns=arrow_schema, vector_index=input_spec)
        table.reload_stats()

        result = table._metadata._vector_index
        result_spec = None if result is None else result.to_vector_index_spec()
        assert result_spec == expected_spec


@pytest.mark.parametrize("schema_name,input_vector_index", [
    ("schema_no_index", None),
    ("schema_l2sq", VectorIndexSpec("embedding", "l2sq")),
    ("schema_ip", VectorIndexSpec("embedding", "ip")),
])
def test_create_table_with_vector_index_basic_metadata(session: Session,
                                                       clean_bucket_name: str,
                                                       schema_name: str,
                                                       input_vector_index: Optional[VectorIndexSpec]):
    expected_vector_index = None
    if input_vector_index is not None:
        expected_vector_index = VectorIndexSpec(input_vector_index.column, input_vector_index.distance_metric,
                                                _DEFAULT_VECTOR_QUANTIZATION, _DEFAULT_VECTOR_QUANTIZATION)
    _run_vector_index_test(session, clean_bucket_name, schema_name,
                           input_vector_index, True, expected_vector_index)


@pytest.mark.parametrize("table_name,vector_index,expected_error", [
    pytest.param("table_non_existent_column",
                 VectorIndexSpec("nonexistent_column", "l2sq"),
                 "invalid vector indexed column name nonexistent_column",
                 id="nonexistent_column"),
    pytest.param("table_invalid_metric",
                 VectorIndexSpec("embedding", "invalid_metric"),
                 "invalid vector index distance metric invalid_metric, supported metrics: 'l2sq', 'ip'",
                 id="invalid_distance_metric"),
])
def test_create_table_with_invalid_vector_index(session: Session,
                                                clean_bucket_name: str,
                                                table_name: str,
                                                vector_index: VectorIndexSpec,
                                                expected_error: str):
    """Test that table creation fails with appropriate error messages for invalid vector index parameters."""
    if vector_index is not None:
        pytest_skip_if_vector_index_not_supported(session)
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


_MALFORMED_QUANTIZATION_VALUES = [
    pytest.param("blabla", id="invalid_option"),
    pytest.param("scalar_", id="incomplete_type"),
]


@pytest.mark.parametrize("quantization_field,quantization_error_name", [
    ("cluster_quantization", "cluster quantization"),
    ("search_quantization", "search quantization"),
])
@pytest.mark.parametrize("bad_quantization", _MALFORMED_QUANTIZATION_VALUES)
def test_create_table_with_malformed_vector_index_quantization(session: Session,
                                                               clean_bucket_name: str,
                                                               quantization_field: str,
                                                               quantization_error_name: str,
                                                               bad_quantization: str):
    """Test that malformed quantization values are rejected with an informative error"""
    pytest_skip_if_vector_index_not_supported(session)

    vector_index = VectorIndexSpec("embedding", "l2sq", **{quantization_field: bad_quantization})
    expected_error = (rf"invalid vector index {quantization_error_name}"
                      rf".*{bad_quantization}.*supported values:")
    schema_name = f"malformed_{quantization_field}_{bad_quantization or 'empty'}"

    with session.transaction() as tx:
        bucket = tx.bucket(clean_bucket_name)
        schema = bucket.create_schema(schema_name)

        vec_type = pa.list_(pa.field('', pa.float32(), False), 128)
        arrow_schema = pa.schema([('id', pa.int64()), ('embedding', vec_type)])

        with pytest.raises(errors.BadRequest, match=expected_error):
            schema.create_table(table_name="vec_table", columns=arrow_schema, vector_index=vector_index)


def test_vector_index_metadata_from_stats(session: Session, clean_bucket_name: str):
    """Test that vector index metadata is correctly retrieved from table stats."""
    pytest_skip_if_vector_index_not_supported(session)

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


@pytest.mark.parametrize("schema_name,input_spec", [
    ("default_quantization", VectorIndexSpec("embedding", "l2sq")),
    ("cluster_scalar_8bit_default_search", VectorIndexSpec("embedding", "l2sq", "scalar_8bit")),
    ("cluster_none_default_search", VectorIndexSpec("embedding", "l2sq", "none")),
    *[
        (f"cluster_{cluster_quant}_search_{search_quant}",
         VectorIndexSpec("embedding", "l2sq", cluster_quantization=cluster_quant, search_quantization=search_quant))
        for cluster_quant, search_quant in itertools.product(_QUANTIZATION_VALUES, repeat=2)
    ],
])
def test_vector_index_quantization_values(session: Session, clean_bucket_name: str,
                                          schema_name: str, input_spec: VectorIndexSpec):
    pytest_skip_if_vector_index_not_supported(session)

    resolved_cluster_quantization = _resolved_quantization(input_spec.cluster_quantization)
    resolved_search_quantization = _resolved_quantization(input_spec.search_quantization)

    is_invalid = (resolved_cluster_quantization, resolved_search_quantization) in _INVALID_QUANTIZATION_COMBINATIONS
    expected_spec = None if is_invalid else VectorIndexSpec(
        input_spec.column,
        input_spec.distance_metric,
        resolved_cluster_quantization,
        resolved_search_quantization,
    )

    _run_vector_index_test(session, clean_bucket_name, schema_name,
                           input_spec, not is_invalid, expected_spec)
