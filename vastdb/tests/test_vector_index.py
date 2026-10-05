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


def _run_vector_index_test(session: Session,
                           clean_bucket_name: str,
                           schema_name: str,
                           input_spec: VectorIndexSpec | None):
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

        table = schema.create_table(table_name="vec_table", columns=arrow_schema, vector_index=input_spec)
        table.reload_stats()

        result = table._metadata._vector_index
        result_spec = None if result is None else result.to_vector_index_spec()

        match input_spec, result_spec:
            case None, None:
                pass
            case (None, _) | (_, None):
                raise AssertionError(f"Mismatched specs: expected={input_spec}, result={result_spec}")
            case _:
                assert result_spec.column == input_spec.column
                assert result_spec.distance_metric == input_spec.distance_metric
                assert (isinstance(result_spec.cluster_quantization, str) and len(result_spec.cluster_quantization) > 0
                        if isinstance(input_spec.cluster_quantization, DefaultQuantization)
                        else result_spec.cluster_quantization == input_spec.cluster_quantization)
                assert (isinstance(result_spec.search_quantization, str) and len(result_spec.search_quantization) > 0
                        if isinstance(input_spec.search_quantization, DefaultQuantization)
                        else result_spec.search_quantization == input_spec.search_quantization)


@pytest.mark.parametrize("schema_name,input_vector_index", [
    ("schema_no_index", None),
    ("schema_l2sq", VectorIndexSpec("embedding", "l2sq")),
    ("schema_ip", VectorIndexSpec("embedding", "ip")),
])
def test_create_table_with_vector_index_basic_metadata(session: Session,
                                                       clean_bucket_name: str,
                                                       schema_name: str,
                                                       input_vector_index: Optional[VectorIndexSpec]):
    _run_vector_index_test(session, clean_bucket_name, schema_name,
                           input_vector_index)


_MALFORMED_QUANTIZATION_VALUES = ["blabla", "scalar_"]


@pytest.mark.parametrize("cluster_quantization", ["", " "])
def test_vector_index_spec_rejects_blank_cluster_quantization(cluster_quantization: str):
    with pytest.raises(ValueError, match="invalid vector index cluster quantization"):
        VectorIndexSpec("embedding", "l2sq", cluster_quantization=cluster_quantization)


@pytest.mark.parametrize("search_quantization", ["", " "])
def test_vector_index_spec_rejects_blank_search_quantization(search_quantization: str):
    with pytest.raises(ValueError, match="invalid vector index search quantization"):
        VectorIndexSpec("embedding", "l2sq", search_quantization=search_quantization)


@pytest.mark.parametrize("vector_index,expected_error_pattern", [
    pytest.param(VectorIndexSpec("nonexistent_column", "l2sq"),
                 "invalid vector indexed column name nonexistent_column",
                 id="nonexistent_column"),
    pytest.param(VectorIndexSpec("embedding", "invalid_metric"),
                 "invalid vector index distance metric invalid_metric, supported metrics: 'l2sq', 'ip'",
                 id="invalid_distance_metric"),
    *[
        pytest.param(VectorIndexSpec("embedding", "l2sq", cluster_quantization=cluster, search_quantization=search),
                     "invalid vector quantization combination cluster=.* search=.*",
                     id=f"invalid_quantization_{cluster}_{search}")
        for cluster, search in _INVALID_QUANTIZATION_COMBINATIONS
    ],
    *[
        pytest.param(VectorIndexSpec("embedding", "l2sq", cluster_quantization=bad_quantization),
                     rf"invalid vector index cluster quantization.*{bad_quantization}.*supported values:",
                     id=f"malformed_cluster_quantization_{bad_quantization}")
        for bad_quantization in _MALFORMED_QUANTIZATION_VALUES
    ],
    *[
        pytest.param(VectorIndexSpec("embedding", "l2sq", search_quantization=bad_quantization),
                     rf"invalid vector index search quantization.*{bad_quantization}.*supported values:",
                     id=f"malformed_search_quantization_{bad_quantization}")
        for bad_quantization in _MALFORMED_QUANTIZATION_VALUES
    ],
])
def test_create_table_with_invalid_vector_index(session: Session,
                                                clean_bucket_name: str,
                                                vector_index: VectorIndexSpec,
                                                expected_error_pattern: str):
    """Test that table creation fails with BadRequest and an informative error for invalid vector index parameters."""
    pytest_skip_if_vector_index_not_supported(session)

    with session.transaction() as tx:
        schema = tx.bucket(clean_bucket_name).create_schema("schema1")
        vec_type = pa.list_(pa.field('', pa.float32(), False), 128)
        arrow_schema = pa.schema([('id', pa.int64()), ('embedding', vec_type)])

        with pytest.raises(errors.BadRequest, match=expected_error_pattern):
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
        (f"cluster_{cluster}_search_{search}",
         VectorIndexSpec("embedding", "l2sq", cluster_quantization=cluster, search_quantization=search))
        for cluster, search in itertools.product(_QUANTIZATION_VALUES, repeat=2)
        if (cluster, search) not in _INVALID_QUANTIZATION_COMBINATIONS
    ],
])
def test_vector_index_quantization_values(session: Session, clean_bucket_name: str,
                                          schema_name: str, input_spec: VectorIndexSpec):
    pytest_skip_if_vector_index_not_supported(session)
    _run_vector_index_test(session, clean_bucket_name, schema_name,
                           input_spec)


@pytest.mark.parametrize(
    "secondary_indices,expected_error",
    [
        (["id", "missing"], "vector secondary index column=missing not found"),
        (["id", "id"], "duplicate vector secondary index column=id"),
    ],
    ids=["missing_column", "duplicate_column"],
)
def test_create_table_rejects_invalid_secondary_index_columns(
    session: Session,
    clean_bucket_name: str,
    secondary_indices: list[str],
    expected_error: str,
):
    if session.features.vast_version != (5, 5, 3, 0):
        pytest.skip("Vector index secondary indices are supported only in 5.5.3 for now")

    with session.transaction() as tx:
        schema = tx.bucket(clean_bucket_name).create_schema("schema_invalid_secondary")
        vec_type = pa.list_(pa.field('', pa.float32(), False), 8)
        arrow_schema = pa.schema([('id', pa.int64()), ('embedding', vec_type)])

        with pytest.raises(errors.BadRequest, match=expected_error):
            schema.create_table(
                table_name="vec_table",
                columns=arrow_schema,
                vector_index=VectorIndexSpec(
                    "embedding",
                    "l2sq",
                    secondary_indices=secondary_indices,
                ),
            )


def test_create_table_with_secondary_indices(session: Session, clean_bucket_name: str):
    if session.features.vast_version != (5, 5, 3, 0):
        pytest.skip("Vector index secondary indices are supported only in 5.5.3 for now")

    with session.transaction() as tx:
        schema = tx.bucket(clean_bucket_name).create_schema("schema_secondary_indices")
        vec_type = pa.list_(pa.field('', pa.float32(), False), 8)
        arrow_schema = pa.schema([
            ('id', pa.int64()),
            ('category', pa.string()),
            ('embedding', vec_type),
            ('payload', pa.string()),
        ])
        input_spec = VectorIndexSpec(
            "embedding",
            "l2sq",
            secondary_indices=["id", "category"],
        )

        table = schema.create_table(
            table_name="vec_table",
            columns=arrow_schema,
            vector_index=input_spec,
        )
        table.reload_schema()
        table.reload_stats()

        assert table.vector_index is not None
        assert table.vector_index.column == "embedding"
        assert table.vector_index.secondary_indices == ["id", "category"]
        assert table.vector_index.to_vector_index_spec().secondary_indices == ["id", "category"]
        assert table._metadata.non_updatable_column_names() == {"embedding", "id", "category"}
        assert isinstance(table.stats.vector_secondary_index_sorting_score, int)
        assert isinstance(table.stats.vector_secondary_index_sorting_done, bool)
