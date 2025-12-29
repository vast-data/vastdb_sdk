from collections import defaultdict

import numpy as np
import pyarrow as pa
import pytest

from vastdb._adbc import _ibis_to_qe_predicates
from vastdb._internal import VectorIndex, VectorIndexSpec
from vastdb._table_interface import IbisPredicate
from vastdb.conftest import SessionFactory
from vastdb.table_metadata import TableMetadata, TableRef, TableType

DIM = 8
TEST_DISTANCE_FUNC = 'array_distance'
TEST_DISTANCE_METRIC = 'l2sq'

VectorColumnArrowType = pa.list_(
    pa.field(name='item', type=pa.float32(), nullable=False), DIM)

query_vector: np.ndarray = np.ones(DIM)
first_closest_vector = (query_vector * 1.1).tolist()
second_closest_vector = (query_vector * 1.2).tolist()
third_closest_vector = (query_vector * 1.3).tolist()
fourth_closest_vector = (query_vector * 1.4).tolist()
fifth_closest_vector = (query_vector * 1.5).tolist()
furthest_vector = (np.ones(DIM) * 5).tolist()

vector_column_name = 'vector_col'

data = [
    {
        "id": 0,
        "n1": 1,
        "n2": 100,
        vector_column_name: first_closest_vector,
    },
    {
        "id": 1,
        "n1": 2,
        "n2": 100,
        vector_column_name: second_closest_vector,
    },
    {
        "id": 2,
        "n1": 3,
        "n2": 200,
        vector_column_name: third_closest_vector,
    },
    {
        "id": 3,
        "n1": 4,
        "n2": 200,
        vector_column_name: fourth_closest_vector,
    },
    {
        "id": 4,
        "n1": 5,
        "n2": 200,
        vector_column_name: fifth_closest_vector,
    },
    {
        "id": 5,
        "n1": 6,
        "n2": 200,
        vector_column_name: furthest_vector,
    },

]


def into_arrow_arrays(data: list[dict]) -> list[list]:
    agg = defaultdict(list)
    for d in data:
        for k, v in d.items():
            agg[k].append(v)

    return [v for v in agg.values()]


def test_sanity(session_factory: SessionFactory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

    arrow_schema = pa.schema([('id', pa.int32()), ('n1', pa.int32(
    )), ('n2', pa.int32()), (vector_column_name, VectorColumnArrowType),])

    limit = 3

    ref = TableRef(clean_bucket_name, 's', 't')
    table_md = TableMetadata(ref,
                             arrow_schema,
                             TableType.Regular,
                             vector_index=VectorIndex(vector_column_name,
                                                      TEST_DISTANCE_METRIC,
                                                      TEST_DISTANCE_FUNC))
    data_table = pa.table(schema=arrow_schema, data=into_arrow_arrays(data))

    with session.transaction() as tx:
        table = tx.bucket(clean_bucket_name).create_schema(
            's').create_table('t', arrow_schema)
        table.insert(data_table)

    # TODO merge in same tx
    with session.transaction() as tx:
        table = tx.table_from_metadata(table_md)

        reader = table.vector_search(vec=query_vector.tolist(),
                                     columns=['id', 'n1', 'n2'],
                                     limit=limit)

        result_table = reader.read_all()

        assert set([v.as_py() for v in result_table['n1']]) == {1, 2, 3}


def test_with_predicates(session_factory: SessionFactory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

    vector_column_name = 'vector_column'
    arrow_schema = pa.schema([('id', pa.int32()), ('n1', pa.int32(
    )), ('n2', pa.int32()), (vector_column_name, VectorColumnArrowType),])
    limit = 3

    ref = TableRef(clean_bucket_name, 's', 't')
    table_md = TableMetadata(ref, arrow_schema, TableType.Regular,
                             vector_index=VectorIndex(vector_column_name,
                                                      TEST_DISTANCE_METRIC,
                                                      TEST_DISTANCE_FUNC))
    data_table = pa.table(schema=arrow_schema, data=into_arrow_arrays(data))

    with session.transaction() as tx:
        table = tx.bucket(clean_bucket_name).create_schema(
            's').create_table('t', arrow_schema)
        table.insert(data_table)

    with session.transaction() as tx:
        table = tx.table_from_metadata(table_md)

        pred = table_md.ibis_table['n2'] == 200
        reader = table.vector_search(vec=query_vector.tolist(),
                                     columns=['id', 'n1', 'n2'],
                                     limit=limit,
                                     predicate=pred)

        result_table = reader.read_all()

        assert set([v.as_py() for v in result_table['n1']]) == {3, 4, 5}


arrow_schema = pa.schema([('id', pa.int32()), ('n1', pa.int32(
)), ('n2', pa.int32()), (vector_column_name, VectorColumnArrowType),])

ref = TableRef('b', 's', 't')
table_md = TableMetadata(ref, arrow_schema, TableType.Regular)


@pytest.mark.parametrize('ibis_predicate, expected', [
    ((table_md.ibis_table['n1'] == 1) & (table_md.ibis_table['n2'] == 2),
     '("n1" = 1) AND ("n2" = 2)'),
    ((table_md.ibis_table['n1'] == 1),
     '"n1" = 1')
])
def test_ibis_to_query_engine_predicates(ibis_predicate: IbisPredicate, expected: str):
    assert _ibis_to_qe_predicates(ibis_predicate) == expected


@pytest.mark.skip(reason="see https://vastdata.atlassian.net/browse/ORION-307908")
def test_with_predicates_get_vector_index_properties_from_server(
    session_factory: SessionFactory,
    clean_bucket_name: str
    ):
    session = session_factory(with_adbc=True)

    vector_column_name = 'vector_column'
    vector_index_distance_metric = 'l2sq'
    vector_index_sql_distance_function = "array_distance"

    arrow_schema = pa.schema([('id', pa.int32()), ('n1', pa.int32(
    )), ('n2', pa.int32()), (vector_column_name, VectorColumnArrowType),])
    limit = 3

    ref = TableRef(clean_bucket_name, 's', 't')
    table_md = TableMetadata(ref, arrow_schema, TableType.Regular)
    data_table = pa.table(schema=arrow_schema, data=into_arrow_arrays(data))

    with session.transaction() as tx:
        table = (tx.bucket(clean_bucket_name)
                 .create_schema('s')
                 .create_table('t',
                               arrow_schema,
                               vector_index=VectorIndexSpec(vector_column_name,
                                                            vector_index_distance_metric)))
        table.insert(data_table)

    with session.transaction() as tx:
        table = tx.table_from_metadata(table_md)

        table.reload_stats()
        assert table.vector_index == VectorIndex(
            column=vector_column_name,
            distance_metric=vector_index_distance_metric,
            sql_distance_function=vector_index_sql_distance_function)

        pred = table_md.ibis_table['n2'] == 200
        reader = table.vector_search(vec=query_vector.tolist(),
                                     columns=['id', 'n1', 'n2'],
                                     limit=limit,
                                     predicate=pred)

        result_table = reader.read_all()

        assert set([v.as_py() for v in result_table['n1']]) == {3, 4, 5}
