import logging
import time

import pyarrow as pa

from vastdb.table import QueryConfig

log = logging.getLogger(__name__)


def test_basic_projections(session, clean_bucket_name):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')
        columns = pa.schema([
            ('a', pa.int8()),
            ('b', pa.int16()),
            ('c', pa.string()),
            ('d', pa.int16()),
            ('e', pa.int64()),
            ('s', pa.struct([('x', pa.int8()), ('y', pa.int16())]))
        ])

        assert s.tables() == []
        t = s.create_table('t1', columns)
        assert s.tables() == [t]

        sorted_columns = ['a']
        unsorted_columns = ['b']
        p1 = t.create_projection('p1', sorted_columns, unsorted_columns)

        sorted_columns = ['b']
        unsorted_columns = ['c', 'd']
        p2 = t.create_projection('p2', sorted_columns, unsorted_columns)

        projs = t.projections()
        assert projs == [t.projection('p1'), t.projection('p2')]
        p1 = t.projection('p1')
        assert p1.name == 'p1'
        p2 = t.projection('p2')
        assert p2.name == 'p2'

        p1.rename('p_new')
        p2.drop()
        projs = t.projections()
        assert len(projs) == 1
        assert projs[0].name == 'p_new'


def test_query_data_with_projection(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.int64()),
        ('s', pa.utf8()),
    ])
    # need to be large enough in order to consider as projection

    GROUP_SIZE = 128 * 1024
    expected = pa.table(schema=columns, data=[
        [i for i in range(GROUP_SIZE)],
        [i for i in reversed(range(GROUP_SIZE))],
        [f's{i}' for i in range(GROUP_SIZE)],
    ])

    expected_projection_p1 = pa.table(schema=columns, data=[
        [i for i in reversed(range(GROUP_SIZE - 5, GROUP_SIZE))],
        [i for i in range(5)],
        [f's{i}' for i in reversed(range(GROUP_SIZE - 5, GROUP_SIZE))],
    ])

    expected_projection_p2 = pa.table(schema=columns, data=[
        [i for i in range(GROUP_SIZE - 5, GROUP_SIZE)],
        [i for i in reversed(range(5))],
        [f's{i}' for i in range(GROUP_SIZE - 5, GROUP_SIZE)],
    ])

    schema_name = "schema"
    table_name = "table"
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, expected.schema)

        sorted_columns = ['b']
        unsorted_columns = ['a', 's']
        t.create_projection('p1', sorted_columns, unsorted_columns)

        sorted_columns = ['a']
        unsorted_columns = ['b', 's']
        t.create_projection('p2', sorted_columns, unsorted_columns)

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema(schema_name)
        t = s.table(table_name)
        t.insert(expected)
        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual == expected

    time.sleep(3)

    with session.transaction() as tx:
        config = QueryConfig()
        # in nfs mock server num row groups per row block is 1 so need to change this in the config
        config.num_row_groups_per_sub_split = 1

        s = tx.bucket(clean_bucket_name).schema(schema_name)
        t = s.table(table_name)
        projection_actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's'], predicate=(t['b'] < 5), config=config))
        # no projection supply - need to be with p1 projeciton
        assert expected_projection_p1 == projection_actual

        projection_actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's'], predicate=(t['b'] < 5), projection='p1', config=config))
        # expecting results of projection p1 since we asked it specificaly
        assert expected_projection_p1 == projection_actual
        projection_actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's'], predicate=(t['b'] < 5), projection='p2', config=config))
        # expecting results of projection p2 since we asked it specificaly
        assert expected_projection_p2 == projection_actual

        t.drop()
        s.drop()
