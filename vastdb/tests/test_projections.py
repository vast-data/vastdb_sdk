import pyarrow as pa
import logging

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
