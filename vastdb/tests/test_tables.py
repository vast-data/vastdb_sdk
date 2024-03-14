import pyarrow as pa


def test_tables(rpc, clean_bucket_name):
    with rpc.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')
        columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
        t = s.create_table('t1', columns)

        rb = pa.record_batch(schema=columns, data=[
            [111, 222],
            [0.5, 1.5],
            ['a', 'b'],
        ])
        expected = pa.Table.from_batches([rb])
        t.insert(rb)

        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual == expected

        actual = pa.Table.from_batches(t.select(columns=['a', 'b']))
        assert actual == expected.select(['a', 'b'])

        actual = pa.Table.from_batches(t.select(columns=['b', 's', 'a']))
        assert actual == expected.select(['b', 's', 'a'])

        actual = pa.Table.from_batches(t.select(columns=['s']))
        assert actual == expected.select(['s'])

        actual = pa.Table.from_batches(t.select(columns=[]))
        assert actual == expected.select([])

        t.drop()
        s.drop()
