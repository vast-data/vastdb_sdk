from vastdb import v2


def test_schemas(test_bucket_name):
    with v2.context() as ctx:
        b = ctx.bucket(test_bucket_name)
        for s in b.schemas():
            s.drop()

        assert b.schemas() == []

        b.create_schema('s1')
        s = b.schema('s1')
        assert s.bucket == b
        assert b.schemas() == [s]

        s.rename('s2')
        assert s.bucket == b
        assert s.name == 's2'
        assert b.schemas()[0].name == 's2'

        s.drop()
        assert b.schemas() == []
