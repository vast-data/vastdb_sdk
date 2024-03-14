import pytest


def test_schemas(rpc, clean_bucket_name):
    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
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


def test_commits_and_rollbacks(rpc, clean_bucket_name):
    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() == []
        b.create_schema("s3")
        assert b.schemas() != []
        # implicit commit

    with pytest.raises(ZeroDivisionError):
        with rpc.transaction() as tx:
            b = tx.bucket(clean_bucket_name)
            b.schema("s3").drop()
            assert b.schemas() == []
            1/0  # rollback schema dropping

    with rpc.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() != []
