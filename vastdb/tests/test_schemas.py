import pytest

from .. import errors


def test_schemas(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() == []

        s = b.create_schema('s1')
        assert s.bucket == b
        assert b.schemas() == [s]

        s.rename('s2')
        assert s.bucket == b
        assert s.name == 's2'
        assert b.schemas()[0].name == 's2'

        s.drop()
        assert b.schemas() == []


def test_exists(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() == []

        s = b.create_schema('s1')

        assert b.schemas() == [s]
        with pytest.raises(errors.SchemaExists):
            b.create_schema('s1')

        assert b.schemas() == [s]
        assert b.create_schema('s1', fail_if_exists=False) == s
        assert b.schemas() == [s]


def test_commits_and_rollbacks(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() == []
        b.create_schema("s3")
        assert b.schemas() != []
        # implicit commit

    with pytest.raises(ZeroDivisionError):
        with session.transaction() as tx:
            b = tx.bucket(clean_bucket_name)
            b.schema("s3").drop()
            assert b.schemas() == []
            1/0  # rollback schema dropping

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() != []

def test_list_snapshots(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.snapshots()
        assert s == []
