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
            1 / 0  # rollback schema dropping

    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        assert b.schemas() != []


def test_list_snapshots(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        b.snapshots()  # VAST Catalog may create some snapshots


def test_nested_schemas(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s1 = b.create_schema('s1')
        s1_s2 = s1.create_schema('s2')
        s1_s3 = s1.create_schema('s3')
        s1_s3_s4 = s1_s3.create_schema('s4')
        s5 = b.create_schema('s5')

        assert b.schema('s1') == s1
        assert s1.schema('s2') == s1_s2
        assert s1.schema('s3') == s1_s3
        assert s1_s3.schema('s4') == s1_s3_s4
        assert b.schema('s5') == s5

        assert b.schemas() == [s1, s5]
        assert s1.schemas() == [s1_s2, s1_s3]
        assert s1_s2.schemas() == []
        assert s1_s3.schemas() == [s1_s3_s4]
        assert s1_s3_s4.schemas() == []
        assert s5.schemas() == []

        s1_s3_s4.drop()
        assert s1_s3.schemas() == []
        s1_s3.drop()
        assert s1.schemas() == [s1_s2]
        s1_s2.drop()
        assert s1.schemas() == []

        assert b.schemas() == [s1, s5]
        s1.drop()
        assert b.schemas() == [s5]
        s5.drop()
        assert b.schemas() == []


def test_schema_pagination(session, clean_bucket_name):
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        names = [f's{i}' for i in range(10)]
        schemas = [b.create_schema(name) for name in names]
        assert b.schemas(batch_size=3) == schemas

        s0 = b.schema('s0')
        names = [f'q{i}' for i in range(10)]
        subschemas = [s0.create_schema(name) for name in names]
        assert s0.schemas(batch_size=3) == subschemas
