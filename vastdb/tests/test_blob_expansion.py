"""Tests for blob expansion functionality."""

import logging

import pyarrow as pa
import pytest

from vastdb import errors
from vastdb.schema import Schema
from vastdb.session import Session
from vastdb.table import (
    BLOB_EXPANSION_MANDATORY_FIELD_METADATA,
    BlobExpansion,
    BlobExpansionConfig,
    ExpansionFormat,
    Table,
)

log = logging.getLogger(__name__)

SCHEMA_NAME = 's1'
SOURCE_TABLE = 't1'
TARGET_TABLE = 't1_expanded'
SOURCE_COLUMN = 'data'
SOURCE_SCHEMA = pa.schema([('id', pa.int64()), (SOURCE_COLUMN, pa.string())])
COPY_SOURCE_COLUMN = f'blob_expansion_{SOURCE_COLUMN}'

KAFKA_TOPIC_PARTITIONS = 4
ERRORS_TOPIC_SUFFIX = "_blob_expansion_errors"
TOPIC_NAME = 'src_topic'
TOPIC_TARGET_TABLE = f'{TOPIC_NAME}_expanded'
TOPIC_SOURCE_COLUMN = 'value'
TOPIC_SCHEMA = pa.schema([(TOPIC_SOURCE_COLUMN, pa.string())])
ERRORS_TOPIC_NAME = TOPIC_NAME + ERRORS_TOPIC_SUFFIX
RETENTION_MS = 86_400_000


def _skip_unless_blob_expansion(session: Session) -> None:
    try:
        session.features.check_blob_expansion()
    except errors.NotSupportedVersion:
        pytest.skip("Blob expansion requires VAST 5.5+")


def _skip_unless_mandatory_fields(session: Session) -> None:
    _skip_unless_blob_expansion(session)
    try:
        session.features.check_blob_expansion_mandatory_fields_error_topic()
    except errors.NotSupportedVersion:
        pytest.skip("Mandatory blob-expansion fields require VAST 5.5.3+")


def _skip_unless_blob_expansion_without_mandatory(session: Session) -> None:
    """Blob expansion yes, mandatory fields no (e.g. 5.5.0-5.5.2)."""
    _skip_unless_blob_expansion(session)
    try:
        session.features.check_blob_expansion_mandatory_fields_error_topic()
    except errors.NotSupportedVersion:
        return
    pytest.skip("Requires blob expansion without mandatory-fields support (<5.5.3)")


def _get_metadata_by_name(schema: pa.Schema) -> dict:
    return {field.name: field.metadata for field in schema}


def _has_mandatory_metadata(metadata: dict | None) -> bool:
    return metadata is not None and all(
        metadata.get(key) == value
        for key, value in BLOB_EXPANSION_MANDATORY_FIELD_METADATA.items()
    )


def _get_metadata_by_path(schema: pa.Schema) -> dict:
    """Map every field to its metadata by dotted path, walking struct, list and map children."""
    by_path: dict = {}

    def walk(field: pa.Field, path: str) -> None:
        by_path[path] = field.metadata
        t = field.type
        if pa.types.is_struct(t):
            children = [t.field(i) for i in range(t.num_fields)]
        elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
            children = [t.value_field]
        elif pa.types.is_map(t):
            children = [t.key_field, t.item_field]
        else:
            children = []
        for child in children:
            walk(child, f"{path}.{child.name}")

    for field in schema:
        walk(field, field.name)
    return by_path


def _assert_mandatory_fields_from_schema(schema: pa.Schema, expected_mandatory: list[str]) -> None:
    """Assert expected fields and every ancestor of a nested expected field are mandatory.

    The FRD requires that marking a nested field also marks its ancestors. Descendants of an
    expected field may inherit mandatoriness. Any other field must not be mandatory.
    """
    by_path = _get_metadata_by_path(schema)
    required: set[str] = set()
    allowed: set[str] = set()
    for path in expected_mandatory:
        parts = path.split('.')
        ancestors_and_self = ['.'.join(parts[:i + 1]) for i in range(len(parts))]
        required.update(ancestors_and_self)
        allowed.update(ancestors_and_self)
        allowed.update(p for p in by_path if p.startswith(f"{path}."))

    for path in required:
        assert path in by_path, f"missing column {path} in {list(by_path)}"
        meta = by_path[path]
        assert _has_mandatory_metadata(meta), f"{path} is not mandatory: {meta}"
    for path, meta in by_path.items():
        if path not in allowed:
            assert not _has_mandatory_metadata(meta), f"{path} unexpectedly mandatory"


def _create_tables(tx, bucket_name: str, target_schema: pa.Schema) -> tuple[Schema, Table]:
    """Create the source table and the expansion target table; return (schema, source_table)."""
    s = tx.bucket(bucket_name).create_schema(SCHEMA_NAME)
    t = s.create_table(SOURCE_TABLE, SOURCE_SCHEMA)
    s.create_table(TARGET_TABLE, target_schema)
    return s, t


def _create_expansion(
    tx,
    bucket_name: str,
    expansion_schema: pa.Schema,
    *,
    target_schema: pa.Schema | None = None,
    config: BlobExpansionConfig | None = None,
) -> tuple[Table, BlobExpansion]:
    """Create the source table, the expansion target table and the expansion itself."""
    _, t = _create_tables(tx, bucket_name, target_schema if target_schema is not None else expansion_schema)
    be = t.create_blob_expansion(
        source_column_name=SOURCE_COLUMN,
        expansion_schema=expansion_schema,
        target_table_name=TARGET_TABLE,
        config=config if config is not None else BlobExpansionConfig(expansion_format=ExpansionFormat.JSON),
    )
    return t, be


def _create_topic_or_skip(tx, bucket_name: str, schema_name: str, topic_name: str, columns: pa.Schema) -> None:
    """Create a Kafka topic via the internal API, skipping if the bucket is not Kafka-enabled."""
    try:
        tx._rpc.api._create_table_internal(
            bucket=bucket_name,
            schema=schema_name,
            name=topic_name,
            arrow_schema=columns,
            txid=tx.txid,
            topic_partitions=KAFKA_TOPIC_PARTITIONS,
        )
    except (errors.HttpError, errors.NotSupported) as e:
        pytest.skip(f"cannot create a Kafka topic in bucket {bucket_name}: {e}")


def _create_topic_source(tx, bucket_name: str, expansion_schema: pa.Schema) -> tuple[Schema, Table]:
    """Create a Kafka topic source plus its expansion target table; return (schema, topic_table)."""
    s = tx.bucket(bucket_name).create_schema(SCHEMA_NAME)
    _create_topic_or_skip(tx, bucket_name, s.name, TOPIC_NAME, TOPIC_SCHEMA)
    s.create_table(TOPIC_TARGET_TABLE, expansion_schema)
    t = s.table(TOPIC_NAME)
    assert t is not None
    return s, t


def _create_topic_expansion(tx, bucket_name: str, expansion_schema: pa.Schema, **config_kwargs):
    """Create a Kafka topic source and an expansion on it; return (schema, topic_table, expansion)."""
    s, t = _create_topic_source(tx, bucket_name, expansion_schema)
    be = t.create_blob_expansion(
        source_column_name=TOPIC_SOURCE_COLUMN,
        expansion_schema=expansion_schema,
        target_table_name=TOPIC_TARGET_TABLE,
        config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, **config_kwargs),
    )
    return s, t, be


def test_mark_mandatory_fields_nested_two_layers():
    """mark_mandatory_fields marks a leaf two struct levels down, not the parents."""
    schema = pa.schema([
        pa.field('outer', pa.struct([
            pa.field('inner', pa.struct([
                pa.field('must', pa.string()),
                pa.field('skip', pa.int32()),
            ])),
            pa.field('other', pa.bool_()),
        ])),
        pa.field('top', pa.int32()),
    ])

    marked = BlobExpansion.mark_mandatory_fields(schema, ['outer.inner.must'])

    outer = marked.field('outer')
    inner = outer.type.field('inner')
    must = inner.type.field('must')
    skip = inner.type.field('skip')

    assert _has_mandatory_metadata(must.metadata)
    assert not _has_mandatory_metadata(skip.metadata)
    assert not _has_mandatory_metadata(inner.metadata)
    assert not _has_mandatory_metadata(outer.metadata)
    assert not _has_mandatory_metadata(marked.field('top').metadata)


MANDATORY_CREATE_CASES = [
    pytest.param(
        pa.schema([
            pa.field('field1', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field('field2', pa.int32()),
            pa.field('field3', pa.float64()),
        ]),
        ['field1'],
        id='one_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('a', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field('b', pa.int32(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field('c', pa.bool_()),
        ]),
        ['a', 'b'],
        id='multiple_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('simple_field', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field(
                'nested_struct',
                pa.struct([
                    ('sub_field1', pa.int32()),
                    ('sub_field2', pa.string()),
                ]),
                metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA,
            ),
            pa.field('list_field', pa.list_(pa.int32())),
        ]),
        ['simple_field', 'nested_struct'],
        id='complex_with_mandatory',
    ),
]


MANDATORY_ADD_CASES = [
    pytest.param(
        pa.schema([
            pa.field('field1', pa.string()),
            pa.field('field2', pa.int32()),
        ]),
        pa.schema([
            pa.field('field3', pa.float64(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field('field4', pa.bool_()),
        ]),
        ['field3'],
        id='add_one_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('base', pa.string()),
        ]),
        pa.schema([
            pa.field('m1', pa.int32(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            pa.field('m2', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
        ]),
        ['m1', 'm2'],
        id='add_multiple_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('base', pa.string()),
        ]),
        pa.schema([
            pa.field(
                'nested',
                pa.struct([('x', pa.int32()), ('y', pa.string())]),
                metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA,
            ),
            pa.field('lst', pa.list_(pa.float64())),
        ]),
        ['nested'],
        id='add_complex_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('base', pa.string()),
        ]),
        pa.schema([
            pa.field('parent', pa.struct([
                pa.field('child', pa.int32(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
                pa.field('sibling', pa.string()),
            ])),
        ]),
        ['parent.child'],
        id='add_nested_child_only_mandatory',
    ),
    pytest.param(
        pa.schema([
            pa.field('base', pa.string()),
        ]),
        pa.schema([
            pa.field('outer', pa.struct([
                pa.field('items', pa.list_(pa.field('item', pa.struct([
                    pa.field('must', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
                    pa.field('skip', pa.int32()),
                ])))),
            ])),
        ]),
        ['outer.items.item.must'],
        id='add_struct_of_list_leaf_mandatory',
    ),
]


def test_basic_blob_expansion(session: Session, clean_bucket_name: str):
    """Create, retrieve and drop a blob expansion."""
    _skip_unless_blob_expansion(session)

    expansion_schema = pa.schema([
        ('field1', pa.string()),
        ('field2', pa.int32()),
        ('field3', pa.float64()),
    ])
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, expansion_schema,
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, copy_source_column=False),
        )

        assert be.source_column_name == SOURCE_COLUMN
        assert be.target_table_name.endswith(f'/{clean_bucket_name}/{SCHEMA_NAME}/{TARGET_TABLE}')
        assert be.config.expansion_format == ExpansionFormat.JSON
        assert be.config.copy_source_column is False

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.source_column_name == be.source_column_name
        assert retrieved.target_table_name == be.target_table_name
        assert retrieved.config.add_missing_values_output == be.config.add_missing_values_output
        assert retrieved.config.add_excessive_values_output == be.config.add_excessive_values_output
        assert retrieved.config.create_errors_topic == be.config.create_errors_topic
        assert retrieved.config.errors_topic_retention_ms == be.config.errors_topic_retention_ms

        be.drop()
        with pytest.raises(errors.MissingBlobExpansion):
            t.blob_expansion(SOURCE_COLUMN)


def test_blob_expansion_nested_schema(session: Session, clean_bucket_name: str):
    """Create an expansion whose schema contains struct and list columns."""
    _skip_unless_blob_expansion(session)

    expansion_schema = pa.schema([
        ('simple_field', pa.string()),
        ('nested_struct', pa.struct([('sub_field1', pa.int32()), ('sub_field2', pa.string())])),
        ('list_field', pa.list_(pa.int32())),
    ])
    with session.transaction() as tx:
        _, be = _create_expansion(tx, clean_bucket_name, expansion_schema)
        assert be.source_column_name == SOURCE_COLUMN
        assert be.target_table_name.endswith(f'/{clean_bucket_name}/{SCHEMA_NAME}/{TARGET_TABLE}')
        be.drop()


def test_blob_expansion_with_copy_source_column(session: Session, clean_bucket_name: str):
    """Create an expansion that copies the source column into the target table."""
    _skip_unless_blob_expansion(session)

    expansion_schema = pa.schema([('extracted_field1', pa.string()), ('extracted_field2', pa.int32())])
    with session.transaction() as tx:
        _, be = _create_expansion(
            tx, clean_bucket_name, expansion_schema,
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, copy_source_column=True),
        )
        assert be.config.copy_source_column is True
        be.drop()


def test_blob_expansion_missing_error(session: Session, clean_bucket_name: str):
    """Getting an expansion for a column that has none reports the full table ref."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        _, t = _create_tables(tx, clean_bucket_name, pa.schema([('field1', pa.string())]))

        with pytest.raises(errors.MissingBlobExpansion) as exc_info:
            t.blob_expansion('nonexistent_column')

        assert exc_info.value.table_ref.bucket == clean_bucket_name
        assert exc_info.value.table_ref.schema == SCHEMA_NAME
        assert exc_info.value.table_ref.table == SOURCE_TABLE
        assert exc_info.value.source_column == 'nonexistent_column'


def test_blob_expansion_add_columns(session: Session, clean_bucket_name: str):
    """Adding columns to an existing expansion extends its schema."""
    _skip_unless_blob_expansion(session)

    initial_schema = pa.schema([('field1', pa.string()), ('field2', pa.int32())])
    added_schema = pa.schema([('field3', pa.float64()), ('field4', pa.bool_())])
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, initial_schema,
            target_schema=pa.schema(list(initial_schema) + list(added_schema)),
        )
        be.add_columns(columns_to_add=added_schema, add_copy_source_column=False)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.schema.names == initial_schema.names + added_schema.names
        be.drop()


def test_blob_expansion_drop_columns(session: Session, clean_bucket_name: str):
    """Dropping columns from an existing expansion leaves the remaining ones intact."""
    _skip_unless_blob_expansion(session)

    kept_schema = pa.schema([('field1', pa.string()), ('field2', pa.int32())])
    dropped_schema = pa.schema([('field3', pa.float64()), ('field4', pa.bool_())])
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, pa.schema(list(kept_schema) + list(dropped_schema)),
        )

        be.drop_columns(columns_to_remove=dropped_schema, remove_copy_source_column=False)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.schema.names == kept_schema.names
        be.drop()


def test_blob_expansion_drop_non_existent_columns(session: Session, clean_bucket_name: str):
    """Dropping a column that was never part of the expansion must fail."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        _, be = _create_expansion(
            tx, clean_bucket_name, pa.schema([('field1', pa.string()), ('field2', pa.int32())]),
        )
        with pytest.raises(errors.NotFound):
            be.drop_columns(columns_to_remove=pa.schema([('non_existent_field', pa.string())]))
        be.drop()


def test_negative_create_duplicate_blob_expansion(session: Session, clean_bucket_name: str):
    """Creating a second expansion on the same source column must fail."""
    _skip_unless_blob_expansion(session)

    expansion_schema = pa.schema([('field1', pa.string())])
    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, expansion_schema)
        with pytest.raises(errors.InternalServerError, match="failed to create blob expansion"):
            t.create_blob_expansion(
                source_column_name=SOURCE_COLUMN,
                expansion_schema=expansion_schema,
                target_table_name=TARGET_TABLE,
                config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON),
            )
        be.drop()


def test_positive_create_with_output_columns_disabled(session: Session, clean_bucket_name: str):
    """Create with missing/excessive output columns off and confirm the GET roundtrip."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, pa.schema([('field1', pa.string())]),
            config=BlobExpansionConfig(
                expansion_format=ExpansionFormat.JSON,
                add_missing_values_output=False,
                add_excessive_values_output=False,
            ),
        )
        assert be.config.add_missing_values_output is False
        assert be.config.add_excessive_values_output is False

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.config.add_missing_values_output is False
        assert retrieved.config.add_excessive_values_output is False
        be.drop()


@pytest.mark.skip(
    reason=(
        "500 InternalError on flag-only AlterBlobExpansion; blocked by "
        "https://vastdata.atlassian.net/browse/ORION-411726"
    )
)
def test_positive_remove_copy_source_column_after_create(session: Session, clean_bucket_name: str):
    """Create with copy-source enabled, then remove that column via a flag-only alter."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, pa.schema([('field1', pa.string())]),
            config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON, copy_source_column=True),
        )
        assert be.config.copy_source_column is True

        be.drop_columns(remove_copy_source_column=True)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.config.copy_source_column is False
        assert COPY_SOURCE_COLUMN not in _get_metadata_by_name(retrieved.schema)
        be.drop()


@pytest.mark.skip(
    reason=(
        "500 InternalError on flag-only AlterBlobExpansion; blocked by "
        "https://vastdata.atlassian.net/browse/ORION-411726"
    )
)
def test_positive_flag_only_add_then_remove_copy_source(session: Session, clean_bucket_name: str):
    """Flag-only alter: add copy-source after create, then remove it."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, pa.schema([('field1', pa.string())]))
        assert be.config.copy_source_column is False

        be.add_columns(add_copy_source_column=True)
        after_add = t.blob_expansion(SOURCE_COLUMN)
        assert after_add.config.copy_source_column is True
        assert COPY_SOURCE_COLUMN in _get_metadata_by_name(after_add.schema)

        be.drop_columns(remove_copy_source_column=True)
        after_remove = t.blob_expansion(SOURCE_COLUMN)
        assert after_remove.config.copy_source_column is False
        assert COPY_SOURCE_COLUMN not in _get_metadata_by_name(after_remove.schema)
        be.drop()


@pytest.mark.skip(
    reason=(
        "500 InternalError on flag-only AlterBlobExpansion; blocked by "
        "https://vastdata.atlassian.net/browse/ORION-411726"
    )
)
def test_negative_remove_copy_source_column_when_never_enabled(session: Session, clean_bucket_name: str):
    """Removing copy-source when it was never created is rejected, and the expansion is unchanged."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, pa.schema([('field1', pa.string())]))
        assert be.config.copy_source_column is False

        with pytest.raises(errors.NotFound, match="Source column .* not found in target table"):
            be.drop_columns(remove_copy_source_column=True)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.config.copy_source_column is False
        be.drop()


@pytest.mark.skip(
    reason=(
        "PANIC when create_errors_topic is used with non-Kafka tables; blocked by "
        "https://vastdata.atlassian.net/browse/ORION-411726"
    )
)
def test_blob_expansion_with_errors_topic(session: Session, clean_bucket_name: str):
    """Create an errors topic on a regular table and verify the config roundtrip via get."""
    _skip_unless_mandatory_fields(session)

    expansion_schema = pa.schema([('field1', pa.string()), ('field2', pa.int32())])
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, expansion_schema,
            config=BlobExpansionConfig(
                expansion_format=ExpansionFormat.JSON,
                create_errors_topic=True,
                errors_topic_retention_ms=RETENTION_MS,
            ),
        )
        assert be.config.create_errors_topic is True
        assert be.config.errors_topic_retention_ms == RETENTION_MS

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert retrieved.config.create_errors_topic is True
        assert retrieved.config.errors_topic_retention_ms == RETENTION_MS

        be.drop()
        with pytest.raises(errors.MissingBlobExpansion):
            t.blob_expansion(SOURCE_COLUMN)


@pytest.mark.skip(reason="PySDK does not support Kafka topic creation")
def test_blob_expansion_errors_topic_on_kafka_topic(session: Session, clean_bucket_name: str):
    """Create an errors topic on a Kafka topic source; dropping the expansion keeps the topic."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        s, t, be = _create_topic_expansion(
            tx, clean_bucket_name, pa.schema([('field1', pa.string()), ('field2', pa.int32())]),
            create_errors_topic=True,
            errors_topic_retention_ms=RETENTION_MS,
        )
        assert be.config.create_errors_topic is True
        assert be.config.errors_topic_retention_ms == RETENTION_MS
        assert s.table(ERRORS_TOPIC_NAME, fail_if_missing=False) is not None

        retrieved = t.blob_expansion(TOPIC_SOURCE_COLUMN)
        assert retrieved.config.create_errors_topic is True
        assert retrieved.config.errors_topic_retention_ms == RETENTION_MS

        be.drop()
        with pytest.raises(errors.MissingBlobExpansion):
            t.blob_expansion(TOPIC_SOURCE_COLUMN)

        # FRD: dropping blob expansion must not delete the errors topic.
        errors_topic = s.table(ERRORS_TOPIC_NAME, fail_if_missing=False)
        assert errors_topic is not None
        errors_topic.drop()


@pytest.mark.skip(reason="PySDK does not support Kafka topic creation")
def test_negative_create_errors_topic_when_errors_topic_exists(session: Session, clean_bucket_name: str):
    """Create with create_errors_topic must fail if the errors topic already exists."""
    _skip_unless_blob_expansion(session)

    expansion_schema = pa.schema([('field1', pa.string())])
    with session.transaction() as tx:
        s, t = _create_topic_source(tx, clean_bucket_name, expansion_schema)
        _create_topic_or_skip(tx, clean_bucket_name, s.name, ERRORS_TOPIC_NAME, TOPIC_SCHEMA)

        with pytest.raises(errors.Conflict):
            t.create_blob_expansion(
                source_column_name=TOPIC_SOURCE_COLUMN,
                expansion_schema=expansion_schema,
                target_table_name=TOPIC_TARGET_TABLE,
                config=BlobExpansionConfig(
                    expansion_format=ExpansionFormat.JSON,
                    create_errors_topic=True,
                ),
            )


@pytest.mark.skip(reason="PySDK does not support Kafka topic creation")
def test_negative_delete_errors_topic_while_expansion_exists(session: Session, clean_bucket_name: str):
    """FRD: the errors topic cannot be deleted while the blob expansion exists."""
    _skip_unless_blob_expansion(session)

    with session.transaction() as tx:
        s, _, be = _create_topic_expansion(
            tx, clean_bucket_name, pa.schema([('field1', pa.string())]),
            create_errors_topic=True,
        )
        errors_topic = s.table(ERRORS_TOPIC_NAME, fail_if_missing=False)
        assert errors_topic is not None
        with pytest.raises(errors.Conflict):
            errors_topic.drop()
        be.drop()


@pytest.mark.parametrize("expansion_schema,expected_mandatory", MANDATORY_CREATE_CASES)
def test_create_blob_expansion_mandatory_roundtrip(
    session: Session,
    clean_bucket_name: str,
    expansion_schema: pa.Schema,
    expected_mandatory: list[str],
):
    """Server must persist and return Vast:mandatory metadata on create."""
    _skip_unless_mandatory_fields(session)

    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, expansion_schema)
        _assert_mandatory_fields_from_schema(be.schema, expected_mandatory)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        _assert_mandatory_fields_from_schema(retrieved.schema, expected_mandatory)
        be.drop()


@pytest.mark.parametrize("initial_schema,columns_to_add,expected_mandatory", MANDATORY_ADD_CASES)
def test_add_columns_with_mandatory_roundtrip(
    session: Session,
    clean_bucket_name: str,
    initial_schema: pa.Schema,
    columns_to_add: pa.Schema,
    expected_mandatory: list[str],
):
    """Server must persist Vast:mandatory metadata for columns added via alter."""
    _skip_unless_mandatory_fields(session)

    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, initial_schema,
            target_schema=pa.schema(list(initial_schema) + list(columns_to_add)),
        )
        be.add_columns(columns_to_add=columns_to_add)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        _assert_mandatory_fields_from_schema(retrieved.schema, expected_mandatory)
        be.drop()


@pytest.mark.parametrize(
    "column_to_recycle",
    [
        pytest.param(
            pa.field('recycled', pa.string(), metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA),
            id='simple_mandatory',
        ),
        pytest.param(
            pa.field(
                'recycled_nested',
                pa.struct([('x', pa.int32()), ('y', pa.utf8())]),
                metadata=BLOB_EXPANSION_MANDATORY_FIELD_METADATA,
            ),
            id='complex_mandatory',
        ),
    ],
)
def test_drop_then_add_column_with_mandatory(
    session: Session,
    clean_bucket_name: str,
    column_to_recycle: pa.Field,
):
    """Drop a column then re-add it with Vast:mandatory metadata (no in-place alter)."""
    _skip_unless_mandatory_fields(session)

    keep = pa.field('keep', pa.int32())
    plain = pa.field(column_to_recycle.name, column_to_recycle.type)
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, pa.schema([keep, plain]),
            target_schema=pa.schema([keep, column_to_recycle]),
        )

        be.drop_columns(columns_to_remove=pa.schema([plain]))
        after_drop = t.blob_expansion(SOURCE_COLUMN)
        assert column_to_recycle.name not in _get_metadata_by_name(after_drop.schema)

        be.add_columns(columns_to_add=pa.schema([column_to_recycle]))
        retrieved = t.blob_expansion(SOURCE_COLUMN)
        _assert_mandatory_fields_from_schema(retrieved.schema, [column_to_recycle.name])
        assert 'keep' in _get_metadata_by_name(retrieved.schema)
        be.drop()


@pytest.mark.parametrize(
    "columns_to_add,mandatory_fields,config_kwargs",
    [
        pytest.param(
            pa.schema([('f1', pa.string()), ('f2', pa.int32())]),
            ['f1'],
            {},
            id='one_mandatory',
        ),
        pytest.param(
            pa.schema([('m1', pa.int32()), ('m2', pa.string()), ('ok', pa.bool_())]),
            ['m1', 'm2'],
            {},
            id='multiple_mandatory',
        ),
        pytest.param(
            pa.schema([
                ('nested', pa.struct([('x', pa.int32())])),
                ('plain', pa.float64()),
            ]),
            ['nested'],
            {'add_copy_source_column': True},
            id='complex_plus_copy_source',
        ),
        pytest.param(
            pa.schema([('f1', pa.string()), ('f2', pa.int32())]),
            ['f1'],
            {
                'add_missing_values_output': True,
                'add_excessive_values_output': True,
            },
            id='mandatory_plus_outputs',
        ),
    ],
)
def test_alter_add_with_mark_mandatory_fields(
    session: Session,
    clean_bucket_name: str,
    columns_to_add: pa.Schema,
    mandatory_fields: list[str],
    config_kwargs: dict,
):
    """Online: add_columns() adds columns marked via mark_mandatory_fields."""
    _skip_unless_mandatory_fields(session)

    initial_schema = pa.schema([('base', pa.string())])
    with session.transaction() as tx:
        t, be = _create_expansion(
            tx, clean_bucket_name, initial_schema,
            target_schema=pa.schema(list(initial_schema) + list(columns_to_add)),
        )
        marked = BlobExpansion.mark_mandatory_fields(columns_to_add, mandatory_fields)
        be.add_columns(columns_to_add=marked, **config_kwargs)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        for field in columns_to_add:
            assert field.name in _get_metadata_by_name(retrieved.schema)
        _assert_mandatory_fields_from_schema(retrieved.schema, mandatory_fields)
        be.drop()


@pytest.mark.parametrize(
    "columns_to_drop,config_kwargs",
    [
        pytest.param(
            pa.schema([('f1', pa.string()), ('f2', pa.bool_())]),
            {},
            id='drop_fields',
        ),
        pytest.param(
            pa.schema([('nested', pa.struct([('x', pa.int32())]))]),
            {'remove_copy_source_column': True},
            id='drop_complex_plus_flag',
            marks=pytest.mark.skip(
                reason=(
                    "404 NoSuchKey when removing an uncreated copy-source column; blocked by "
                    "https://vastdata.atlassian.net/browse/ORION-411726"
                )
            ),
        ),
    ],
)
def test_alter_remove_with_mark_mandatory_fields(
    session: Session,
    clean_bucket_name: str,
    columns_to_drop: pa.Schema,
    config_kwargs: dict,
):
    """Online: drop_columns() drops columns built via mark_mandatory_fields."""
    _skip_unless_mandatory_fields(session)

    keep = pa.field('keep', pa.int32())
    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, pa.schema([keep, *columns_to_drop]))

        # Marking before remove exercises the same helper path as add; marks are irrelevant after drop.
        marked = BlobExpansion.mark_mandatory_fields(columns_to_drop, [columns_to_drop.field(0).name])
        be.drop_columns(columns_to_remove=marked, **config_kwargs)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        assert 'keep' in _get_metadata_by_name(retrieved.schema)
        for field in columns_to_drop:
            assert field.name not in _get_metadata_by_name(retrieved.schema)
        be.drop()


def test_alter_fields_to_mandatory(session: Session, clean_bucket_name: str):
    """Mark existing expansion fields as mandatory after the expansion was created."""
    _skip_unless_mandatory_fields(session)

    expansion_schema = pa.schema([('field1', pa.string()), ('field2', pa.int32())])
    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, expansion_schema)

        marked = BlobExpansion.mark_mandatory_fields(expansion_schema, ['field1', 'field2'])
        be.add_columns(columns_to_add=marked)
        retrieved = t.blob_expansion(SOURCE_COLUMN)
        _assert_mandatory_fields_from_schema(retrieved.schema, ['field1', 'field2'])
        be.drop()


def test_create_blob_expansion_mandatory_unsupported(session: Session, clean_bucket_name: str):
    """Create with Vast:mandatory must raise when the cluster lacks mandatory-fields support."""
    _skip_unless_blob_expansion_without_mandatory(session)

    expansion_schema = BlobExpansion.mark_mandatory_fields(
        pa.schema([('field1', pa.string()), ('field2', pa.int32())]),
        ['field1'],
    )
    with session.transaction() as tx:
        _, t = _create_tables(tx, clean_bucket_name, expansion_schema)
        with pytest.raises(errors.NotSupportedVersion):
            t.create_blob_expansion(
                source_column_name=SOURCE_COLUMN,
                expansion_schema=expansion_schema,
                target_table_name=TARGET_TABLE,
                config=BlobExpansionConfig(expansion_format=ExpansionFormat.JSON),
            )


def test_alter_mandatory_unsupported(session: Session, clean_bucket_name: str):
    """Adding mandatory fields to an existing expansion must raise when unsupported."""
    _skip_unless_blob_expansion_without_mandatory(session)

    initial_schema = pa.schema([('base', pa.string())])
    columns_to_add = BlobExpansion.mark_mandatory_fields(pa.schema([('f1', pa.string())]), ['f1'])
    with session.transaction() as tx:
        _, be = _create_expansion(
            tx, clean_bucket_name, initial_schema,
            target_schema=pa.schema(list(initial_schema) + list(columns_to_add)),
        )
        with pytest.raises(errors.NotSupportedVersion):
            be.add_columns(columns_to_add=columns_to_add)
        be.drop()


@pytest.mark.parametrize("initial_schema,columns_to_add,expected_mandatory", MANDATORY_ADD_CASES)
def test_add_columns_with_mandatory_roundtrip_two_transactions(
    session: Session,
    clean_bucket_name: str,
    initial_schema: pa.Schema,
    columns_to_add: pa.Schema,
    expected_mandatory: list[str],
):
    """Vast:mandatory metadata survives a commit: create in one transaction, add in another."""
    _skip_unless_mandatory_fields(session)

    with session.transaction() as tx:
        _create_expansion(
            tx, clean_bucket_name, initial_schema,
            target_schema=pa.schema(list(initial_schema) + list(columns_to_add)),
        )

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(SCHEMA_NAME).table(SOURCE_TABLE)
        be = t.blob_expansion(SOURCE_COLUMN)
        try:
            be.add_columns(columns_to_add=columns_to_add)

            retrieved = t.blob_expansion(SOURCE_COLUMN)
            _assert_mandatory_fields_from_schema(retrieved.schema, expected_mandatory)
        finally:
            be.drop()


@pytest.mark.parametrize("mandatory_field", ['parent', 'parent.child'])
def test_create_mandatory_nested_top_vs_bottom(
    session: Session,
    clean_bucket_name: str,
    mandatory_field: str,
):
    """Server must return Vast:mandatory exactly where it was set: on the parent or on the child."""
    _skip_unless_mandatory_fields(session)

    expansion_schema = BlobExpansion.mark_mandatory_fields(
        pa.schema([
            pa.field('parent', pa.struct([('child', pa.int32()), ('sibling', pa.string())])),
            pa.field('plain', pa.string()),
        ]),
        [mandatory_field],
    )
    with session.transaction() as tx:
        t, be = _create_expansion(tx, clean_bucket_name, expansion_schema)

        retrieved = t.blob_expansion(SOURCE_COLUMN)
        _assert_mandatory_fields_from_schema(retrieved.schema, [mandatory_field])
        be.drop()
