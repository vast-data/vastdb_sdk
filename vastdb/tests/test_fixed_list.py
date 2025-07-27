import datetime
import decimal
import itertools
import random
from typing import Any, Union, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import vastdb.errors

from .util import (
    assert_pandas_df_equal,
    convert_pandas_df_to_hashable_values,
    prepare_data,
)

supported_fixed_list_element_types = [
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.float32(),
    pa.float64(),
    pa.decimal128(10),
    pa.date32(),
    pa.timestamp("s"),
    pa.time32("ms"),
    pa.time64("us"),
]

# All the supported element types are supported as non-nullable.
supported_fixed_list_element_fields = [
    pa.field(name="item", type=element_type, nullable=False)
    for element_type in supported_fixed_list_element_types
]

unsupported_fixed_list_element_types = [
    pa.string(),
    pa.list_(pa.int64()),
    pa.list_(pa.int64(), 1),
    pa.map_(pa.utf8(), pa.float64()),
    pa.struct([("x", pa.int16())]),
    pa.bool_(),
    pa.binary(),
]

unsupported_fixed_list_element_fields = [  # Nullable types are not supported.
                                            pa.field(name="item", type=element_type, nullable=True)
                                            for element_type in itertools.chain(
        supported_fixed_list_element_types, unsupported_fixed_list_element_types
    )
                                        ] + [  # Not nullable unsupported type are unsupported.
                                            pa.field(name="item", type=element_type, nullable=False)
                                            for element_type in unsupported_fixed_list_element_types
                                        ]

unsupported_fixed_list_types = (
        [
            pa.list_(element_field, 1)
            for element_field in unsupported_fixed_list_element_fields
        ] +
        # Fixed list with amount of elements exceeding the supported limit.
        [pa.list_(
            pa.field("item", pa.int64(), nullable=False), np.iinfo(np.int32).max
        )]
)

invalid_fixed_list_types = [
    # Fixed list 0 elements.
    pa.list_(pa.field("item", pa.int64(), nullable=False), 0),
]


def test_vectors(session, clean_bucket_name):
    """
    Test table with efficient vector type - pa.FixedSizeListArray[not nullable numeric].
    """
    dimension = 100
    element_type = pa.float32()
    num_rows = 50

    columns = pa.schema(
        [("id", pa.int64()), ("vec", pa.list_(pa.field(name="item", type=element_type, nullable=False), dimension),)]
    )
    ids = range(num_rows)
    expected = pa.table(
        schema=columns,
        data=[
            ids,
            [[i] * dimension for i in ids],
        ],
    )

    with prepare_data(session, clean_bucket_name, "s", "t", expected) as t:
        assert t.arrow_schema == columns

        # Full scan.
        actual = t.select().read_all()
        assert actual == expected

        # Select by id.
        select_id = random.randint(0, num_rows)
        actual = t.select(predicate=(t["id"] == select_id)).read_all()
        assert actual.to_pydict()["vec"] == [[select_id] * dimension]
        assert actual == expected.filter(pc.field("id") == select_id)


def convert_scalar_type_pyarrow_to_numpy(arrow_type: pa.DataType):
    return pa.array([], type=arrow_type).to_numpy().dtype.type


def generate_random_pyarrow_value(
        element: Union[pa.DataType, pa.Field], nulls_prob: float = 0.2
) -> Any:
    """
    Generates a random value compatible with the provided PyArrow type.

    Args:
        element: The pyarrow field/type to generate values for.
        nulls_prob: Probability of creating nulls.
    """
    assert 0 <= nulls_prob <= 1

    nullable = True

    # Convert Field to DataType.
    if isinstance(element, pa.DataType):
        pa_type = element
    elif isinstance(element, pa.Field):
        pa_type = element.type
        nullable = element.nullable
    else:
        raise TypeError(
            f"Expected pyarrow.DataType or pyarrow.Field, got {type(element)}"
        )

    if nullable and random.random() < nulls_prob:
        return None

    if pa.types.is_boolean(pa_type):
        return random.choice([True, False])
    if pa.types.is_integer(pa_type):
        np_type = convert_scalar_type_pyarrow_to_numpy(pa_type)
        iinfo = np.iinfo(np_type)
        return np.random.randint(iinfo.min, iinfo.max, dtype=np_type)
    if pa.types.is_floating(pa_type):
        np_type = convert_scalar_type_pyarrow_to_numpy(pa_type)
        finfo = np.finfo(np_type)
        return np_type(random.uniform(float(finfo.min), float(finfo.max)))
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "".join(
            random.choices("abcdefghijklmnopqrstuvwxyz ", k=random.randint(5, 20))
        )
    if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
        return random.randbytes(random.randint(5, 20))
    if pa.types.is_timestamp(pa_type):
        # Generate a random timestamp within a range (e.g., last 10 years)
        start_datetime = datetime.datetime(2015, 1, 1, tzinfo=datetime.timezone.utc)
        end_datetime = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        random_seconds = random.uniform(
            0, (end_datetime - start_datetime).total_seconds()
        )
        return start_datetime + datetime.timedelta(seconds=random_seconds)
    if pa.types.is_date(pa_type):
        start_date = datetime.date(2000, 1, 1)
        end_date = datetime.date(2025, 1, 1)
        random_days = random.randint(0, (end_date - start_date).days)
        return start_date + datetime.timedelta(days=random_days)
    if pa.types.is_time(pa_type):
        return datetime.time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
    if pa.types.is_decimal(pa_type):
        pa_type = cast(pa.Decimal128Type, pa_type)
        decimal_value = decimal.Decimal(
            round(random.uniform(-1000.0, 1000.0), pa_type.precision)
        )
        quantize_template = decimal.Decimal("1e-%d" % pa_type.scale)
        return decimal_value.quantize(quantize_template)
    if pa.types.is_null(pa_type):  # Explicit NullType
        return None
    if pa.types.is_list(pa_type) or pa.types.is_fixed_size_list(pa_type):
        # For ListType, recursively generate elements for the value_type
        pa_type = (
            cast(pa.FixedSizeListType, pa_type)
            if pa.types.is_fixed_size_list(pa_type)
            else cast(pa.ListType, pa_type)
        )
        list_size = (
            pa_type.list_size
            if pa.types.is_fixed_size_list(pa_type)
            else random.randint(0, 5)
        )
        list_elements = [
            generate_random_pyarrow_value(pa_type.value_field, nulls_prob)
            for _ in range(list_size)
        ]
        return list_elements
    if pa.types.is_struct(pa_type):
        struct_dict = {}
        for field in cast(pa.StructType, pa_type):
            # Recursively generate value for each field in the struct
            struct_dict[field.name] = generate_random_pyarrow_value(field, nulls_prob)
        return struct_dict
    if pa.types.is_map(pa_type):
        num_entries = random.randint(0, 3)  # Random number of map entries
        pa_type = cast(pa.MapType, pa_type)
        return {
            generate_random_pyarrow_value(pa_type.key_field, nulls_prob): generate_random_pyarrow_value(
                pa_type.item_field, nulls_prob)
            for _ in range(num_entries)
        }

    raise NotImplementedError(
        f"Generation for PyArrow type {pa_type} not implemented yet."
    )


@pytest.mark.parametrize("element_field", supported_fixed_list_element_fields)
def test_fixed_list_type_values(session, clean_bucket_name, element_field):
    list_size = 250
    num_rows = 100

    vec_type = pa.list_(element_field, list_size)
    schema = pa.schema(
        {"id": pa.int64(), "vec": vec_type, "random_int": pa.int64()})
    expected = pa.table(
        schema=schema,
        data=[list(range(num_rows))] + [[generate_random_pyarrow_value(schema.field(col_name)) for _ in range(num_rows)]
                                        for col_name in
                                        schema.names[1:]],
    )
    # Convert the list to tuple in order to support comparison as a whole.
    pd_expected = convert_pandas_df_to_hashable_values(expected.to_pandas())

    with prepare_data(session, clean_bucket_name, "s", "t", expected) as table:
        assert table.arrow_schema == schema
        actual = table.select().read_all()
        assert actual == expected

        # Select by id.
        id_to_select = random.randint(0, num_rows - 1)
        select_by_id = table.select(predicate=(table["id"] == id_to_select)).read_all()
        assert len(select_by_id) == 1  # ID is unique.
        assert select_by_id == expected.filter(pc.field("id") == id_to_select)

        # Choose a random vector which is not null. Nulls should not be selected using == , != operators, but by isnull.
        # In addition, nulls are discarded unless isnull is used (meaning != 1 will return both not nulls and not 1).
        vector_to_select = random.choice(expected.filter(~pc.field('vec').is_null())['vec'].to_numpy())

        # TODO VSDK-36: Remove this workaround when the issue with negative decimals is predicate is fixed.
        if pa.types.is_decimal(element_field.type):
            vector_to_select = abs(vector_to_select)

        # Dtype is not asserted since pandas convert the dtype of integer to float when there are (or could be)
        # NaN/None values.
        # Select by vector value.
        select_by_vector = table.select(predicate=(table["vec"] == vector_to_select)).read_all()
        assert_pandas_df_equal(select_by_vector.to_pandas(),
                               pd_expected.loc[pd_expected['vec'] == tuple(vector_to_select)], check_dtype=False)

        # Not equal to vector value.
        select_by_vector = table.select(predicate=(table["vec"] != vector_to_select)).read_all()
        assert_pandas_df_equal(select_by_vector.to_pandas(),
                               pd_expected.loc[(pd_expected['vec'] != tuple(vector_to_select)) &
                                               pd_expected['vec'].notnull()], check_dtype=False)

        # Not equal to vector value or null.
        select_by_vector = table.select(
            predicate=((table["vec"] != vector_to_select) | (table['vec'].isnull()))).read_all()
        assert_pandas_df_equal(select_by_vector.to_pandas(),
                               pd_expected.loc[pd_expected['vec'] != tuple(vector_to_select)], check_dtype=False)

        # Lexicographically greater than vector.
        select_by_vector = table.select(predicate=(table["vec"] > vector_to_select)).read_all()
        assert_pandas_df_equal(select_by_vector.to_pandas(), pd_expected.loc[
            pd_expected['vec'].notnull() & (pd_expected['vec'] > tuple(vector_to_select))], check_dtype=False)

        # Lexicographically less than vector.
        select_by_vector = table.select(predicate=(table["vec"] < vector_to_select)).read_all()
        assert_pandas_df_equal(select_by_vector.to_pandas(), pd_expected.loc[
            pd_expected['vec'].notnull() & (pd_expected['vec'] < tuple(vector_to_select))], check_dtype=False)


@pytest.mark.parametrize("list_type", unsupported_fixed_list_types)
def test_unsupported_fixed_list_types(session, clean_bucket_name, list_type):
    schema = pa.schema({"fixed_list": list_type})
    empty_table = pa.table(schema=schema, data=[[]])

    with pytest.raises((vastdb.errors.BadRequest, vastdb.errors.NotSupported), match=r'TabularUnsupportedColumnType'):
        with prepare_data(session, clean_bucket_name, "s", "t", empty_table):
            pass


@pytest.mark.parametrize("list_type", invalid_fixed_list_types)
def test_invalid_fixed_list_types(session, clean_bucket_name, list_type):
    schema = pa.schema({"fixed_list": list_type})
    empty_table = pa.table(schema=schema, data=[[]])

    with pytest.raises(vastdb.errors.BadRequest, match=r'TabularInvalidColumnTypeParam'):
        with prepare_data(session, clean_bucket_name, "s", "t", empty_table):
            pass


def test_invalid_values_fixed_list(session, clean_bucket_name):
    dimension = 10
    element_type = pa.float32()

    col_name = "vec"
    schema = pa.schema([(col_name, pa.list_(pa.field(name="item", type=element_type, nullable=False), dimension))])
    empty_table = pa.table(schema=schema, data=[[]])

    with prepare_data(session, clean_bucket_name, "s", "t", empty_table) as table:
        invalid_fields = [
            pa.field(col_name, pa.list_(pa.field(name="item", type=element_type, nullable=False), dimension - 1)),
            pa.field(col_name, pa.list_(pa.field(name="item", type=element_type, nullable=False), dimension + 1)),
            pa.field(col_name, pa.list_(pa.field(name="item", type=element_type, nullable=True), dimension)),
            schema.field(0).with_nullable(False),
        ]
        for field in invalid_fields:
            # Everything that could be null should be in order to be invalid regarding the values and not just the type.
            rb = pa.record_batch(
                schema=pa.schema([field]),
                data=[[[1] * field.type.list_size]]
            )
            with pytest.raises((vastdb.errors.BadRequest, vastdb.errors.NotFound, vastdb.errors.NotSupported),
                               match=r'(TabularInvalidColumnTypeParam)|(TabularUnsupportedColumnType)|(TabularMismatchColumnType)'):
                table.insert(rb)

        # Amount of elements in fixed list is not equal to the list size is enforced by Arrow.
        with pytest.raises(pa.ArrowInvalid):
            # Insert with empty list.
            pa.record_batch(
                schema=schema,
                data=[[[generate_random_pyarrow_value(element_type, 0) for _ in range(dimension + 1)]]],
            )
