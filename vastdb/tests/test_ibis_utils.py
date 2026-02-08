import ibis
import pytest

from vastdb import _ibis_support as isup


@pytest.mark.parametrize(
    "pred_generator, expected_pred_generator",
    [
        (
            lambda t: t.a > ibis.literal(4),
            lambda t: t.a > ibis.literal(4)
        ),
        (
            lambda t: t.c > 4,
            lambda _: None
        ),
        (
            lambda t: (t.a > 4) & (t.c > 4) & (t.b > 7),
            lambda t: (t.a > 4) & (t.b > 7)
        ),
        (
            lambda t: ((t.a > 4) & (t.b < 16)) & ((t.c > 7) & (t.b != 8)),
            lambda t: ((t.a > 4) & (t.b < 16)) & (t.b != 8),
        ),
        (
            lambda t: ((t.a > 4) & (t.b < 16)) & (t.b != 8),
            lambda t: ((t.a > 4) & (t.b < 16)) & (t.b != 8),
        ),
        (
            lambda t: ((t.c > 4) & (t.c < 16)) & (t.b != 8),
            lambda t: t.b != 8,
        ),
        (
            lambda t: ((t.c > 4) & (t.c < 16)) & (t.c != 8),
            lambda _: None,
        ),
    ],
)
def test_retain_predicates(pred_generator, expected_pred_generator):
    required_names = {"a", "b"}
    t = ibis.table(
        {
            "a": "int64",
            "b": "int32",
            "c": "int8",
        },
        name="my_table",
    )

    pred = isup.retain_predicates(pred_generator(t).op(), required_names)
    expected = expected_pred_generator(t)

    if expected is None:
        assert pred is None
    else:
        assert pred.to_expr().equals(expected)


@pytest.mark.parametrize(
    "pred_generator, expected_pred_generator",
    [
        (
            lambda t: t.a > ibis.literal(4),
            lambda t: t.z > ibis.literal(4)
        ),
        (
            lambda t: t.b > ibis.literal(4),
            lambda t: t.y > ibis.literal(4),
        ),
        (
            lambda t: (t.a > 4) & (t.c > 4) & (t.b > 7),
            lambda t: (t.z > 4) & (t.x > 4) & (t.y > 7),
        ),
        (
            lambda t: ((t.a > 4) & (t.b < 16)) & ((t.c > 7) & (t.b != 8)),
            lambda t: ((t.z > 4) & (t.y < 16)) & ((t.x > 7) & (t.y != 8)),
        ),
        (
            lambda t: ((t.a > 4) & (t.b < 16)) & (t.b != 8),
            lambda t: ((t.z > 4) & (t.y < 16)) & (t.y != 8),
        ),
        (
            lambda t: ((t.c > 4) & (t.c < 16)) & (t.b != 8),
            lambda t: ((t.x > 4) & (t.x < 16)) & (t.y != 8),
        ),
    ],
)
def test_change_column_names(pred_generator, expected_pred_generator):
    t1 = ibis.table(
        {
            "a": "int64",
            "b": "int32",
            "c": "int8",
        },
        name="my_table",
    )

    t2 = ibis.table(
        {
            "x": "int8",
            "y": "int32",
            "z": "int64",
        },
        name="my_table2",
    )
    fields_map = {
        "a": t2.z,
        "b": t2.y,
        "c": t2.x,
    }

    pred = isup.change_columns(pred_generator(t1).op(), fields_map)
    expected = expected_pred_generator(t2)
    assert pred.to_expr().equals(expected)
