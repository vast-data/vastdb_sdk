from typing import Optional

import ibis
import pyarrow as pa
from ibis.expr.operations import Field, Node
from ibis.expr.operations.generic import Literal
from ibis.expr.operations.logical import And, Or
from ibis.expr.operations.structs import StructField
from ibis.expr.types.structs import IbisError

from vastdb import errors


def validate_ibis_support_schema(arrow_schema: pa.Schema):
    """Validate that the provided Arrow schema is compatible with Ibis.

    Raises NotSupportedSchema if the schema contains unsupported fields.
    """
    unsupported_fields = []
    first_exception = None
    for f in arrow_schema:
        try:
            ibis.Schema.from_pyarrow(pa.schema([f]))
        except (IbisError, ValueError, KeyError) as e:
            if first_exception is None:
                first_exception = e
            unsupported_fields.append(f)

    if unsupported_fields:
        raise errors.NotSupportedSchema(
            message=f"Ibis does not support the schema {unsupported_fields=}",
            schema=arrow_schema,
            cause=first_exception
        )


def _is_field(op) -> bool:
    while isinstance(op, StructField):
        op = op.arg

    return isinstance(op, Field)


def _name_of(op) -> str:
    name = []

    while isinstance(op, StructField):
        name.append(op.name)
        op = op.arg

    if not isinstance(op, Field):
        raise NotImplementedError()

    name.append(op.name)
    return ".".join(reversed(name))


def _is_literal(op) -> bool:
    return isinstance(op, Literal)


def retain_predicates(node, requried_names: set[str]) -> Optional[Node]:
    if _is_field(node):
        return node if _name_of(node) in requried_names else None

    if not isinstance(node, Node):
        return node

    new_args = [retain_predicates(arg, requried_names) for arg in node.args]

    if all(arg is not None for arg in new_args):
        return type(node)(*new_args)

    if isinstance(node, And):
        left, right = new_args

        match (left, right):
            case (None, None):
                return None
            case (left, None):
                return left
            case (None, right):
                return right
            case (left, right):
                return And(left, right)

    if isinstance(node, Or):
        left, right = new_args

        if left is None or right is None:
            return None

        return Or(left, right)

    # maybe check specific operators
    return None


def change_columns(node, fields_map: dict[str, Field]) -> Node:
    if _is_field(node):
        return fields_map[node.name]

    if _is_literal(node):
        return node

    new_args = [change_columns(arg, fields_map) for arg in node.args]
    return type(node)(*new_args)
