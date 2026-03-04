import ibis
import pyarrow as pa
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
