import logging
import struct
import urllib.parse
from collections import defaultdict, namedtuple
from datetime import datetime
from enum import Enum
from typing import Union, Optional, Iterator
import ibis
import xmltodict
import math
from functools import cmp_to_key
import pyarrow.parquet as pq
import flatbuffers
import pyarrow as pa
import requests
import json
import itertools
from aws_requests_auth.aws_auth import AWSRequestsAuth
import urllib3
import re

from . import errors
from ipaddress import IPv4Address, IPv6Address

import vast_flatbuf.org.apache.arrow.computeir.flatbuf.BinaryLiteral as fb_binary_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.BooleanLiteral as fb_bool_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Call as fb_call
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.DateLiteral as fb_date32_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.DecimalLiteral as fb_decimal_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Expression as fb_expression
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.FieldIndex as fb_field_index
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.FieldRef as fb_field_ref
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Float32Literal as fb_float32_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Float64Literal as fb_float64_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Int16Literal as fb_int16_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Int32Literal as fb_int32_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Int64Literal as fb_int64_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Int8Literal as fb_int8_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Literal as fb_literal
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Relation as fb_relation
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.RelationImpl as rel_impl
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.Source as fb_source
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.StringLiteral as fb_string_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.TimeLiteral as fb_time_lit
import vast_flatbuf.org.apache.arrow.computeir.flatbuf.TimestampLiteral as fb_timestamp_lit
import vast_flatbuf.org.apache.arrow.flatbuf.Binary as fb_binary
import vast_flatbuf.org.apache.arrow.flatbuf.Bool as fb_bool
import vast_flatbuf.org.apache.arrow.flatbuf.Date as fb_date
import vast_flatbuf.org.apache.arrow.flatbuf.Decimal as fb_decimal
import vast_flatbuf.org.apache.arrow.flatbuf.Field as fb_field
import vast_flatbuf.org.apache.arrow.flatbuf.FloatingPoint as fb_floating_point
import vast_flatbuf.org.apache.arrow.flatbuf.Int as fb_int
import vast_flatbuf.org.apache.arrow.flatbuf.Schema as fb_schema
import vast_flatbuf.org.apache.arrow.flatbuf.Time as fb_time
import vast_flatbuf.org.apache.arrow.flatbuf.Struct_ as fb_struct
import vast_flatbuf.org.apache.arrow.flatbuf.List as fb_list
import vast_flatbuf.org.apache.arrow.flatbuf.Map as fb_map
import vast_flatbuf.org.apache.arrow.flatbuf.FixedSizeBinary as fb_fixed_size_binary
import vast_flatbuf.org.apache.arrow.flatbuf.Timestamp as fb_timestamp
import vast_flatbuf.org.apache.arrow.flatbuf.Utf8 as fb_utf8
import vast_flatbuf.tabular.AlterColumnRequest as tabular_alter_column
import vast_flatbuf.tabular.AlterSchemaRequest as tabular_alter_schema
import vast_flatbuf.tabular.AlterTableRequest as tabular_alter_table
import vast_flatbuf.tabular.AlterProjectionTableRequest as tabular_alter_projection
import vast_flatbuf.tabular.CreateSchemaRequest as tabular_create_schema
import vast_flatbuf.tabular.ImportDataRequest as tabular_import_data
import vast_flatbuf.tabular.S3File as tabular_s3_file
import vast_flatbuf.tabular.CreateProjectionRequest as tabular_create_projection
import vast_flatbuf.tabular.Column as tabular_projecion_column
import vast_flatbuf.tabular.ColumnType as tabular_proj_column_type

from vast_flatbuf.org.apache.arrow.computeir.flatbuf.Deref import Deref
from vast_flatbuf.org.apache.arrow.computeir.flatbuf.ExpressionImpl import ExpressionImpl
from vast_flatbuf.org.apache.arrow.computeir.flatbuf.LiteralImpl import LiteralImpl
from vast_flatbuf.org.apache.arrow.flatbuf.DateUnit import DateUnit
from vast_flatbuf.org.apache.arrow.flatbuf.TimeUnit import TimeUnit
from vast_flatbuf.org.apache.arrow.flatbuf.Type import Type
from vast_flatbuf.tabular.ListSchemasResponse import ListSchemasResponse as list_schemas
from vast_flatbuf.tabular.ListTablesResponse import ListTablesResponse as list_tables
from vast_flatbuf.tabular.GetTableStatsResponse import GetTableStatsResponse as get_table_stats
from vast_flatbuf.tabular.GetProjectionTableStatsResponse import GetProjectionTableStatsResponse as get_projection_table_stats
from vast_flatbuf.tabular.ListProjectionsResponse import ListProjectionsResponse as list_projections

UINT64_MAX = 18446744073709551615

TABULAR_KEEP_ALIVE_STREAM_ID = 0xFFFFFFFF
TABULAR_QUERY_DATA_COMPLETED_STREAM_ID = 0xFFFFFFFF - 1
TABULAR_QUERY_DATA_FAILED_STREAM_ID = 0xFFFFFFFF - 2
TABULAR_INVALID_ROW_ID = 0xFFFFFFFFFFFF # (1<<48)-1
ESTORE_INVALID_EHANDLE = UINT64_MAX
IMPORTED_OBJECTS_TABLE_NAME = "vastdb-imported-objects"

"""
S3 Tabular API
"""


_logger = logging.getLogger(__name__)


def _flatten_args(op, op_type):
    if isinstance(op, op_type):
        for arg in op.args:
            yield from _flatten_args(arg, op_type)
    else:
        yield op


class AuthType(Enum):
    SIGV4 = "s3v4"
    SIGV2 = "s3"
    BASIC = "basic"


def get_unit_to_flatbuff_time_unit(type):
    unit_to_flatbuff_time_unit = {
        'ns': TimeUnit.NANOSECOND,
        'us': TimeUnit.MICROSECOND,
        'ms': TimeUnit.MILLISECOND,
        's': TimeUnit.SECOND
    }
    return unit_to_flatbuff_time_unit[type]

class Predicate:
    unit_to_epoch = {
        'ns': 1_000_000,
        'us': 1_000,
        'ms': 1,
        's': 0.001
    }

    def __init__(self, schema: 'pa.Schema', expr: ibis.expr.types.BooleanColumn):
        self.schema = schema
        self.expr = expr
        self.builder = None

    def get_field_indexes(self, field: 'pa.Field', field_name_per_index: list) -> None:
        field_name_per_index.append(field.name)

        if isinstance(field.type, pa.StructType):
            flat_fields = field.flatten()
        elif isinstance(field.type, pa.MapType):
            flat_fields = [pa.field(f'{field.name}.entries', pa.struct([field.type.key_field, field.type.item_field]))]
        elif isinstance(field.type, pa.ListType):
            flat_fields = [pa.field(f'{field.name}.{field.type.value_field.name}', field.type.value_field.type)]
        else:
            return

        for flat_field in flat_fields:
            self.get_field_indexes(flat_field, field_name_per_index)

    @property
    def field_name_per_index(self):
        if self._field_name_per_index is None:
            _field_name_per_index = []
            for field in self.schema:
                self.get_field_indexes(field, _field_name_per_index)
            self._field_name_per_index = {field: index for index, field in enumerate(_field_name_per_index)}
        return self._field_name_per_index

    def get_projections(self, builder: 'flatbuffers.builder.Builder', field_names: list = None):
        if not field_names:
            field_names = self.field_name_per_index.keys()
        projection_fields = []
        for field_name in field_names:
            fb_field_index.Start(builder)
            fb_field_index.AddPosition(builder, self.field_name_per_index[field_name])
            offset = fb_field_index.End(builder)
            projection_fields.append(offset)
        fb_source.StartProjectionVector(builder, len(projection_fields))
        for offset in reversed(projection_fields):
            builder.PrependUOffsetTRelative(offset)
        return builder.EndVector()

    def serialize(self, builder: 'flatbuffers.builder.Builder'):
        from ibis.expr.operations.generic import TableColumn, Literal, IsNull
        from ibis.expr.operations.logical import Greater, GreaterEqual, Less, LessEqual, Equals, NotEquals, And, Or, Not
        from ibis.expr.operations.strings import StringContains

        builder_map = {
            Greater: self.build_greater,
            GreaterEqual: self.build_greater_equal,
            Less: self.build_less,
            LessEqual: self.build_less_equal,
            Equals: self.build_equal,
            NotEquals: self.build_not_equal,
            IsNull: self.build_is_null,
            Not: self.build_is_not_null,
            StringContains: self.build_match_substring,
        }

        positions_map = dict((f.name, index) for index, f in enumerate(self.schema)) # TODO: BFS

        self.builder = builder

        offsets = []

        if self.expr is not None:
            and_args = list(_flatten_args(self.expr.op(), And))
            _logger.debug('AND args: %s ops %s', and_args, self.expr.op())
            for op in and_args:
                or_args = list(_flatten_args(op, Or))
                _logger.debug('OR args: %s op %s', or_args, op)
                inner_offsets = []

                prev_field_name = None
                for inner_op in or_args:
                    _logger.debug('inner_op %s', inner_op)
                    builder_func = builder_map.get(type(inner_op))
                    if not builder_func:
                        raise NotImplementedError(inner_op.name)

                    if builder_func == self.build_is_null:
                        column, = inner_op.args
                        literal = None
                    elif builder_func == self.build_is_not_null:
                        not_arg, = inner_op.args
                        # currently we only support not is_null, checking we really got is_null under the not:
                        if not builder_map.get(type(not_arg)) == self.build_is_null:
                            raise NotImplementedError(not_arg.args[0].name)
                        column, = not_arg.args
                        literal = None
                    else:
                        column, literal = inner_op.args
                        if not isinstance(literal, Literal):
                            raise NotImplementedError(inner_op.name)

                    if not isinstance(column, TableColumn):
                        raise NotImplementedError(inner_op.name)

                    field_name = column.name
                    if prev_field_name is None:
                        prev_field_name = field_name
                    elif prev_field_name != field_name:
                        raise NotImplementedError(op.name)

                    args_offsets = [self.build_column(position=positions_map[field_name])]
                    if literal:
                        field = self.schema.field(field_name)
                        args_offsets.append(self.build_literal(field=field, value=literal.value))

                    inner_offsets.append(builder_func(*args_offsets))

                domain_offset = self.build_or(inner_offsets)
                offsets.append(domain_offset)

        return self.build_and(offsets)

    def build_column(self, position: int):
        fb_field_index.Start(self.builder)
        fb_field_index.AddPosition(self.builder, position)
        index = fb_field_index.End(self.builder)

        fb_field_ref.Start(self.builder)
        fb_field_ref.AddRefType(self.builder, Deref.FieldIndex)
        fb_field_ref.AddRef(self.builder, index)
        ref = fb_field_ref.End(self.builder)

        fb_expression.Start(self.builder)
        fb_expression.AddImplType(self.builder, ExpressionImpl.FieldRef)
        fb_expression.AddImpl(self.builder, ref)
        return fb_expression.End(self.builder)

    def build_domain(self, column: int, field_name: str):
        offsets = []
        filters = self.filters[field_name]
        if not filters:
            return self.build_or([self.build_is_not_null(column)])

        field_name, *field_attrs = field_name.split('.')
        field = self.schema.field(field_name)
        for attr in field_attrs:
            field = field.type[attr]
        for filter_by_name in filters:
            offsets.append(self.build_range(column=column, field=field, filter_by_name=filter_by_name))
        return self.build_or(offsets)

    def rule_to_operator(self, raw_rule: str):
        operator_matcher = {
            'eq': self.build_equal,
            'ge': self.build_greater_equal,
            'gt': self.build_greater,
            'lt': self.build_less,
            'le': self.build_less_equal,
            'is_not_null': self.build_is_not_null,
            'is_null': self.build_is_null
        }
        if raw_rule in ('is_not_null', 'is_null'):
            # handle is_null or is_valid rule
            op = raw_rule
            value = None
        else:
            op, value = raw_rule.split(maxsplit=1)
        return operator_matcher[op], value

    def build_range(self, column: int, field: pa.Field, filter_by_name: Union[tuple, str]):
        if isinstance(filter_by_name, str):
            filter_by_name = (filter_by_name,)
        if isinstance(filter_by_name, tuple) and len(filter_by_name) == 1:
            op, value = self.rule_to_operator(filter_by_name[0])
            if value:
                literal = self.build_literal(field=field, value=value)
                return op(column, literal)
            return op(column)  # is_null or is_not_null operation

        rules = []
        for rule in filter_by_name:
            op, value = self.rule_to_operator(rule)
            literal = self.build_literal(field=field, value=value)
            rules.append(op(column, literal))

        return self.build_and(rules)

    def build_function(self, name: str, *offsets):
        offset_name = self.builder.CreateString(name)
        fb_call.StartArgumentsVector(self.builder, len(offsets))
        for offset in reversed(offsets):
            self.builder.PrependUOffsetTRelative(offset)
        offset_arguments = self.builder.EndVector()

        fb_call.Start(self.builder)
        fb_call.AddName(self.builder, offset_name)
        fb_call.AddArguments(self.builder, offset_arguments)

        offset_call = fb_call.CallEnd(self.builder)

        fb_expression.Start(self.builder)
        fb_expression.AddImplType(self.builder, ExpressionImpl.Call)
        fb_expression.AddImpl(self.builder, offset_call)
        return fb_expression.End(self.builder)

    def build_literal(self, field: pa.Field, value):
        if field.type.equals(pa.int64()):
            literal_type = fb_int64_lit
            literal_impl = LiteralImpl.Int64Literal

            field_type_type = Type.Int
            fb_int.Start(self.builder)
            fb_int.AddBitWidth(self.builder, field.type.bit_width)
            fb_int.AddIsSigned(self.builder, True)
            field_type = fb_int.End(self.builder)

            value = int(value)
        elif field.type.equals(pa.int32()):
            literal_type = fb_int32_lit
            literal_impl = LiteralImpl.Int32Literal

            field_type_type = Type.Int
            fb_int.Start(self.builder)
            fb_int.AddBitWidth(self.builder, field.type.bit_width)
            fb_int.AddIsSigned(self.builder, True)
            field_type = fb_int.End(self.builder)

            value = int(value)
        elif field.type.equals(pa.int16()):
            literal_type = fb_int16_lit
            literal_impl = LiteralImpl.Int16Literal

            field_type_type = Type.Int
            fb_int.Start(self.builder)
            fb_int.AddBitWidth(self.builder, field.type.bit_width)
            fb_int.AddIsSigned(self.builder, True)
            field_type = fb_int.End(self.builder)

            value = int(value)
        elif field.type.equals(pa.int8()):
            literal_type = fb_int8_lit
            literal_impl = LiteralImpl.Int8Literal

            field_type_type = Type.Int
            fb_int.Start(self.builder)
            fb_int.AddBitWidth(self.builder, field.type.bit_width)
            fb_int.AddIsSigned(self.builder, True)
            field_type = fb_int.End(self.builder)

            value = int(value)
        elif field.type.equals(pa.float32()):
            literal_type = fb_float32_lit
            literal_impl = LiteralImpl.Float32Literal

            field_type_type = Type.FloatingPoint
            fb_floating_point.Start(self.builder)
            fb_floating_point.AddPrecision(self.builder, 1)  # single
            field_type = fb_floating_point.End(self.builder)

            value = float(value)
        elif field.type.equals(pa.float64()):
            literal_type = fb_float64_lit
            literal_impl = LiteralImpl.Float64Literal

            field_type_type = Type.FloatingPoint
            fb_floating_point.Start(self.builder)
            fb_floating_point.AddPrecision(self.builder, 2)  # double
            field_type = fb_floating_point.End(self.builder)

            value = float(value)
        elif field.type.equals(pa.string()):
            literal_type = fb_string_lit
            literal_impl = LiteralImpl.StringLiteral

            field_type_type = Type.Utf8
            fb_utf8.Start(self.builder)
            field_type = fb_utf8.End(self.builder)

            value = self.builder.CreateString(value)
        elif field.type.equals(pa.date32()):  # pa.date64()
            literal_type = fb_date32_lit
            literal_impl = LiteralImpl.DateLiteral

            field_type_type = Type.Date
            fb_date.Start(self.builder)
            fb_date.AddUnit(self.builder, DateUnit.DAY)
            field_type = fb_date.End(self.builder)

            start_date = datetime.fromtimestamp(0).date()
            date_delta = value - start_date
            value = date_delta.days
        elif isinstance(field.type, pa.TimestampType):
            literal_type = fb_timestamp_lit
            literal_impl = LiteralImpl.TimestampLiteral

            field_type_type = Type.Timestamp
            fb_timestamp.Start(self.builder)
            fb_timestamp.AddUnit(self.builder, get_unit_to_flatbuff_time_unit(field.type.unit))
            field_type = fb_timestamp.End(self.builder)

            value = int(int(value) * self.unit_to_epoch[field.type.unit])
        elif field.type.equals(pa.time32('s')) or field.type.equals(pa.time32('ms')) or field.type.equals(pa.time64('us')) or field.type.equals(pa.time64('ns')):

            literal_type = fb_time_lit
            literal_impl = LiteralImpl.TimeLiteral

            field_type_str = str(field.type)
            start = field_type_str.index('[')
            end = field_type_str.index(']')
            unit = field_type_str[start + 1:end]

            field_type_type = Type.Time
            fb_time.Start(self.builder)
            fb_time.AddBitWidth(self.builder, field.type.bit_width)
            fb_time.AddUnit(self.builder, get_unit_to_flatbuff_time_unit(unit))
            field_type = fb_time.End(self.builder)

            value = int(value) * self.unit_to_epoch[unit]
        elif field.type.equals(pa.bool_()):
            literal_type = fb_bool_lit
            literal_impl = LiteralImpl.BooleanLiteral

            field_type_type = Type.Bool
            fb_bool.Start(self.builder)
            field_type = fb_bool.End(self.builder)

            value = True if value == 'true' else False  # not cover all cases
        elif isinstance(field.type, pa.Decimal128Type):
            literal_type = fb_decimal_lit
            literal_impl = LiteralImpl.DecimalLiteral

            field_type_type = Type.Decimal
            fb_decimal.Start(self.builder)
            fb_decimal.AddPrecision(self.builder, field.type.precision)
            fb_decimal.AddScale(self.builder, field.type.scale)
            field_type = fb_decimal.End(self.builder)
            int_value = int(float(value) * 10 ** field.type.scale)
            binary_value = int_value.to_bytes(16, 'little')

            value = self.builder.CreateByteVector(binary_value)
        elif field.type.equals(pa.binary()):
            literal_type = fb_binary_lit
            literal_impl = LiteralImpl.BinaryLiteral

            field_type_type = Type.Binary
            fb_binary.Start(self.builder)
            field_type = fb_binary.End(self.builder)

            value = self.builder.CreateByteVector(value)
        else:
            raise ValueError(f'unsupported predicate for type={field.type}, value={value}')

        literal_type.Start(self.builder)
        literal_type.AddValue(self.builder, value)
        buffer_value = literal_type.End(self.builder)

        fb_field.Start(self.builder)
        fb_field.AddTypeType(self.builder, field_type_type)
        fb_field.AddType(self.builder, field_type)
        buffer_field = fb_field.End(self.builder)

        fb_literal.Start(self.builder)
        fb_literal.AddImplType(self.builder, literal_impl)
        fb_literal.AddImpl(self.builder, buffer_value)
        fb_literal.AddType(self.builder, buffer_field)
        buffer_literal = fb_literal.End(self.builder)

        fb_expression.Start(self.builder)
        fb_expression.AddImplType(self.builder, ExpressionImpl.Literal)
        fb_expression.AddImpl(self.builder, buffer_literal)
        return fb_expression.End(self.builder)

    def build_or(self, offsets: list):
        return self.build_function('or', *offsets)

    def build_and(self, offsets: list):
        return self.build_function('and', *offsets)

    def build_equal(self, column: int, literal: int):
        return self.build_function('equal', column, literal)

    def build_not_equal(self, column: int, literal: int):
        return self.build_function('not_equal', column, literal)

    def build_greater(self, column: int, literal: int):
        return self.build_function('greater', column, literal)

    def build_greater_equal(self, column: int, literal: int):
        return self.build_function('greater_equal', column, literal)

    def build_less(self, column: int, literal: int):
        return self.build_function('less', column, literal)

    def build_less_equal(self, column: int, literal: int):
        return self.build_function('less_equal', column, literal)

    def build_is_null(self, column: int):
        return self.build_function('is_null', column)

    def build_is_not_null(self, column: int):
        return self.build_function('is_valid', column)

    def build_match_substring(self, column: int, literal: int):
        return self.build_function('match_substring', column, literal)


class FieldNode:
    """Helper class for representing nested Arrow fields and handling QueryData requests"""
    def __init__(self, field: pa.Field, index_iter, parent: Optional['FieldNode'] = None, debug: bool = False):
        self.index = next(index_iter) # we use DFS-first enumeration for communicating the column positions to VAST
        self.field = field
        self.type = field.type
        self.parent = parent # will be None if this is the top-level field
        self.debug = debug
        if isinstance(self.type, pa.StructType):
            self.children = [FieldNode(field, index_iter, parent=self) for field in self.type]
        elif isinstance(self.type, pa.ListType):
            self.children = [FieldNode(self.type.value_field, index_iter, parent=self)]
        elif isinstance(self.type, pa.MapType):
            # Map is represented as List<Struct<K, V>> in Arrow
            # TODO: this is a workaround for PyArrow 6 (which has no way to access the internal "entries" Map's field)
            # when we upgrade to latest PyArrow, all nested types can be enumerated using:
            #
            #   if isinstance(self.type, (pa.ListType,  pa.StructType, pa.MapType)):
            #       self.children = [FieldNode(self.type.field(i), index_iter, parent=self) for i in range(self.type.num_fields)]
            #
            field = pa.field('entries', pa.struct([self.type.key_field, self.type.item_field]))
            self.children = [FieldNode(field, index_iter, parent=self)]
        else:
            self.children = [] # for non-nested types

        # will be set during by the parser (see below)
        self.buffers = None # a list of Arrow buffers (https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout)
        self.length = None # each array must have it's length specified (https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array.from_buffers)
        self.is_projected = False
        self.projected_field = self.field

    def _iter_to_root(self) -> Iterator['FieldNode']:
        yield self
        if self.parent is not None:
            yield from self.parent._iter_to_root()

    def _iter_nodes(self) -> Iterator['FieldNode']:
        """Generate all nodes."""
        yield self
        for child in self.children:
            yield from child._iter_nodes()

    def _iter_leaves(self) -> Iterator['FieldNode']:
        """Generate only leaf nodes (i.e. columns having scalar types)."""
        if not self.children:
            yield self
        else:
            for child in self.children:
                yield from child._iter_leaves()

    def _iter_projected_leaves(self) -> Iterator['FieldNode']:
        """Generate only leaf nodes (i.e. columns having scalar types)."""
        if not self.children:
            if self.is_projected:
                yield self
        else:
            for child in self.children:
                if child.is_projected:
                    yield from child._iter_projected_leaves()

    def debug_log(self, level=0):
        """Recursively dump this node state to log."""
        bufs = self.buffers and [b and b.hex() for b in self.buffers]
        _logger.debug('%s%d: %s, bufs=%s, len=%s', '    '*level, self.index, self.field, bufs, self.length)
        for child in self.children:
            child.debug_log(level=level+1)

    def set(self, arr: pa.Array):
        """
        Assign the relevant Arrow buffers from the received array into this node.

        VAST can send only a single column at a time - together with its nesting levels.
        For example, `List<List<Int32>>` can be sent in a single go.

        Sending `Struct<a: Int32, b: Float64>` is not supported today, so each column is sent separately
        (possibly duplicating the nesting levels above it).
        For example, `Struct<A, B>` is sent as two separate columns: `Struct<A>` and `Struct<B>`.
        Also, `Map<K, V>` is sent (as its underlying representation): `List<Struct<K>>` and `List<Struct<V>>`
        """
        buffers = arr.buffers()[:arr.type.num_buffers] # slicing is needed because Array.buffers() returns also nested array buffers
        if self.debug:
            _logger.debug("set: index=%d %s %s", self.index, self.field, [b and b.hex() for b in buffers])
        if self.buffers is None:
            self.buffers = buffers
            self.length = len(arr)
        else:
            # Make sure subsequent assignments are consistent with each other
            if self.debug:
                if not self.buffers == buffers:
                    raise ValueError(f'self.buffers: {self.buffers} are not equal with buffers: {buffers}')
            if not self.length == len(arr):
                raise ValueError(f'self.length: {self.length} are not equal with len(arr): {len(arr)}')

    def build(self) -> pa.Array:
        """Construct an Arrow array from the collected buffers (recursively)."""
        children = self.children and [node.build() for node in self.children if node.is_projected]
        _logger.debug('build: self.field.name=%s, self.projected_field.type=%s, self.length=%s, self.buffers=%s children=%s',
                      self.field.name, self.projected_field.type, self.length, self.buffers, children)
        result = pa.Array.from_buffers(self.projected_field.type, self.length, buffers=self.buffers, children=children)
        if self.debug:
            _logger.debug('%s result=%s', self.field, result)
        return result

    def build_projected_field(self):
        if isinstance(self.type, pa.StructType):
            [child.build_projected_field() for child in self.children if child.is_projected]
            self.projected_field = pa.field(self.field.name,
                                            pa.struct([child.projected_field for child in self.children if child.is_projected]),
                                            self.field.nullable,
                                            self.field.metadata)

class QueryDataParser:
    """Used to parse VAST QueryData RPC response."""
    def __init__(self, arrow_schema: pa.Schema, *, debug=False, projection_positions=None):
        self.arrow_schema = arrow_schema
        self.projection_positions = projection_positions
        index = itertools.count() # used to generate leaf column positions for VAST QueryData RPC
        self.nodes = [FieldNode(field, index, debug=debug) for field in arrow_schema]
        self.debug = debug
        if self.debug:
            for node in self.nodes:
                node.debug_log()
        self.leaves = [leaf for node in self.nodes for leaf in node._iter_leaves()]
        self.mark_projected_nodes()
        [node.build_projected_field() for node in self.nodes]
        self.projected_leaves = [leaf for node in self.nodes for leaf in node._iter_projected_leaves()]

        self.leaf_offset = 0

    def mark_projected_nodes(self):
        for leaf in self.leaves:
            if self.projection_positions is None or leaf.index in self.projection_positions:
                for node in leaf._iter_to_root():
                    node.is_projected = True

    def parse(self, column: pa.Array):
        """Parse a single column response from VAST (see FieldNode.set for details)"""
        if not self.leaf_offset < len(self.projected_leaves):
            raise ValueError(f'self.leaf_offset: {self.leaf_offset} are not < '
                             f'than len(self.leaves): {len(self.leaves)}')
        leaf = self.projected_leaves[self.leaf_offset]

        # A column response may be sent in multiple chunks, therefore we need to combine
        # it into a single chunk to allow reconstruction using `Array.from_buffers()`.
        column = column.combine_chunks()

        if self.debug:
            _logger.debug("parse: index=%d buffers=%s", leaf.index, [b and b.hex() for b in column.buffers()])
        node_list = list(leaf._iter_to_root())  # a FieldNode list (from leaf to root)
        node_list.reverse()  # (from root to leaf)
        # Collect all nesting levels, e.g. `List<Struct<A>` will be split into [`List`, `Struct`, `A`].
        # This way we can extract the buffers from each level and assign them to the appropriate node in loop below.
        array_list = list(_iter_nested_arrays(column))
        if len(array_list) != len(node_list):
            raise ValueError(f'len(array_list): {len(array_list)} are not eq '
                             f'with len(node_list): {len(node_list)}')
        for node, arr in zip(node_list, array_list):
            node.set(arr)

        self.leaf_offset += 1

    def build(self, output_field_names=None) -> Optional[pa.Table]:
        """Try to build the resulting Table object (if all columns were parsed)"""
        if self.projection_positions is not None:
            if self.leaf_offset < len(self.projection_positions):
                return None
        else:
            if self.leaf_offset < len(self.leaves):
                return None

        if self.debug:
            for node in self.nodes:
                node.debug_log()

        # sort resulting table according to the output field names
        projected_nodes = [node for node in self.nodes if node.is_projected]
        if output_field_names is not None:
            def key_func(projected_node):
                return output_field_names.index(projected_node.field.name)
            sorted_projected_nodes = sorted(projected_nodes, key=key_func)
        else:
            sorted_projected_nodes = projected_nodes

        result = pa.Table.from_arrays(
            arrays=[node.build() for node in sorted_projected_nodes],
            schema = pa.schema([node.projected_field for node in sorted_projected_nodes]))
        result.validate(full=True) # does expensive validation checks only if debug is enabled
        return result

def _iter_nested_arrays(column: pa.Array) -> Iterator[pa.Array]:
    """Iterate over a single column response, and recursively generate all of its children."""
    yield column
    if isinstance(column.type, pa.StructType):
        if not column.type.num_fields == 1:  # Note: VAST serializes only a single struct field at a time
            raise ValueError(f'column.type.num_fields: {column.type.num_fields} not eq to 1')
        yield from _iter_nested_arrays(column.field(0))
    elif isinstance(column.type, pa.ListType):
        yield from _iter_nested_arrays(column.values)  # Note: Map is serialized in VAST as a List<Struct<K, V>>


TableInfo = namedtuple('table_info', 'name properties handle num_rows size_in_bytes')
def _parse_table_info(obj):

    name = obj.Name().decode()
    properties = obj.Properties().decode()
    handle = obj.Handle().decode()
    num_rows = obj.NumRows()
    used_bytes = obj.SizeInBytes()
    return TableInfo(name, properties, handle, num_rows, used_bytes)

def build_record_batch(column_info, column_values):
    fields = [pa.field(column_name, column_type) for column_type, column_name in column_info]
    schema = pa.schema(fields)
    arrays = [pa.array(column_values[column_type], type=column_type) for column_type, _ in column_info]
    batch = pa.record_batch(arrays, schema)
    return serialize_record_batch(batch)

def serialize_record_batch(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write(batch)
    return sink.getvalue()

# Results that returns from tablestats
TableStatsResult = namedtuple("TableStatsResult",["num_rows", "size_in_bytes", "is_external_rowid_alloc", "endpoints"])

class VastdbApi:
    # we expect the vast version to be <major>.<minor>.<patch>.<protocol>
    VAST_VERSION_REGEX = re.compile(r'^vast (\d+\.\d+\.\d+\.\d+)$')

    def __init__(self, endpoint, access_key, secret_key, username=None, password=None,
                 secure=False, auth_type=AuthType.SIGV4):
        url_dict = urllib3.util.parse_url(endpoint)._asdict()
        self.access_key = access_key
        self.secret_key = secret_key
        self.username = username
        self.password = password
        self.secure = secure
        self.auth_type = auth_type
        self.executor_hosts = [endpoint]  # TODO: remove

        username = username or ''
        password = password or ''
        if not url_dict['port']:
            url_dict['port'] = 443 if secure else 80

        self.port = url_dict['port']

        self.default_max_list_columns_page_size = 1000
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers['user-agent'] = "VastData Tabular API 1.0 - 2022 (c)"
        if auth_type == AuthType.BASIC:
            self.session.auth = requests.auth.HTTPBasicAuth(username, password)
        else:
            if url_dict['port'] != 80 and url_dict['port'] != 443:
                self.aws_host = '{host}:{port}'.format(**url_dict)
            else:
                self.aws_host = '{host}'.format(**url_dict)

            self.session.auth = AWSRequestsAuth(aws_access_key=access_key,
                                                aws_secret_access_key=secret_key,
                                                aws_host=self.aws_host,
                                                aws_region='us-east-1',
                                                aws_service='s3')

        if not url_dict['scheme']:
            url_dict['scheme'] = "https" if secure else "http"

        url = urllib3.util.Url(**url_dict)
        self.url = str(url)
        _logger.debug('url=%s aws_host=%s', self.url, self.aws_host)

        # probe the cluster for its version
        self.vast_version = None
        res = self.session.options(self.url)
        server_header = res.headers.get("Server")
        if server_header is None:
            _logger.error("OPTIONS response doesn't contain 'Server' header")
        else:
            _logger.debug("Server header is '%s'", server_header)
            if m := self.VAST_VERSION_REGEX.match(server_header):
                self.vast_version, = m.groups()
                return
            else:
                _logger.error("'Server' header '%s' doesn't match the expected pattern", server_header)

        msg = (
            f'Please use `vastdb` <= 0.0.5.x with current VAST cluster version ("{server_header or "N/A"}"). '
            'To use the latest SDK, please upgrade your cluster to the latest service pack. '
            'Please contact customer.support@vastdata.com for more details.'
        )
        _logger.critical(msg)
        raise NotImplementedError(msg)

    def update_mgmt_session(self, access_key: str, secret_key: str, auth_type=AuthType.SIGV4):
        if auth_type != AuthType.BASIC:
            self.session.auth = AWSRequestsAuth(aws_access_key=access_key,
                                                aws_secret_access_key=secret_key,
                                                aws_host=self.aws_host,
                                                aws_region='us-east-1',
                                                aws_service='s3')

    def _api_prefix(self, bucket="", schema="", table="", command="", url_params={}):
        prefix_list = [self.url]
        if len(bucket):
            prefix_list.append(bucket)

        if len(schema):
            prefix_list.append(schema)

        if len(table):
            prefix_list.append(table)

        prefix = '/'.join(prefix_list)

        params_list = []
        if command:
            prefix += '?'
            if command != 'list':  # S3 listing request has no command. It looks like 'GET /bucket_name?list-type=2&delimite=Delimiter...'
                params_list.append(command)

        # Query string values must be URL-encoded
        for k, v in sorted(url_params.items()):
            params_list.append(f"{k}={urllib.parse.quote_plus(v)}")

        prefix += '&'.join(params_list)
        return prefix

    def _fill_common_headers(self, txid=0, client_tags=[], version_id=1):
        common_headers = {'tabular-txid': str(txid), 'tabular-api-version-id': str(version_id),
                          'tabular-client-name': 'tabular-api'}
        for tag in client_tags:
            common_headers['tabular-client-tags-%d' % client_tags.index(tag)] = tag

        return common_headers

    def _check_res(self, res, cmd="", expected_retvals=[]):
        if exc := errors.from_response(res):
            raise exc
        return res

    def create_schema(self, bucket, name, txid=0, client_tags=[], schema_properties="", expected_retvals=[]):
        """
        Create a collection of tables, use the following request
        POST /bucket/schema?schema HTTP/1.1

        The body of the POST request contains schema properties as flatbuffer
        Request Headers
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        builder = flatbuffers.Builder(1024)

        properties_offset = builder.CreateString(schema_properties)
        tabular_create_schema.Start(builder)
        tabular_create_schema.AddProperties(builder, properties_offset)
        params = tabular_create_schema.End(builder)
        builder.Finish(params)
        create_schema_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(create_schema_req))
        res = self.session.post(self._api_prefix(bucket=bucket, schema=name, command="schema"),
                                data=create_schema_req, headers=headers, stream=True)

        return self._check_res(res, "create_schema", expected_retvals)

    def alter_schema(self, bucket, name, txid=0, client_tags=[], schema_properties="", new_name="", expected_retvals=[]):
        """
        ALTER an existing schema name and/or properties
        PUT /bucket/schema?schema&tabular-new-schema-name=NewSchemaName HTTP/1.1

        Request Headers
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        builder = flatbuffers.Builder(1024)

        properties = builder.CreateString(schema_properties)
        tabular_alter_schema.Start(builder)
        if len(schema_properties):
            tabular_alter_schema.AddProperties(builder, properties)

        params = tabular_alter_schema.End(builder)
        builder.Finish(params)
        alter_schema_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(alter_schema_req))
        url_params = {'tabular-new-schema-name': new_name} if len(new_name) else {}

        res = self.session.put(self._api_prefix(bucket=bucket, schema=name, command="schema", url_params=url_params),
                               data=alter_schema_req, headers=headers)

        return self._check_res(res, "alter_schema", expected_retvals)

    def drop_schema(self, bucket, name, txid=0, client_tags=[], expected_retvals=[]):
        """
        Drop (delete) a schema
        DELETE /bucket/schema?schema HTTP/1.1

        Request Headers
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)

        res = self.session.delete(self._api_prefix(bucket=bucket, schema=name, command="schema"), headers=headers)

        return self._check_res(res, "drop_schema", expected_retvals)

    def list_schemas(self, bucket, schema="", txid=0, client_tags=[], max_keys=1000, next_key=0, name_prefix="",
                     exact_match=False, expected_retvals=[], count_only=False):
        """
        List all schemas
        GET /bucket/schema_path?schema HTTP/1.1

        Request Headers
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-name-prefix: SchemaNamePrefix
        tabular-max-keys: 1000
        tabular-next-key: NextKey (Schema Name for continuation request)

        The List will return the list in flatbuf format
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-max-keys'] = str(max_keys)
        headers['tabular-next-key'] = str(next_key)
        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        headers['tabular-list-count-only'] = str(count_only)

        schemas = []
        schema = schema or ""
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, command="schema"), headers=headers, stream=True)
        self._check_res(res, "list_schemas", expected_retvals)
        if res.status_code == 200:
            res_headers = res.headers
            next_key = int(res_headers['tabular-next-key'])
            is_truncated = res_headers['tabular-is-truncated'] == 'true'
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            lists = list_schemas.GetRootAs(flatbuf)
            bucket_name = lists.BucketName().decode()
            if not bucket.startswith(bucket_name):
                raise ValueError(f'bucket: {bucket} did not start from {bucket_name}')
            schemas_length = lists.SchemasLength()
            count = int(res_headers['tabular-list-count']) if 'tabular-list-count' in res_headers else schemas_length
            for i in range(schemas_length):
                schema_obj = lists.Schemas(i)
                name = schema_obj.Name().decode()
                properties = schema_obj.Properties().decode()
                schemas.append([name, properties])

            return bucket_name, schemas, next_key, is_truncated, count

    def list_snapshots(self, bucket, max_keys=1000, next_token=None, expected_retvals=None):
        next_token = next_token or ''
        expected_retvals = expected_retvals or []
        url_params = {'list_type': '2', 'prefix': '.snapshot/', 'delimiter': '/', 'max_keys': str(max_keys)}
        if next_token:
            url_params['continuation-token'] = next_token

        res = self.session.get(self._api_prefix(bucket=bucket, command="list", url_params=url_params), headers={}, stream=True)
        self._check_res(res, "list_snapshots", expected_retvals)
        if res.status_code == 200:
            out = b''.join(res.iter_content(chunk_size=128))
            xml_str = out.decode()
            xml_dict = xmltodict.parse(xml_str)
            list_res = xml_dict['ListBucketResult']
            is_truncated = list_res['IsTruncated'] == 'true'
            marker = list_res['Marker']
            common_prefixes = list_res['CommonPrefixes'] if 'CommonPrefixes' in list_res else []
            snapshots = [v['Prefix'] for v in common_prefixes]

            return snapshots, is_truncated, marker


    def create_table(self, bucket, schema, name, arrow_schema, txid=0, client_tags=[], expected_retvals=[],
                     topic_partitions=0, create_imports_table=False):
        """
        Create a table, use the following request
        POST /bucket/schema/table?table HTTP/1.1

        Request Headers
        tabular-txid: <integer> TransactionId
        tabular-client-tag: <string> ClientTag

        The body of the POST request contains table column properties as arrow schema
        which include field_name, field_type and properties

        In order to create vastdb-imported-objects table that tracks all imported files and avoid duplicate imports,
        just set create_imports_table=True
        The request will look like:
        POST /bucket/schema/table?table&sub-table=vastdb-imported-objects HTTP/1.1
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)

        serialized_schema = arrow_schema.serialize()
        headers['Content-Length'] = str(len(serialized_schema))
        url_params = {'topic_partitions': str(topic_partitions)} if topic_partitions else {}
        if create_imports_table:
            url_params['sub-table'] = IMPORTED_OBJECTS_TABLE_NAME

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=name, command="table", url_params=url_params),
                                data=serialized_schema, headers=headers)
        return self._check_res(res, "create_table", expected_retvals)

    def create_table_from_parquet_schema(self, bucket, schema, name, parquet_path=None,
                                         parquet_bucket_name=None, parquet_object_name=None,
                                         txid=0, client_tags=[], expected_retvals=[]):

        # Use pyarrow.parquet.ParquetDataset to open the Parquet file
        if parquet_path:
            parquet_ds = pq.ParquetDataset(parquet_path)
        elif parquet_bucket_name and parquet_object_name:
            s3fs  = pa.fs.S3FileSystem(access_key=self.access_key, secret_key=self.secret_key, endpoint_override=self.url)
            parquet_ds = pq.ParquetDataset('/'.join([parquet_bucket_name,parquet_object_name]), filesystem=s3fs)
        else:
            raise RuntimeError(f'invalid params parquet_path={parquet_path} parquet_bucket_name={parquet_bucket_name} parquet_object_name={parquet_object_name}')

        # Get the schema of the Parquet file
        if isinstance(parquet_ds.schema, pq.ParquetSchema):
            arrow_schema = parquet_ds.schema.to_arrow_schema()
        elif isinstance(parquet_ds.schema, pa.Schema):
            arrow_schema = parquet_ds.schema
        else:
            raise RuntimeError(f'invalid type(parquet_ds.schema) = {type(parquet_ds.schema)}')

        # create the table
        return self.create_table(bucket, schema, name, arrow_schema, txid, client_tags, expected_retvals)


    def get_table_stats(self, bucket, schema, name, txid=0, client_tags=[], expected_retvals=[]):
        """
        GET /mybucket/myschema/mytable?stats HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        The Command will return the statistics in flatbuf format
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=name, command="stats"), headers=headers)
        if res.status_code == 200:
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            stats = get_table_stats.GetRootAs(flatbuf)
            num_rows = stats.NumRows()
            size_in_bytes = stats.SizeInBytes()
            is_external_rowid_alloc = stats.IsExternalRowidAlloc()
            endpoints = []
            if stats.VipsLength() == 0:
                endpoints.append(self.url)
            else:
                ip_cls = IPv6Address if (stats.AddressType() == "ipv6") else IPv4Address
                vips = [stats.Vips(i) for i in range(stats.VipsLength())]
                ips = []
                # extract the vips into list of IPs
                for vip in vips:
                    start_ip = int(ip_cls(vip.StartAddress().decode()))
                    ips.extend(ip_cls(start_ip + i) for i  in range(vip.AddressCount()))
                for ip in ips:
                    prefix = "http" if not self.secure else "https"
                    endpoints.append(f"{prefix}://{str(ip)}:{self.port}")
            return TableStatsResult(num_rows, size_in_bytes, is_external_rowid_alloc, endpoints)

        return self._check_res(res, "get_table_stats", expected_retvals)

    def alter_table(self, bucket, schema, name, txid=0, client_tags=[], table_properties="",
                    new_name="", expected_retvals=[]):
        """
        PUT /mybucket/myschema/mytable?table HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body Flatbuffer
        Table properties
        """
        builder = flatbuffers.Builder(1024)

        properties = builder.CreateString(table_properties)
        tabular_alter_table.Start(builder)
        if len(table_properties):
            tabular_alter_table.AddProperties(builder, properties)

        params = tabular_alter_table.End(builder)
        builder.Finish(params)
        alter_table_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(alter_table_req))
        url_params = {'tabular-new-table-name': schema + "/" + new_name} if len(new_name) else {}

        res = self.session.put(self._api_prefix(bucket=bucket, schema=schema, table=name, command="table", url_params=url_params),
                               data=alter_table_req, headers=headers)

        return self._check_res(res, "alter_table", expected_retvals)

    def drop_table(self, bucket, schema, name, txid=0, client_tags=[], expected_retvals=[], remove_imports_table=False):
        """
        DELETE /mybucket/schema_path/mytable?table HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        To remove the internal vastdb-imported-objects table just set remove_imports_table=True
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        url_params = {'sub-table': IMPORTED_OBJECTS_TABLE_NAME} if remove_imports_table else {}

        res = self.session.delete(self._api_prefix(bucket=bucket, schema=schema, table=name, command="table", url_params=url_params),
                                  headers=headers)
        return self._check_res(res, "drop_table", expected_retvals)

    def list_tables(self, bucket, schema, txid=0, client_tags=[], max_keys=1000, next_key=0, name_prefix="",
                    exact_match=False, expected_retvals=[], include_list_stats=False, count_only=False):
        """
        GET /mybucket/schema_path?table HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        x-tabluar-name-prefix: TableNamePrefix
        tabular-max-keys: 1000
        tabular-next-key: NextKey (Name)
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-max-keys'] = str(max_keys)
        headers['tabular-next-key'] = str(next_key)
        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        headers['tabular-list-count-only'] = str(count_only)
        headers['tabular-include-list-stats'] = str(include_list_stats)

        tables = []
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, command="table"), headers=headers)
        self._check_res(res, "list_table", expected_retvals)
        if res.status_code == 200:
            res_headers = res.headers
            next_key = int(res_headers['tabular-next-key'])
            is_truncated = res_headers['tabular-is-truncated'] == 'true'
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            lists = list_tables.GetRootAs(flatbuf)
            bucket_name = lists.BucketName().decode()
            schema_name = lists.SchemaName().decode()
            if not bucket.startswith(bucket_name):  # ignore snapshot name
                raise ValueError(f'bucket: {bucket} did not start from {bucket_name}')
            tables_length = lists.TablesLength()
            count = int(res_headers['tabular-list-count']) if 'tabular-list-count' in res_headers else tables_length
            for i in range(tables_length):
                tables.append(_parse_table_info(lists.Tables(i)))

            return bucket_name, schema_name, tables, next_key, is_truncated, count


    def add_columns(self, bucket, schema, name, arrow_schema, txid=0, client_tags=[], expected_retvals=[]):
        """
        Add a column to table, use the following request
        POST /bucket/schema/table?column HTTP/1.1

        Request Headers
        tabular-txid: <integer> TransactionId
        tabular-client-tag: <string> ClientTag

        The body of the POST request contains table column properties as json
        {
            "format": "string",
            "column_names": {"name1":"type1", "name2":"type2", ...},
            "table_properties": {"key1":"val1", "key2":"val2", ...}
        }
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        serialized_schema = arrow_schema.serialize()
        headers['Content-Length'] = str(len(serialized_schema))

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=name, command="column"),
                                data=serialized_schema, headers=headers)
        return self._check_res(res, "add_columns", expected_retvals)

    def alter_column(self, bucket, schema, table, name, txid=0, client_tags=[], column_properties="",
                     new_name="", column_sep = ".", column_stats="", expected_retvals=[]):
        """
        PUT /bucket/schema/table?column&tabular-column-name=ColumnName&tabular-new-column-name=NewColumnName HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-column-sep: ColumnSep

        Request Body Flatbuffer
        Table properties
        """
        builder = flatbuffers.Builder(1024)

        properties = builder.CreateString(column_properties)
        stats = builder.CreateString(column_stats)
        tabular_alter_column.Start(builder)
        if len(column_properties):
            tabular_alter_column.AddProperties(builder, properties)
        if len(column_stats):
            tabular_alter_column.AddStats(builder, stats)

        params = tabular_alter_column.End(builder)
        builder.Finish(params)
        alter_column_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-column-sep'] = column_sep
        headers['Content-Length'] = str(len(alter_column_req))

        url_params = {'tabular-column-name': name }
        if len(new_name):
            url_params['tabular-new-column-name'] = new_name

        res = self.session.put(self._api_prefix(bucket=bucket, schema=schema, table=table, command="column", url_params=url_params),
                               data=alter_column_req, headers=headers)
        return self._check_res(res, "alter_column", expected_retvals)

    def drop_columns(self, bucket, schema, table, arrow_schema, txid=0, client_tags=[], expected_retvals=[]):
        """
        DELETE /mybucket/myschema/mytable?column HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-column-name: OldColumnName
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        serialized_schema = arrow_schema.serialize()
        headers['Content-Length'] = str(len(serialized_schema))

        res = self.session.delete(self._api_prefix(bucket=bucket, schema=schema, table=table, command="column"),
                                data=serialized_schema, headers=headers)
        return self._check_res(res, "drop_columns", expected_retvals)

    def list_columns(self, bucket, schema, table, *, txid=0, client_tags=None, max_keys=None, next_key=0,
                     count_only=False, name_prefix="", exact_match=False,
                     expected_retvals=None, bc_list_internals=False, list_imports_table=False):
        """
        GET /mybucket/myschema/mytable?columns HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        x-tabluar-name-prefix: TableNamePrefix
        tabular-max-keys: 1000
        tabular-next-key: NextColumnId

        To list the columns of the internal vastdb-imported-objects table, set list_import_table=True
        """
        max_keys = max_keys or self.default_max_list_columns_page_size
        client_tags = client_tags or []
        expected_retvals = expected_retvals or []

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-max-keys'] = str(max_keys)
        headers['tabular-next-key'] = str(next_key)
        headers['tabular-list-count-only'] = str(count_only)
        if bc_list_internals:
            headers['tabular-bc-list-internal-col'] = "true"

        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        url_params = {'sub-table': IMPORTED_OBJECTS_TABLE_NAME} if list_imports_table else {}
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="column",
                                                url_params=url_params),
                               headers=headers, stream=True)
        self._check_res(res, "list_columns", expected_retvals)
        if res.status_code == 200:
            res_headers = res.headers
            next_key = int(res_headers['tabular-next-key'])
            is_truncated = res_headers['tabular-is-truncated'] == 'true'
            count = int(res_headers['tabular-list-count'])
            columns = []
            if not count_only:
                schema_buf = b''.join(res.iter_content(chunk_size=128))
                schema_out = pa.ipc.open_stream(schema_buf).schema
                columns = schema_out

            return columns, next_key, is_truncated, count

    def begin_transaction(self, client_tags=[], expected_retvals=[]):
        """
        POST /?transaction HTTP/1.1
        tabular-client-tag: ClientTag

        Response
        tabular-txid: TransactionId
        """
        headers = self._fill_common_headers(client_tags=client_tags)
        res = self.session.post(self._api_prefix(command="transaction"), headers=headers)
        return self._check_res(res, "begin_transaction", expected_retvals)

    def commit_transaction(self, txid, client_tags=[], expected_retvals=[]):
        """
        PUT /?transaction HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        res = self.session.put(self._api_prefix(command="transaction"), headers=headers)
        return self._check_res(res, "commit_transaction", expected_retvals)

    def rollback_transaction(self, txid, client_tags=[], expected_retvals=[]):
        """
        DELETE /?transaction HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        res = self.session.delete(self._api_prefix(command="transaction"), headers=headers)
        return self._check_res(res, "rollback_transaction", expected_retvals)

    def get_transaction(self, txid, client_tags=[], expected_retvals=[]):
        """
        GET /?transaction HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        res = self.session.get(self._api_prefix(command="transaction"), headers=headers)
        return self._check_res(res, "get_transaction", expected_retvals)

    def select_row_ids(self, bucket, schema, table, params, txid=0, client_tags=[], expected_retvals=[],
                       retry_count=0, enable_sorted_projections=True):
        """
        POST /mybucket/myschema/mytable?query-data=SelectRowIds HTTP/1.1
        """

        # add query option select-only and read-only
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(params))
        headers['tabular-enable-sorted-projections'] = str(enable_sorted_projections)
        if retry_count > 0:
            headers['tabular-retry-count'] = str(retry_count)

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="query-data=SelectRowIds",),
                                data=params, headers=headers, stream=True)
        return self._check_res(res, "query_data", expected_retvals)

    def read_columns_data(self, bucket, schema, table, params, txid=0, client_tags=[], expected_retvals=[], tenant_guid=None,
                          retry_count=0, enable_sorted_projections=True):
        """
        POST /mybucket/myschema/mytable?query-data=ReadColumns HTTP/1.1
        """

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(params))
        headers['tabular-enable-sorted-projections'] = str(enable_sorted_projections)
        if retry_count > 0:
            headers['tabular-retry-count'] = str(retry_count)

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="query-data=ReadColumns",),
                               data=params, headers=headers, stream=True)
        return self._check_res(res, "query_data", expected_retvals)

    def count_rows(self, bucket, schema, table, params, txid=0, client_tags=[], expected_retvals=[], tenant_guid=None,
                   retry_count=0, enable_sorted_projections=True):
        """
        POST /mybucket/myschema/mytable?query-data=CountRows HTTP/1.1
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(params))
        headers['tabular-enable-sorted-projections'] = str(enable_sorted_projections)
        if retry_count > 0:
            headers['tabular-retry-count'] = str(retry_count)

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="query-data=CountRows",),
                               data=params, headers=headers, stream=True)
        return self._check_res(res, "query_data", expected_retvals)

    def _build_query_data_headers(self, txid, client_tags, params, split, num_sub_splits, request_format, response_format,
                                  enable_sorted_projections, limit_rows, schedule_id, retry_count, search_path, tenant_guid,
                                  sub_split_start_row_ids):
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(params))
        headers['tabular-split'] = ','.join(map(str, split))
        headers['tabular-num-of-subsplits'] = str(num_sub_splits)
        headers['tabular-request-format'] = request_format
        headers['tabular-response-format'] = response_format
        headers['tabular-enable-sorted-projections'] = str(enable_sorted_projections)
        if limit_rows > 0:
            headers['tabular-limit-rows'] = str(limit_rows)
        if schedule_id is not None:
            headers['tabular-schedule-id'] = str(schedule_id)
        if retry_count > 0:
            headers['tabular-retry-count'] = str(retry_count)
        if search_path:
            headers['tabular-bc-search-path'] = str(search_path)
        if tenant_guid:
            headers['tabular-tenant-guid'] = str(tenant_guid)

        if len(sub_split_start_row_ids) == 0:
            sub_split_start_row_ids = [(n, 0) for n in range(num_sub_splits)]

        for sub_split_id, start_row_id in sub_split_start_row_ids:
            headers[f'tabular-start-row-id-{sub_split_id}'] = f"{sub_split_id},{start_row_id}"

        return headers

    def _build_query_data_url_params(self, projection, query_imports_table):
        if query_imports_table and projection:
            raise ValueError("Can't query both imports and projection table")

        url_params = {}
        if query_imports_table:
            url_params['sub-table'] = IMPORTED_OBJECTS_TABLE_NAME
        elif projection:
            url_params['name'] = projection
        return url_params

    def legacy_query_data(self, bucket, schema, table, params, split=(0, 1, 8), num_sub_splits=1, response_row_id=False,
                      txid=0, client_tags=[], expected_retvals=[], limit_rows=0, schedule_id=None, retry_count=0,
                      search_path=None, sub_split_start_row_ids=[], tenant_guid=None, projection='', enable_sorted_projections=True,
                      request_format='string', response_format='string', query_imports_table=False):
        """
        POST /mybucket/myschema/mytable?query-data=LegacyQueryData HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-split: "split_id,total_splits,num_row_groups_per_split"
        tabular-num-of-subsplits: "total"
        tabular-request-format: "string"
        tabular-response-format: "string" #arrow/trino
        tabular-schedule-id: "schedule-id"

        Request Body (flatbuf)
        projections_chunk [expressions]
        predicate_chunk "formatted_data", (required)

        """
        headers = self._build_query_data_headers(txid, client_tags, params, split, num_sub_splits, request_format, response_format,
                                                  enable_sorted_projections, limit_rows, schedule_id, retry_count, search_path, tenant_guid,
                                                  sub_split_start_row_ids)
        url_params = self._build_query_data_url_params(projection, query_imports_table)

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="query-data=LegacyQueryData",
                                                  url_params=url_params), data=params, headers=headers, stream=True)
        return self._check_res(res, "legacy_query_data", expected_retvals)

    def query_data(self, bucket, schema, table, params, split=(0, 1, 8), num_sub_splits=1, response_row_id=False,
                   txid=0, client_tags=[], expected_retvals=[], limit_rows=0, schedule_id=None, retry_count=0,
                   search_path=None, sub_split_start_row_ids=[], tenant_guid=None, projection='', enable_sorted_projections=True,
                   request_format='string', response_format='string', query_imports_table=False):
        """
        GET /mybucket/myschema/mytable?data HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-split: "split_id,total_splits,num_row_groups_per_split"
        tabular-num-of-subsplits: "total"
        tabular-request-format: "string"
        tabular-response-format: "string" #arrow/trino
        tabular-schedule-id: "schedule-id"

        Request Body (flatbuf)
        projections_chunk [expressions]
        predicate_chunk "formatted_data", (required)

        To query the internal vastdb-imported-objects table, set query_imports_table=True
        """
        # add query option select-only and read-only

        headers = self._build_query_data_headers(txid, client_tags, params, split, num_sub_splits, request_format, response_format,
                                                 enable_sorted_projections, limit_rows, schedule_id, retry_count, search_path, tenant_guid,
                                                 sub_split_start_row_ids)

        url_params = self._build_query_data_url_params(projection, query_imports_table)

        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="data", url_params=url_params),
                               data=params, headers=headers, stream=True)
        return self._check_res(res, "query_data", expected_retvals)

    """
    source_files: list of (bucket_name, file_name)
    """
    def import_data(self, bucket, schema, table, source_files, txid=0, client_tags=[], expected_retvals=[], case_sensitive=True,
                    schedule_id=None, retry_count=0, blocking=True):
        """
        POST /mybucket/myschema/mytable?data HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        tabular-case-sensitive: True (default)

        Request Body flatbuffer
        table S3File {
          format: "parquet";
          bucket_name: "bucket1";
          file_name: "path/to/file1";
          partitions: [ubyte]; # serialized as a Arrow schema + single-row Arrow page, if exist
        }
        table ImportDataRequest {
          s3_files: [S3File]
        }

        """
        builder = flatbuffers.Builder(1024)
        s3_files = []
        for (src_bucket_name, src_obj_name), partition_record_bytes in source_files.items():
            fmt = builder.CreateString('parquet')
            bname = builder.CreateString(src_bucket_name)
            fname = builder.CreateString(src_obj_name)
            partition = builder.CreateByteVector(partition_record_bytes)
            tabular_s3_file.Start(builder)
            tabular_s3_file.AddFormat(builder, fmt)
            tabular_s3_file.AddBucketName(builder, bname)
            tabular_s3_file.AddFileName(builder, fname)
            if len(partition_record_bytes) > 0:
                tabular_s3_file.AddPartitions(builder, partition)

            s3_file = tabular_s3_file.End(builder)
            s3_files.append(s3_file)

        tabular_import_data.StartS3FilesVector(builder, len(s3_files))
        for f in reversed(s3_files):
            builder.PrependUOffsetTRelative(f)

        files = builder.EndVector()
        tabular_import_data.Start(builder)
        tabular_import_data.AddS3Files(builder, files)
        params = tabular_import_data.End(builder)
        builder.Finish(params)
        import_req = builder.Output()

        def iterate_over_import_data_response(response):
            if response.status_code != 200:
                return response

            chunk_size = 1024
            for chunk in response.iter_content(chunk_size=chunk_size):
                chunk_dict = json.loads(chunk)
                _logger.debug("import data chunk=%s, result: %s", chunk_dict, chunk_dict['res'])
                if chunk_dict['res'] != 'Success' and chunk_dict['res'] != 'TabularInProgress' and chunk_dict['res'] != 'TabularAlreadyImported':
                    raise errors.ImportFilesError(
                        f"Encountered an error during import_data. status: {chunk_dict['res']}, "
                        f"error message: {chunk_dict['err_msg'] or 'Unexpected error'} during import of "
                        f"object name: {chunk_dict['object_name']}", chunk_dict)
                else:
                    _logger.debug("import_data of object name '%s' is in progress. "
                                  "status: %s", chunk_dict['object_name'], chunk_dict['res'])
            return response

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(import_req))
        headers['tabular-case-sensitive'] = str(case_sensitive)
        if schedule_id is not None:
            headers['tabular-schedule-id'] = str(schedule_id)
        if retry_count > 0:
            headers['tabular-retry-count'] = str(retry_count)
        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="data"),
                                data=import_req, headers=headers, stream=True)
        if blocking:
            res = iterate_over_import_data_response(res)

        return self._check_res(res, "import_data", expected_retvals)

    def _record_batch_slices(self, batch, rows_per_slice=None):
        max_slice_size_in_bytes = int(0.9*5*1024*1024) # 0.9 * 5MB
        batch_len = len(batch)
        serialized_batch = serialize_record_batch(batch)
        batch_size_in_bytes = len(serialized_batch)
        _logger.debug('max_slice_size_in_bytes=%d batch_len=%d batch_size_in_bytes=%d',
                      max_slice_size_in_bytes, batch_len, batch_size_in_bytes)

        if not rows_per_slice:
            if batch_size_in_bytes < max_slice_size_in_bytes:
                rows_per_slice = batch_len
            else:
                rows_per_slice = int(0.9 * batch_len * max_slice_size_in_bytes / batch_size_in_bytes)

        done_slicing = False
        while not done_slicing:
            # Attempt slicing according to the current rows_per_slice
            offset = 0
            serialized_slices = []
            for i in range(math.ceil(batch_len/rows_per_slice)):
                offset = rows_per_slice * i
                if offset >= batch_len:
                    done_slicing=True
                    break
                slice_batch = batch.slice(offset, rows_per_slice)
                serialized_slice_batch = serialize_record_batch(slice_batch)
                sizeof_serialized_slice_batch = len(serialized_slice_batch)

                if sizeof_serialized_slice_batch <= max_slice_size_in_bytes:
                    serialized_slices.append(serialized_slice_batch)
                else:
                    _logger.info(f'Using rows_per_slice {rows_per_slice} slice {i} size {sizeof_serialized_slice_batch} exceeds {max_slice_size_in_bytes} bytes, trying smaller rows_per_slice')
                    # We have a slice that is too large
                    rows_per_slice = int(rows_per_slice/2)
                    if rows_per_slice < 1:
                        raise ValueError('cannot decrease batch size below 1 row')
                    break
            else:
                done_slicing = True

        return serialized_slices

    def insert_rows(self, bucket, schema, table, record_batch, txid=0, client_tags=[], expected_retvals=[]):
        """
        POST /mybucket/myschema/mytable?rows HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body
            RecordBatch
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(record_batch))
        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="rows"),
                                data=record_batch, headers=headers, stream=True)
        return self._check_res(res, "insert_rows", expected_retvals)

    def update_rows(self, bucket, schema, table, record_batch, txid=0, client_tags=[], expected_retvals=[]):
        """
        PUT /mybucket/myschema/mytable?rows HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body
            RecordBatch where first column must be row_id
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(record_batch))
        res = self.session.put(self._api_prefix(bucket=bucket, schema=schema, table=table, command="rows"),
                                data=record_batch, headers=headers)
        return self._check_res(res, "update_rows", expected_retvals)

    def delete_rows(self, bucket, schema, table, record_batch, txid=0, client_tags=[], expected_retvals=[]):
        """
        DELETE /mybucket/myschema/mytable?rows HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body
            RecordBatch with single column '$row_id'
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(record_batch))
        res = self.session.delete(self._api_prefix(bucket=bucket, schema=schema, table=table, command="rows"),
                               data=record_batch, headers=headers)
        return self._check_res(res, "delete_rows", expected_retvals)

    def create_projection(self, bucket, schema, table, name, columns, txid=0, client_tags=[], expected_retvals=[]):
        """
        Create a table, use the following request
        POST /bucket/schema/table?projection&name=my_projection HTTP/1.1

        The user may define several Sorted and/Or Unsorted columns from the original table
        columns = [('col1','Sorted'), ('col2','Unsorted'), ('col3','Unsorted')]

        Request Headers
        tabular-txid: <integer> TransactionId
        tabular-client-tag: <string> ClientTag

        The body of the POST request contains table column properties as Flatbuffer
        {
            column_type: Sorted, Unsorted
        }
        """
        builder = flatbuffers.Builder(1024)
        proj_columns = []
        for (col_name, col_type) in columns:
            pname = builder.CreateString(col_name)
            ptype = tabular_proj_column_type.ColumnType.Sorted if col_type == "Sorted" else tabular_proj_column_type.ColumnType.Unsorted
            tabular_projecion_column.Start(builder)
            tabular_projecion_column.AddName(builder, pname)
            tabular_projecion_column.AddType(builder, ptype)

            column_info = tabular_projecion_column.End(builder)
            proj_columns.append(column_info)

        tabular_create_projection.StartColumnsVector(builder, len(proj_columns))
        for c in reversed(proj_columns):
            builder.PrependUOffsetTRelative(c)

        columns = builder.EndVector()
        tabular_create_projection.Start(builder)
        tabular_create_projection.AddColumns(builder, columns)
        params = tabular_create_projection.End(builder)
        builder.Finish(params)
        create_projection_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)

        headers['Content-Length'] = str(len(create_projection_req))
        url_params = {'name': name}

        res = self.session.post(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection", url_params=url_params),
                                data=create_projection_req, headers=headers)
        return self._check_res(res, "create_projection", expected_retvals)

    def get_projection_stats(self, bucket, schema, table, name, txid=0, client_tags=[], expected_retvals=[]):
        """
        GET /mybucket/myschema/mytable?projection-stats&name=my_projection HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        The Command will return the statistics in flatbuf format
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        url_params = {'name': name}
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection-stats", url_params=url_params),
                               headers=headers)
        if res.status_code == 200:
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            stats = get_projection_table_stats.GetRootAs(flatbuf)
            num_rows = stats.NumRows()
            size_in_bytes = stats.SizeInBytes()
            dirty_blocks_percentage = stats.DirtyBlocksPercentage()
            initial_sync_progress = stats.InitialSyncProgress()
            return num_rows, size_in_bytes, dirty_blocks_percentage, initial_sync_progress

        return self._check_res(res, "get_projection_stats", expected_retvals)

    def alter_projection(self, bucket, schema, table, name, txid=0, client_tags=[], table_properties="",
                         new_name="", expected_retvals=[]):
        """
        PUT /mybucket/myschema/mytable?projection&name=my_projection HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body Flatbuffer
        Table properties
        """
        builder = flatbuffers.Builder(1024)

        properties = builder.CreateString(table_properties)
        table_name = builder.CreateString(new_name)
        tabular_alter_projection.Start(builder)
        if new_name:
            tabular_alter_projection.AddNewName(builder, table_name)

        if table_properties:
            tabular_alter_projection.AddProperties(builder, properties)

        params = tabular_alter_projection.End(builder)
        builder.Finish(params)
        alter_projection_req = builder.Output()

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['Content-Length'] = str(len(alter_projection_req))
        url_params = {'name': name}

        res = self.session.put(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection", url_params=url_params),
                               data=alter_projection_req, headers=headers)

        return self._check_res(res, "alter_projection", expected_retvals)

    def drop_projection(self, bucket, schema, table, name, txid=0, client_tags=[], expected_retvals=[]):
        """
        DELETE /mybucket/schema_path/mytable?projection&name=my_projection HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        url_params = {'name': name}

        res = self.session.delete(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection", url_params=url_params),
                                  headers=headers)
        return self._check_res(res, "drop_projection", expected_retvals)

    def list_projections(self, bucket, schema, table, txid=0, client_tags=[], max_keys=1000, next_key=0, name_prefix="",
                         exact_match=False, expected_retvals=[], include_list_stats=False, count_only=False):
        """
        GET /mybucket/schema_path/my_table?projection HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        x-tabluar-name-prefix: TableNamePrefix
        tabular-max-keys: 1000
        tabular-next-key: NextKey (Name)
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-max-keys'] = str(max_keys)
        headers['tabular-next-key'] = str(next_key)
        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        headers['tabular-list-count-only'] = str(count_only)
        headers['tabular-include-list-stats'] = str(include_list_stats)

        projections = []
        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection"), headers=headers)
        self._check_res(res, "list_projections", expected_retvals)
        if res.status_code == 200:
            res_headers = res.headers
            next_key = int(res_headers['tabular-next-key'])
            is_truncated = res_headers['tabular-is-truncated'] == 'true'
            count = int(res_headers['tabular-list-count'])
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            lists = list_projections.GetRootAs(flatbuf)
            bucket_name = lists.BucketName().decode()
            schema_name = lists.SchemaName().decode()
            table_name = lists.TableName().decode()
            if not bucket.startswith(bucket_name):  # ignore snapshot name
                raise ValueError(f'bucket: {bucket} did not start from {bucket_name}')
            projections_length = lists.ProjectionsLength()
            for i in range(projections_length):
                projections.append(_parse_table_info(lists.Projections(i)))

            return bucket_name, schema_name, table_name, projections, next_key, is_truncated, count

    def list_projection_columns(self, bucket, schema, table, projection, txid=0, client_tags=[], max_keys=1000,
                                next_key=0, count_only=False, name_prefix="", exact_match=False,
                                expected_retvals=None):
        """
        GET /mybucket/myschema/mytable?projection-columns HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        x-tabluar-name-prefix: TableNamePrefix
        tabular-max-keys: 1000
        tabular-next-key: NextColumnId
        """
        client_tags = client_tags or []
        expected_retvals = expected_retvals or []

        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)
        headers['tabular-max-keys'] = str(max_keys)
        headers['tabular-next-key'] = str(next_key)
        headers['tabular-list-count-only'] = str(count_only)

        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        url_params = {'name': projection}

        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="projection-columns", url_params=url_params),
                               headers=headers, stream=True)
        self._check_res(res, "list_projection_columns", expected_retvals)
        # list projection columns response will also show column type Sorted/UnSorted
        if res.status_code == 200:
            res_headers = res.headers
            next_key = int(res_headers['tabular-next-key'])
            is_truncated = res_headers['tabular-is-truncated'] == 'true'
            count = int(res_headers['tabular-list-count'])
            columns = []
            if not count_only:
                schema_buf = b''.join(res.iter_content(chunk_size=128))
                schema_out = pa.ipc.open_stream(schema_buf).schema
                for f in schema_out:
                    columns.append([f.name, f.type, f.metadata])
                #   sort_type = f.metadata[b'VAST:sort_type'].decode()

            return columns, next_key, is_truncated, count


def _iter_query_data_response_columns(fileobj, stream_ids=None):
    readers = {}  # {stream_id: pa.ipc.RecordBatchStreamReader}
    while True:
        stream_id_bytes = fileobj.read(4)
        if not stream_id_bytes:
            if readers:
                raise EOFError(f'no readers ({readers}) should be open at EOF')
            break

        stream_id, = struct.unpack('<L', stream_id_bytes)
        if stream_ids is not None:
            stream_ids.update([stream_id])  # count stream IDs using a collections.Counter
        if stream_id == TABULAR_KEEP_ALIVE_STREAM_ID:
            continue

        if stream_id == TABULAR_QUERY_DATA_COMPLETED_STREAM_ID:
            # read the terminating end chunk from socket
            res = fileobj.read()
            _logger.debug("stream_id=%d res=%s (finish)", stream_id, res)
            return

        if stream_id == TABULAR_QUERY_DATA_FAILED_STREAM_ID:
            # read the terminating end chunk from socket
            res = fileobj.read()
            _logger.warning("stream_id=%d res=%s (failed)", stream_id, res)
            raise IOError(f"Query data stream failed res={res}")

        next_row_id_bytes = fileobj.read(8)
        next_row_id, = struct.unpack('<Q', next_row_id_bytes)
        _logger.debug("stream_id=%d next_row_id=%d", stream_id, next_row_id)

        if stream_id not in readers:
            # we implicitly read 1st message (Arrow schema) when constructing RecordBatchStreamReader
            reader = pa.ipc.RecordBatchStreamReader(fileobj)
            _logger.debug("stream_id=%d schema=%s", stream_id, reader.schema)
            readers[stream_id] = (reader, [])
            continue

        (reader, batches) = readers[stream_id]
        try:
            batch = reader.read_next_batch() # read single-column chunk data
            _logger.debug("stream_id=%d rows=%d chunk=%s", stream_id, len(batch), batch)
            batches.append(batch)
        except StopIteration:  # we got an end-of-stream IPC message for a given stream ID
            reader, batches = readers.pop(stream_id)  # end of column
            table = pa.Table.from_batches(batches)  # concatenate all column chunks (as a single)
            _logger.debug("stream_id=%d rows=%d column=%s", stream_id, len(table), table)
            yield (stream_id, next_row_id, table)


def parse_query_data_response(conn, schema, stream_ids=None, start_row_ids=None, debug=False):
    """
    Generates pyarrow.Table objects from QueryData API response stream.

    A pyarrow.Table is a helper class that combines a Schema with multiple RecordBatches and allows easy data access.
    """
    if start_row_ids is None:
        start_row_ids = {}
    projection_positions = schema.projection_positions
    arrow_schema = schema.arrow_schema
    output_field_names = schema.output_field_names
    _logger.debug(f'projection_positions={projection_positions} len(arrow_schema)={len(arrow_schema)} arrow_schema={arrow_schema}')
    is_empty_projection = (len(projection_positions) == 0)
    parsers = defaultdict(lambda: QueryDataParser(arrow_schema, debug=debug, projection_positions=projection_positions))  # {stream_id: QueryDataParser}
    for stream_id, next_row_id, table in _iter_query_data_response_columns(conn, stream_ids):
        parser = parsers[stream_id]
        for column in table.columns:
            parser.parse(column)

        parsed_table = parser.build(output_field_names)
        if parsed_table is not None:  # when we got all columns (and before starting a new "select_rows" cycle)
            parsers.pop(stream_id)
            if is_empty_projection:  # VAST returns an empty RecordBatch, with the correct rows' count
                parsed_table = table

            _logger.debug("stream_id=%d rows=%d next_row_id=%d table=%s",
                          stream_id, len(parsed_table), next_row_id, parsed_table)
            start_row_ids[stream_id] = next_row_id
            yield parsed_table  # the result of a single "select_rows()" cycle

    if parsers:
        raise EOFError(f'all streams should be done before EOF. {parsers}')

def get_field_type(builder: flatbuffers.Builder, field: pa.Field):
    if field.type.equals(pa.int64()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, True)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.int32()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, True)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.int16()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, True)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.int8()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, True)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.uint64()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, False)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.uint32()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, False)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.uint16()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, False)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.uint8()):
        field_type_type = Type.Int
        fb_int.Start(builder)
        fb_int.AddBitWidth(builder, field.type.bit_width)
        fb_int.AddIsSigned(builder, False)
        field_type = fb_int.End(builder)

    elif field.type.equals(pa.float32()):
        field_type_type = Type.FloatingPoint
        fb_floating_point.Start(builder)
        fb_floating_point.AddPrecision(builder, 1)  # single
        field_type = fb_floating_point.End(builder)

    elif field.type.equals(pa.float64()):
        field_type_type = Type.FloatingPoint
        fb_floating_point.Start(builder)
        fb_floating_point.AddPrecision(builder, 2)  # double
        field_type = fb_floating_point.End(builder)

    elif field.type.equals(pa.string()):
        field_type_type = Type.Utf8
        fb_utf8.Start(builder)
        field_type = fb_utf8.End(builder)

    elif field.type.equals(pa.date32()):  # pa.date64()
        field_type_type = Type.Date
        fb_date.Start(builder)
        fb_date.AddUnit(builder, DateUnit.DAY)
        field_type = fb_date.End(builder)

    elif isinstance(field.type, pa.TimestampType):
        field_type_type = Type.Timestamp
        fb_timestamp.Start(builder)
        fb_timestamp.AddUnit(builder, get_unit_to_flatbuff_time_unit(field.type.unit))
        field_type = fb_timestamp.End(builder)

    elif field.type.equals(pa.time32('s')) or field.type.equals(pa.time32('ms')) or field.type.equals(pa.time64('us')) or field.type.equals(pa.time64('ns')):
        field_type_str = str(field.type)
        start = field_type_str.index('[')
        end = field_type_str.index(']')
        unit = field_type_str[start + 1:end]

        field_type_type = Type.Time
        fb_time.Start(builder)
        fb_time.AddBitWidth(builder, field.type.bit_width)
        fb_time.AddUnit(builder, get_unit_to_flatbuff_time_unit(unit))
        field_type = fb_time.End(builder)

    elif field.type.equals(pa.bool_()):
        field_type_type = Type.Bool
        fb_bool.Start(builder)
        field_type = fb_bool.End(builder)

    elif isinstance(field.type, pa.Decimal128Type):
        field_type_type = Type.Decimal
        fb_decimal.Start(builder)
        fb_decimal.AddPrecision(builder, field.type.precision)
        fb_decimal.AddScale(builder, field.type.scale)
        field_type = fb_decimal.End(builder)

    elif field.type.equals(pa.binary()):
        field_type_type = Type.Binary
        fb_binary.Start(builder)
        field_type = fb_binary.End(builder)

    elif isinstance(field.type, pa.StructType):
        field_type_type = Type.Struct_
        fb_struct.Start(builder)
        field_type = fb_struct.End(builder)

    elif isinstance(field.type, pa.ListType):
        field_type_type = Type.List
        fb_list.Start(builder)
        field_type = fb_list.End(builder)

    elif isinstance(field.type, pa.MapType):
        field_type_type = Type.Map
        fb_map.Start(builder)
        field_type = fb_map.End(builder)

    elif isinstance(field.type, pa.FixedSizeBinaryType):
        field_type_type = Type.FixedSizeBinary
        fb_fixed_size_binary.Start(builder)
        fb_fixed_size_binary.AddByteWidth(builder, field.type.byte_width)
        field_type = fb_fixed_size_binary.End(builder)

    else:
        raise ValueError(f'unsupported predicate for type={field.type}')

    return field_type, field_type_type

def build_field(builder: flatbuffers.Builder, f: pa.Field, name: str):
    children = None
    if isinstance(f.type, pa.StructType):
        children = [build_field(builder, child, child.name) for child in list(f.type)]
    if isinstance(f.type, pa.ListType):
        children = [build_field(builder, f.type.value_field, "item")]
    if isinstance(f.type, pa.MapType):
        children = [
            build_field(builder, f.type.key_field, "key"),
            build_field(builder, f.type.item_field, "value"),
        ]

        # adding "entries" column:
        fb_field.StartChildrenVector(builder, len(children))
        for offset in reversed(children):
            builder.PrependUOffsetTRelative(offset)
        children = builder.EndVector()

        field_type, field_type_type = get_field_type(builder, f)

        child_col_name = builder.CreateString("entries")
        fb_field.Start(builder)
        fb_field.AddTypeType(builder, field_type_type)
        fb_field.AddType(builder, field_type)
        fb_field.AddName(builder, child_col_name)
        fb_field.AddChildren(builder, children)

        children = [fb_field.End(builder)]

    if children is not None:
        fb_field.StartChildrenVector(builder, len(children))
        for offset in reversed(children):
            builder.PrependUOffsetTRelative(offset)
        children = builder.EndVector()

    col_name = builder.CreateString(name)
    field_type, field_type_type = get_field_type(builder, f)
    fb_field.Start(builder)
    fb_field.AddName(builder, col_name)
    fb_field.AddTypeType(builder, field_type_type)
    fb_field.AddType(builder, field_type)
    if children is not None:
        fb_field.AddChildren(builder, children)
    return fb_field.End(builder)


class VastDBResponseSchema:
    def __init__(self, arrow_schema, projection_positions, output_field_names):
        self.arrow_schema = arrow_schema
        self.projection_positions = projection_positions
        self.output_field_names = output_field_names

class QueryDataRequest:
    def __init__(self, serialized, response_schema):
        self.serialized = serialized
        self.response_schema = response_schema


def build_query_data_request(schema: 'pa.Schema' = pa.schema([]), predicate: ibis.expr.types.BooleanColumn = None, field_names: list = None):
    builder = flatbuffers.Builder(1024)

    source_name = builder.CreateString('')  # required

    fields = [build_field(builder, f, f.name) for f in schema]

    fb_schema.StartFieldsVector(builder, len(fields))
    for offset in reversed(fields):
        builder.PrependUOffsetTRelative(offset)
    fields = builder.EndVector()

    fb_schema.Start(builder)
    fb_schema.AddFields(builder, fields)
    schema_obj = fb_schema.End(builder)

    predicate = Predicate(schema=schema, expr=predicate)
    filter_obj = predicate.serialize(builder)

    parser = QueryDataParser(schema)
    leaves_map = {}
    for node in parser.nodes:
        for descendent in node._iter_nodes():
            if descendent.parent and isinstance(descendent.parent.type, (pa.ListType, pa.MapType)):
                continue
            iter_from_root = reversed(list(descendent._iter_to_root()))
            descendent_full_name = '.'.join([n.field.name for n in iter_from_root])
            descendent_leaves = [leaf.index for leaf in descendent._iter_leaves()]
            leaves_map[descendent_full_name] = descendent_leaves

    output_field_names = None
    if field_names is None:
        field_names = [field.name for field in schema]
    else:
        output_field_names  = [f.split('.')[0] for f in field_names]
        # sort projected field_names according to positions to maintain ordering according to the schema
        def compare_field_names_by_pos(field_name1, field_name2):
            return leaves_map[field_name1][0]-leaves_map[field_name2][0]
        field_names = sorted(field_names, key=cmp_to_key(compare_field_names_by_pos))

    projection_fields = []
    projection_positions = []
    for field_name in field_names:
        positions = leaves_map[field_name]
        projection_positions.extend(positions)
        for leaf_position in positions:
            fb_field_index.Start(builder)
            fb_field_index.AddPosition(builder, leaf_position)
            offset = fb_field_index.End(builder)
            projection_fields.append(offset)
    fb_source.StartProjectionVector(builder, len(projection_fields))
    for offset in reversed(projection_fields):
        builder.PrependUOffsetTRelative(offset)
    projection = builder.EndVector()

    response_schema = VastDBResponseSchema(schema, projection_positions, output_field_names=output_field_names)

    fb_source.Start(builder)
    fb_source.AddName(builder, source_name)
    fb_source.AddSchema(builder, schema_obj)
    fb_source.AddFilter(builder, filter_obj)
    fb_source.AddProjection(builder, projection)
    source = fb_source.End(builder)

    fb_relation.Start(builder)
    fb_relation.AddImplType(builder, rel_impl.RelationImpl.Source)
    fb_relation.AddImpl(builder, source)
    relation = fb_relation.End(builder)

    builder.Finish(relation)
    return QueryDataRequest(serialized=builder.Output(), response_schema=response_schema)


def convert_column_types(table: 'pa.Table') -> 'pa.Table':
    """
    Adjusting table values

    1. Because the timestamp resolution is too high it is necessary to trim it. ORION-96961
    2. Since the values of nfs_mode_bits are returned in decimal, need to convert them to octal,
    as in all representations, so that the mode of 448 turn into 700
    3. for owner_name and group_owner_name 0 -> root, and 65534 -> nobody
    """
    ts_indexes = []
    indexes_of_fields_to_change = {}
    sid_to_name = {
        '0': 'root',
        '65534': 'nobody'  # NFSNOBODY_UID_16_BIT
    }
    column_matcher = {  # column_name: custom converting rule
        'nfs_mode_bits': lambda val: int(oct(val).replace('0o', '')) if val is not None else val,
        'owner_name': lambda val: sid_to_name.get(val, val),
        'group_owner_name': lambda val: sid_to_name.get(val, val),
    }
    for index, field in enumerate(table.schema):
        if isinstance(field.type, pa.TimestampType) and field.type.unit == 'ns':
            ts_indexes.append(index)
        if field.name in column_matcher:
            indexes_of_fields_to_change[field.name] = index
    for changing_index in ts_indexes:
        field_name = table.schema[changing_index].name
        new_column = table[field_name].cast(pa.timestamp('us'), safe=False)
        table = table.set_column(changing_index, field_name, new_column)
    for field_name, changing_index in indexes_of_fields_to_change.items():
        new_column = table[field_name].to_pylist()
        new_column = list(map(column_matcher[field_name], new_column))
        new_column = pa.array(new_column, table[field_name].type)
        table = table.set_column(changing_index, field_name, new_column)
    return table
