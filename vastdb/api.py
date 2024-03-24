import array
import logging
import struct
import urllib.parse
from collections import defaultdict, namedtuple
from datetime import datetime
from enum import Enum
from typing import List, Union, Optional, Iterator
import xmltodict
import concurrent.futures
import threading
import queue
import math
import socket
from functools import cmp_to_key
import pyarrow.parquet as pq
import flatbuffers
import pyarrow as pa
import requests
import datetime
import hashlib
import hmac
import json
import itertools
from aws_requests_auth.aws_auth import AWSRequestsAuth
from io import BytesIO

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

"""
S3 Tabular API
"""


def get_logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.ERROR)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.set_name('tabular_stream_handler')
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
    ch.setFormatter(formatter)
    log.addHandler(ch)
    log.propagate = False
    return log


_logger = get_logger(__name__)


def set_tabular_log_level(level: int = logging.INFO):
    _logger.setLevel(level)


class AuthType(Enum):
    SIGV4 = "s3v4"
    SIGV2 = "s3"
    BASIC = "basic"


class TabularException(Exception):
    pass


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

    def __init__(self, schema: 'pa.Schema', filters: dict):
        self.schema = schema
        self.filters = filters
        self.builder = None
        self._field_name_per_index = None

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
            _logger.debug(f'field_name_per_index: {self._field_name_per_index}')
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
        self.builder = builder
        offsets = []
        for field_name in self.filters:
            offsets.append(self.build_domain(self.build_column(self.field_name_per_index[field_name]), field_name))
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
        _logger.info(f'trying to append field: {field} with domains: {filters}')
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
        _logger.info(f'name: {name}, offsets: {offsets}')
        offset_name = self.builder.CreateString(name)
        fb_call.StartArgumentsVector(self.builder, len(offsets))
        for offset in reversed(offsets):
            _logger.info(f'offset: {offset}')
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

    def build_literal(self, field: pa.Field, value: str):
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
            date_value = datetime.strptime(value, '%Y-%m-%d').date()
            date_delta = date_value - start_date
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

            value = self.builder.CreateByteVector(value.encode())
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
        _logger.debug(f'build: self.field.name={self.field.name}, '
                      f'self.projected_field.type={self.projected_field.type}, self.length={self.length} '
                      f'self.buffers={self.buffers} children={children}')
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
        _logger.debug(f'QueryDataParser: self.leaves = {[(leaf.field.name, leaf.index) for leaf in self.leaves]}')
        self.mark_projected_nodes()
        [node.build_projected_field() for node in self.nodes]
        self.projected_leaves = [leaf for node in self.nodes for leaf in node._iter_projected_leaves()]
        _logger.debug(f'QueryDataParser: self.projected_leaves = {[(leaf.field.name, leaf.index) for leaf in self.projected_leaves]}')

        self.leaf_offset = 0

    def mark_projected_nodes(self):
        for leaf in self.leaves:
            if self.projection_positions is None or leaf.index in self.projection_positions:
                for node in leaf._iter_to_root():
                    node.is_projected = True
                    _logger.debug(f'mark_projected_nodes node.field.name={node.field.name}')

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
    _logger.info(f"column_info={column_info}")
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

def generate_ip_range(ip_range_str):
    start, end = ip_range_str.split(':')
    start_parts = start.split('.')
    start_last_part = int(start_parts[-1])
    end_parts = end.split('.')
    end_last_part = int(end_parts[-1])
    if start_last_part>=end_last_part or True in [start_parts[i] != end_parts[i] for i in range(3)]:
        raise ValueError(f'illegal ip range {ip_range_str}')
    num_ips = 1 + end_last_part - start_last_part
    ips = ['.'.join(start_parts[:-1] + [str(start_last_part + i)]) for i in range(num_ips)]
    return ips

def parse_executor_hosts(host):
        executor_hosts_parsed = host.split(',')
        executor_hosts_parsed = [host.strip() for host in executor_hosts_parsed]
        executor_hosts = []
        for executor_host in executor_hosts_parsed:
            is_ip_range=False
            if ':' in executor_host:
                try:
                    socket.inet_aton(executor_host.split(':')[0])
                    socket.inet_aton(executor_host.split(':')[1])
                    is_ip_range = True
                except:
                    pass
            if is_ip_range:
                executor_hosts.extend(generate_ip_range(executor_host))
            else:
                executor_hosts.append(executor_host)
        return executor_hosts

class VastdbApi:
    def __init__(self, host, access_key, secret_key, username=None, password=None, port=None,
                 secure=False, auth_type=AuthType.SIGV4):
        executor_hosts = parse_executor_hosts(host)
        host = executor_hosts[0]
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.username = username
        self.password = password
        self.port = port
        self.secure = secure
        self.auth_type = auth_type
        self.executor_hosts = executor_hosts

        username = username or ''
        password = password or ''
        if not port:
            port = 443 if secure else 80

        self.session = requests.Session()
        self.session.verify = False
        self.session.headers['user-agent'] = "VastData Tabular API 1.0 - 2022 (c)"
        if auth_type == AuthType.BASIC:
            self.session.auth = requests.auth.HTTPBasicAuth(username, password)
        else:
            if port != 80 and port != 443:
                self.aws_host = f'{host}:{port}'
            else:
                self.aws_host = f'{host}'

            self.session.auth = AWSRequestsAuth(aws_access_key=access_key,
                                                aws_secret_access_key=secret_key,
                                                aws_host=self.aws_host,
                                                aws_region='us-east-1',
                                                aws_service='s3')

        proto = "https" if secure else "http"
        self.url = f"{proto}://{self.aws_host}"

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
        try:
            res.raise_for_status()
            if res.status_code != 200:
                if not  res.status_code in expected_retvals:
                    raise ValueError(f"Expected status code mismatch. status_code={res.status_code}")
            else:
                if not len(expected_retvals) == 0:
                    raise ValueError(f"Expected {expected_retvals} but status_code={res.status_code}")
            return res
        except requests.HTTPError as e:
            if res.status_code in expected_retvals:
                _logger.info(f"{cmd} has failed as expected res={res}")
                return res
            else:
                raise e

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


    def create_table(self, bucket, schema, name, arrow_schema, txid=0, client_tags=[], expected_retvals=[], topic_partitions=0):
        """
        Create a table, use the following request
        POST /bucket/schema/table?table HTTP/1.1

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
        url_params = {'topic_partitions': str(topic_partitions)} if topic_partitions else {}

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
        _logger.info(f'type(parquet_ds.schema) = {type(parquet_ds.schema)}')
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
            res_headers = res.headers
            flatbuf = b''.join(res.iter_content(chunk_size=128))
            stats = get_table_stats.GetRootAs(flatbuf)
            num_rows = stats.NumRows()
            size_in_bytes = stats.SizeInBytes()
            is_external_rowid_alloc = stats.IsExternalRowidAlloc()
            return num_rows, size_in_bytes, is_external_rowid_alloc
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
        url_params = {'tabular-new-table-name': new_name} if len(new_name) else {}

        res = self.session.put(self._api_prefix(bucket=bucket, schema=schema, table=name, command="table", url_params=url_params),
                               data=alter_table_req, headers=headers)

        return self._check_res(res, "alter_table", expected_retvals)

    def drop_table(self, bucket, schema, name, txid=0, client_tags=[], expected_retvals=[]):
        """
        DELETE /mybucket/schema_path/mytable?table HTTP/1.1
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag
        """
        headers = self._fill_common_headers(txid=txid, client_tags=client_tags)

        res = self.session.delete(self._api_prefix(bucket=bucket, schema=schema, table=name, command="table"), headers=headers)
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

    def list_columns(self, bucket, schema, table, *, txid=0, client_tags=None, max_keys=1000, next_key=0,
                     count_only=False, name_prefix="", exact_match=False,
                     expected_retvals=None, bc_list_internals=False):
        """
        GET /mybucket/myschema/mytable?columns HTTP/1.1
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
        if bc_list_internals:
            headers['tabular-bc-list-internal-col'] = "true"

        if exact_match:
            headers['tabular-name-exact-match'] = name_prefix
        else:
            headers['tabular-name-prefix'] = name_prefix

        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="column"),
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
    #            _logger.info(f"schema={schema_out}")
                for f in schema_out:
                    columns.append([f.name, f.type, f.metadata, f])

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
                       retry_count=0, enable_sorted_projections=False):
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
                          retry_count=0, enable_sorted_projections=False):
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
                   retry_count=0, enable_sorted_projections=False):
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

    def query_data(self, bucket, schema, table, params, split=(0, 1, 8), num_sub_splits=1, response_row_id=False,
                   txid=0, client_tags=[], expected_retvals=[], limit_rows=0, schedule_id=None, retry_count=0,
                   search_path=None, sub_split_start_row_ids=[], tenant_guid=None, projection='', enable_sorted_projections=True,
                   request_format='string', response_format='string'):
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

        """
        # add query option select-only and read-only
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

        url_params = {'name': projection} if projection else {}

        res = self.session.get(self._api_prefix(bucket=bucket, schema=schema, table=table, command="data", url_params=url_params),
                               data=params, headers=headers, stream=True)
        return self._check_res(res, "query_data", expected_retvals)

    def _list_table_columns(self, bucket, schema, table, filters=None, field_names=None, txid=0):
        # build a list of the queried column names
        queried_columns = []
        # get all columns from the table
        all_listed_columns = []
        next_key = 0
        while True:
            cur_columns, next_key, is_truncated, count = self.list_columns(
                bucket=bucket, schema=schema, table=table, next_key=next_key, txid=txid)
            if not cur_columns:
                break
            all_listed_columns.extend(cur_columns)
            if not is_truncated:
                break

        # build a list of the queried columns
        queried_column_names = set()
        if filters:
            filtered_column_names = ([column_name.split('.')[0] for column_name in filters.keys()]) # use top level of the filter column names
            queried_column_names.update(filtered_column_names)
            _logger.debug(f"_list_table_columns: filtered_column_names={filtered_column_names}")

        if field_names:
            field_column_names = ([column_name.split('.')[0] for column_name in field_names]) # use top level of the field column names
        else:
            field_column_names = [column[0] for column in all_listed_columns]
        _logger.debug(f"_list_table_columns: field_column_names={field_column_names}")
        queried_column_names.update(field_column_names)

        all_listed_column_and_leaves_names = set()
        for column in all_listed_columns:
            # Collect the column and leaves names for verification below that all the filters and field names are in the table
            column_and_leaves_names = [column[0]] + [f.name for f in column[3].flatten()]
            all_listed_column_and_leaves_names.update(column_and_leaves_names)

            # check if this column is needed for the query
            if column[0] in queried_column_names:
                queried_columns.append(column)

        # verify that all the filters and field names are in the table
        if filters:
            for filter_column_name in filters.keys():
                if filter_column_name not in all_listed_column_and_leaves_names:
                    raise KeyError((f'filter column name: {filter_column_name} does not appear in the table'))
        if field_names:
            for field_name in field_names:
                if field_name not in all_listed_column_and_leaves_names:
                    raise ValueError((f'field name: {field_name} does not appear in the table'))
        return list(queried_columns)

    def _begin_tx_if_necessary(self, txid):
        if not txid:
            created_txid = True
            res = self.begin_transaction()
            txid = res.headers.get('tabular-txid')
        else:
            created_txid = False

        return txid, created_txid

    def _prepare_query(self, bucket, schema, table, num_sub_splits, filters=None, field_names=None,
                       queried_columns=None, response_row_id=False, txid=0):
        queried_fields = []
        if response_row_id:
            queried_fields.append(pa.field('$row_id', pa.uint64()))

        if not queried_columns:
            queried_columns = self._list_table_columns(bucket, schema, table, filters, field_names, txid=txid)

        queried_fields.extend(pa.field(column[0], column[1]) for column in queried_columns)
        arrow_schema = pa.schema(queried_fields)

        _logger.debug(f'_prepare_query: arrow_schema = {arrow_schema}')

        query_data_request = build_query_data_request(schema=arrow_schema, filters=filters, field_names=field_names)
        if self.executor_hosts:
            executor_hosts = self.executor_hosts
        else:
            executor_hosts = [self.host]
        executor_sessions = [VastdbApi(executor_hosts[i], self.access_key, self.secret_key, self.username,
                                       self.password, self.port, self.secure, self.auth_type) for i in range(len(executor_hosts))]

        return queried_columns, arrow_schema, query_data_request, executor_sessions

    def _more_pages_exist(self, start_row_ids):
        for row_id in start_row_ids.values():
            if row_id != TABULAR_INVALID_ROW_ID:
                return True
        return False

    def _query_page(self, bucket, schema, table, query_data_request, split=(0, 1, 8), num_sub_splits=1, response_row_id=False,
                   txid=0, limit_rows=0, sub_split_start_row_ids=[], filters=None, field_names=None):
        res = self.query_data(bucket=bucket, schema=schema, table=table, params=query_data_request.serialized, split=split,
                              num_sub_splits=num_sub_splits, response_row_id=response_row_id, txid=txid,
                              limit_rows=limit_rows, sub_split_start_row_ids=sub_split_start_row_ids)
        start_row_ids = {}
        sub_split_tables = parse_query_data_response(res.raw, query_data_request.response_schema,
                                                    start_row_ids=start_row_ids)
        table_page = pa.concat_tables(sub_split_tables)
        _logger.info("query_page: table_page num_rows=%s start_row_ids len=%s",
                     len(table_page), len(start_row_ids))

        return table_page, start_row_ids

    def _query_page_iterator(self, bucket, schema, table, query_data_request, split=(0, 1, 8), num_sub_splits=1, response_row_id=False,
                             txid=0, limit_rows=0, start_row_ids={}, filters=None, field_names=None):
        res = self.query_data(bucket=bucket, schema=schema, table=table, params=query_data_request.serialized, split=split,
                              num_sub_splits=num_sub_splits, response_row_id=response_row_id, txid=txid,
                              limit_rows=limit_rows, sub_split_start_row_ids=start_row_ids.items())
        for sub_split_table in parse_query_data_response(res.raw, query_data_request.response_schema,
                                                        start_row_ids=start_row_ids):
            for record_batch in sub_split_table.to_batches():
                yield record_batch
        _logger.info(f"query_page_iterator: start_row_ids={start_row_ids}")

    def query_iterator(self, bucket, schema, table, num_sub_splits=1, num_row_groups_per_sub_split=8,
                       response_row_id=False, txid=0, limit_per_sub_split=128*1024, filters=None, field_names=None):
        """
        query rows into a table.

        Parameters
        ----------
        bucket : string
            The bucket of the table.
        schema : string
            The schema of the table.
        table : string
            The table name.
        num_sub_splits : integer
            The number of sub_splits per split - determines the parallelism inside a VastDB compute node
            default: 1
        num_row_groups_per_sub_split : integer
            The number of consecutive row groups per sub_split. Each row group consists of 64K row ids.
            default: 8
        response_row_id : boolean
            Return a column with the internal row ids of the table
            default: False
        txid : integer
            A transaction id. The transaction may be initiated before the query, and if not, the query will initiate it
            default: 0 (will be created by the api)
        limit_per_sub_split : integer
            Limit the number of rows from a single sub_split for a single rpc
            default:131072
        filters : dict
            A dictionary whose keys are column names, and values are lists of string expressions that represent
            filter conditions on the column. AND is applied on the conditions. The condition formats are:
            'column_name eq some_value'
            default: None
        field_names : list
            A list of column names to be returned in the output table
            default: None

        Returns
        -------
        Query iterator generator

        Yields
        ------
        pyarrow.RecordBatch

        Examples
        --------
        for record_batch in query_iterator('some_bucket', 'some_schema', 'some_table',
                                           filters={'name': ['eq Alice', 'eq Bob']}
                                           field_names=['name','age']):
            ...

        """

        # create a transaction if necessary
        txid, created_txid = self._begin_tx_if_necessary(txid)
        executor_sessions = []

        try:
            # prepare query
            queried_columns, arrow_schema, query_data_request, executor_sessions = \
                self._prepare_query(bucket, schema, table, num_sub_splits, filters, field_names, response_row_id=response_row_id, txid=txid)

            # define the per split threaded query func
            def query_iterator_split_id(self, split_id):
                _logger.info(f"query_iterator_split_id: split_id={split_id}")
                try:
                    start_row_ids = {i:0 for i in range(num_sub_splits)}
                    session = executor_sessions[split_id]
                    while not next_sems[split_id].acquire(timeout=1):
                        # check if killed externally
                        if killall:
                            raise RuntimeError(f'query_iterator_split_id: split_id {split_id} received killall')

                    while self._more_pages_exist(start_row_ids):
                        for record_batch in session._query_page_iterator(bucket=bucket, schema=schema, table=table, query_data_request=query_data_request,
                                                                         split=(split_id, num_splits, num_row_groups_per_sub_split),
                                                                         num_sub_splits=num_sub_splits, response_row_id=response_row_id,
                                                                         txid=txid, limit_rows=limit_per_sub_split,
                                                                         start_row_ids=start_row_ids):
                            output_queue.put((split_id, record_batch))
                            while not next_sems[split_id].acquire(timeout=1): # wait for the main thread to request the next record batch
                                if killall:
                                    raise RuntimeError(f'split_id {split_id} received killall')
                    # end of split
                    output_queue.put((split_id,None))

                except Exception as e:
                    _logger.exception('query_iterator_split_id: exception occurred')
                    try:
                        self.rollback_transaction(txid)
                    except:
                        _logger.exception(f'failed to rollback txid {txid}')
                    error_queue.put(None)
                    raise e

            # kickoff executors
            num_splits = len(executor_sessions)
            output_queue = queue.Queue()
            error_queue = queue.Queue()
            next_sems = [threading.Semaphore(value=1) for i in range(num_splits)]
            killall = False
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_splits) as executor:
                # start executors
                futures = []
                for i in range(num_splits):
                    futures.append(executor.submit(query_iterator_split_id, self, i))

                # receive outputs and yield them
                done_count = 0
                while done_count < num_splits:
                    # check for errors
                    try:
                        error_queue.get(block=False)
                        _logger.error('received error from a thread')
                        killall = True
                        # wait for all executors to complete
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result() # trigger an exception if occurred in any thread
                            except Exception:
                                _logger.exception('exception occurred')
                        raise RuntimeError('received error from a thread')
                    except queue.Empty:
                        pass

                    # try to get a value from the output queue
                    try:
                        (split_id, record_batch) = output_queue.get(timeout=1)
                    except queue.Empty:
                        continue

                    if record_batch:
                        # signal to the thread to read the next record batch and yield the current
                        next_sems[split_id].release()
                        try:
                            yield record_batch
                        except GeneratorExit:
                            killall = True
                            _logger.debug("cancelling query_iterator")
                            raise
                    else:
                        done_count += 1

                # wait for all executors to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result() # trigger an exception if occurred in any thread
                    except Exception:
                        _logger.exception('exception occurred')

            # commit if needed
            if created_txid:
                self.commit_transaction(txid)

        except Exception as e:
            _logger.exception('exception occurred')
            try:
                self.rollback_transaction(txid)
            except:
                _logger.exception(f'failed to rollback txid {txid}')
            raise e

        finally:
            killall = True
            for session in executor_sessions:
                try:
                    session.session.close()
                except Exception:
                    _logger.exception(f'failed to close session {session}')

    def query(self, bucket, schema, table, num_sub_splits=1, num_row_groups_per_sub_split=8,
              response_row_id=False, txid=0, limit=0, limit_per_sub_split=131072, filters=None, field_names=None,
              queried_columns=None):
        """
        query rows into a table.

        Parameters
        ----------
        bucket : string
            The bucket of the table.
        schema : string
            The schema of the table.
        table : string
            The table name.
        num_sub_splits : integer
            The number of sub_splits per split - determines the parallelism inside a VastDB compute node
            default: 1
        num_row_groups_per_sub_split : integer
            The number of consecutive row groups per sub_split. Each row group consists of 64K row ids.
            default: 8
        response_row_id : boolean
            Return a column with the internal row ids of the table
            default: False
        txid : integer
            A transaction id. The transaction may be initiated before the query, and be used to provide
            multiple ACID operations
            default: 0 (will be created by the api)
        limit : integer
            Limit the number of rows in the response
            default: 0 (no limit)
        limit_per_sub_split : integer
            Limit the number of rows from a single sub_split for a single rpc
            default:131072
        filters : dict
            A dictionary whose keys are column names, and values are lists of string expressions that represent
            filter conditions on the column. AND is applied on the conditions. The condition formats are:
            'column_name eq some_value'
            default: None
        field_names : list
            A list of column names to be returned to the output table
            default: None
        queried_columns: list of pyArrow.column
            A list of the columns to be queried
            default: None

        Returns
        -------
        pyarrow.Table


        Examples
        --------
        table = query('some_bucket', 'some_schema', 'some_table',
                      filters={'name': ['eq Alice', 'eq Bob']}
                      field_names=['name','age'])

        """

        # create a transaction
        txid, created_txid = self._begin_tx_if_necessary(txid)
        executor_sessions = []
        try:
            # prepare query
            queried_columns, arrow_schema, query_data_request, executor_sessions = \
                self._prepare_query(bucket, schema, table, num_sub_splits, filters, field_names, response_row_id=response_row_id, txid=txid)

            # define the per split threaded query func
            def query_split_id(self, split_id):
                try:
                    start_row_ids = {i:0 for i in range(num_sub_splits)}
                    session = executor_sessions[split_id]
                    row_count = 0
                    while (self._more_pages_exist(start_row_ids) and
                           (not limit or row_count < limit)):
                        # check if killed externally
                        if killall:
                            raise RuntimeError(f'query_split_id: split_id {split_id} received killall')

                        # determine the limit rows
                        if limit:
                            limit_rows = min(limit_per_sub_split, limit-row_count)
                        else:
                            limit_rows = limit_per_sub_split

                        # query one page
                        table_page, start_row_ids = session._query_page(bucket=bucket, schema=schema, table=table, query_data_request=query_data_request,
                                                                        split=(split_id, num_splits, num_row_groups_per_sub_split),
                                                                        num_sub_splits=num_sub_splits, response_row_id=response_row_id,
                                                                        txid=txid, limit_rows=limit_rows,
                                                                        sub_split_start_row_ids=start_row_ids.items())
                        with lock:
                            table_pages.append(table_page)
                            row_counts[split_id] += len(table_page)
                            row_count = sum(row_counts)
                        _logger.info(f"query_split_id: table_pages split_id={split_id} row_count={row_count}")
                except Exception as e:
                    _logger.exception('query_split_id: exception occurred')
                    try:
                        self.rollback_transaction(txid)
                    except:
                        _logger.exception(f'failed to rollback txid {txid}')
                    raise e

            table_pages = []
            num_splits = len(executor_sessions)
            killall = False
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_splits) as executor:
                futures = []
                row_counts = [0] * num_splits
                lock = threading.Lock()
                for i in range(num_splits):
                    futures.append(executor.submit(query_split_id, self, i))
                for future in concurrent.futures.as_completed(futures):
                    future.result() # trigger an exception if occurred in any thread

            # commit if needed
            if created_txid:
                self.commit_transaction(txid)

            # concatenate all table pages and return result
            out_table = pa.concat_tables(table_pages)
            out_table = out_table.slice(length=limit) if limit else out_table
            _logger.info("query: out_table len=%s row_count=%s",
                          len(out_table), len(out_table))
            return out_table

        except Exception as e:
            _logger.exception('exception occurred')
            try:
                self.rollback_transaction(txid)
            except:
                _logger.exception(f'failed to rollback txid {txid}')
            raise e

        finally:
            killall = True
            for session in executor_sessions:
                try:
                    session.session.close()
                except Exception:
                    _logger.exception(f'failed to close session {session}')

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

        def iterate_over_import_data_response(response, expected_retvals):
            if response.status_code != 200:
                return response

            chunk_size = 1024
            for chunk in res.iter_content(chunk_size=chunk_size):
                chunk_dict = json.loads(chunk)
                _logger.info(f"import data chunk={chunk}, result: {chunk_dict['res']}")
                if chunk_dict['res'] in expected_retvals:
                    _logger.info(f"import finished with expected result={chunk_dict['res']}, error message: {chunk_dict['err_msg']}")
                    return response
                elif chunk_dict['res'] != 'Success' and chunk_dict['res'] != 'TabularInProgress':
                    raise TabularException(f"Received unexpected error in import_data. "
                                           f"status: {chunk_dict['res']}, error message: {chunk_dict['err_msg']}")
                _logger.info(f"import_data is in progress. status: {chunk_dict['res']}")
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
            res = iterate_over_import_data_response(res, expected_retvals)

        return self._check_res(res, "import_data", expected_retvals)

    def merge_data(self):
        """
        TODO

        POST /mybucket/myschema/mytable?data HTTP/1.1
        Content-Length: ContentLength
        tabular-txid: TransactionId
        tabular-client-tag: ClientTag

        Request Body
        {
          "format": "string",
          "select_source": "formatted data"
          "predicate": "formatted_data"
        }
        """
        pass

    def _record_batch_slices(self, batch, rows_per_slice=None):
        max_slice_size_in_bytes = int(0.9*5*1024*1024) # 0.9 * 5MB
        batch_len = len(batch)
        serialized_batch = serialize_record_batch(batch)
        batch_size_in_bytes = len(serialized_batch)
        _logger.info(f'max_slice_size_in_bytes={max_slice_size_in_bytes} batch_len={batch_len} batch_size_in_bytes={batch_size_in_bytes}')

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

                if sizeof_serialized_slice_batch <= max_slice_size_in_bytes or rows_per_slice < 10000:
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

    def insert(self, bucket, schema, table, rows=None, record_batch=None, rows_per_insert=None, txid=0):
        """
        Insert rows into a table. The operation may be split into multiple commands, such that by default no more than 512KB will be inserted per command.

        Parameters
        ----------
        bucket : string
            The bucket of the table.
        schema : string
            The schema of the table.
        table : string
            The table name.
        rows : dict
            The rows to insert.
            dictionary key: column name
            dictionary value: array of cell values to insert
            default: None (if None, record_batch must be provided)
        record_batch : pyarrow.RecordBatch
            A pyarrow RecordBatch
            default: None (if None, rows dictionary must be provided)
        rows_per_insert : integer
            Split the operation so that each insert command will be limited to this value
            default: None (will be selected automatically)
        txid : integer
            A transaction id. The transaction may be initiated before the insert, and be used to provide
            multiple ACID operations
            default: 0 (will be created by the api)

        Returns
        -------
        None


        Examples
        --------
        insert('some_bucket', 'some_schema', 'some_table', {'name': ['Alice','Bob'], 'age': [25,24]})

        """
        if (not rows and not record_batch) or (rows and record_batch):
            raise ValueError(f'insert: missing argument - either rows or record_batch must be provided')

        # create a transaction
        txid, created_txid = self._begin_tx_if_necessary(txid)

        if rows:
            columns = self._list_table_columns(bucket, schema, table, field_names=rows.keys(), txid=txid)
            columns_dict = dict([(column[0], column[1]) for column in columns])
            arrow_schema = pa.schema([])
            arrays = []
            for column_name, column_values in rows.items():
                column_type = columns_dict[column_name]
                field = pa.field(column_name, column_type)
                arrow_schema = arrow_schema.append(field)
                arrays.append(pa.array(column_values, column_type))
            record_batch = pa.record_batch(arrays, arrow_schema)

        # split the record batch into multiple slices
        serialized_slices = self._record_batch_slices(record_batch, rows_per_insert)
        _logger.info(f'inserting record batch using {len(serialized_slices)} slices')

        insert_queue = queue.Queue()

        [insert_queue.put(insert_rows_req) for insert_rows_req in serialized_slices]

        try:
            executor_sessions = [VastdbApi(self.executor_hosts[i], self.access_key, self.secret_key, self.username,
                                           self.password, self.port, self.secure, self.auth_type) for i in range(len(self.executor_hosts))]

            def insert_executor(self, split_id):

                try:
                    _logger.info(f'insert_executor split_id={split_id} starting')
                    session = executor_sessions[split_id]
                    num_inserts = 0
                    while not killall:
                        try:
                            insert_rows_req = insert_queue.get(block=False)
                        except queue.Empty:
                            break
                        session.insert_rows(bucket=bucket, schema=schema,
                                            table=table, record_batch=insert_rows_req, txid=txid)
                        num_inserts += 1
                    _logger.info(f'insert_executor split_id={split_id} num_inserts={num_inserts}')
                    if killall:
                        _logger.info('insert_executor killall=True')

                except Exception as e:
                    _logger.exception('insert_executor hit exception')
                    raise e

            num_splits = len(executor_sessions)
            killall = False
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_splits) as executor:
                futures = []
                for i in range(num_splits):
                    futures.append(executor.submit(insert_executor, self, i))
                for future in concurrent.futures.as_completed(futures):
                    future.result() # trigger an exception if occurred in any thread

            # commit if needed
            if created_txid:
                self.commit_transaction(txid)

        except Exception as e:
            _logger.exception('exception occurred')
            try:
                self.rollback_transaction(txid)
            except:
                _logger.exception(f'failed to rollback txid {txid}')
            raise e

        finally:
            killall = True
            for session in executor_sessions:
                try:
                    session.session.close()
                except Exception:
                    _logger.exception(f'failed to close session {session}')

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
#            _logger.info(f"stream_id={stream_id} (skipping)")
            continue

        if stream_id == TABULAR_QUERY_DATA_COMPLETED_STREAM_ID:
            # read the terminating end chunk from socket
            res = fileobj.read()
            _logger.info(f"stream_id={stream_id} res={res} (finish)")
            return

        if stream_id == TABULAR_QUERY_DATA_FAILED_STREAM_ID:
            # read the terminating end chunk from socket
            res = fileobj.read()
            _logger.info(f"stream_id={stream_id} res={res} (failed)")
            raise IOError(f"Query data stream failed res={res}")

        next_row_id_bytes = fileobj.read(8)
        next_row_id, = struct.unpack('<Q', next_row_id_bytes)
        _logger.info(f"stream_id={stream_id} next_row_id={next_row_id}")

        if stream_id not in readers:
            # we implicitly read 1st message (Arrow schema) when constructing RecordBatchStreamReader
            reader = pa.ipc.RecordBatchStreamReader(fileobj)
            _logger.info(f"stream_id={stream_id} schema={reader.schema}")
            readers[stream_id] = (reader, [])
            continue

        (reader, batches) = readers[stream_id]
        try:
            batch = reader.read_next_batch() # read single-column chunk data
            _logger.info(f"stream_id={stream_id} rows={len(batch)} chunk={batch}")
            batches.append(batch)
        except StopIteration:  # we got an end-of-stream IPC message for a given stream ID
            reader, batches = readers.pop(stream_id)  # end of column
            table = pa.Table.from_batches(batches)  # concatenate all column chunks (as a single)
            _logger.info(f"stream_id={stream_id} rows={len(table)} column={table}")
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

            _logger.info(f"stream_id={stream_id} rows={len(parsed_table)} next_row_id={next_row_id} table={parsed_table}")
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
    _logger.info(f"name={f.name}")
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

        _logger.info(f"added key and map to entries")
        children = [fb_field.End(builder)]

    if children is not None:
        fb_field.StartChildrenVector(builder, len(children))
        for offset in reversed(children):
            builder.PrependUOffsetTRelative(offset)
        children = builder.EndVector()

    col_name = builder.CreateString(name)
    field_type, field_type_type = get_field_type(builder, f)
    _logger.info(f"add col_name={name} type_type={field_type_type} to fb")
    fb_field.Start(builder)
    fb_field.AddName(builder, col_name)
    fb_field.AddTypeType(builder, field_type_type)
    fb_field.AddType(builder, field_type)
    if children is not None:
        _logger.info(f"add col_name={name} childern")
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


def build_query_data_request(schema: 'pa.Schema' = pa.schema([]), filters: dict = None, field_names: list = None):
    filters = filters or {}

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

    predicate = Predicate(schema, filters)
    filter_obj = predicate.serialize(builder)

    parser = QueryDataParser(schema)
    leaves_map = {}
    for node in parser.nodes:
        for descendent in node._iter_nodes():
            if descendent.parent and isinstance(descendent.parent.type, (pa.ListType, pa.MapType)):
                continue
            iter_from_root = reversed(list(descendent._iter_to_root()))
            descendent_full_name = '.'.join([n.field.name for n in iter_from_root])
            _logger.debug(f'build_query_data_request: descendent_full_name={descendent_full_name}')
            descendent_leaves = [leaf.index for leaf in descendent._iter_leaves()]
            leaves_map[descendent_full_name] = descendent_leaves
    _logger.debug(f'build_query_data_request: leaves_map={leaves_map}')

    output_field_names = None
    if field_names is None:
        field_names = [field.name for field in schema]
    else:
        output_field_names  = [f.split('.')[0] for f in field_names]
        # sort projected field_names according to positions to maintain ordering according to the schema
        def compare_field_names_by_pos(field_name1, field_name2):
            return leaves_map[field_name1][0]-leaves_map[field_name2][0]
        field_names = sorted(field_names, key=cmp_to_key(compare_field_names_by_pos))
    _logger.debug(f'build_query_data_request: sorted field_names={field_names} schema={schema}')

    projection_fields = []
    projection_positions = []
    for field_name in field_names:
        positions = leaves_map[field_name]
        _logger.info("projecting field=%s positions=%s", field_name, positions)
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
        _logger.info(f'changing resolution for {field_name} to us')
        new_column = table[field_name].cast(pa.timestamp('us'), safe=False)
        table = table.set_column(changing_index, field_name, new_column)
    for field_name, changing_index in indexes_of_fields_to_change.items():
        _logger.info(f'applying custom rules to {field_name}')
        new_column = table[field_name].to_pylist()
        new_column = list(map(column_matcher[field_name], new_column))
        new_column = pa.array(new_column, table[field_name].type)
        table = table.set_column(changing_index, field_name, new_column)
    return table
