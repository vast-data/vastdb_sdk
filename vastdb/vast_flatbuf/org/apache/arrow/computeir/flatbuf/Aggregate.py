# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Aggregate operation
class Aggregate(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Aggregate()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsAggregate(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Aggregate
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # An identifiier for the relation. The identifier should be unique over the
    # entire plan. Optional.
    # Aggregate
    def Id(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.RelId import RelId
            obj = RelId()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Child relation
    # Aggregate
    def Rel(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.Relation import Relation
            obj = Relation()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Expressions which will be evaluated to produce to
    # the rows of the aggregate relation's output.
    # Aggregate
    def Measures(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.Expression import Expression
            obj = Expression()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Aggregate
    def MeasuresLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Aggregate
    def MeasuresIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # Keys by which `aggregations` will be grouped.
    #
    # The nested list here is to support grouping sets
    # eg
    #
    # SELECT a, b, c, sum(d)
    # FROM t
    # GROUP BY
    #   GROUPING SETS (
    #     (a, b, c),
    #     (a, b),
    #     (a),
    #     ()
    #   );
    # Aggregate
    def Groupings(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.Grouping import Grouping
            obj = Grouping()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Aggregate
    def GroupingsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Aggregate
    def GroupingsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

def Start(builder): builder.StartObject(4)
def AggregateStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddId(builder, id): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(id), 0)
def AggregateAddId(builder, id):
    """This method is deprecated. Please switch to AddId."""
    return AddId(builder, id)
def AddRel(builder, rel): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(rel), 0)
def AggregateAddRel(builder, rel):
    """This method is deprecated. Please switch to AddRel."""
    return AddRel(builder, rel)
def AddMeasures(builder, measures): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(measures), 0)
def AggregateAddMeasures(builder, measures):
    """This method is deprecated. Please switch to AddMeasures."""
    return AddMeasures(builder, measures)
def StartMeasuresVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def AggregateStartMeasuresVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartMeasuresVector(builder, numElems)
def AddGroupings(builder, groupings): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(groupings), 0)
def AggregateAddGroupings(builder, groupings):
    """This method is deprecated. Please switch to AddGroupings."""
    return AddGroupings(builder, groupings)
def StartGroupingsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def AggregateStartGroupingsVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartGroupingsVector(builder, numElems)
def End(builder): return builder.EndObject()
def AggregateEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)