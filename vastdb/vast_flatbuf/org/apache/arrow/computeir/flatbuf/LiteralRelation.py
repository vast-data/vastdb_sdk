# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Literal relation
class LiteralRelation(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = LiteralRelation()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsLiteralRelation(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # LiteralRelation
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # An identifiier for the relation. The identifier should be unique over the
    # entire plan. Optional.
    # LiteralRelation
    def Id(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.RelId import RelId
            obj = RelId()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # The columns of this literal relation.
    # LiteralRelation
    def Columns(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.computeir.flatbuf.LiteralColumn import LiteralColumn
            obj = LiteralColumn()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # LiteralRelation
    def ColumnsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # LiteralRelation
    def ColumnsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

def Start(builder): builder.StartObject(2)
def LiteralRelationStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddId(builder, id): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(id), 0)
def LiteralRelationAddId(builder, id):
    """This method is deprecated. Please switch to AddId."""
    return AddId(builder, id)
def AddColumns(builder, columns): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(columns), 0)
def LiteralRelationAddColumns(builder, columns):
    """This method is deprecated. Please switch to AddColumns."""
    return AddColumns(builder, columns)
def StartColumnsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def LiteralRelationStartColumnsVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartColumnsVector(builder, numElems)
def End(builder): return builder.EndObject()
def LiteralRelationEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)