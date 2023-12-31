# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class IntervalLiteral(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = IntervalLiteral()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsIntervalLiteral(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # IntervalLiteral
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # IntervalLiteral
    def ValueType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # IntervalLiteral
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

def Start(builder): builder.StartObject(2)
def IntervalLiteralStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddValueType(builder, valueType): builder.PrependUint8Slot(0, valueType, 0)
def IntervalLiteralAddValueType(builder, valueType):
    """This method is deprecated. Please switch to AddValueType."""
    return AddValueType(builder, valueType)
def AddValue(builder, value): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)
def IntervalLiteralAddValue(builder, value):
    """This method is deprecated. Please switch to AddValue."""
    return AddValue(builder, value)
def End(builder): return builder.EndObject()
def IntervalLiteralEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)