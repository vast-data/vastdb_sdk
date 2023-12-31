# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class DecimalLiteral(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = DecimalLiteral()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsDecimalLiteral(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # DecimalLiteral
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Bytes of a Decimal value; bytes must be in little-endian order.
    # DecimalLiteral
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int8Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # DecimalLiteral
    def ValueAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int8Flags, o)
        return 0

    # DecimalLiteral
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # DecimalLiteral
    def ValueIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

def Start(builder): builder.StartObject(1)
def DecimalLiteralStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddValue(builder, value): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)
def DecimalLiteralAddValue(builder, value):
    """This method is deprecated. Please switch to AddValue."""
    return AddValue(builder, value)
def StartValueVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def DecimalLiteralStartValueVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartValueVector(builder, numElems)
def End(builder): return builder.EndObject()
def DecimalLiteralEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)