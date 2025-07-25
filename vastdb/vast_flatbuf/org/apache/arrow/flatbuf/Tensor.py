# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Tensor(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Tensor()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsTensor(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Tensor
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Tensor
    def TypeType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # The type of data contained in a value cell. Currently only fixed-width
    # value types are supported, no strings or nested types
    # Tensor
    def Type(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # The dimensions of the tensor, optionally named
    # Tensor
    def Shape(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.flatbuf.TensorDim import TensorDim
            obj = TensorDim()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Tensor
    def ShapeLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Tensor
    def ShapeIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # Non-negative byte offsets to advance one value cell along each dimension
    # If omitted, default to row-major order (C-like).
    # Tensor
    def Strides(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int64Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 8))
        return 0

    # Tensor
    def StridesAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int64Flags, o)
        return 0

    # Tensor
    def StridesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Tensor
    def StridesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

    # The location and size of the tensor's data
    # Tensor
    def Data(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            x = o + self._tab.Pos
            from vastdb.vast_flatbuf.org.apache.arrow.flatbuf.Buffer import Buffer
            obj = Buffer()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def Start(builder): builder.StartObject(5)
def TensorStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddTypeType(builder, typeType): builder.PrependUint8Slot(0, typeType, 0)
def TensorAddTypeType(builder, typeType):
    """This method is deprecated. Please switch to AddTypeType."""
    return AddTypeType(builder, typeType)
def AddType(builder, type): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(type), 0)
def TensorAddType(builder, type):
    """This method is deprecated. Please switch to AddType."""
    return AddType(builder, type)
def AddShape(builder, shape): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(shape), 0)
def TensorAddShape(builder, shape):
    """This method is deprecated. Please switch to AddShape."""
    return AddShape(builder, shape)
def StartShapeVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def TensorStartShapeVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartShapeVector(builder, numElems)
def AddStrides(builder, strides): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(strides), 0)
def TensorAddStrides(builder, strides):
    """This method is deprecated. Please switch to AddStrides."""
    return AddStrides(builder, strides)
def StartStridesVector(builder, numElems): return builder.StartVector(8, numElems, 8)
def TensorStartStridesVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartStridesVector(builder, numElems)
def AddData(builder, data): builder.PrependStructSlot(4, flatbuffers.number_types.UOffsetTFlags.py_type(data), 0)
def TensorAddData(builder, data):
    """This method is deprecated. Please switch to AddData."""
    return AddData(builder, data)
def End(builder): return builder.EndObject()
def TensorEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)