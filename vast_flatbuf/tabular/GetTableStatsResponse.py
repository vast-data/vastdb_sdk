# automatically generated by the FlatBuffers compiler, do not modify

# namespace: tabular

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class GetTableStatsResponse(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = GetTableStatsResponse()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsGetTableStatsResponse(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # GetTableStatsResponse
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # GetTableStatsResponse
    def NumRows(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # GetTableStatsResponse
    def SizeInBytes(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # GetTableStatsResponse
    def IsExternalRowidAlloc(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return False

def Start(builder): builder.StartObject(3)
def GetTableStatsResponseStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddNumRows(builder, numRows): builder.PrependInt64Slot(0, numRows, 0)
def GetTableStatsResponseAddNumRows(builder, numRows):
    """This method is deprecated. Please switch to AddNumRows."""
    return AddNumRows(builder, numRows)
def AddSizeInBytes(builder, sizeInBytes): builder.PrependInt64Slot(1, sizeInBytes, 0)
def GetTableStatsResponseAddSizeInBytes(builder, sizeInBytes):
    """This method is deprecated. Please switch to AddSizeInBytes."""
    return AddSizeInBytes(builder, sizeInBytes)
def AddIsExternalRowidAlloc(builder, isExternalRowidAlloc): builder.PrependBoolSlot(2, isExternalRowidAlloc, 0)
def GetTableStatsResponseAddIsExternalRowidAlloc(builder, isExternalRowidAlloc):
    """This method is deprecated. Please switch to AddIsExternalRowidAlloc."""
    return AddIsExternalRowidAlloc(builder, isExternalRowidAlloc)
def End(builder): return builder.EndObject()
def GetTableStatsResponseEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)