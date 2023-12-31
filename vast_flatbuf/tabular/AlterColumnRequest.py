# automatically generated by the FlatBuffers compiler, do not modify

# namespace: tabular

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class AlterColumnRequest(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = AlterColumnRequest()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsAlterColumnRequest(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # AlterColumnRequest
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # AlterColumnRequest
    def Properties(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # AlterColumnRequest
    def Stats(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def Start(builder): builder.StartObject(2)
def AlterColumnRequestStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddProperties(builder, properties): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(properties), 0)
def AlterColumnRequestAddProperties(builder, properties):
    """This method is deprecated. Please switch to AddProperties."""
    return AddProperties(builder, properties)
def AddStats(builder, stats): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(stats), 0)
def AlterColumnRequestAddStats(builder, stats):
    """This method is deprecated. Please switch to AddStats."""
    return AddStats(builder, stats)
def End(builder): return builder.EndObject()
def AlterColumnRequestEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)