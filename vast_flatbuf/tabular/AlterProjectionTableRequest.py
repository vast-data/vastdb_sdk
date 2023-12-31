# automatically generated by the FlatBuffers compiler, do not modify

# namespace: tabular

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class AlterProjectionTableRequest(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = AlterProjectionTableRequest()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsAlterProjectionTableRequest(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # AlterProjectionTableRequest
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # AlterProjectionTableRequest
    def NewName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # AlterProjectionTableRequest
    def Properties(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def Start(builder): builder.StartObject(2)
def AlterProjectionTableRequestStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddNewName(builder, newName): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(newName), 0)
def AlterProjectionTableRequestAddNewName(builder, newName):
    """This method is deprecated. Please switch to AddNewName."""
    return AddNewName(builder, newName)
def AddProperties(builder, properties): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(properties), 0)
def AlterProjectionTableRequestAddProperties(builder, properties):
    """This method is deprecated. Please switch to AddProperties."""
    return AddProperties(builder, properties)
def End(builder): return builder.EndObject()
def AlterProjectionTableRequestEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)