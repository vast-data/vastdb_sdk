# automatically generated by the FlatBuffers compiler, do not modify

# namespace: tabular

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class VipRange(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = VipRange()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsVipRange(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # VipRange
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # VipRange
    def StartAddress(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # VipRange
    def AddressCount(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint16Flags, o + self._tab.Pos)
        return 0

def Start(builder): builder.StartObject(2)
def VipRangeStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddStartAddress(builder, startAddress): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(startAddress), 0)
def VipRangeAddStartAddress(builder, startAddress):
    """This method is deprecated. Please switch to AddStartAddress."""
    return AddStartAddress(builder, startAddress)
def AddAddressCount(builder, addressCount): builder.PrependUint16Slot(1, addressCount, 0)
def VipRangeAddAddressCount(builder, addressCount):
    """This method is deprecated. Please switch to AddAddressCount."""
    return AddAddressCount(builder, addressCount)
def End(builder): return builder.EndObject()
def VipRangeEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)