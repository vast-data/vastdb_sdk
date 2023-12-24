# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class IntervalLiteralMonths(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = IntervalLiteralMonths()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsIntervalLiteralMonths(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # IntervalLiteralMonths
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # IntervalLiteralMonths
    def Months(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

def Start(builder): builder.StartObject(1)
def IntervalLiteralMonthsStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddMonths(builder, months): builder.PrependInt32Slot(0, months, 0)
def IntervalLiteralMonthsAddMonths(builder, months):
    """This method is deprecated. Please switch to AddMonths."""
    return AddMonths(builder, months)
def End(builder): return builder.EndObject()
def IntervalLiteralMonthsEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)