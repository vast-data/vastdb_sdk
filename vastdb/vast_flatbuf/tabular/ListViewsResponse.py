# automatically generated by the FlatBuffers compiler, do not modify

# namespace: tabular

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class ListViewsResponse(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ListViewsResponse()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsListViewsResponse(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # ListViewsResponse
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ListViewsResponse
    def BucketName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # ListViewsResponse
    def SchemaName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # ListViewsResponse
    def Views(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.tabular.ObjectDetails import ObjectDetails
            obj = ObjectDetails()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # ListViewsResponse
    def ViewsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # ListViewsResponse
    def ViewsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

def Start(builder): builder.StartObject(3)
def ListViewsResponseStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddBucketName(builder, bucketName): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(bucketName), 0)
def ListViewsResponseAddBucketName(builder, bucketName):
    """This method is deprecated. Please switch to AddBucketName."""
    return AddBucketName(builder, bucketName)
def AddSchemaName(builder, schemaName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(schemaName), 0)
def ListViewsResponseAddSchemaName(builder, schemaName):
    """This method is deprecated. Please switch to AddSchemaName."""
    return AddSchemaName(builder, schemaName)
def AddViews(builder, views): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(views), 0)
def ListViewsResponseAddViews(builder, views):
    """This method is deprecated. Please switch to AddViews."""
    return AddViews(builder, views)
def StartViewsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def ListViewsResponseStartViewsVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartViewsVector(builder, numElems)
def End(builder): return builder.EndObject()
def ListViewsResponseEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)