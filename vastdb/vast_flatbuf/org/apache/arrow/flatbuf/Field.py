# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# ----------------------------------------------------------------------
# A field represents a named column in a record / row batch or child of a
# nested type.
class Field(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Field()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsField(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Field
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Name is not required, in i.e. a List
    # Field
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Whether or not this field can contain nulls. Should be true in general.
    # Field
    def Nullable(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return False

    # Field
    def TypeType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # This is the type of the decoded value if the field is dictionary encoded.
    # Field
    def Type(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # Present only if the field is dictionary encoded.
    # Field
    def Dictionary(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from vastdb.vast_flatbuf.org.apache.arrow.flatbuf.DictionaryEncoding import DictionaryEncoding
            obj = DictionaryEncoding()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # children apply only to nested data types like Struct, List and Union. For
    # primitive types children will have length 0.
    # Field
    def Children(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.flatbuf.Field import Field
            obj = Field()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Field
    def ChildrenLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Field
    def ChildrenIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        return o == 0

    # User-defined metadata
    # Field
    def CustomMetadata(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from vastdb.vast_flatbuf.org.apache.arrow.flatbuf.KeyValue import KeyValue
            obj = KeyValue()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Field
    def CustomMetadataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Field
    def CustomMetadataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        return o == 0

def Start(builder): builder.StartObject(7)
def FieldStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddName(builder, name): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def FieldAddName(builder, name):
    """This method is deprecated. Please switch to AddName."""
    return AddName(builder, name)
def AddNullable(builder, nullable): builder.PrependBoolSlot(1, nullable, 0)
def FieldAddNullable(builder, nullable):
    """This method is deprecated. Please switch to AddNullable."""
    return AddNullable(builder, nullable)
def AddTypeType(builder, typeType): builder.PrependUint8Slot(2, typeType, 0)
def FieldAddTypeType(builder, typeType):
    """This method is deprecated. Please switch to AddTypeType."""
    return AddTypeType(builder, typeType)
def AddType(builder, type): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(type), 0)
def FieldAddType(builder, type):
    """This method is deprecated. Please switch to AddType."""
    return AddType(builder, type)
def AddDictionary(builder, dictionary): builder.PrependUOffsetTRelativeSlot(4, flatbuffers.number_types.UOffsetTFlags.py_type(dictionary), 0)
def FieldAddDictionary(builder, dictionary):
    """This method is deprecated. Please switch to AddDictionary."""
    return AddDictionary(builder, dictionary)
def AddChildren(builder, children): builder.PrependUOffsetTRelativeSlot(5, flatbuffers.number_types.UOffsetTFlags.py_type(children), 0)
def FieldAddChildren(builder, children):
    """This method is deprecated. Please switch to AddChildren."""
    return AddChildren(builder, children)
def StartChildrenVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def FieldStartChildrenVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartChildrenVector(builder, numElems)
def AddCustomMetadata(builder, customMetadata): builder.PrependUOffsetTRelativeSlot(6, flatbuffers.number_types.UOffsetTFlags.py_type(customMetadata), 0)
def FieldAddCustomMetadata(builder, customMetadata):
    """This method is deprecated. Please switch to AddCustomMetadata."""
    return AddCustomMetadata(builder, customMetadata)
def StartCustomMetadataVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def FieldStartCustomMetadataVector(builder, numElems):
    """This method is deprecated. Please switch to Start."""
    return StartCustomMetadataVector(builder, numElems)
def End(builder): return builder.EndObject()
def FieldEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)