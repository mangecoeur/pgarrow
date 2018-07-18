from builderlib cimport CBooleanBuilder, CPrimitiveBuilder, CAdaptiveIntBuilder, CInt64Builder, CDoubleBuilder, CTimestampBuilder

cdef class Int64Builder:
    cdef CInt64Builder* c_builder
    cpdef _append(self, long val)
    cpdef _append_null(self)
    cpdef append_bytes(self, bytes dat)
    cpdef finish(self)

cdef class DoubleBuilder:
    cdef CDoubleBuilder* c_builder
    cpdef _append(self, double val)
    cpdef _append_null(self)
    cpdef append_bytes(self, bytes dat)
    cpdef finish(self)
#
# cdef class TimestampBuilder:
#     cdef CTimestampBuilder* c_builder
#     cpdef _append(self, long val)
#     cpdef _append_null(self)
#     cpdef append_bytes(self, bytes dat)
#     cpdef finish(self)