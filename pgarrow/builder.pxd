from builderlib cimport CBooleanBuilder, CPrimitiveBuilder, CAdaptiveIntBuilder, CInt64Builder, CDoubleBuilder, CTimestampBuilder


cdef class AbstractBuilder:
    cdef _append_null(self)

    cpdef append_bytes(self, bytes dat)

    # cpdef finish(self)


cdef class Int64Builder(AbstractBuilder):
    cdef CInt64Builder* c_builder
    cdef _append(self, long val)
    cdef _append_null(self)
    cpdef append_bytes(self, bytes dat)
    cpdef finish(self)


cdef class DoubleBuilder(AbstractBuilder):
    cdef CDoubleBuilder* c_builder
    cdef _append(self, double val)
    cdef _append_null(self)
    cpdef append_bytes(self, bytes dat)
    cpdef finish(self)
#
# cdef class TimestampBuilder:
#     cdef CTimestampBuilder* c_builder
#     cpdef _append(self, long val)
#     cpdef _append_null(self)
#     cpdef append_bytes(self, bytes dat)
#     cpdef finish(self)