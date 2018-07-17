from builder cimport CBooleanBuilder, CPrimitiveBuilder, CAdaptiveIntBuilder, CInt64Builder, CDoubleBuilder

cdef class Int64Builder:
    cdef CInt64Builder* c_builder
    cdef _append(self, long val)
    cdef _append_null(self)
    cpdef finish(self)

cdef class DoubleBuilder:
    cdef CDoubleBuilder* c_builder
    cdef _append(self, double val)
    cdef _append_null(self)
    cpdef finish(self)