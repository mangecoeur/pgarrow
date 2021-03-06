from builderlib cimport CBooleanBuilder, CPrimitiveBuilder, CAdaptiveIntBuilder, CInt64Builder, CFloatBuilder, CDoubleBuilder, CTimestampBuilder

#
# cdef class AbstractBuilder:
#     cdef  void append_null(self)
#
#     cdef  void append_bytes(self, char* dat) nogil
#
#     # cpdef finish(self)
#
#
# cdef class Int64Builder:
#     cdef CInt64Builder* c_builder
#     cdef _append(self, long val)
#     cdef void  append_null(self)
#     cdef  void append_bytes(self, char* dat) nogil
#     cpdef finish(self)
#
#
# cdef class FloatBuilder(AbstractBuilder):
#     cdef CFloatBuilder* c_builder
#     cdef _append(self, float val)
#     cdef  void append_null(self)
#     cdef  void append_bytes(self, char* dat) nogil
#     cpdef finish(self)
#
# cdef class DoubleBuilder(AbstractBuilder):
#     cdef CDoubleBuilder* c_builder
#     cdef _append(self, double val)
#     cdef  void append_null(self)
#     cdef  void append_bytes(self, char* dat) nogil
#     cpdef finish(self)

# cdef class TimestampBuilder(AbstractBuilder):
#     cdef CTimestampBuilder* c_builder
#     cdef _append(self, long val)
#     cdef void  append_null(self)
#     cdef  void append_bytes(self, char* dat) nogil
#     cpdef finish(self)