# # distutils: language=c++
# # cython: profile=True
# import six
# import numpy as np
#
# from libcpp.memory cimport shared_ptr, unique_ptr
# from hton cimport unpack_int16, unpack_int32, unpack_int64, unpack_float, unpack_double
#
# from pyarrow.lib cimport CArray, CStatus, CMemoryPool
# from pyarrow.lib cimport pyarrow_wrap_array, check_status, MemoryPool, maybe_unbox_memory_pool
# from builderlib cimport CArrayBuilder, CBooleanBuilder, CPrimitiveBuilder, CAdaptiveIntBuilder, CInt64Builder, CFloatBuilder, CDoubleBuilder, CTimestampBuilder
#
#
# cdef class AbstractBuilder:
#     cdef void append_null(self):
#         pass
#
#     cdef void append_bytes(self, char* dat) nogil:
#         pass
#
#     def append(self, val):
#         if val is None:
#             self.append_null()
#         else:
#             self.append(val)
#
# cdef class ArrayBuilder:
#     """
#     Builder class for Int64. This class exposes facilities for
#     incrementally adding values and building the null bitmap
#     for a pyarrow.Array (type='string').
#     """
#     cdef:
#         unique_ptr[CArrayBuilder] builder
#
#     def __cinit__(self, MemoryPool memory_pool=None):
#         cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
#         self.builder.reset(new ArrayBuilder(pool))
#
#     def append(self, value):
#         """
#         Append a single value to the builder. The value can either be a
#         string/bytes object or a null value (np.nan or None).
#
#         Parameters
#         ----------
#         value : string/bytes or np.nan/None
#             The value to append to the string array builder
#         """
#         if value is None or value is np.nan:
#             self.builder.get().AppendNull()
#         elif isinstance(value, (six.string_types, six.binary_type)):
#             self.builder.get().Append(value)
#         else:
#             raise TypeError('StringBuilder only accepts string objects')
#
#     def append_values(self, values):
#         """
#         Append all the values in an iterable to the string array builder
#         object.
#
#         Parameters
#         ----------
#         values : iterable of string/bytes or np.nan/None values
#             The values to append to the string array builder
#         """
#         for value in values:
#             self.append(value)
#
#     def finish(self):
#         """
#         Return result of builder as an Array object; also resets the builder.
#
#         Returns
#         -------
#         array : pyarrow.Array
#         """
#         cdef shared_ptr[CArray] out
#         with nogil:
#             self.builder.get().Finish(&out)
#         return pyarrow_wrap_array(out)
#
#
#
# # cdef class Int64Builder(AbstractBuilder):
# #     def __cinit__(self):
# #         self.c_builder = new CInt64Builder()
# #
# #     cdef _append(self, long val):
# #         self.c_builder.Append(val)
# #
# #     cdef  void append_null(self):
# #         self.c_builder.AppendNull()
# #
# #     cdef void  append_bytes(self, char* dat) nogil:
# #         cdef long field_dat = unpack_int64(dat)
# #         self.c_builder.Append(field_dat)
# #
# #     cpdef finish(self):
# #         cdef shared_ptr[CArray] out
# #         cdef int res = check_status(self.c_builder.Finish(&out))
# #         return pyarrow_wrap_array(out)
#
#
# cdef class Int64Builder:
#     """
#     Builder class for Int64. This class exposes facilities for
#     incrementally adding values and building the null bitmap
#     for a pyarrow.Array (type='string').
#     """
#     cdef:
#         unique_ptr[CInt64Builder] builder
#
#     def __cinit__(self, MemoryPool memory_pool=None):
#         cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
#         self.builder.reset(new Int64Builder(pool))
#
#     def append(self, value):
#         """
#         Append a single value to the builder. The value can either be a
#         string/bytes object or a null value (np.nan or None).
#
#         Parameters
#         ----------
#         value : string/bytes or np.nan/None
#             The value to append to the string array builder
#         """
#         if value is None or value is np.nan:
#             self.builder.get().AppendNull()
#         elif isinstance(value, (six.string_types, six.binary_type)):
#             self.builder.get().Append(value)
#         else:
#             raise TypeError('Int64Builder only accepts int objects')
#
#     def append_values(self, values):
#         """
#         Append all the values in an iterable to the string array builder
#         object.
#
#         Parameters
#         ----------
#         values : iterable of string/bytes or np.nan/None values
#             The values to append to the string array builder
#         """
#         for value in values:
#             self.append(value)
#
#     def finish(self):
#         """
#         Return result of builder as an Array object; also resets the builder.
#
#         Returns
#         -------
#         array : pyarrow.Array
#         """
#         cdef shared_ptr[CArray] out
#         with nogil:
#             self.builder.get().Finish(&out)
#         return pyarrow_wrap_array(out)
#
#     @property
#     def null_count(self):
#         return self.builder.get().null_count()
#
#     def __len__(self):
#         return self.builder.get().length()
#
#
# cdef class FloatBuilder(AbstractBuilder):
#     def __cinit__(self):
#         self.c_builder = new CFloatBuilder()
#
#     cdef _append(self, float val):
#         self.c_builder.Append(val)
#
#     cdef void  append_null(self):
#         self.c_builder.AppendNull()
#
#     cdef void  append_bytes(self, char* dat) nogil:
#         cdef float field_dat = unpack_float(dat)
#         self.c_builder.Append(field_dat)
#
#
#     cpdef finish(self):
#         cdef shared_ptr[CArray] id_array
#         cdef int res = check_status(self.c_builder.Finish(&id_array))
#         return pyarrow_wrap_array(id_array)
#
#
# cdef class DoubleBuilder(AbstractBuilder):
#     def __cinit__(self):
#         self.c_builder = new CDoubleBuilder()
#
#     cdef _append(self, double val):
#         self.c_builder.Append(val)
#
#     cdef void  append_null(self):
#         self.c_builder.AppendNull()
#
#     cdef void  append_bytes(self, char* dat) nogil:
#         cdef double field_dat = unpack_double(dat)
#         self.c_builder.Append(field_dat)
#
#
#     cpdef finish(self):
#         cdef shared_ptr[CArray] id_array
#         cdef int res = check_status(self.c_builder.Finish(&id_array))
#         return pyarrow_wrap_array(id_array)
#
#
# # cdef class TimestampBuilder:
# #     def __cinit__(self):
# #         self.c_builder = new CTimestampBuilder()
# #
# #     cdef _append(self, long val):
# #         self.c_builder.Append(val)
# #
# #     cdef void  append_null(self):
# #         self.c_builder.AppendNull()
# #
# #     cdef void  append_bytes(self, char* dat) nogil:
# #         cdef double field_dat = unpack_int64(dat)
# #         self.c_builder.Append(field_dat)
# #
# #     cpdef finish(self):
# #         cdef shared_ptr[CArray] id_array
# #         cdef int res = check_status(self.c_builder.Finish(&id_array))
# #         # TODO check return value and andle
# #         return pyarrow_wrap_array(id_array)
#
#
# # cdef class Date64Builder(AbstractBuilder):
# #     def __cinit__(self):
# #         self.c_builder = new CInt64Builder()
# #
# #     cdef _append(self, long val):
# #         self.c_builder.Append(val)
# #
# #     cdef  void append_null(self):
# #         self.c_builder.AppendNull()
# #
# #     cdef void  append_bytes(self, char* dat) nogil:
# #         cdef long field_dat = unpack_int64(dat)
# #         self.c_builder.Append(field_dat)
# #
# #     cpdef finish(self):
# #         cdef shared_ptr[CArray] id_array
# #         cdef int res = check_status(self.c_builder.Finish(&id_array))
# #         return pyarrow_wrap_array(id_array)
#
#
#
# #
# #         CArrayBuilder(const shared_ptr[CDataType], CMemoryPool*)
# #         CArrayBuilder()
# #         CStatus Append(val)
# #         CStatus AppendNull()
# #         CStatus Finish(shared_ptr[CArray]*)
# #
# #     cdef cppclass CBooleanBuilder" arrow::PrimitiveBuilder"(CArrayBuilder):
# #         CBooleanBuilder(CMemoryPool*)
# #         CBooleanBuilder()
# #         CStatus Append(const bool)
# #         CStatus AppendNull()
# #
# #
# #     cdef cppclass CPrimitiveBuilder" arrow::PrimitiveBuilder"(CArrayBuilder):
# #         CStatus AppendNull()
# #
# #     cdef cppclass CNumericBuilder" arrow::NumericBuilder"[CDataType](CPrimitiveBuilder):
# #         CNumericBuilder(const shared_ptr[CDataType], CMemoryPool*)
# #         CNumericBuilder()
# #         CStatus Append(CDataType&)
# #
# #     cdef cppclass CAdaptiveIntBuilder" arrow::AdaptiveIntBuilder"(CArrayBuilder):
# #         CAdaptiveIntBuilder(CMemoryPool*)
# #         CStatus Append(const uint64_t)
# #
# #     cdef cppclass CUInt8Builder" arrow::UInt8Builder"(CNumericBuilder):
# #         CUInt8Builder(CMemoryPool*)
# #         CUInt8Builder()
# #
# #     cdef cppclass CUInt16Builder" arrow::UInt16Builder"(CNumericBuilder):
# #         CUInt16Builder(CMemoryPool*)
# #         CUInt16Builder()
# #         CStatus Append(uint16_t)
# #
# #
# #     cdef cppclass CUInt32Builder" arrow::UInt32Builder"(CNumericBuilder):
# #         CUInt32Builder(CMemoryPool*)
# #         CUInt32Builder()
# #         CStatus Append(uint32_t)
# #
# #     cdef cppclass CUInt64Builder" arrow::UInt64Builder"(CNumericBuilder):
# #         CUInt64Builder(CMemoryPool*)
# #         CUInt64Builder()
# #         CStatus Append(uint64_t)
# #
# #
# #     cdef cppclass CInt8Builder" arrow::Int8Builder"(CNumericBuilder):
# #         CInt8Builder(CMemoryPool*)
# #         CInt8Builder()
# #     cdef cppclass CInt16Builder" arrow::Int16Builder"(CNumericBuilder):
# #         CInt16Builder(CMemoryPool*)
# #         CInt16Builder()
# #         CStatus Append(int16_t)
# #
# #     cdef cppclass CInt32Builder" arrow::Int32Builder"(CNumericBuilder):
# #         CInt32Builder(CMemoryPool*)
# #         CInt32Builder()
# #         CStatus Append(int32_t)
# #     cdef cppclass CInt64Builder" arrow::Int64Builder"(CNumericBuilder):
# #         CInt64Builder(CMemoryPool*)
# #         CInt64Builder()
# #         CStatus Append(int64_t)
# #
# #
# #     cdef cppclass CTimestampBuilder" arrow::TimestampBuilder"(CNumericBuilder):
# #         pass
# #
# #     cdef cppclass CTime32Builder" arrow::Time32Builder"(CNumericBuilder):
# #         pass
# #
# #     cdef cppclass CTime64Builder" arrow::Time64Builder"(CNumericBuilder):
# #         pass
# #
# #     cdef cppclass CDate32Builder" arrow::Date32Builder"(CNumericBuilder):
# #         pass
# #     cdef cppclass CDate64Builder" arrow::Date64Builder"(CNumericBuilder):
# #         CDate64Builder(CMemoryPool*)
# #         CDate64Builder()
# #
# #     cdef cppclass CHalfFloatBuilder" arrow::HalfFloatBuilder"(CNumericBuilder):
# #         CHalfFloatBuilder(CMemoryPool*)
# #         CHalfFloatBuilder()
# #     cdef cppclass CFloatBuilder" arrow::FloatBuilder"(CNumericBuilder):
# #         CFloatBuilder(CMemoryPool*)
# #         CFloatBuilder()
# #         CStatus Append(float)
# #     cdef cppclass CDoubleBuilder" arrow::DoubleBuilder"(CNumericBuilder):
# #         CDoubleBuilder(CMemoryPool*)
# #         CDoubleBuilder()
# #         CStatus Append(double)