# distutils: language = c++
from libcpp.memory cimport shared_ptr
from libcpp cimport bool
from libc.stdint cimport uint16_t, uint32_t, uint64_t, int16_t, int32_t, int64_t

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport CStatus, CArray, CMemoryPool, CDataType, CTimestampType
# from pyarrow.includes.libarrow cimport *



cdef extern from "arrow/builder.h" namespace "arrow" nogil:
    cdef cppclass CArrayBuilder" arrow::ArrayBuilder":
        CArrayBuilder()
        CArrayBuilder(const shared_ptr[CDataType], CMemoryPool*)
        CStatus Append(val)
        # CStatus AppendNull()
        CStatus Finish(shared_ptr[CArray]*)

    cdef cppclass CBooleanBuilder" arrow::BooleanBuilder"(CArrayBuilder):
        CBooleanBuilder(CMemoryPool*)
        CBooleanBuilder()
        CStatus Append(const bool)
        CStatus AppendNull()


    cdef cppclass CPrimitiveBuilder" arrow::PrimitiveBuilder"[CDataType](CArrayBuilder):
        CStatus AppendNull()

    cdef cppclass CNumericBuilder" arrow::NumericBuilder"[CDataType](CPrimitiveBuilder):
        CNumericBuilder(const shared_ptr[CDataType], CMemoryPool*)
        CNumericBuilder()
        CStatus Append(CDataType&)

    cdef cppclass CAdaptiveIntBuilder" arrow::AdaptiveIntBuilder"(CArrayBuilder):
        CAdaptiveIntBuilder(CMemoryPool*)
        CStatus Append(const uint64_t)

    cdef cppclass CUInt8Builder" arrow::UInt8Builder"(CNumericBuilder):
        CUInt8Builder(CMemoryPool*)
        CUInt8Builder()
        CStatus Append(CUInt8Type)

    cdef cppclass CUInt16Builder" arrow::UInt16Builder"(CNumericBuilder):
        CUInt16Builder(CMemoryPool*)
        CUInt16Builder()
        CStatus Append(uint16_t)


    cdef cppclass CUInt32Builder" arrow::UInt32Builder"(CNumericBuilder):
        CUInt32Builder(CMemoryPool*)
        CUInt32Builder()
        CStatus Append(uint32_t)

    cdef cppclass CUInt64Builder" arrow::UInt64Builder"(CNumericBuilder):
        CUInt64Builder(CMemoryPool*)
        CUInt64Builder()
        CStatus Append(uint64_t)


    cdef cppclass CInt8Builder" arrow::Int8Builder"(CNumericBuilder):
        CInt8Builder(CMemoryPool*)
        CInt8Builder()
        CStatus Append(CInt8Type)

    cdef cppclass CInt16Builder" arrow::Int16Builder"(CNumericBuilder):
        CInt16Builder(CMemoryPool*)
        CInt16Builder()
        CStatus Append(int16_t)

    cdef cppclass CInt32Builder" arrow::Int32Builder"(CNumericBuilder):
        CInt32Builder(CMemoryPool*)
        CInt32Builder()
        CStatus Append(int32_t)
    cdef cppclass CInt64Builder" arrow::Int64Builder"(CNumericBuilder):
        CInt64Builder(CMemoryPool*)
        CInt64Builder()
        CStatus Append(int64_t)

    cdef cppclass CHalfFloatBuilder" arrow::HalfFloatBuilder"(CNumericBuilder):
        CHalfFloatBuilder(CMemoryPool*)
        CHalfFloatBuilder()
        CStatus Append(CHalfFloatType)


    cdef cppclass CFloatBuilder" arrow::FloatBuilder"(CNumericBuilder):
        CFloatBuilder(CMemoryPool*)
        CFloatBuilder()
        CStatus Append(float)

    cdef cppclass CDoubleBuilder" arrow::DoubleBuilder"(CNumericBuilder):
        CDoubleBuilder(CMemoryPool*)
        CDoubleBuilder()
        CStatus Append(double)

    cdef cppclass CTimestampBuilder" arrow::TimestampBuilder"(CNumericBuilder):
        CTimestampBuilder(CMemoryPool*)
        CTimestampBuilder()
        CStatus Append(int64_t)

    cdef cppclass CTime32Builder" arrow::Time32Builder"(CNumericBuilder):
        CTime32Builder(CMemoryPool*)
        CTime32Builder()

    cdef cppclass CTime64Builder" arrow::Time64Builder"(CNumericBuilder):
        CTime64Builder(CMemoryPool*)
        CTime64Builder()

    cdef cppclass CDate32Builder" arrow::Date32Builder"(CNumericBuilder):
        CDate32Builder(CMemoryPool*)
        CDate32Builder()
        CStatus Append(CDate32Type)

    cdef cppclass CDate64Builder" arrow::Date64Builder"(CNumericBuilder):
        CDate64Builder(CMemoryPool*)
        CDate64Builder()
        CStatus Append(CDate64Type)


