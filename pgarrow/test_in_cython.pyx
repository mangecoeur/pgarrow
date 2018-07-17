# distutils: language = c++

from builder cimport CInt64Builder
from libcpp.memory cimport shared_ptr

from pyarrow.lib cimport CArray, Type, CStatus, CMemoryPool
from pyarrow.lib cimport MemoryPool, maybe_unbox_memory_pool, check_status, pyarrow_wrap_array

# import pyarrow as pa
#
# postgres_arrow_type_map = {
#     'int4': pa.int32,
#     'int8': pa.int64,
#     'float4': pa.float32,
#     'float8': pa.float64,
#     'timestamp': pa.


cpdef dummy():
    print(Type._Type_NA)
    cdef CMemoryPool* pool = maybe_unbox_memory_pool(MemoryPool())

    cdef shared_ptr[CArray] id_array
    cdef CInt64Builder* b = new CInt64Builder(pool)

    b.Append(12)
    b.Append(12)
    b.AppendNull()
    b.Append(12)

    res = check_status(b.Finish(&id_array))
    a = pyarrow_wrap_array(id_array)
    return a


cdef builder_for_type():
    pass

cdef build_builders():
    field_types = ['int8', 'timestamp', 'float8', 'float8', 'float8', 'float8']


def run_dummy():
    print(dummy())