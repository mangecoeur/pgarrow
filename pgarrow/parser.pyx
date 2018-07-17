# distutils: language=c++


# TODO a lot of this is overkill for MVP  -do better wrap of builder types and use the wrapped ones, let Cython deal with the rest.

from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
# cimport hton

from hton cimport unpack_int16, unpack_int32, unpack_int64, unpack_float, unpack_double
import datetime

import pyarrow as pa

from builder cimport (CArrayBuilder, CInt32Builder, CInt64Builder,
CFloatBuilder, CDoubleBuilder, CAdaptiveIntBuilder,
CBooleanBuilder, CPrimitiveBuilder,
# CTimestampBuilder,
)

from builderlib cimport DoubleBuilder, Int64Builder

cimport pyarrow.lib as palib
from pyarrow.lib cimport *
from pyarrow.lib cimport CTable, CColumn, CField
from pyarrow.lib cimport Table, Column, Field, DataType
from pyarrow.lib cimport check_status, pyarrow_unwrap_schema, pyarrow_unwrap_field, pyarrow_wrap_column, pyarrow_wrap_table


include "typemap.pxi"
# include "protocol/pgtypes.pxi"


cdef get_feild_oids(field_types):
    tmap = {v: k for k, v in TYPEMAP.items()}
    return [tmap[t] for t in field_types]


cdef make_array_builder(DataType patyp,bool strings_as_dictionary, bool adaptive_integers):
    if patyp == pa.float64():
        return DoubleBuilder()
    elif patyp == pa.int64():
        return Int64Builder()
    # elif patyp == palib.int32():
    #     return unique_ptr[CArrayBuilder](new CInt32Builder())
    # elif patyp == palib.bool_():
    #     return unique_ptr[CArrayBuilder](new CBooleanBuilder())


cdef prepare_column_builders(field_types):
    # TODO need to enforce that these are never None
    return [make_array_builder(typ, False, False) for typ in field_types]


cdef shared_ptr[CArray] finish_builder(unique_ptr[CArrayBuilder] builder):
    cdef shared_ptr[CArray] id_array
    res = check_status(builder.get()[0].Finish(&id_array))
    return id_array

cdef shared_ptr[CColumn] builder_to_column(unique_ptr[CArrayBuilder] builder, shared_ptr[CField] field):
    cdef shared_ptr[CArray] id_array
    res = check_status(builder.get()[0].Finish(&id_array))
    cdef shared_ptr[CColumn] col = shared_ptr[CColumn](new CColumn(field, id_array))
    return col




# TODO probably want to prepare the fields somewhere else, at the start of the process
cdef columns_to_arrow_table(columns, field_names, field_types):
    # TODO could put more of this in C++
    fields = [pa.field(n, t) for n, t in zip(field_names, field_types)]
    pa_schema = pa.schema(fields)
    data = []
    for i in range(len(columns)):
        field = fields[i]
        column_builder = columns[i]
        data.append(column_builder.finish())

    return Table.from_arrays(data, field_names)


pg_epoch_datetime = datetime.datetime(2000, 1, 1)

# TODO rather decode directly to arrow datetime format
def decode_timestamp(ts):
    seconds = (ts / 1000000)
    microseconds = (ts % 1000000)

    return pg_epoch_datetime.__add__(datetime.timedelta(0, seconds, microseconds))


cdef parse_header(buffer):
    intro = buffer.read(11)
    # TODO should check PGCOPY intro for errors
    flags = unpack_int32(buffer.read(4))
    header_extension = unpack_int32(buffer.read(4))
    return buffer


cdef read_tuple(buffer, field_types, columns):
    # Field count
    cdef int16_t n_fields = unpack_int16(buffer.read(2))
    if n_fields == -1:
        # End reached
        return False

    cdef int32_t len_field = 0
    cdef DataType pgtyp
    # cdef int i = 0

    for i in range(n_fields):
        len_field = unpack_int32(buffer.read(4))
        if len_field == -1:
            # Null field
            columns[i]._append_null()
            continue

        field_dat = buffer.read(len_field)

        pgtyp = field_types[i]

        # TODO get rid of call into pure-python world for type code (map to type IDs?)
        if pgtyp == pa.int16():
            field_dat =  unpack_int16(field_dat)

        elif pgtyp == pa.int32():
            field_dat =  unpack_int32(field_dat)
        elif pgtyp == pa.int64():
            field_dat =  unpack_int64(field_dat)

        elif pgtyp == pa.float32():
            field_dat =  unpack_float(field_dat)

        elif pgtyp == pa.float64():
            field_dat =  unpack_double(field_dat)

        elif pgtyp == pa.timestamp('ns'):
            field_dat =  unpack_double(field_dat)

        # FIXME: would need to cast to right kind of arraybuilder?
        columns[i]._append(field_dat)

    return True



cdef process_buffer(buffer, field_types, column_builders):
    parse_header(buffer)
    cont = True
    while cont:
        cont = read_tuple(buffer, field_types, column_builders)

# TODO:
# - Generate a list a builders matching the field types. note that since we are using Cython mapped
# C++ buildings, without Python wrappers, not sure we can stick them in a list - maybe need
# Something like a list of pointers (or unique_ptr)
# - Convert the IO to use pyarrow buffer lib (C versions). Want to check how this buffering works,
# are we forced to read everyting from the PG stream or can we just read the bit we want and
# let the system decide how to queue everyting.
#
# cdef c_read_pg_file(filename, field_names, field_types):
#     cdef vector[unique_ptr[CArrayBuilder]] column_builders = prepare_column_builders(field_types)
#
#     field_oids = get_feild_oids(field_types)
#
#     with open(filename, 'rb') as buffer:
#         process_buffer(buffer, field_oids, column_builders)
#
#     return c_columns_to_arrow_table(column_builders, field_names, field_types)

cdef _read_pg_file(filename, field_names, field_types):
    column_builders = prepare_column_builders(field_types)

    # field_oids = get_feild_oids(field_types)

    with open(filename, 'rb') as buffer:
        process_buffer(buffer, field_types, column_builders)

    return columns_to_arrow_table(column_builders, field_names, field_types)

def read_pg_file(filename, field_names, field_types):
    return _read_pg_file(filename, field_names, field_types)

#
#
# cdef vector[unique_ptr[CArrayBuilder]] c_prepare_column_builders(field_types):
#     cdef vector[unique_ptr[CArrayBuilder]] builders
#     for typ in field_types:
#         builders.push_back(c_make_array_builder(typ, False, False))
#     return builders
#
#
#
# # TODO maybe use DataType?
# cdef unique_ptr[CArrayBuilder] c_make_array_builder(DataType patyp,
#                                                   bool strings_as_dictionary, bool adaptive_integers):
#
#     if patyp == pa.float64():
#         return unique_ptr[CArrayBuilder](new CDoubleBuilder())
#     elif patyp == pa.int64():
#         return unique_ptr[CArrayBuilder](new CInt64Builder())
#     elif patyp == pa.int32():
#         return unique_ptr[CArrayBuilder](new CInt32Builder())
#     elif patyp == pa.bool_():
#         return unique_ptr[CArrayBuilder](new CBooleanBuilder())
#     # elif isinstance(patyp, pa.TimestampType):
#     #     return unique_ptr[CArrayBuilder](new CTimestampBuilder())
#             #     return unique_ptr[CArrayBuilder](new CInt64Builder())    elif patyp == pa.int64():
#         # if (adaptive_integers) :
#         #     return unique_ptr[CArrayBuilder](new CAdaptiveIntBuilder())
#         # else :
#         #     return unique_ptr[CArrayBuilder](new CInt64Builder())
#     # return unique_ptr[TimestampBuilder](new TimestampBuilder(arrow::timestamp(TimeUnit::MICRO), default_memory_pool()))
#     # elif pgtyp == TIMESTAMPOID:
#     #     return unique_ptr[CDate32Builder](new CDate32Builder())
#     # else:
#     #     if strings_as_dictionary:
#     #         return unique_ptr[StringDictionaryBuilder](new StringDictionaryBuilder(::arrow::utf8(), ::arrow::default_memory_pool()))
#     #     else :
#     #         return unique_ptr<StringBuilder>(new StringBuilder())
#
#
# # TODO probably want to prepare the fields somewhere else, at the start of the process
# cdef c_columns_to_arrow_table(vector[unique_ptr[CArrayBuilder]] columns, field_names, field_types):
#     # TODO could put more of this in C++
#     fields = [pa.field(n, t) for n, t in zip(field_names, field_types)]
#     pa_schema = pa.schema(fields)
#
#     cdef shared_ptr[CSchema] cpa_schema = pyarrow_unwrap_schema(pa_schema)
#     cdef const vector[shared_ptr[CColumn]] data
#     cdef shared_ptr[CColumn] col
#     cdef shared_ptr[CField] field
#     cdef unique_ptr[CArrayBuilder] column_builder
#
#     for i in range(columns.size()):
#         # TODO put this in a field building function somewhere else
#         field = pyarrow_unwrap_field(fields[i])
#         # column_builder = columns[i]
#         col = builder_to_column(columns[i], field)
#
#         data.push_back( col )
#     table = CTable.Make(cpa_schema, data)
#     return pyarrow_wrap_table(table)
#
#
# # TODO to avoid branch condition in 'Hot' code, could pre-set this by
# # supplying columns as set of 'codec' objects which are thin wrappers around
# # the array builders and already know internally about the pgtype->arrow type conversion
# # Since we already know the type sequence we don't then need to check the type field for every
# # tuple read.
# cdef c_read_tuple(buffer, field_oids, vector[unique_ptr[CArrayBuilder]] columns):
#     # Field count
#     cdef int16_t n_fields = hton.unpack_int16(buffer.read(2))
#     if n_fields == -1:
#         # End reached
#         return False
#
#     cdef int32_t len_field = 0
#     cdef int pgtyp = 0
#     cdef int i = 0
#
#     for i in range(n_fields):
#         len_field = hton.unpack_int32(buffer.read(4))
#         if len_field == -1:
#             # Null field
#             # How to add NaN value? -> might need to parse
#             col = <CPrimitiveBuilder> columns[i].get()
#             col.AppendNull()
#             continue
#
#         field_dat = buffer.read(len_field)
#
#         pgtyp = field_oids[i]
#
#         if pgtyp == INT4OID:
#             field_dat =  hton.unpack_int16(field_dat)
#
#         elif pgtyp == INT8OID:
#             field_dat =  hton.unpack_int32(field_dat)
#
#         elif pgtyp == FLOAT4OID:
#             field_dat =  hton.unpack_float(field_dat)
#
#         elif pgtyp == FLOAT8OID:
#             field_dat =  hton.unpack_double(field_dat)
#
#         elif pgtyp == TIMESTAMPOID:
#             field_dat =  hton.unpack_double(field_dat)
#
#         # FIXME: would need to cast to right kind of arraybuilder?
#         columns[i].get().Append(field_dat)
#
#     return True