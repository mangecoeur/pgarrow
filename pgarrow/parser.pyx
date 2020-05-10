# distutils: language=c++
# cython: infer_types=True
# cython: profile=True

import io
from libcpp cimport bool
from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector

from cpython cimport array
import array

from hton cimport unpack_int16, unpack_int32, unpack_int64, unpack_float, unpack_double
import datetime

import pyarrow as pa
cimport pyarrow.lib as palib

# from pyarrow.lib cimport CArray, CTable, CColumn, CField, CDataType, NativeFile
# from pyarrow.lib cimport Table, Column, Field, DataType, Type
# from pyarrow.lib cimport check_status, pyarrow_unwrap_schema, pyarrow_unwrap_field, pyarrow_wrap_column, pyarrow_wrap_table, pyarrow_unwrap_data_type

from pyarrow.lib cimport *

# from builder cimport AbstractBuilder, DoubleBuilder, Int64Builder



include "typemap.pxi"


cdef dict PG_PA_TYPEMAP = {
    BOOLOID: pa.bool_(),
    INT2OID: pa.int16(),
    INT4OID: pa.int32(),
    INT8OID: pa.int64(),
    FLOAT4OID: pa.float32(),
    FLOAT8OID: pa.float64(),
    TIMESTAMPOID: pa.timestamp('ns')

}


cdef dict PA_PY_TYPEMAP = {
    pa.int64(): 'l',
    pa.float64(): 'd',
    pa.timestamp('ns'): 'd'

}


cdef get_pg_oids(field_types):
    """
    Convert text field types to PG field OIDs
    :param field_types: 
    :return: 
    """
    tmap = {v: k for k, v in TYPEMAP.items()}
    return [tmap[t] for t in field_types]

# cdef make_array_builder_inttype(patyp):
#     if patyp == Type._Type_DOUBLE:
#         return DoubleBuilder()
#     elif patyp == Type._Type_INT64:
#         return Int64Builder()
#     elif patyp == Type._Type_TIMESTAMP:
#         return DoubleBuilder()

cdef make_array_builder(DataType patyp):
    pytype = PA_PY_TYPEMAP[patyp]
    cdef array.array a = array.array(pytype)

    return a
    # if patyp == pa.float64():
    #     return DoubleBuilder()
    # elif patyp == pa.int64():
    #     return Int64Builder()
    # elif patyp == pa.timestamp('ns'):
    #     return Int64Builder()


cdef prepare_column_builders(field_types):
    # TODO need to enforce that these are never None
    return [make_array_builder(typ) for typ in field_types]


cdef columns_to_arrow_table(columns, fields, data_types, n_rows):
    data = []
    for i in range(len(columns)):
        dtype = data_types[i]
        field = fields[i]
        column_builder = columns[i]
        data.append(pa.array(column_builder))
        # data.append(column_builder.finish())

    return Table.from_arrays(data, [f.name for f in fields])


cdef parse_header(buffer):
    intro = buffer.read(11)
    # TODO should check PGCOPY intro for errors
    flags = unpack_int32(buffer.read(4))
    header_extension = unpack_int32(buffer.read(4))
    return buffer

# TODO compare using builders to python lists that cpython will optimize
# cdef bool read_tuple(buffer, column_builders):
#     # Field count
#     cdef int16_t n_fields = unpack_int16(buffer.read(2))
#     if n_fields == -1:
#         # End reached
#         return False
#
#     cdef int32_t len_field = 0
#     cdef AbstractBuilder builder
#
#     cdef bytes field_dat
#
#     for i in range(n_fields):
#         len_field = unpack_int32(buffer.read(4))
#
#         builder = column_builders[i]
#         if len_field == -1:
#             # Null field
#             builder.append_null()
#             continue
#
#         field_dat = buffer.read(len_field)
#         builder.append_bytes(field_dat)
#
#     return True


cdef pg_bswap64(x):
    return (
        ((x << 56) & 0xff00000000000000) |
        ((x << 40) & 0x00ff000000000000) |
        ((x << 24) & 0x0000ff0000000000) |
        ((x << 8) & 0x000000ff00000000) |
        ((x >> 8) & 0x00000000ff000000) |
        ((x >> 24) & 0x0000000000ff0000) |
        ((x >> 40) & 0x000000000000ff00) |
        ((x >> 56) & 0x00000000000000ff)
    )


cdef bool read_tuple(buffer, column_builders):
    # Field count
    cdef int16_t n_fields = unpack_int16(buffer.read(2))
    if n_fields == -1:
        # End reached
        return False

    cdef int32_t len_field = 0
    cdef array.array builder

    cdef bytes field_dat

    for i in range(n_fields):
        len_field = unpack_int32(buffer.read(4))

        # builder = column_builders[i]
        builder = column_builders[i]
        # builder.resize_smart(1)

        if len_field == -1:
            # Null field
            # builder.append(None)
            # TODO add field len
            builder.append(b'\x00\x00\x00\x00')
            # mask.append(True)
            continue

        field_dat = buffer.read(len_field)

        builder.append(unpack_int64(field_dat))
        column_builders[i] = builder
        # builder += pg_bswap64(<uint64_t>field_dat)
        # builder.append(field_dat)

    return True


cdef int process_buffer(buffer, column_builders):
    parse_header(buffer)
    cont = True
    cdef int counter = 0
    while cont:
        cont = read_tuple(buffer, column_builders)
        counter += 1
    return counter


cdef _read_pg_buffer(buffer, field_names, field_types):

    # wrangle field type defs
    pg_oids = get_pg_oids(field_types)
    pa_object_field_type = [PG_PA_TYPEMAP[oid] for oid in pg_oids]

    # pa_field_types = [PG_OID_TYPEMAP[pg_oid] for pg_oid in pg_oids]

    fields = [pa.field(n, t) for n, t in zip(field_names, pa_object_field_type)]

    column_builders = prepare_column_builders(pa_object_field_type)
    n_rows = process_buffer(buffer, column_builders)

    return columns_to_arrow_table(column_builders, fields, pa_object_field_type, n_rows)


def read_pg_buffer(buffer, field_names, field_types):
    return _read_pg_buffer(buffer, field_names, field_types)


cdef _read_pg_file(filename, field_names, field_types):
    with open(filename, 'rb') as buffer:
        return _read_pg_buffer(buffer, field_names, field_types)


def read_pg_file(filename, field_names, field_types):
    return _read_pg_file(filename, field_names, field_types)

# NOTE: possible to just build a list of values and then to array, but not very fast
# (about 2/3rds or 1.5x faster, aiming for 2-3x)

cdef _read_pg_query(cursor, query, field_names, field_types):
    # buffer = _ParserIO()
    # cursor.copy_expert(query, buffer)
    # with _ParserIO() as buffer:

        # buffer.seek(0)
        # return _read_pg_buffer(buffer, field_names, field_types)
    with io.BytesIO() as buffer:
        cursor.copy_expert(query, buffer)
        buffer.seek(0)
        return _read_pg_buffer(buffer, field_names, field_types)


def read_pg_query(cursor, query, field_names, field_types):
    return _read_pg_query(cursor, query, field_names, field_types)
