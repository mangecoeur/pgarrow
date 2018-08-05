# distutils: language=c++
# cython: infer_types=True
# cython: profile=True

import io
from libcpp cimport bool
from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector

from hton cimport unpack_int16, unpack_int32, unpack_int64, unpack_float, unpack_double
import datetime

import pyarrow as pa
cimport pyarrow.lib as palib

from pyarrow.lib cimport CArray, CTable, CColumn, CField, CDataType, NativeFile
from pyarrow.lib cimport Table, Column, Field, DataType, Type
from pyarrow.lib cimport check_status, pyarrow_unwrap_schema, pyarrow_unwrap_field, pyarrow_wrap_column, pyarrow_wrap_table, pyarrow_unwrap_data_type

# from builder cimport (CArrayBuilder, CInt32Builder, CInt64Builder,
# CFloatBuilder, CDoubleBuilder, CAdaptiveIntBuilder,
# CBooleanBuilder, CPrimitiveBuilder,
# # CTimestampBuilder,
# )

from builder cimport AbstractBuilder, DoubleBuilder, Int64Builder



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
    if patyp == pa.float64():
        return DoubleBuilder()
    elif patyp == pa.int64():
        return Int64Builder()
    elif patyp == pa.timestamp('ns'):
        return Int64Builder()


cdef prepare_column_builders(field_types):
    # TODO need to enforce that these are never None
    return [make_array_builder(typ) for typ in field_types]


cdef columns_to_arrow_table(columns, fields):
    data = []
    for i in range(len(columns)):
        field = fields[i]
        column_builder = columns[i]
        data.append(column_builder.finish())

    return Table.from_arrays(data, [f.name for f in fields])

#
# pg_epoch_datetime = datetime.datetime(2000, 1, 1)
#
# # TODO rather decode directly to arrow datetime format
# def decode_timestamp(ts):
#     seconds = (ts / 1000000)
#     microseconds = (ts % 1000000)
#
#     return pg_epoch_datetime.__add__(datetime.timedelta(0, seconds, microseconds))


cdef parse_header(buffer):
    intro = buffer.read(11)
    # TODO should check PGCOPY intro for errors
    flags = unpack_int32(buffer.read(4))
    header_extension = unpack_int32(buffer.read(4))
    return buffer


# Older version uses switch case on field name, prefer to prepare column builders
# cdef read_tuple(buffer, field_types, columns):
#     # Field count
#     cdef int16_t n_fields = unpack_int16(buffer.read(2))
#     if n_fields == -1:
#         # End reached
#         return False
#
#     cdef int32_t len_field = 0
#     # cdef DataType pgtyp
#     cdef Type pgtyp
#
#     for i in range(n_fields):
#         len_field = unpack_int32(buffer.read(4))
#         if len_field == -1:
#             # Null field
#             columns[i]._append_null()
#             continue
#
#         field_dat = buffer.read(len_field)
#
#         pgtyp = field_types[i]
#
#         if pgtyp == Type._Type_INT16:
#             field_dat =  unpack_int16(field_dat)
#
#         elif pgtyp ==  Type._Type_INT32:
#             field_dat =  unpack_int32(field_dat)
#         elif pgtyp ==  Type._Type_INT64:
#             field_dat =  unpack_int64(field_dat)
#
#         elif pgtyp == Type._Type_FLOAT:
#             field_dat =  unpack_float(field_dat)
#
#         elif pgtyp ==  Type._Type_DOUBLE:
#             field_dat =  unpack_double(field_dat)
#
#         elif pgtyp == Type._Type_TIMESTAMP:
#             field_dat =  unpack_double(field_dat)
#
#         columns[i]._append(field_dat)
#
#     return True

# TODO want to pass data from buffer without copying.
cdef bool read_tuple(buffer, column_builders):
    # Field count
    cdef int16_t n_fields = unpack_int16(buffer.read(2))
    if n_fields == -1:
        # End reached
        return False

    cdef int32_t len_field = 0
    cdef AbstractBuilder builder

    cdef bytes field_dat

    for i in range(n_fields):
        len_field = unpack_int32(buffer.read(4))

        builder = column_builders[i]
        if len_field == -1:
            # Null field
            builder.append_null()
            continue

        field_dat = buffer.read(len_field)
        builder.append_bytes(field_dat)

    return True

# TODO idea: currently using worst of both worlds wrt buffer (memory use of full buffer array but slowness of sequential reads)
# try to instead treat buffer as full bytes sequence and pass slice views instead of copies
# # TODO want to pass data from buffer without copying.
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



cdef process_buffer(buffer, column_builders):
    parse_header(buffer)
    cont = True
    while cont:
        cont = read_tuple(buffer, column_builders)


cdef _read_pg_buffer(buffer, field_names, field_types):

    # wrangle field type defs
    pg_oids = get_pg_oids(field_types)
    pa_object_field_type = [PG_PA_TYPEMAP[oid] for oid in pg_oids]

    # pa_field_types = [PG_OID_TYPEMAP[pg_oid] for pg_oid in pg_oids]

    fields = [pa.field(n, t) for n, t in zip(field_names, pa_object_field_type)]

    column_builders = prepare_column_builders(pa_object_field_type)

    process_buffer(buffer, column_builders)

    return columns_to_arrow_table(column_builders, fields)


def read_pg_buffer(buffer, field_names, field_types):
    return _read_pg_buffer(buffer, field_names, field_types)


cdef _read_pg_file(filename, field_names, field_types):
    with open(filename, 'rb') as buffer:
        return _read_pg_buffer(buffer, field_names, field_types)


def read_pg_file(filename, field_names, field_types):
    return _read_pg_file(filename, field_names, field_types)


# TODO make this Cdef class...
# TODO probably over-complicating things...
class _ParserIO():
    def __init__(self, field_builders):
        super().__init__()
        self.internal_buffer = b''
        self.internal_buffer_len = 0
        # Len header is 19, but just throw away that much for now.
        self.bytes_needed = 19
        self.field_builders = field_builders

    def write(self, content):
        l = len(content)
        self.internal_buffer += content
        # len of content might be constant?
        self.internal_buffer_len += l

        if self.internal_buffer_len >= self.bytes_needed:
            data = self.internal_buffer[self.bytes_needed]

            self.field_builders



        # TODO set how many bytes are needed for the next read
        # First skip the header
        # Then will either be 4 bytes for int32 field length value, or the number of bytes in the field itself.
        # Build a state machine style parser - each feild is a state, cycle through the builders
        #
        # Wait until len buffer > len field length value, then consume that much of the buffer
        # If field length =-1, append a null.
        # Then wait until len buffer > len of feild, then consume that much of buffer
        # Write value using builder for field.
        # Finally bump to next field


        return l

    def close(self):
        pass

#         # internal_buffer
#
# cdef class _ParserIO(io.BytesIO):
#     cdef bytes internal_buffer
#     cdef write(self, content):
#         # TODO not sure how much gets written at a time...
#         l = len(content)
#         print(l)
#         return l
#         # internal_buffer



# TODO operate on stream instead of reading into buffer
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
