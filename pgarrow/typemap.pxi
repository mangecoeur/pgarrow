# from pyarrow.lib cimport Type
cimport pyarrow.lib as pa
# import pyarrow as pa

include "protocol/pgtypes.pxi"

cdef dict PGTYPEMAP = {
    BOOLOID: pa.bool_(),
    INT2OID: pa.int16(),
    INT4OID: pa.int32(),
    INT8OID: pa.int64(),
    FLOAT4OID: pa.float32(),
    FLOAT8OID: pa.float64(),
    TIMESTAMPOID: pa.timestamp('ns')

}

# TODO decide if we use type codes or DataType
# NOTE type codes could be consider private so to avoid...
# cdef dict PGTYPEMAP = {
#     BOOLOID: Type._Type_BOOL,
#     INT2OID: Type._Type_INT16,
#     INT4OID: Type._Type_INT32,
#     INT8OID: Type._Type_INT64,
#     FLOAT4OID: Type._Type_FLOAT,
#     FLOAT8OID: Type._Type_DOUBLE,
#     TIMESTAMPOID: Type._Type_TIMESTAMP
# }

# BYTEAOID
# CHAROID
# NAMEOID
#
# REGPROCOID
# TEXTOID
# OIDOID
# TIDOID
# XIDOID
# CIDOID
# PG_DDL_COMMANDOID
# JSONOID
# XMLOID
# PG_NODE_TREEOID
# SMGROID
# INDEX_AM_HANDLEROID
# POINTOID
# LSEGOID
# PATHOID
# BOXOID
# POLYGONOID
# LINEOID
# CIDROID
#
# ABSTIMEOID
# RELTIMEOID
# TINTERVALOID
# UNKNOWNOID
# CIRCLEOID
# MACADDR8OID
# MONEYOID
# MACADDROID
# INETOID
# _TEXTOID
# _OIDOID
# ACLITEMOID
# BPCHAROID
# VARCHAROID
# DATEOID
# TIMEOID
# TIMESTAMPOID
# TIMESTAMPTZOID
# INTERVALOID
# TIMETZOID
# BITOID
# VARBITOID
# NUMERICOID
# REFCURSOROID
# REGPROCEDUREOID
# REGOPEROID
# REGOPERATOROID
# REGCLASSOID
# REGTYPEOID
# RECORDOID
# CSTRINGOID
# ANYOID
# ANYARRAYOID
# VOIDOID
# TRIGGEROID
# LANGUAGE_HANDLEROID
# INTERNALOID
# OPAQUEOID
# ANYELEMENTOID
# ANYNONARRAYOID
# UUIDOID
# TXID_SNAPSHOTOID
# FDW_HANDLEROID
# PG_LSNOID
# TSM_HANDLEROID
# PG_NDISTINCTOID
# PG_DEPENDENCIESOID
# ANYENUMOID
# TSVECTOROID
# TSQUERYOID
# GTSVECTOROID
# REGCONFIGOID
# REGDICTIONARYOID
# JSONBOID
# ANYRANGEOID
# EVENT_TRIGGEROID
# REGNAMESPACEOID
# REGROLEOID
#
#         _Type_NA
#
#         _Type_BOOL
#
#         _Type_UINT8
#         _Type_INT8
#         _Type_UINT16
#         _Type_INT16
#         _Type_UINT32
#         _Type_INT32
#         _Type_UINT64
#         _Type_INT64
#
#         _Type_HALF_FLOAT
#         _Type_FLOAT
#         _Type_DOUBLE
#
#         _Type_DECIMAL
#
#         _Type_DATE32
#         _Type_DATE64
#         _Type_TIMESTAMP
#         _Type_TIME32
#         _Type_TIME64
#         _Type_BINARY
#         _Type_STRING
#         _Type_FIXED_SIZE_BINARY
#
#         _Type_LIST
#         _Type_STRUCT
#         _Type_UNION
#         _Type_DICTIONARY
#         _Type_MAP