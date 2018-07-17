# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef init_monetary_codecs():
    moneyoids = [
        MONEYOID,
    ]

    for oid in moneyoids:
        register_core_codec(oid,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_TEXT)


init_monetary_codecs()
