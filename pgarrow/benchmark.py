import asyncio
import io
import pandas as pd
import psycopg2

from sqlalchemy import create_engine

from pgarrow.tools import timeit

POSTGRES_URI = 'postgres://jonathanchambers@localhost:5432/jonathanchambers'


def pg_read_daily_table(engine):
    return pd.read_sql('SELECT * FROM edrp_edf.elec_daily', engine)


@timeit
def bench_pd_read_sql():
    engine = create_engine(POSTGRES_URI)
    table = pg_read_daily_table(engine)

    print(len(table))


def psyco_read_daily_table(conn):
    with io.StringIO() as file, conn.cursor() as cur:
        cur.copy_expert('COPY edrp_edf.elec_daily TO STDOUT', file)
        dat = file.getvalue()
    return dat


@timeit
def bench_psyco_read_sql():
    engine = psycopg2.connect(POSTGRES_URI)
    table = psyco_read_daily_table(engine)

    print(len(table))


def psyco_bin_read_daily_table(conn):
    with io.BytesIO() as file, conn.cursor() as cur:
        cur.copy_expert('COPY edrp_edf.elec_daily TO STDOUT WITH (FORMAT BINARY)', file)
        dat = file.getvalue()

    return dat

@timeit
def bench_psyco_bin_read_sql():
    engine = psycopg2.connect(POSTGRES_URI)
    table = psyco_bin_read_daily_table(engine)

    print(len(table))



import asyncpg


async def apg_bin_read_daily_table():
    conn = await asyncpg.connect(POSTGRES_URI)

    with io.BytesIO() as file:
        result = await conn.copy_from_query('SELECT * FROM edrp_edf.elec_daily', output=file, format='binary')
        dat = file.getvalue()
    return dat

@timeit
def bench_apg_bin_read_sql():
    table = asyncio.get_event_loop().run_until_complete(apg_bin_read_daily_table())
    print(len(table))


def save_pg_binary():
    conn = psycopg2.connect(POSTGRES_URI)
    with open('elec_daily.pgdat', 'wb') as file, conn.cursor() as cur:
        cur.copy_expert('COPY edrp_edf.elec_daily TO STDOUT WITH (FORMAT BINARY)', file)

if __name__ == '__main__':
    bench_pd_read_sql()
    # bench_psyco_read_sql()
    # bench_psyco_bin_read_sql()
    # bench_apg_bin_read_sql()

    # save_pg_binary()