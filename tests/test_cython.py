import io

import psycopg2

from pgarrow import parser
from pgarrow.tools import timeit



def test_edrp_readdat(filename):
    # hardcode from edrp_daily table
    field_names = ['site_id', 'reading_date', 'temperature_max', 'temperature_min', 'temperature_mean', 'power']
    field_types = ['int8', 'timestamp', 'float8', 'float8', 'float8', 'float8']
    # field_types = [pa.int64(), pa.float64(), pa.float64(), pa.float64(), pa.float64(), pa.float64()]
    return parser.read_pg_file(filename, field_names, field_types)


def test_edrp_readq(conn):
    field_names = ['site_id', 'reading_date', 'temperature_max', 'temperature_min', 'temperature_mean', 'power']
    field_types = ['int8', 'timestamp', 'float8', 'float8', 'float8', 'float8']
    query = 'COPY (SELECT * FROM edrp_edf.elec_daily) TO STDOUT WITH (FORMAT BINARY)'

    with conn.cursor() as cur:
        return parser.read_pg_query(cur, query, field_names, field_types)



@timeit
def main():
    import os
    # path = os.path.dirname(os.path.realpath(__file__))
    # r = test_edrp_readdat(os.path.join(path, 'fixtures', 'elec_daily.pgdat'))
    # print(r)
    # r = parser.test_edrp_readdat('elec_daily.pgdat')
    #
    pg_uri = os.environ['POSTGRES_URI']
    engine = psycopg2.connect(pg_uri)
    r = test_edrp_readq(engine)
    print(r)


if __name__ == '__main__':
    main()
