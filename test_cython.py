import io
from pgarrow import parser
from pgarrow.tools import timeit
import pyarrow as pa


def test_in_cython():
    import pgarrow.test_in_cython
    pgarrow.test_in_cython.run_dummy()

def test_edrp_readdat(filename):
    # hardcode from edrp_daily table
    field_names = ['site_id', 'reading_date', 'temperature_max', 'temperature_min', 'temperature_mean', 'power']
    # field_types = ['int8', 'timestamp', 'float8', 'float8', 'float8', 'float8']
    field_types = [pa.int64(), pa.float64(), pa.float64(), pa.float64(), pa.float64(), pa.float64()]
    return parser.read_pg_file(filename, field_names, field_types)


def test_edrp_readq(conn):
    with io.BytesIO() as buffer, conn.cursor() as cur:
        # cur.copy_expert('COPY edrp_edf.elec_daily TO STDOUT WITH (FORMAT BINARY)', buffer)
        # TODO currently reads the whole thing to memory. So could easily send whole char buffer to C++
        # for processing (no need to share file descriptor)
        cur.copy_expert('COPY (SELECT * FROM edrp_edf.elec_daily LIMIT 1000) TO STDOUT WITH (FORMAT BINARY)', buffer)
        buffer.seek(0)


        # hardcode from edrp_daily table
        field_names = ['site_id', 'reading_date', 'temperature_max', 'temperature_min', 'temperature_mean', 'power']
        field_types = ['int8', 'timestamp', 'float8', 'float8', 'float8', 'float8']

        out_table = parser.prepare_out_table(field_types)

        parser.process_buffer(buffer, field_types, out_table)

        return parser.columns_to_arrow_table(out_table, field_names, field_types)


@timeit
def main():
    # test_in_cython()
    # r = test_edrp_readdat('elec_daily_smol.pgdat')

    # TODO: this read should be do-able in under 9seconds
    r = test_edrp_readdat('elec_daily.pgdat')
    print(r)
    # r = parser.test_edrp_readdat('elec_daily.pgdat')
    # engine = psycopg2.connect(POSTGRES_URI)
    # r = test_edrp_readq(engine)
    # print(r)

if __name__ == '__main__':
    main()
