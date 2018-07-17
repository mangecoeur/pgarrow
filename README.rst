===============================
pgarrow
===============================


.. image:: https://img.shields.io/travis/mangecoeur/pgarrow.svg
        :target: https://travis-ci.org/mangecoeur/pgarrow


postgres to arrow


Plan
----

- Benchmark/profile pandas read_sql - where is the time really spent?
    - maybe as notebook for viz etc
- Benchmark read from copy
- Benchmark old copy -> csv code
- Save some binary data as a file for testing
- Start building parser for pg binary format
- Create arrow table from pg data
- Convert to cython code, use pyarrow cython api

NOTE or write arrow creation in rust - since whole point is mem sharing should be trivial to create and pass ref to python


Notes
-----

Initial bench suggests more than half of the overall time is spent outside of reading from DB,
and that reading as format binary is again 2x as fast as reading as text:

Reading with pandas:
'bench_pd_read_sql'  10s
Copy table to in memory text file (format text)
'bench_psyco_read_sql'  5s
Copy table in in memory bytes file (format binary)
'bench_psyco_bin_read_sql'  2.4s

Therefore if it was possible to convert from postgres binary straight to pandas (or at least via arrow)
we could expect maybe 3x speedup (4x speedup plus overhead of conversion)