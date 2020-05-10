# What is pg2arrow?

**pg2arrow** is a simple and lightweight utility to query PostgreSQL relational database, and to dump the query results in Apache Arrow format.

Apache Arrow is a kind of file format that can save massive amount of structured data using columnar layout, thus, optimized to analytic workloads for insert-only dataset.

## Advantages
* Binary data transfer
    * It uses binary transfer mode of `libpq`. It eliminates parse/deparse operations of text encoded query results. Binary operations are fundamentally lightweight, and has less problems to handle cstring literals.
* Exact data type mapping
    * It fetches exact data-type information from PostgreSQL, and keeps data types as is. For example, user defined composite data types are written as `Struct` type (it can have nested child types), however, some implementation encodes these unknown types to string representation anyway.
* Less memory consumption
    * It writes out `RecordBatch` (that is a certain amount of rows) to the result file per specified size (default: 512MB). Since it does not load entire dataset on memory prior to writing out, we can dump billion rows even if it is larger than physical memory.

For more details, see our wikipage: https://github.com/heterodb/pg2arrow/wiki
