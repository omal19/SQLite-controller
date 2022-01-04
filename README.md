[![Python](https://img.shields.io/static/v1?label=Python&message=3.6+|+3.7+|+3.8+|+3.9+|+3.10&color=2b5d80)](https://github.com/python)
[![sqlite3](https://img.shields.io/static/v1?label=SQLite3&message=+&color=2b5d80)](https://github.com/python/cpython/tree/f4c03484da59049eb62a9bf7777b963e2267d187/Lib/sqlite3)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/omal19/SQLite-controller/blob/main/LICENSE)

## About

SqliteController is a simple wrapper build over SQLite3 library, to make it's use simple by taking care of connection (open/close) and error handling.


## Usage

   **select_query(query_string)** - returns cursor/generator object
   **select_query_fetchall(query_string)** - returns list of tuple
   **select_query_fetchall(query_string, as_dict=True)** - returns list of row as dict object
   **execute_query(query_string)** - to create, drop, delete table 
   **insert_update_row(query_string, values_list)** - insert/Update a single row
   **bulk_insert_update_rows(query_string, 2D_list_of_values)** - insert/Update a multiple rows

Take a look at the [sample_usage.py](https://github.com/omal19/SQLite-controller/blob/main/sqliteController/sample_usage.py)


## License

See [LICENSE](https://github.com/omal19/SQLite-controller/blob/main/LICENSE)
