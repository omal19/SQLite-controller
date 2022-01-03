"""
Author : Omal Bharuka
Licence : MIT License
"""

import sqlite3
from contextlib import contextmanager
from sqlite3 import Error


class SqliteOperator:
    """
    SqliteOperator is a simple wrapper build over SQLite3 library. To make it's use simple.
    To handle connection (open/close) and error handling.
    """

    def __init__(self, database_path):
        """
        :param database_path: full path location to SQLite db
        """
        self.database_path = database_path

    def connection_handler(func):
        """
        Decorator to handle SQLite3 connection (open and close)
        To avoid having to deal manually with rollback or commit, context manager used.
        :return:
        """
        def handle(self, *args, **kwargs):
            autocommit = kwargs.pop('autocommit', False)
            wal_mode = kwargs.pop('wal_mode', False)
            as_dict = kwargs.get('as_dict', False)
            con = self.create_connection(autocommit=autocommit, wal_mode=wal_mode,
                                         caller_func=func.__name__)
            if as_dict:
                con.row_factory = sqlite3.Row
            if autocommit:
                with self.transaction(con):
                    return_value = func(self, con, *args, **kwargs)
            else:
                with con:
                    # connection object as a context manager
                    return_value = func(self, con, *args, **kwargs)

            if func.__name__ != "select_query":
                # as select_query() returns a iterable cursor for which connection cannot be closed
                con.close()
            return return_value
        return handle

    def create_connection(self, autocommit=False, wal_mode=False, caller_func=None):
        """
        Open a connection to SQLite database
        :return: connection object
        :param autocommit: to enable auto commit
        :param caller_func:
        :param wal_mode: (optional) True - to open multiple read connection object
        """
        try:
            db_path = self.database_path
            uri = False
            if caller_func and 'select_query' in caller_func:
                uri = True
                # to open db connection in readonly mode - for select queries
                db_path = f'file:{self.database_path}?mode=ro'

            # Open database in autocommit mode by setting isolation_level to None.
            if autocommit and isinstance(autocommit, bool):
                conn = sqlite3.connect(db_path, isolation_level=None, uri=uri)
            else:
                conn = sqlite3.connect(db_path, uri=uri)

            # Set journal mode to WAL.
            # WAL-mode allows multiple readers to co-exist with a single writer.
            if wal_mode and isinstance(autocommit, bool):
                conn.execute('pragma journal_mode=wal')

            return conn
        except Error as e:
            print(e)
        return None

    @contextmanager
    def transaction(self, conn):
        # We must issue a "BEGIN" explicitly when running in auto-commit mode.
        conn.execute('BEGIN')
        try:
            # Yield control back to the caller.
            yield
        except Error as e:
            # Roll back all changes if an exception occurs.
            conn.rollback()
            print(e)
            raise
        else:
            conn.commit()

    @connection_handler
    def select_query(self, conn, query, *args, **kwargs):
        """
        Select query that loops over cursor (act as generator)
        :param conn: connection object
        :param query: select query
        :param as_dict: (optional) True - to get rows as python dictionary
        :param wal_mode: (optional) True - to open multiple read connection object
        :return:
        """
        try:
            for i in conn.execute(query):
                if 'as_dict' in kwargs:
                    if kwargs['as_dict']:
                        yield dict(i)
                else:
                    yield i
            conn.close()
        except Error as e:
            print(e)
        return None

    @connection_handler
    def select_query_fetchall(self, conn, sql_query,  *args, **kwargs):
        """
        Select query that fetches & returns all the rows
        Loads all the data into memory (python variable)
        Not memory efficient. Use select_query(), preferred.

        :param conn: connection object
        :param sql_query: select query
        :param as_dict: (optional) True - to get rows as python dictionary
        :param wal_mode: (optional) True - to open multiple read connection object
        :return:
        """
        try:
            crsr = conn.cursor()
            crsr.execute(sql_query)
            if 'as_dict' in kwargs:
                if kwargs['as_dict']:
                    return (dict(row) for row in crsr.fetchall())
            else:
                return crsr.fetchall()
        except Error as e:
            print(e)
        return None

    @connection_handler
    def insert_update_row(self, conn, insert_update_row_query, values,  *args, **kwargs):
        """
        Insert / Update query for single row.
        Use bulk_insert_update_rows(), for multiple insert/update rows.
        
        :param conn: connection object
        :param insert_update_row_query: 
        :param values: list of values to be inserted/updated
        :param autocommit: (optional) True - to enable custom context manager &
                                            auto commit using isolation_level=None
        :return:
        """
        try:
            conn.execute(insert_update_row_query, values)
        except Error as e:
            print(e)
        return None

    @connection_handler
    def bulk_insert_update_rows(self, conn, insert_update_row_query, values,  *args, **kwargs):
        """
        Bulk insert or update rows.
        :param conn: connection object
        :param insert_update_row_query:
        :param values: value list to be inserted/updated (list of list)
        :return:
        :param autocommit: (optional) True - to enable custom context manager &
                                            auto commit using isolation_level=None
        """
        try:
            conn.executemany(insert_update_row_query, values)
        except Error as e:
            print(e)
        return None

    @connection_handler
    def execute_query(self, conn, query, *args, **kwargs):
        """
        To execute query provided (create, delete, truncate, drop)
        :param conn: connection object
        :param query: (create, delete, truncate, drop) query
        :param autocommit (optional): True - to enable custom context manager &
                                            auto commit using isolation_level=None

        :return:
        """
        try:
            conn.execute(query)
        except Error as e:
            print(e)
        return None
