import os
from sqliteController import *

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(BASE_DIR, 'db.sqlite3')
db_obj = SqliteOperator(path)
db_obj.execute_query(""" CREATE TABLE Person (
            Email VARCHAR(255) NOT NULL,
            First_Name CHAR(25) NOT NULL,
            Last_Name CHAR(25),
            Score INT
        ); """)
db_obj.insert_update_row(
    "Insert into Person VALUES (?, ?, ?, ?)",
    ('abc@email.com', 'ab', 'c', 95), autocommit=True
)
for i in db_obj.select_query("Select * from Person"):
    print(i)
for i in db_obj.select_query_fetchall("Select * from Person", as_dict=True):
    print(i)
db_obj.execute_query("DELETE FROM Person")
for i in db_obj.select_query_fetchall("Select * from Person", as_dict=True):
    print(i)
db_obj.execute_query("Drop Table Person")
