import os
from sqliteController import *


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(BASE_DIR, 'db.sqlite3')
db_obj = SqliteOperator(path)

# create table
db_obj.execute_query(""" CREATE TABLE Person (
            Email VARCHAR(255) NOT NULL,
            First_Name CHAR(25) NOT NULL,
            Last_Name CHAR(25),
            Score INT
        ); """)

# insert single row
db_obj.insert_update_row(
    "Insert into Person VALUES (?, ?, ?, ?)",
    ('abc@email.com', 'ab', 'c', 95), autocommit=True
)

# insert multiple row
db_obj.bulk_insert_update_rows(
    "Insert into Person VALUES (?, ?, ?, ?)",
    (('xyz@email.com', 'xy', 'z', 87),
     ('fake@email.com', 'fake', '', 32),
    ), autocommit=True
)

# update single row
db_obj.insert_update_row(
    "Update Person set First_Name=? WHERE Email=?",
    ('new','abc@email.com'), autocommit=True
)

# reading rows
for i in db_obj.select_query("Select * from Person"):
    print(i)
    
# read all rows    
for i in db_obj.select_query_fetchall("Select * from Person", as_dict=True):
    print(i)

# truncate table
db_obj.execute_query("DELETE FROM Person")

# read all rows 
for i in db_obj.select_query_fetchall("Select * from Person", as_dict=True):
    print(i)
    
# drop table
db_obj.execute_query("Drop Table Person")
