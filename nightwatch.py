import sys
import os
import pyodbc
import sqlite3
import datetime
from toolz.itertoolz import *



if not len(sys.argv) == 3:
    print("please enter two arguments. the first is json file path, the second is sqlite db path")

# Test checksum file exists. file gets passes as a parameter
json_path = sys.argv[1]

if not os.path.exists(json_path):
    print("Json file can not be located. Path: {0}".format(json_path))
    sys.exit(1)

sqlite_path = sys.argv[2]
if not os.path.exists(sqlite_path):
    print("Sqlite db can not be located. Path: {0}".format(sqlite_path))
    sys.exit(1)

# Test mysql connection correct
try:
    sql_con = pyodbc.connect("DSN=wtp_data")
except pyodbc.Error as e:
    print("can not connect to wtp_data db")
    sys.exit(1)

# Test sqlite connection correctly
try:
    sqlite_con = sqlite3.connect(sqlite_path)
except sqlite3.Error:
    print("can not connect to sqlite {0}".format(sqlite_path))
    sys.exit(1)

# check checksum file
st = os.stat(json_path)
mtime = st.st_mtime
mtime_stamp = datetime.datetime.fromtimestamp(mtime)

## Compare this date to date
if not mtime_stamp.date() == datetime.datetime.today().date():
    print("The file has not been updated daily")
    sys.exit(1)

print("Everything seems to be OK")

