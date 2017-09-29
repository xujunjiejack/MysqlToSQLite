from connection_coordinator import get_coordinator, DataSource
from typing import *

def read_sql_tables_from_sql_cursor(cur):
    result = cur.tables()
    return result


def read_sqlite_tables_from_sqlite_cursor(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cur.fetchall()]


def read_table_names_from_sql_cursor(sql_cur) -> List[str]:
    sql_tables = read_sql_tables_from_sql_cursor(sql_cur)

    # iterate through all tables to get create table statments list
    table_names = ["`{0}`".format(get_name_from_tuple(table_info)) for table_info in sql_tables]
    return table_names


def read_table_names_without_quote(sql_cur) -> List[str]:
    sql_tables = read_sql_tables_from_sql_cursor(sql_cur)
    table_names = ["{0}".format(get_name_from_tuple(table_info)) for table_info in sql_tables]
    return table_names


# abstraction for different format of the cur
def get_name_from_tuple(table: Tuple) -> str:
    #('wtp_collab', '', 'user_sample_anxietypaper_2007', 'TABLE', '')
    return table[2]

def test():
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test.db'
    cc.connect()
    # list = read_sqlite_tables_from_sqlite_cursor(cur=cc.sqlite_conn.cursor())
    list = read_sqlite_tables_from_sqlite_cursor(cc.sql_cur)

    print(list)
