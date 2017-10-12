import logging
from typing import *
from util.fetch_all_table_name import read_table_names_from_sql_cursor, read_table_names_without_quote

from connection_coordinator import  get_coordinator, DataSource
from migrator_kit.statement_cleaner import clean_statment
import pyodbc
import sqlite3

table_names = ["user_5r_disc_p_120415","user_5r_disc_t_120415"]
logger = logging.getLogger("sql2sqlite.table_creator")
logger.setLevel(logging.INFO)

def delete_the_table(table_name, cc):
    try:
        cc.connect()
        sqlite_cur = cc.sqlite_cur
        sqlite_cur.execute("DROP TABLE IF EXISTS '{0}'".format(table_name))
        return True
    except Exception as e:
        logger.critical(e)
    finally:
        cc.close_all_connection()


def delete_the_temp_table(table_name, temp_table_postfix, cc):

    new_table_name = table_name + temp_table_postfix
    return delete_the_table(new_table_name, cc)


def rename_one_to_another_table(old_table_name, new_table_name, cc):
    try:
        cc.connect()
        sqlite_cur = cc.sqlite_cur
        sqlite_cur.execute("ALTER TABLE {OLD_NAME} RENAME TO {NEW_NAME}".format(OLD_NAME=old_table_name,
                                                                            NEW_NAME=new_table_name))
        return True
    except Exception as e:
        logger.critical(e)
        return False
    finally:
        cc.close_all_connection()


def rename_the_temp_table_back_to_table(origin_table_name, temp_table_postfix, cc):
    temp_table_name = origin_table_name + temp_table_postfix
    return rename_one_to_another_table(temp_table_name, origin_table_name, cc)


def rename_one_to_temp_table(old_table_name, temp_table_postfix, cc):
    cc.connect()
    new_table_name = old_table_name + temp_table_postfix
    return rename_one_to_another_table(old_table_name, new_table_name, cc)


def create_one_specific_new_table_with_temp_(table_name, cc, exporter):
    sqlite_cur = cc.sqlite_cur
    sql_cur = cc.sql_cur

    if not _create_one_table_in_sqlite_(exporter,table_name,sql_cur,sqlite_cur):
        raise Exception()


# Gonna refactor the code to allow a new function

def create_new_table(table_names : Optional[Union[str, List[str]]] = None ,
                     data_source = DataSource.WTP_COLLAB,
                     exporter = None) \
                        -> (List[str], List[str]):
    '''
        This function create new tables in sqlite based on the data source. If none
        of table_names are given, this function will create tables for all data source in
        source database

        If given a list, then the function will create for those tables specifically in the list.

    :param table_names:
    :param data_source:
    :return:
    '''

    coordinator = get_coordinator(data_souce=DataSource.WTP_DATA)
    coordinator.connect()
    f = open("create_st.txt", "w")

    # get conn and cur for both sqlite and sql
    sqlite_cur = coordinator.sqlite_cur
    sql_cur = coordinator.sql_cur

    success_tables = []
    failure_tables = []

    # iterate through all tables to get create table statments list
    # If no table names have been specified
    if table_names is None:
        table_names = read_table_names_from_sql_cursor(sql_cur)

    elif type(table_names) is str:
        table_names = [table_names]

    for table_name in table_names:

        # table name will have ` around the itself, so get rid of it before using regular
        # in the function want_to_export
        if _create_one_table_in_sqlite_(table_name, coordinator, exporter, f):
            coordinator.commit()
            success_tables.append(table_name)
        else:
            failure_tables.append(table_name)

        print("------------------------------------------")
    coordinator.close_all_connection()
    return success_tables, failure_tables


def _get_origin_create_statement_for_a_table_(table_name: str, cc) -> Optional[str]:
    if cc.sqlite_conn is None:
        cc.connect()

    print("Fetching create table stmt for table: %s" % table_name)

    # get the create statement from the mysql database
    get_create_table_sql = "SHOW CREATE TABLE %s" % table_name
    try:
        cc.sql_cur.execute(get_create_table_sql)
    except Exception as e:
        print(e)
        logging.critical("%s: %s" % (table_name, e))
        cc.close_all_connection()
        return None

    # after get the statement,clean the create statement
    return cc.sql_cur.fetchone()[1]


def _create_one_table_in_sqlite_(table_name, cc, exporter,  debug_file = None):
    if cc.sqlite_conn is None:
        cc.connect()

    if not exporter.want_to_export(table_name.strip("`")):
        return False

    print("Fetching create table stmt for table: %s" % table_name)

    # get the create statement from the mysql database
    get_create_table_sql = "SHOW CREATE TABLE %s" % table_name
    use_create_table_sql = ''

    try:
        cc.sql_cur.execute(get_create_table_sql)
        result = cc.sql_cur.fetchone()
        use_create_table_sql = clean_statment(result[1])

        # after get the statement,clean the create statement
        if debug_file is not None:  debug_file.write(use_create_table_sql)
        print("Creating table for table: %s" % table_name)

    except pyodbc.Error as e:
        print(e)
        logging.critical("%s: %s" % (table_name, e))
        cc.close_all_connection()
        return False


    # Execute the create statement
    try:
        cc.sqlite_cur.execute(use_create_table_sql)
    except sqlite3.OperationalError as e:
        print("ERROR: %s" % e)
        logger.critical("%s: %s\n stmt: %s" % (table_name, e, use_create_table_sql))

        # if the error message shows that the table is there
        if "exists" in str(e):
            try:
                # try execute drop code. If successful. Then pass, otherwise, return false
                cc.sqlite_cur.execute("DROP TABLE IF EXISTS '{0}'".format(table_name))

                # If drop successful. Start to redo it.
                cc.sqlite_cur.execute(use_create_table_sql)

            except pyodbc.Error as e:
                logger.critical(e)
                cc.close_all_connection()
                return False
        else:
            cc.close_all_connection()
            return False
    print("------------------------------------------")
    return True


def findTables():

    StmtLine = str
    TableName = str

    def _whether_smallint_in_(creating_stmts: List[StmtLine])-> bool:
        for index, line in enumerate(creating_stmts):
            if " smallint(5) " in line:
                return True
        return False

    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab.db'
    cc.connect()
    # list = read_sqlite_tables_from_sqlite_cursor(cur=cc.sqlite_conn.cursor())
    l = read_table_names_from_sql_cursor(cc.sql_cur)  # type: List[TableName]

    tables_with_smallint = []
    for table_name in l:
        creating_stmt = _get_origin_create_statement_for_a_table_(table_name, cc)
        if creating_stmt is not None:
            if _whether_smallint_in_(creating_stmt.split("\n")):
                tables_with_smallint.append(table_name)

    print(tables_with_smallint)

#  print(list)

def test():
    pass
    #data_table = "calc_1_bi_f"
    #get_create_table_sql = "SHOW CREATE TABLE %s" % data_table

    #sql_cur.execute(get_create_table_sql)

    #result = sql_cur.fetchone()
    #print(result)

    #use_create_table_sql = clean_statment(result[1])

    #sqlite_conn.execute(use_create_table_sql)
    #sqlite_conn.commit()
    #sqlite_conn.close()

#findTables()