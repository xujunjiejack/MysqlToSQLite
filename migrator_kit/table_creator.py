import logging

from util.fetch_all_table_name import  read_table_names_from_sql_cursor

from connection_coordinator import  get_coordinator, DataSource
from migrator_kit.statement_cleaner import clean_statment

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

def create_new_table(table_names = None, data_source = DataSource.WTP_COLLAB, exporter = None):
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

    # iterate through all tables to get create table statments list
    # If no table names have been specified
    if table_names is None:
        table_names = read_table_names_from_sql_cursor(sql_cur)

    if type(table_names) is str:
        table_names = [table_names]

    for table_name in table_names:

        # table name will have ` around the itself, so get rid of it before using regular
        # in the function want_to_export
        if _create_one_table_in_sqlite_(table_name,coordinator, exporter,f):
            coordinator.commit()

        print("------------------------------------------")
    coordinator.close_all_connection()


def _create_one_table_in_sqlite_(table_name, cc, exporter,  debug_file = None):
    if cc.sqlite_conn is None:
        cc.connect()

    if not exporter.want_to_export(table_name.strip("`")):
        return False

    print("Fetching create table stmt for table: %s" % table_name)

    # get the create statement from the mysql database
    get_create_table_sql = "SHOW CREATE TABLE %s" % table_name
    try:
        cc.sql_cur.execute(get_create_table_sql)
    except Exception as e:
        print(e)
        logging.critical("%s: %s" % (table_name, e))
        cc.close_all_connection()
        return False

    # after get the statement,clean the create statement
    result = cc.sql_cur.fetchone()
    use_create_table_sql = clean_statment(result[1])
    if debug_file is not None:
        debug_file.write(use_create_table_sql)
    print("Creating table for table: %s" % table_name)

    # Execute the create statement
    try:
        cc.sqlite_cur.execute(use_create_table_sql)
    except Exception as e:
        print("ERROR: %s" % e)
        logger.critical("%s: %s\n stmt: %s" % (table_name, e, use_create_table_sql))
        return False
    print("------------------------------------------")
    return True


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
