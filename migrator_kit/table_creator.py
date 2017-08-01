import logging

from util.fetch_all_table_name import  read_table_names_from_sql_cursor

from connection_coordinator import  get_coordinator, DataSource
from migrator_kit.statement_cleaner import clean_statment

table_names = ["user_5r_disc_p_120415","user_5r_disc_t_120415"]
logging.basicConfig(filename='table_not_created.logs', level=logging.INFO)

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
        if not exporter.want_to_export(table_name.strip("`")):
            continue

        print("Fetching create table stmt for table: %s" % table_name)

        # get the create statement from the mysql database
        get_create_table_sql = "SHOW CREATE TABLE %s" % table_name
        try:
            sql_cur.execute(get_create_table_sql)
        except Exception as e:
            print(e)
            logging.critical("%s: %s" %(table_name, e))
            continue

        # after get the statement,clean the create statement
        result = sql_cur.fetchone()
        use_create_table_sql = clean_statment(result[1])
        f.write(use_create_table_sql)
        print("Creating table for table: %s" % table_name)

        # Execute the create statement
        try:
            sqlite_cur.execute(use_create_table_sql)
        except Exception as e:
            print("ERROR: %s" %e)
            logging.critical("%s: %s\n stmt: %s" % (table_name, e, use_create_table_sql))
            continue
        coordinator.commit()
        print("------------------------------------------")
    coordinator.close_all_connection()


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
