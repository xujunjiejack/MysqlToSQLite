'''
    This package includes many functions that can abstract common database commands
    Many of these functions are for either sql or ms sql via MS Access. 

    These are mostly not tested and can be potentially dangerous. so If things aren't working.
    remember to check here.

    Assumptions. This uses pypyodbc. and prints log statments using a logger. 
    Param descriptions:
     logger = the logger object responsible for printing. can be a list of different ones.
     if it does not take a logger, it does not print anything. 

     conn = a pypyodbc connection object. call db_connect to get one. 

     lsit of functions:
    db_connect # opens a connection to a db, sensitive to DSN or path to mdb
    dosql   # does a general sql statment. 
            # this can be a huge problem if the sql isn't valid according to the driver
            # it will not work. 
    bulk_insert     # copies a list of rows from a table into a table
                    # useful for bulk insert
    create_table    #creates the table with the given description in a given db 
    copy_table_to_db    # copies a table from one db to another
    drop_all_tables # drops all tables within a database
    get_connect_string  # given a pypyodbc connection you can determine what kind of connection it is. Access or mysql
    get_all_tabledetails    # gets a list of all table details things
    get_all_tablenames  # just gets a lit of all the tablenames in a databse
    drop_table  # drops a single table from a databse
    drop_columns    # drops sepcific columsn from a table in a databse 

'''
import logging
import pypyodbc as db
from datetime import datetime
from db_exporter_kit.waisman_utils.waisman_general_utils import iterable_is_all_equal, prettify_str
from db_exporter_kit.waisman_utils.waisman_errors import *
import sqlite3
import pyodbc
from typing import *
import copy as c


# open connetion to a database
def db_connect(**kwargs):
    '''
        Takes DSN='connection name' or MDB='path to access file'
        returns a connection to the specified database.
        throws Keyerror if you provide the wrong thing.
        Throws pypyodbc connect error if connection string is not valid.
    '''
    if 'DSN' in kwargs:
        return db.connect('DSN=' + kwargs['DSN'], autocommit=True)
    if 'MDB' in kwargs:
        try:
            return db.win_connect_mdb(kwargs['MDB'])
        except:
            pass
        #try creating it then connecting
        db.win_create_mdb(kwargs['MDB'])
        return db.win_connect_mdb(kwargs['MDB'])
    else:
        raise KeyError('Provide either a DNS keyword or MDB keyword')


# does an sql statement & takes care of all the infrastructure
def dosql_with_cursor(curs, conn, statement, *params, logger=logging):
        '''
            Does an sql statement an returns the description (i.e. header)
            and the body of the table. and closes the cursor connection.

            If you screw something up this can cause exceptions.
            prints nothing
        '''
        #curs = conn.cursor() # cursors are what do things. so get one.
        curs.execute(statement, *params) # execute the statement.
        desc = curs.description # breakout the reply
        rows = None             # fetchall can cause exceptions. so protect the user form them
        try:
            if 'select' in statement.lower():
                rows = curs.fetchall() # grab all the rows returned. iff there is a select
        except Exception as e:
            #logger.error('Error fetching rows returned from statement {%s}.\n desc-> %s\n Error-> %s'%(statement,desc,  e))
            pass # but don't do anything

        if isinstance(conn, sqlite3.Connection):
            return desc, rows

        curs.commit()   # effect any changes
        curs.close()    # close the connection safely
        return desc,rows


def dosql(conn, statement, *params, logger=logging) -> (Tuple[Tuple], List[tuple]):
    '''
        Does an sql statement an returns the description (i.e. header)
        and the body of the table. and closes the cursor connection.

        If you screw something up this can cause exceptions.
        prints nothing
    '''
    curs = conn.cursor()  # cursors are what do things. so get one.
    curs.execute(statement, *params)  # execute the statement.
    desc = curs.description  # breakout the reply
    rows = None  # fetchall can cause exceptions. so protect the user form them
    try:
        if 'select' in statement.lower():
            rows = curs.fetchall()  # grab all the rows returned. iff there is a select
    except Exception as e:
        # logger.error('Error fetching rows returned from statement {%s}.\n desc-> %s\n Error-> %s'%(statement,desc,  e))
        pass  # but don't do anything

    # Why this?
    if isinstance(conn, sqlite3.Connection):
        curs.close()
        return desc, rows

    curs.commit()  # effect any changes
    curs.close()  # close the connection safely
    return desc, rows

def dosql_without_commit(curs, statement, *params, logger=logging):
    '''
        This method is used by bulk insert to execute insert command fast.
        curs.commit can slow down the process, so it's not efficient to commit each step
        of bulk insert. Thus, it's the caller's responsibility to commit the change appropriately

    :param curs: cursor of the connection used for executing sql command
    :param statement: sql command
    :param params:
    :param logger:
    :return:
    '''

    curs.execute(statement, *params)  # execute the statement.
    desc = curs.description  # breakout the reply
    # fetchall can cause exceptions. so protect the user form them
    try:
        if 'select' in statement.lower():
            # grab all the rows returned by curs.fetchall(), iff there is a select
            rows = curs.fetchall()
            return desc, rows

    except Exception as e:
        # logger.error('Error fetching rows returned from statement {%s}.\n desc-> %s\n Error-> %s'%(statement,desc,  e))
        pass  # but don't do anything

    return desc, None # return None as rows if the statment is not a select or the selection fails

# inserts a table into a db
def bulk_insert(con, tablename, rows, logger = logging):
        '''
            This function is here to bulk insert a list of rows into a table
            - rows is as expected from a db cursor cur.rows()
            [[row1],[row2],[row3]]

            prints to logger 
            info: inserting rows into table (tablename)
            info:--------------------------------------
            info:   row#\total#rows of table: tablename. running: sqlstatment
            ERROR: There was an error inserting record into table. 
            info: -------------------------------------

            returns a list of tablnemes that failed. 
                in an array of tuples i.e.
                [
                ([list of values], error causing problem)
                ]
        '''
        failedrows = []
        logger.info('Inserting rows into table (%s)'%tablename)
        logger.info('---------------------------------------------')
        curs = con.cursor()
        for i,row in enumerate(rows): # go through all the rows in the table from the original. 
            insert_statement =''
            try:
                insert_statement = 'insert into "' + tablename + '" values ('
                insert_statement += ','.join(['?'] * len(row))
                insert_statement += ');'
                # --> insert into tablename values(?,?,?,?,...,?)
                logger.info('\t %s/%s of table: %s. Running: %s'%(str(i), str(len(rows)), tablename, insert_statement))
                #dosql(con, insert_statement, row) # the row is a list of values
                dosql_without_commit(curs, insert_statement,row)
            except Exception as e:
                failedrows.append((row, str(e)))
                row_str = str(row)
                logger.error('-> There was an error inserting record:\n %s\nMade Note of it.\ncommand->%s\n Error-> %s'%(row_str, insert_statement, e))
        logger.info('-----------------------------------------------')
        curs.commit()
        curs.close()
        return(failedrows)

# creates a table in the database
def create_table(con, tablename, desc, keys = [], logger=logging):
    '''
    This function creates a table in the specified database
    the table will have the name specified and columns as specified in desc
    desc -> [(col_name, <class col_type>, [size, ...]),...] # the return value from get_all_tabledetails
    strings need a size, otherwise you don't. size should be an int. 
    '''
    # build the create statement
    # initilaize it
    create_statement = 'Create table "' + tablename +'" ('
    # add all the column names and types
    remain = len(desc) # the number of columns remaining
    for col in desc:
        create_statement += '"' + col[0].replace('-','_').lower() + '"' + ' ' # at least one table has -, pypyodbc calls that a syntax error
        type = col[1]
        if type is int or  type is float:
            create_statement += 'INT'

        elif type is float:
            create_statement += 'FLOAT(7,4)'

        elif type is str:
            size = str(col[2]) \
                    if str(col[2]) != '-1' \
                    else 50 # default to 50

            create_statement += 'Varchar(' + str(size) + ')'

        elif type is datetime:
            create_statement += 'Varchar(50)' # it would be nice to use datetime, but pypy is calling it a syntax error. idk why. 
        else: # if it's not a recognized type fail spectatularly.
            logger.error('There was an unexpected type(%s,%s) in the table (%s) error-> %s' %(str(col[0]),str(col[1]),tablename, str(e)))
            raise Exception('There was an unexpected type(%s,%s) in the table (%s) error-> %s' %(str(col[0]),str(col[1]),tablename, str(e)))
        # conctatenater them with commas
        if remain > 1: # don't add a commma after the last item. 
            create_statement += ','
        # go onto the next one
        remain -= 1
    # add the primary keys
    if len(keys) > 0:
        create_statement += ', PRIMARY KEY ('
        keys = ['"' + k + '"' for k in keys]
        create_statement += ','.join(keys)
        create_statement += ')'
    
    # finish the create statement
    create_statement += ');'
    try:    
        logger.info('-- <Creating Table> with : %s ' % create_statement)
        dosql(con, create_statement)
    except Exception as e:
        logger.error('There was an Error Creating the table %s in the database (%s)\n with statement "%s" & params (n/a)\n-->Error: %s' 
            %(tablename, get_connect_string(con), create_statement, str(e)))
        raise e
    
# returns a list of column names that form the primary key
def get_primary_keys(con, tablename, logger = logging):
    ''' pypyodbc offers a few utility functions '''
    cursor = con.cursor() # this is the interface to pypyodbc functinons
    try:
        cursor.primaryKeys(table = tablename)
    except Exception as e:
        logger.error('Error getting the primary keys for table %s. error-> %s'%(tablename,e))
        raise e
    # this object is iterable. one row per primary key. 
    # each row has (table_cat, table_scheme, tablename, column_name, key_seq, pk_name)
    # i.e.         (None    , None          , data_w14_tr , family_id, 1, PRIMARY    )
    keys = [row[3].lower() for row in  cursor]
    cursor.close()
    return keys


def is_col_in_fields(con, tablename, col):
    return col in get_columns(con, tablename)


# add a column of a certain type to a table
def add_column(con, tablename, col_name, type, primary_key=False, *args, logger = logging):
    '''
        This function adds a column to a table. type should be a string that's something useful
        like integer, varchar(50), date, etc. it should be in the SQL syntax. if it's not. it won't work

        primary key will add this column to the tables keys.
        effectively running the sql 
        ALTER TABLE tablename ADD COLUMN colname type *args. so you can modify the sql. 
    '''
    if primary_key:
        raise NotImplementedError('adding a primary column has not been implemented yet')
    else:

        if not is_col_in_fields(con, tablename, col_name):
            sql = 'ALTER TABLE "%s" ADD COLUMN %s %s'%(tablename, col_name.lower(), type)
            logger.info(sql)
            return dosql(con,sql)
        logger.info("column already exists")


# copies a table into another database
def copy_table_to_db(tablename, from_con, to_con, override_table = False, supress_output = False, logger=logging):
        '''
            This function will copy one table form the from database 
            and insert it into the to database 

            if 'select * from tablename' fails it will return None and print an error. 
            if the table cannot be created. it will fail and return all rows. 

            returns a list of rows that failed.
                in the form [
                            ([list of values for this row], explanation),
                            ...
                            ]
            returns None if we cannot get key attributes of the table for some reason. 
            i.e. there is an exception thrown while trying to get the description 
            or finding the keys 

            if supress_output is true. returns nothing. 
        '''
        # decide whether to drop table
        if override_table:
            if tablename in get_all_tablenames(to_con):
                try:
                    logger.info('Dropping table %s'%tablename)
                    drop_table(to_con, tablename)
                except Exception as e:
                    raise Exception('unable to drop table, we cannot override table as requested. error-> %s'%str(e))
        
        # get the info from the table in the from db

        # encapsulate the tablename in " to accomodate spaces. a few tables have spaces
        select_stmnt = 'SELECT * FROM "{tablename}";'.format(tablename=tablename)

        desc = None # column descriptions
        rows = None # records matching column descriptions
        keys = None # list of column names that form the primary key
        try:
            desc,rows = dosql(from_con, select_stmnt)
        except Exception as e:
            logger.error('Error Running sql: %s error -> %s' %(select_stmnt, str(e)))
            return None
        try:
            keys = get_primary_keys(from_con, tablename)
        except Exception as e:
            logger.error('Error finding the primary keys for table %s. error -> %s' %(tablename, str(e)))
            return None
            
        # create the new table
        try:
            create_table(to_con, tablename, desc, keys=keys, logger=logger)
        except Exception as e:
            logger.error('Error Creating table %s. Error-> %s'%(tablename, str(e)))
            if not supress_output : return [(row, 'Could not create table bc error: %s'%str(e)) for row in rows] # make it a list of tuples
            else: return None 
        # insert the old table into the new table
        failedrows = bulk_insert(to_con, tablename, rows, logger=logger)
        if not supress_output: return failedrows # a list of tuples (row values, explanation)
        else: return None 

# clears a database
def drop_all_tables(con,logger=logging):
    '''
        drops all tables in a database. 
        if a table cannot be dropped for some reason tha name of the table will
        be appended to a list and that list will be returned at the end.
    '''
    failedtables = []
    tnames = get_all_tablenames(con)
    for name in tnames:
        try:
            drop_statement = 'drop table ' + name 
            print(drop_statement)
            dosql(con, drop_statement)
        except Exception as e:
            logger.error("--> Failed statement: %s. stack:\n%s" %(drop_statement,trace.print_stack()))
            failedtables.append(name)
    return failedtables

# get the connection string for a connection
def get_connect_string(conn):
    ''' 
        returns the string used to open the connection 
        We use 2 types of connections. via mysql.waisman.wisc.edu
        and paths to MS access. 

        if the connection is to a MYsql server this will return DSN='dbconnection name'
        if the connection is to an access path this will return DBQ='path'

        if it's an access path you will need to use db.win_connect_mdb to open.
        while mysql can be opened via the following:
            db.connect(get_connect_string(conn))
    '''
    # __dict__ returns all members of conn,
    # pypyodbc connection objects have a connectString member
    # which has 2 pieces of info, the driver and the DB path seperated by ';'
    return str(conn.__dict__['connectString'].split(';')[-1])

def get_all_tabledetails(con):
    '''
        returns table details on all tables
    '''
    cur = con.cursor()
    return [t for t in cur.tables()]


def read_sqlite_tables_from_sqlite_cursor(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cur.fetchall()]

def get_all_tablenames(con):
    '''
        returns the names of all the tables in the database
    '''

    if isinstance(con, sqlite3.Connection):
        return read_sqlite_tables_from_sqlite_cursor(con.cursor())

    tabledetails = get_all_tabledetails(con)
    return [t[2] for t in tabledetails]

def drop_table(con,tablename):
    '''
        drops a table
    '''
    dosql(con,'drop table '+'"'+'%s"' %tablename)


def set_one_column_to_null_sqlite_and_sql(con: sqlite3.Connection, tablename: str, column: str, logger=logging) -> bool:
    statement = "UPDATE `{tablename}` SET `{column}` = NULL;".format(tablename=tablename,column=column)
    try:
        dosql(con, statement)
        return True
    except Exception as e:
        logger.WARN(str(e))
        return False


def set_columns_to_null(con: Union[pyodbc.Connection,sqlite3.Connection],
                            tablename: str , columns: Union[str, List[str]], logger=logging) -> List[str]:

    if type(columns) is str:
        # Convert one column to a list
        columns = [columns]

    if type(columns) is not list:
        raise ValueError("wrong column type")

    if type(con) is not pyodbc.Connection and type(con) is not sqlite3.Connection:
        raise ValueError("Wrong connection type")

    # process columns as list.
    # sqlite and sql use the same syntax for setting column

    failed_to_clean_columns = []
    for col in columns:
        if not set_one_column_to_null_sqlite_and_sql(con, tablename, col, logger):
            failed_to_clean_columns.append(col)
    return failed_to_clean_columns

def drop_columns(con,tablename, columns, logger=logging):
    '''     Drops a single column or list of columns from a table in db connection
            provide columns as a string or a list of strings.
            will return a list of tuples for each column that failed to be dropped

            It differentiates the implementation for sqlite3 from mysql, but sqlite3 might
            be slower in executing
    '''


    cols_failed_to_be_dropped = []
    if type(columns) is str:
        columns = [columns]
    if type(columns) is list:

        if isinstance(con, sqlite3.Connection):
            drop_column_sqlite3(con, tablename, columns)
            return

        for column in columns:
            statement = 'ALTER TABLE "%s" DROP Column "%s";'%(tablename,column)
            logger.info(statement)
            try:
                dosql(con, statement)
            except Exception as e:
                cols_failed_to_be_dropped.append((column, e))

def drop_column_sqlite3(con, tablename, columns, logger=logging):
    """
     It can be awkward for Sqlite to drop a column because it doesn't support the drop column
    by its design ( syntax support limited). The work around can cost too much
    since it requires copy of a table for droping a column. If I want to manipulate column in the future
    for table in sqlite3, it's better to do it in the memory representation and then create a new table for it.

    :param con: connection
    :param tablename: the table which the columns should get dropped from
    :param columns: list of columns that need dropped
    :param logger: logger
    :return:
    """
    all_cols = get_columns(con, tablename=tablename)
    allowed_cols = filter(lambda col: col not in columns, all_cols)

    temp_table_name = "temp1"
    new_table_stmt = "CREATE TABLE IF NOT EXISTS {tempTable} AS SELECT {columns} FROM {sourceTable};" \
        .format(columns=",".join(allowed_cols), sourceTable=tablename, tempTable=temp_table_name)
    drop_table_stmt = "DROP TABLE {sourceTable};".format(sourceTable=tablename)
    rename_table_stmt = "ALTER TABLE {tempTable} RENAME TO {sourceTable};" \
        .format(tempTable=temp_table_name, sourceTable=tablename)

    dosql(con, new_table_stmt)
    dosql(con, drop_table_stmt)
    dosql(con, rename_table_stmt)
    return

def get_columns(con, tablename):
    '''
        returns a list of the column names of a table
    '''

    if isinstance(con, sqlite3.Connection):
        sqlite_cur= con.cursor()
        sqlite_cur.execute('SELECT * from "%s" LIMIT 1' % tablename)
        return [ i[0] for i in sqlite_cur.description]
    else:
        cur = con.cursor()
        cur.columns(table=tablename)
        return [i[3] for i in cur]

def get_rows(con,tablename, logger = logging):
    '''
        Returns the rows as lists
    '''
    sql = "SELECT * from '%s';" %tablename
    desc,rows = dosql(con,sql, logger=logger)

    return rows

def get_table(con,tablename):
    '''
        gets the rows and details of a table
        returns desc,rows. 
    '''
    return dosql(con, 'SELECT * FROM '+'"'+'%s"'%tablename)


# updates an entire column of a table.
def update_column(con, tablename = None, values_to_set = None, col_to_update = None, pk_rows = None, override_columns = True, logger = logging):
    '''
        inserts all the values from values_to_set into table.
        the location of the insert is dependant on primaries

        table -> string, name of table to update,
        values_to_set -> list of values to set, or list of lists
        col_to_update -> column where values to set will be inserted, or list of column names The ordering should match the values
        pk_rows -> dict of rows where the keys are column names and the values are rows in that column

        NOTE: that the rows in pk must match those of the values. 
        i.e. we use the keys to decide were to insert the value and they are linked by index
        values = [1,2,3]
        keys {'familyid':[a,b,c]}
        UPDATE TABLE table set columnname = 1 where familyid=a
        UPDATE TABLE table set columnname = 2 where familyid=b
        UPDATE TABLE table set columnname = 3 where familyid=c

        if any records fail to get updated the primary keys will be saved for inspection afterwards and an error logged.
        the list contains a string that is the where_clause i.e. 'pk=value AND pk=value'. that should be enough to identify the problem.  

        returns -> list of successful_updates, list of failed_updates, list of statuses to allow mapping between what you 
                passed in and what came out. True corresponds to success, false to failure. all the things should be ordered.  

        raises -> waisman_errors.empty_list_warn if no values to update are passed in

        '''
    successful_updates = []
    failed_updates = []
    stats = []  

    if len(values_to_set) == 0:
        raise empty_list_warn('You did not supply any values to be updated')

    if not override_columns:
        raise NotImplementedError('Override columns is the only implemented option at the moment. Do not pass false to override_columns')

    ### __ CONVERT ARGUMENTS TO MOST GENERAL FORM __
    # We're allowing multiple columns to be updated at once. so make it look like there are multiple columns
    if type(col_to_update) is str: # if they only supplied one column name
        col_to_update = [col_to_update]
    # if values is a list of srtings. make it a list of one list of strings. so  it can be like a list of many string lists.
    if type(values_to_set[0]) is not list: # this is obviouly just a one-deep array. we want it to be two deep to be general
        values_to_set = [values_to_set]

    ### __ check arguments __
    # number of columns provided should = number of value lists provided
    if len(col_to_update)  is not len(values_to_set):
        raise AssertionError('Number of columns to update (%d) did not match number of value lists provided (%d)'%(len(col_to_update), len(values_to_set))) 
    # all the sets of rows provided should be the same length
    num_rows = {}
    # add the length of each list to a list of lengths
    for pk in pk_rows: 
        num_rows[pk] = len(pk_rows[pk])
    for i,li in enumerate(values_to_set):
        num_rows['values_to_set_%s'%i] = len(li)
    if not iterable_is_all_equal(num_rows.values()):
        raise AssertionError('not all lists provided are of the same length! %s' %prettify_str(num_rows))


    ### __ WRITE THE SQL COMMAND FOR EACH RECORD __

    logger.info('Updating columns one at a time. %s rows'%len(values_to_set[0]))
    cur = con.cursor()

    sqls = []

    for row in range(len(values_to_set[0])):
        sql = 'UPDATE "%s" SET '%tablename
         
        sql += ', '.join([
                        '%s = %s'%(col_to_update[ind], values_to_set[ind][row]) 
                        for ind in range(len(col_to_update))
                        ]) 
        # UPDATE tablename SET 'colname= value, col = value, col = value'
        sql += ' WHERE '
        where_clause = ' AND '.join([
                            "%s='%s'" %(pk, str(pk_rows[pk][row])) for pk in pk_rows
                            ])
        sql += where_clause
        sql += ";"
        # UPDATE tablename SET col = value, col = value WHERE pkcol = value AND pkcol = value

        stat = None
        try:
            # logger.debug('RUNNING SQL: %s' % sql)
            cur.execute(sql)
            # successful_updates.append(where_clause)
            stat = True
        except Exception as e:
            logger.error('  ERROR with SQL (%s)' % e)
            # failed_updates.append(where_clause)
            stat = False
        finally:
            stats.append(stat)
        successful_updates.append(where_clause)

    stat = False
    cur.close()
    con.commit()
    return successful_updates, failed_updates, stats

def create_add_column_stmt_for_string_column(con, basetable, new_col):

    if isinstance(con, sqlite3.Connection):
        return 'ALTER TABLE "%s" ADD COLUMN %s varchar(50)' % (basetable, new_col)
    else:
        return 'ALTER TABLE "%s" ADD COLUMN (%s varchar(50))' % (basetable, new_col)

def duplicate_table_with_primary_key(con, source_dt, dest_dt, primary_keys):
    # It's hard to manipulate sqlite in a column level. Thus, I need to work around it before I create the table
    # Let's try this first.
    columns = get_columns(con, source_dt)
    keys = list(filter(lambda key: key in columns, primary_keys))

    if len(list(keys)) == 0:
        raise fail_to_duplicate_table_err()

    sql = "CREATE TABLE '{des_table}' AS SELECT {key} FROM '{sou_table}';".format(des_table=dest_dt,
                                                                             key=",".join(keys), sou_table = source_dt)

    dosql(con, sql)


def test():
    from connection_coordinator import get_coordinator
    cc = get_coordinator()
    cc.connect()
    #duplicate_table_with_primary_key(cc.sqlite_conn, "calc_4_au_f", "calc_4_au_f_temp", ["twin", "familyid"])
    duplicate_table_with_primary_key(cc.sqlite_conn, "user_sd_auditory original lists", "user_sd_auditory original lists_temp", ["twin", "familyid"])

    # I can write a test for sql
### EOF