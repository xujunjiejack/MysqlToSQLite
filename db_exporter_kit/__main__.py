from waisman_utils import db_utils as db # contains all the database functions
from waisman_utils import table_conventions as tc # contains all the table convention functions
import export_db as exp #import want_to_export, add_ages_to_table, sanitize, load_settings
import logging # used to make logs statements
from waisman_utils import waisman_general_utils as u
from waisman_utils import waisman_errors as we
import traceback

exporter = exp.db_exporter()

# load the settings from file
settings = exporter.settings

# grab the loggers
diag_logger = exporter.diag_logger # to hold the details as it goes through stuff
table_logger = exporter.table_logger # to hold details about what tables are missing ages
errors_logger = exporter.errors_logger # to hold high level errors that we can fix manually like tables that don't get set up and stuff. 


# exports all tables, & sanitizes them. good for starting fresh.
def doall():
    db.drop_all_tables()
    export_all_tables()
    db.close_old_db()
    sanitize()

def export_all_tables(startAt = None):
    errors_logger.info('\n\n')
    errors_logger.info('=======================================')
    errors_logger.info('\t ERRORS WHILE IMPORTING TABLES ')
    errors_logger.info('=======================================\n\n')
    
    ### __ EXPORT TABLES FROM OLD TO NEW DB __
    # export all the tables to the new database
    tablenamelist = db.get_all_tablenames(exporter.wtp_data)
    if startAt:
        try:
            # get the list starting after 
            tablenamelist = tablenamelist[tablenamelist.index(startAt):] 
        except:
            raise Exception('You asked us to start at table %s. '%str(startAt) 
                                +'However that table was not found. Stopping.')
    for table in tablenamelist:
        # unless it is a table we want nothing to do with.
        if exporter.want_to_export(table,logger=diag_logger):
            failed_records = db.copy_table_to_db(table, 
                                                exporter.wtp_data, 
                                                exporter.wtp_collab,
                                                logger=diag_logger, 
                                                override_table=True)
            if len(failed_records) > 0:
                errors_messages_for_records = map(
                        lambda record: '\t {0}:{{\t\t EXPLANATION :"{1}"}},'.format(str(record[0]), str(record[1])),
                        failed_records
                        )
                errors_message = '__{0}__\n {{ {1}\n }}'.format(table, "\n".join(errors_messages_for_records))
                errors_logger.info(errors_message)

               # errors_logger.info('__%s__\n{'%table)
               #     for rec in failed_records:
                    # the first index is the record itself,
                    # the second is the explanation
              #      errors_logger.info('\t {0}:{\t\t EXPLANATION :"{1}"},'.format(str(rec[0]), str(rec[1])))

                    # rec_str = str(rec[0]) # it's a tuple.
                    # explanation = str(rec[1])

                    # errors_logger.info('\t "%s":{'%rec_str)
                    # errors_logger.info('\t\t EXPLANATION: "%s"'%explanation)
                    # errors_logger.info('\t},')
           #     errors_logger.info('\n}')
    

def close_old_db():
    # close old database we don't need it anymore
    exporter.wtp_data.close()
    
def add_age_cols(startAt=None):
    errors_logger.info('=======================================' + \
                        '\n=======================================' + \
                        '\n=======================================')
    errors_logger.info('\t ERRORS WHILE ADDING DATE COLUMNS TO ALL TABLES ')
    errors_logger.info('=======================================\n\n')
    ### __ ADD ALL AGE COLUMNS __
    # iterate through all the tables in the new db.
    tablenamelist = db.get_all_tablenames(exporter.wtp_collab)
    if startAt:
        try:
            # get the list starting after given
            tablenamelist = tablenamelist[tablenamelist.index(startAt):] 
        except:
            raise ValueError('You asked us to start at table %s.'%str(startAt) 
                    + ' However that table was not found. Stopping.')

    for i, table in enumerate(tablenamelist):
        diag_logger.handlers[0].flush()
        errors_logger.handlers[0].flush()
        table_logger.handlers[0].flush()
        # iterate for logs

        # calculate the ages that can be calculated for this table and append them
        diag_logger.info('\n=======================================' +\
                            '\n=======================================')
        diag_logger.info('\tADDING AGES TO TABLE: ___  %s ___ '% table)
        try:
            new_columns,            \
            people_not_added,       \
            explanations = exporter.add_ages_to_table(exporter.wtp_collab, 
                                                        table, 
                                                        logger=diag_logger,
                                                        verb=True)
            # new_columns is a dictionary

            # Output missing information
            if len(people_not_added) > 0:
                errors_logger.info('People whos ages were failed to be computed:'  )
                for person in people_not_added:
                    errors_logger.info('\t%s, Reason: %s'%(person[0],person[1].__repr__()))
            
            people_with_missing_age_info = [explanation[2] != 'okay' for explanation in explanations]
            #for explanation in explanations:
            #    if explanation[2] != 'okay':
            #        people_with_missing_age_info.append(explanation)

            if len(people_with_missing_age_info) > 0:
                diag_logger.info("table %s has %d people where "%(table,
                                            len(people_with_missing_age_info)) 
                                + "age couldn't be calculated")
                table_logger.info('\n===================================')
                table_logger.info('===================================')
                table_logger.info('\t People in table %s '%table + 
                                    'with missing age info')
                for person in people_with_missing_age_info:
                    table_logger.info('%s : %s'%(person[1], person[2]))

            diag_logger.debug(str(new_columns)) # print to console the compacted lists. they can be quite long.

            # construct the error message for the tables file. 
            to_print_to_tables = { person_type: content['failed']
                                  for person_type, content in new_columns.items() if len(content['failed']) > 0}
     #       for person_type in new_columns:
     #           if len(new_columns[person_type]['failed']) > 0:
     #               to_print_to_tables[person_type] = new_columns[person_type]['failed']

            if len(to_print_to_tables) > 0:
                table_logger.info(u.prettify_str(to_print_to_tables))

        except we.fail_to_create_col_err as e: # if columns can't be appended for some reason
            diag_logger.info(e)
            errors_logger.info(e)

def sanitize():
    errors_logger.info('\n\n')
    errors_logger.info('=======================================')
    errors_logger.info('\t ERRORS WHILE SANITIZING ALL TABLES ')
    errors_logger.info('=======================================\n\n')
    ### __ REMOVE ALL SENSITIVE DATA __
    # iterate through the tables again
    for table in db.get_all_tablenames(exporter.wtp_collab):
        # sanitize it
        failed_columns = []  # sanitize returns a list of failed columns or throws an exception
        try:
            failed_columns = exporter.sanitize(table, con=db.db_connect(DSN="wtp_collab"))
        except AssertionError as e: # happens if table could not be dropped some reason
            errors_logger.error(str(e))
        except TypeError as e: # happens if settings file is wrong
            errors_logger.info(str(e))

        if failed_columns is None:
            pass
        elif len(failed_columns) > 0:
            errors_logger.info('__%s__:'%table)
            errors_logger.info('{')
            for col in failed_columns:
                col_name = col[0] # it's a tuple the first index is the column name
                expl = str(col[1])       # the second ndex is the error
                errors_logger.info('\t%s: %s' % (col_name, expl))

def sanitize_one_table(table_name):
    errors_logger.info('\n\n')
    errors_logger.info('=======================================')
    errors_logger.info('\t ERRORS WHILE SANITIZING ONE TABLE ')
    errors_logger.info('=======================================\n\n')
    failed_columns = []

    try:
        exporter.sanitize(table_name)
    except AssertionError as e:
        errors_logger.error(str(e))
    except TypeError as e:
        errors_logger.info(str(e))

    if len(failed_columns) > 0:
        errors_logger.info('__%s__:' % table_name)
        errors_logger.info('{')
        for col in failed_columns:
            col_name = col[0]  # it's a tuple the first index is the column name
            expl = str(col[1])  # the second ndex is the error
            errors_logger.info('\t%s: %s' % (col_name, expl))


print('This Runs in a loop. The Run lots of tables option goes through tables in \norder according to how pypyodbc serves them. I think alphabetically')
while True:
    print('')
    print('1 = export lots of tables to collaborator db')
    print('2 = export one table to collab db')
    print('3 = append ages to all tables in collab db')
    print('4 = append ages to one table in collab db')
    print('5 = sanitize all tables in collab db')
    print('6 = sanitize one table in collab db')
    print('7 = freeform')
    print('8 = get a table')
    print('quit to quit')
    inp = input('>>> ')
    if inp == 'quit':break
    if inp == '1': # export all tables
        inp = input('Would you like to start at a specific table? if so, enter the name of the table. Enter nothing to start from the beginning \n>>>')
        if inp == '':
            inp = None
        export_all_tables(startAt = inp)
    elif inp == '2': # export one table
        inp = input('what table would you like to copy over?\n>>>')

        # copy over a table to the other db. 
        failed_records = db.copy_table_to_db(inp, exporter.wtp_data, exporter.wtp_collab,logger=diag_logger, override_table=True)
        if len(failed_records) > 0:
            errors_logger.info('__%s__\n{'%inp)
            for rec in failed_records:
                rec_str = str(rec[0]) # it's a tuple. the first index is the record itself, the second is the explanation
                explanation = str(rec[1])
                errors_logger.info('\t "%s":{'%rec_str)
                errors_logger.info('\t\t EXPLANATION: "%s"'%explanation)
                errors_logger.info('\t},') 
            errors_logger.info('\n}')
    elif inp == '3': # append ages to column  
        try:
            inp = input('Would you like to start at a specific table? if so, enter the name of the table. Enter nothing to start from the beginning \n>>>')
            if inp == '':
                inp = None
            add_age_cols(startAt=inp)
        except Exception as e:
            errors_logger.critical(traceback.print_exc())
            errors_logger.critical(e)
    elif inp == '4':
        print('not implemented')
    elif inp == '5': # sanitize all tables
        sanitize()
    elif inp == '6':
        #print('not implemented')
        sanitize_one_table("gen_twins")
    elif inp == '7':
        print('We will now run anything you type. if you enter exit NOT THE FUNCTION it will break the loop ')
        cont = True
        while cont:
            inp = input('>>>')
            if inp == 'exit':
                break
            try:
                print(exec(inp))
            except Exception as e:
                print(e)
    elif inp == '8':
        new_con = None
        inp = input(' What database would you like to select from?\n DSN=')
        try:
            new_con = db.db_connect(DSN=inp)
            inp= input('What table would you like to select?\n>>>')
            columns = db.get_columns(new_con,inp)
            rows = db.get_rows(new_con, inp)

            diag_logger.info('Columns: %s'%str(columns))
            for row in rows:
                diag_logger.info(row)

        except Exception as e:
            print(e)
        finally:
            if new_con:
                new_con.close()

