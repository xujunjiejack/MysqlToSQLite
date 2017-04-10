import json # for loading settings file
import logging # for logs statements
import re # for regex
import copy as c # for copying lists temporarily 
from db_exporter_kit.waisman_utils import table_conventions as tc
from db_exporter_kit.waisman_utils import db_utils as db
from db_exporter_kit.waisman_utils import waisman_general_utils as u
from datetime import datetime
from db_exporter_kit.waisman_utils import waisman_errors as we
from connection_coordinator import get_coordinator
import sqlite3

exporter = None

class db_exporter():
    
    def __init__(self, path_to_settings = 'settings.json'):
        self.wtp_collab = db.db_connect(DSN='wtp_collab')
        self.wtp_data =  db.db_connect(DSN='wtp_data')
        self.sqlite = get_coordinator().sqlite_conn

        self.load_settings(path_to_settings = path_to_settings)

        # open a diagnostic logger
        self.diag_logger = u.setup_logger('diag_logger',
                                            logfile= self.settings['diagnostic_logfile'],
                                            format = '%(levelname)s:%(message)s')

        self.errors_logger = u.setup_logger('action_errorslog',
                                            logfile = self.settings['errors_logfile'], 
                                            format = '%(message)s', stdout=False)
        
        self.table_logger = u.setup_logger('tables_logger',
                                            logfile = self.settings['tables_logfile'], 
                                            format='%(message)s', stdout=False)                         
    #####################
    ### PUBLIC INTERFACES
    #####################

    # sets self.settings and checks for validity.
    def load_settings(self, path_to_settings = 'settings.json',logger=logging):
        ''' This grabs the json settings file located in this package.
            it also checks that it has all the fields required for the export_db package
            
            The path can be modified in the static section above.


            Settings should have the following:
            collab_db_conn: a string to the DNS of wtp_collab
            wtp_data_conn: a string to the DNS of wtp_data
            diagnostic_logfile: the name for the diagnostic_logfile include commands in `` using datetime to get timestamps
            tables_logfile : the name for the tables logfile include commands in `` using datetime to get timestamps
            tables_we_dont_want_anything_to_do_with : is a list of tablenames we don't want to import and we don't need to use. tables we don't want at the end should just be sanitized
            dob keys: a dict of the form
            {'_column_': # identifiable as a normal primary key
                { '_person_ : a type of person we can associate with this column i.e. father for familyid
                    {'table': where to get dob for this person
                     'column' : where in the table to get dob
                      equalities : [[this.column, that.colum],...]
                      primary_keys : [column name for the keys on this table.],
                      new_col : the name of the new column to contain the age for this person.  
                    }
                    ...
                }
                ...
            }
            known_phases: a dict of the form... each phase has a most recent date associated with it. it's different for each family however. 
            { 'phase name':
                { 'table': where to get the recent date for this table 
                  'column': where in the table to get the date
                }
            }
        '''
        self.settings = None
        errors = []
        self.settings = json.load(open(path_to_settings))

        # Code not clean, but it makes sense. Clean it later if necessary
        if 'collab_db_conn' not in self.settings:
            errors.append('collab_db_conn not found in settings')
        if 'wtp_data_conn' not in self.settings:
            errors.append('wtp_data_conn not found in settings')

        if 'diagnostic_logfile' not in self.settings:
            errors.append('diagnostic_logfile not found in settings')
        else: # WE NEED TO PARSE THE logfile name
            logfile_name = self.settings['diagnostic_logfile']
            new_logfile_name = '' # initialize empty string
            try:
                # the name might have ` characters which indicate a datetime command
                for i, block in enumerate(logfile_name.split('`')):
                    if i%2 == 0: # normal string
                        new_logfile_name += block
                    else: #'now' is block: # a datetime command
                        now = str(datetime.now())[:16] # get only date, hour, minute.
                        now = now.replace('-','_') # illegal character in filenames
                        now = now.replace(' ','') # illegal character in filenames
                        now = now.replace(':','') # illegal character in filenames
                        new_logfile_name += now
                self.settings['diagnostic_logfile'] = new_logfile_name
            except Exception as e:
                errors.append('Incorrect function found in diagnostic logfile name %s. Error: %s'%(self.settings['diagnostic_logfile'], e))
        if('errors_logfile' not in self.settings):
            errors.append('errors_logfile not found in settings')
        else: # WE NEED TO PARSE THE logfile name
            logfile_name = self.settings['errors_logfile']
            new_logfile_name = '' # initialize empty string
            try:
                    # the name might have ` characters which indicate a datetime command
                    for i,block in enumerate(logfile_name.split('`')):
                        if i%2 == 0: # normal string
                            new_logfile_name += block
                        else: #'now' is block: # a datetime command
                            now = str(datetime.now())[:16] # get only date, hour, minute.
                            now = now.replace('-','_') # illegal character in filenames
                            now = now.replace(' ','') # illegal character in filenames
                            now = now.replace(':','') # illegal character in filenames
                            new_logfile_name += now
                    self.settings['errors_logfile'] = new_logfile_name
            except Exception as e:
                errors.append('Incorrect function found in diagnostic logfile name %s. Error: %s'%(self.settings['diagnostic_logfile'], e))
        
        if('tables_logfile' not in self.settings):
            errors.append('tables_logfile not found in settings')
        else: # WE NEED TO PARSE THE logfile name
                logfile_name = self.settings['tables_logfile']
                new_logfile_name = '' # initialize empty string
                try:
                    # the name might have ` characters which indicate a datetime command
                    for i,block in enumerate(logfile_name.split('`')):
                        if i%2 == 0: # normal string
                            new_logfile_name += block
                        else:# 'now' is block: # a datetime command
                            now = str(datetime.now())[:16] # 
                            now = now.replace('-','_') # illegal character in filenames
                            now = now.replace(' ','') # illegal character in filenames
                            now = now.replace(':','') # illegal character in filenames
                            new_logfile_name += now
                    self.settings['tables_logfile'] = new_logfile_name
                except Exception as e:
                    errors.append('Incorrect function found in diagnostic logfile name %s. Error: %s'%(self.settings['diagnostic_logfile'], e))
        if('tables_we_dont_want_anything_to_do_with' not in self.settings):
            errors.append('tables_we_dont_want_anything_to_do_with not found in settings')
        
        # check dob keys dict
        if('dob_keys' not in self.settings):
            errors.append('dob_keys not found in settings')
        else:
            for column in self.settings['dob_keys']:
                for person in self.settings['dob_keys'][column]:
                    if('table' not in self.settings['dob_keys'][column][person]):
                        errors.append('table not found in settings[dob_keys][%s][%s]'%(column,person))
                    if('column' not in self.settings['dob_keys'][column][person]):
                        errors.append('column not found in settings[dob_keys][%s][%s]'%(column,person))
                    if('equalities' not in self.settings['dob_keys'][column][person]):
                        errors.append('equalities not found in settings[dob_keys][%s][%s]'%(column,person))
                    if('primary_keys' not in self.settings['dob_keys'][column][person]):
                        errors.append('primary_keys not found in settings[dob_keys][%s][%s]'%(column,person))
                    if('new_col' not in self.settings['dob_keys'][column][person]):
                        errors.append('new_col not found in settings[dob_keys][%s][%s]'%(column,person))
        
        # check to ensure known phases are all correct
        if ('known_phases' not in self.settings):
                errors.append('Known_phases not found in settings')
        else:
            for phase in self.settings['known_phases']:
                if('table' not in self.settings['known_phases'][phase]):
                    errors.append('table not found in settings[known_phases][%s]'%phase)
                if('column' not in self.settings['known_phases'][phase]):
                    errors.append('column not found in settings[known_phases][%s]'%phase)
        
        # check sanitization settings exist
        if('tables_to_sanitize' not in self.settings):
                errors.append('tables_to_sanitize not found in settings')
        else: # ensure all of them are of the right syntax.
            sanlist = self.settings['tables_to_sanitize']
            for table in sanlist:
                if sanlist[table] != 'all' and sanlist[table] != 'drop' and type(sanlist[table]) != list:
                    errors.append('in tables_to_sanitize there is a syntax error in table %s. should be either "all", "drop", or list of column names')%str(table)
        ######
        # Done checking for errors.
        if len(errors) is not 0: # did we find any? 
            logging.critical('Settings errors:\n\t%s'%('\n\t'.join(errors)))
            exit()
        return self.settings # no errors found. return. 
        
    # compares tablename with a regex list of tables not to include in settings
    def want_to_export(self, tablename, logger=logging):
        '''
        This checks to see if the tablename matches anything we don't want any part of
        if it's somethed we don't want in the new db. return False. else True. 
        '''
        for unwanted_name in self.settings['tables_we_dont_want_anything_to_do_with']:
            if re.compile(unwanted_name).match(tablename): # do regex of the unwanted name to match tablename
                logging.info('table: %s; we dont want anything to do with it. '% tablename
                    +'It matched %s from settings' %unwanted_name)
                return False # we don't want this table
        return True
        ## END want_to_export

    # computes which ages to add to a table. then computes them and adds them
    def add_ages_to_table(self, con, origin_table,
                          recentdate_dict=None,
                          override_columns = True,
                          logger=logging, verb=False):
        '''
            this function is responsible for adding all the ages to a table. 
            it does this by comparing a recent date (i.e. roughly around the 
                time the instrument/study was conducted)
            with the birthdate of the subject. 
                (the subject being everyone we can find)

            table -> table to be calc'ed
            recentdate_dict -> a dictionary of recent dates. you can use this 
                            to specify what dates to use.
                        If this is left None they will be automatically 
                            determined.
                        form->
                        {

                        }
            override_columns -> True/False if False, & table already with these 
                                columns will not be updated for speediness
                                if True. all rows will be updated  (True 
                                by default)

            throws  assertion error if we don't know how to find the most 
                        recent dates.
                    runtime error if we can't create append a column needed


            RETURNS: 
            new_columns = a dict with keys equal to columns added to the table
                            each key points to a dict of successful and
                            unsuccessful rows identified by their primary keys
                            see below for example
            people_not_added = a list of people that failed with a corresponding
                            error message.
                            e.g.
                            [ (primary_key to identify them, error message)
                            ]
            explanations = a zip of all columns that were attempted to be 
                            updated. 
                            e.g.
                            zip(
                                [   (Added T/F, str(primary key), status),
                                    (Added T/F, str(primary key), status)
                                ])
                                Added is true if a value was added to the table
                                meaning the SQL command executed successfully.

                                status will be something having to do with the 
                                age like 'okay' meaning it was calculated and 
                                added successfully
                                or dob(9998) wrong format
                                or recent date missing or something like that.




            -----
            if it cannot parse the tablename given and the recentdate_dict is 
                not provided it throws LookupError
            If successful a dict of each new column added with lists of failed 
                and successfully updated rows. 
            e.g.
            { father age:
                { successful: [familyid=1111 AND twin = 1, 
                                familyid=2222 AND twin = 2]
                  failed : []
                }

            }
            AND a list of people that failed along with the corresponding error.

            really what this does is...
            0. Create a temporary table to hold the information of those primary keys
            1. add a recent date column to the temporary table
            2. add all the dob columns to the temporary table
            3. for each dob column calculate the diff in months between 
                recent and dob
            4. add a new column for each dob to the origin table
            5. drop the temporary table
            (the decision of temporary table is made due to the limit of SQLite that doesn't support drop column
             The work for dropping column require a deepcopy of table, which might hurt the performance

            throws we.fail_to_create_col_err if recent column can't be created 
                for some reason.
            
            '''

        # Can I just filter the table that can calculate the age here? If you have familyid

        # decide whether the table should be added with the age
        # This will throw exception if the table shouldn't have age
        self._decide_table_necessaity_for_age(origin_table, recentdate_dict, logger)

        # Also I think I need to check the table key to decide whether it's valid. If there is no
        # allowed key like "familyid", I should jump
        if not db.is_col_in_fields(con, origin_table, "familyid"):
            raise we.erroneous_col_err("familyid not in the field")

        # Step 0: copy a table with primary key
        # table name will be called temp1
        logger = logging.getLogger("sql2sqlite.exporter")
        temp_table_name = self._duplicate_table_to_temp1(con, source_table=origin_table, verb=verb, logger=logger)

        ### 1. add recent date
        recent_date_col = None # name of column containing the recent date (i.e. date of study)
        cleanupflag = True # we don't need to remove the column if we don't need to add one. i.e. the column we need is already in the table.
        try:
            if verb: logger.info('Finding recent column')

            # append the date column
            recent_date_col = self.add_recent_column(con, temp_table_name, recentdate_dict=recentdate_dict, logger=logger)

            if verb: logger.info('recent_date_col = %s'%recent_date_col)
            # throws errors when things go wrong.
        except we.no_need_warn as e:
            recent_date_col = str(e)
            cleanupflag = False
        except (we.parse_err, we.type_is_misc_warning, we.phase_unrecognized_err) as e:
            errmsg = 'could not append ANY age columns to table %s.'%temp_table_name
            raise we.fail_to_create_col_err(errmsg + 'due to errors: %s'%str(e))

        ### 2. ADD DOBs for all people based on the person_type_dict
        # find which people

        # This require the json setting file I guess
        person_type_dict = self.people_from_table(con, temp_table_name)
            ###TEMP PERSON_TYPE_DICT DEFINITION
                #{  'father':                   # out keys are the type of people supported
                #    {  'table':'',             # defines the table to find the dob for father in
                #       'column':'',            # defines the column in the table above
                #       'primary_keys':['familyid','twin',etc...],        # deifnes the primary keys in the table above
                #       'new_col':'fatherage',  # the name for the age column for his person type
                #       'equalities':[[]]       # standard equalities defined elsewhere. it will probably be tied to primary keys
                #    }
                #   'mother',
                #   'twin',
                #   'sibling'
                #} 
        if verb: logger.info('found the people in the table: %s'%[p for p in person_type_dict])

        new_cols = {}
        cols_to_clean = [] # save for cleanup
        if cleanupflag:
            cols_to_clean.append(recent_date_col) # if we chould clean up the recent_date_col
        failed_people = [] # save for indication of error to the user via return

        for person in person_type_dict:
            dob_col = None
            try:
                if verb: logger.info('\n\n\tadding the dob column for %s'%person)

                dob_col = self.add_dob_column(con, temp_table_name,
                                    person_type_dict=person_type_dict,
                                    person=person,
                                    logger=logger)
                # throws error if something goes wrong. handle them
                
                # if it gets here then table is created successfully.
                cols_to_clean.append(dob_col) # save a list of dob cols for cleanup.
            except we.no_need_warn as e: # if the table is the same as this throws no_need_warn with table name as the arg
                if verb: logger.info('No Need to add any columns, col %s already in this table.'%e)
                dob_col = str(e)
            except we.fail_to_create_col_err as e: # happens if dob column could not be added
                if verb: logger.info('Failed to add a column for %s because %s'%(person, e))
                failed_people.append((person, we.fail_to_create_col_err('Could not create dob col because: %s'%str(e))))
                continue # go to the next person. without doing anything more. here. the dob column for this person was not created.

            if verb: logger.info('calculating and adding ages for %s'%person)
            ### 3/4. __ CALCULATE & ADD AGES __
            # this doesn't throw errors
            # Why does this use the self.wtp_collab instead of con
            successful, failed, explanation = self.append_ages_for_table(con, temp_table_name,
                                    recent_col = recent_date_col, 
                                    dob_col = dob_col,
                                    new_col_name = person_type_dict[person]['new_col'],
                                    primary_keys = person_type_dict[person]['primary_keys'],
                                    override_columns = override_columns,
                                    logger = logger,
                                    dest_table=origin_table
                                    )


            if verb:
                logger.info('Successfully added %s ages'%str(len(successful)))
                logger.info('Failed to add %s ages. check the errors file for specifics'%(len(failed)))
            new_cols[ person_type_dict[person]['new_col'] ] = {}
            new_cols[ person_type_dict[person]['new_col'] ]['successful'] = successful
            new_cols[ person_type_dict[person]['new_col'] ]['failed'] = failed

        # END person loop 

        # 5. __ DROP TEMPARORY TABLE __ i.e. cleanup the table
        logger.info('dropping these columns to cleanup the table %s'%str(cols_to_clean))
        con.commit()
        db.drop_table(con, temp_table_name)

        return new_cols, failed_people
        ################################
        ##END add_ages_to_table


    def _duplicate_table_to_temp1(self, con, source_table, verb, logger):
        if verb: logger.info("Duplicating the table with its keys for future use")
        temporary_table_name = "{source_table}_temp".format(source_table=source_table)

        try:
            db.duplicate_table_with_primary_key(con, source_dt=source_table,
                                            dest_dt=temporary_table_name, primary_keys=["familyid", "twin"])
        except sqlite3.OperationalError as e:
            # Drop the duplicate if necessarily
            db.drop_table(con, temporary_table_name)
            db.duplicate_table_with_primary_key(con, source_dt=source_table,
                                                dest_dt=temporary_table_name, primary_keys=["familyid", "twin"])
            #raise we.fail_to_duplicate_table_err("%s"%e)

        if verb: logger.info("Duplicating table success")
        return temporary_table_name

    def _decide_table_necessaity_for_age(self, table, recentdate_dict, logger):
        try:
            self.determine_recent_date_location(table, recentdate_dict,logger=logger)
            # throws errors if anything goes wrong.
            # like the recent date can't be determined.
        except we.parse_err as e:
            raise we.parse_err('Could not determine recent date. due to parsing error: %s' % str(e))
        except we.type_is_misc_warning as e:
            raise we.type_is_misc_warning(
                'Could not determine recent date as type of %s was determined to be misc' % table)
        except we.phase_unrecognized_err as e:
            raise we.phase_unrecognized_err(
                'Could not determine recent date to to phase being unrecognized: %s' % str(e))

    # ensures that this table doesn't have anything that needs to happen to it.
    # if it does contain sensitive data it will remove it.
    def sanitize(self, table, con, logger=logging):
        ''' This function is supposed to remove columns from a table so that the table no longer has any 
            sensitive information. however, we need to know what columns contain information that's sensitive.
            We get that from the settings file. there should be a dict called tables_to_sanitize.

            this dict should contain keys that are tablenames and be a list of column names to remove.
            or the keystring "drop". which tells this function to drop the table.
            or the keystring "all". which tells this function to drop all columns from a table, but keep the table

            if table should be dropped and it fails to do so will throw an assertionError
            if the settings file has an unrecognized type it will throw a TypeError
            if the it tries to drop any number of columns and any fail it will return a list of tuples like so:
                (columnname that failed to be dropped, error)
            '''
        sanlist = self.settings['tables_to_sanitize']
        if table in sanlist:
            if sanlist[table] == 'drop':
                try:
                    db.drop_table(con, table)
                except db.ProgrammingError as e:
                    raise AssertionError('Table %s could not be dropped error-> %s'%(table, str(e)))
            elif type(sanlist[table]) is list: # sanlist[table] is a list of columns to be dropped
                cols_failed_to_be_dropped = db.drop_columns(con,table,sanlist[table],logger=logger)
                return cols_failed_to_be_dropped
            elif sanlist[table] is 'all':
                cols_failed_to_be_dropped = db.drop_columns(con,table, db.get_columns(con,table), logger=logger)
                return cols_failed_to_be_dropped
            else: 
                raise TypeError('tables_to_sanitize at table %s is an unrecognized type or keyword.'%table\
                                     + ' not sure what to do with it. value=%s')%str(sanlist[table])


        ## END sanitize

    # adds a single dob column to a table
    def add_dob_column(self, con, tablename, person, person_type_dict = None, logger=logging):
        ''' Primary abstraction used to clean up adding ages to a table. but also useful if you're working in the command line. 
            this is should be a stateful function. so it will either add the column correctly or leave it unchanged.
            args: 
            con: the db in which this table exists
            tablename : the table which to add to
            person_type_dict: a dictionary in the form returned from people_from_table. the raw definitions are found in settings.
            logger : where to print to. this doesn't actually print anything. but still

            returns :
                If column added successfully -> the name of the column created
                if column not added successfully -> throws we.fail_to_create_col_err
                if it turns out the table to add to already has the date needed in it Throws we.no_need_warn(col_name, "just use the same table")

        '''
        if person_type_dict is None:
            raise we.expected_arg_not_provided_err('person_type_dict is expected')

        # add the dob of that type of person (i.e. cg1, cg2, twin, sibling, etc.)
        cols_appended = None
        cols_failed = None
        cols_not_full = None
        cols_not_dropped = None
        try:
            cols_appended, cols_failed, cols_not_full, cols_not_dropped = \
            self.append_date_col_to_table(\
                con=con,
                basetable= tablename,
                from_table = person_type_dict[person]['table'],
                date_col = person_type_dict[person]['column'],
                equalities = person_type_dict[person]['equalities'],
                logger = logger)
        except we.no_need_warn:
            raise we.no_need_warn(person_type_dict[person]['column'])
    
        if len(cols_appended) is not 1: # then we did not create and fill the column
            if len(cols_not_full) is not 0: # then we created the column. but we did not fill the column. drop it. 
                for col in cols_not_full: # it supports multiples. 
                    db.drop_col(con, tablename, col, logger=logger)
            raise we.fail_to_create_col_err('Did not create dob column! append_date_col_to_table returned %s'%str(ret))
        
        dob_col = cols_appended[0] # if everything went okay 
        return dob_col

    # adds a the recent date column to a table
    def add_recent_column(self, con, tablename, recentdate_dict =None, logger=logging ):
        ''' Primary abstraction used to clean up adding_ages_to_table. but also useful for high level stuff.
            this should be a stateful funtion. so it will either work completely or not at all.
            args:
            con: the db the table exists in
            tablename: the table to add dates to
            recentdate_dict : a dictionary with all the definitions needed. in the form provided by...... we'll figure that out....
            logger : where to write to. not that this writes too much....

            returns:
                If we don't need to add any columns
                    throws we.no_need_warn(col_name) -> with the name of the column already in the table to use
                if successfully adds column -> column name
                if not -> depends on why;
                    if we couldn't determine the most recent date to use.
                        throws we.parse_err if tablename couldn't be parsed
                        throws we.type_is_misc_warning if table type determined to eb misc. i.e no recent date
                        throws we.phase_unrecognized_err if phase is unrecognized 
                    if we fail to create th column
                        throws we.fail_to_create_col_err
            '''
        ##### __ WHERE TO GET RECENT DATES FROM? __
        # this handles if recentdate_dict is None...
        try:
            recent_table, recent_date_col = self.determine_recent_date_location(tablename, recentdate_dict,logger=logger)
            # throws errors if anything goes wrong.
            # like the recent date can't be determined. 
        except we.parse_err as e:
            raise we.parse_err('Could not determine recent date. due to parsing error: %s'%str(e))
        except we.type_is_misc_warning as e:
            raise we.type_is_misc_warning('Could not determine recent date as type of %s was determined to be misc'%tablename)
        except we.phase_unrecognized_err as e:
            raise we.phase_unrecognized_err('Could not determine recent date to to phase being unrecognized: %s'%str(e))
        

        ### __ ADD RECENT DATES TO TABLE __
        # append recent_date column to table. 
        equalities = [['familyid', 'familyid']]
        # equalities is a list of tuples with the name of columns to match while appending
        if recentdate_dict and 'equalities' in recentdate_dict:
            equalities = recentdate_dict['equalities'] 
        
        try:
            ret = self.append_date_col_to_table(\
                con = con,
                basetable=tablename,            # name of the table to append this too  
                from_table= recent_table,     # name of the table to get the recnt dates from
                date_col= recent_date_col,# name of the column in recent_table to append
                equalities=equalities,          # list fields to match while appending
                logger=logger)
        # returns (cols appended, cols failed to be created, cols, failed to be filled, cols failed to be dropped)
        except we.no_need_warn: # thrown if table already contains the column needed
            raise we.no_need_warn(recent_date_col)
        
        # Check that it was successful so that we can continue.
        cols_appended = ret[0]
        cols_failed_to_be_filled = ret[2]
        if len(cols_appended) is not 1: # then we did not create and fill the column! we cannot continue
            if len(cols_failed_to_be_filled) is not 0: # then we created the column. but we did not fill the column. drop it. 
                for col in cols_failed_to_be_filled: # it supports multiples. 
                    db.drop_columns(con, tablename, col, logger=logger)
            raise we.fail_to_create_col_err('Did not create recent column! append_date_col_to_table returned %s'%str(ret))
        
        recent_col = cols_appended[0] # if everything went okay
        return recent_col 



    ##########################
    ### PRIVATE METHODS
    ##########################

    # finds the table and column with the date of any instrument conducted 
    def determine_recent_date_location(self,tablename, recentdate_dict,logger=logging):
        '''
            Helper method for add_ages_to_table. This determines the best 
            location to pull recent dates from.
    
            If recentdate_dict is provided just use that.
            else, we need to figure it out based on the settings provided
    
            recentdate_dict -> 
            {
                table:'tablename',
                column:['column',column...] # list of columns considered valid
            }
            returns recent_table, recent_date_col
            if the table turns out to be a misc table throws we.type_is_misc_warning
            if table is parsed and phase is unrecognized throws we.phase_unrecognized_err
            if table is parsed in a way so that I can't do anything with it and it's not misc throw parse_err
        '''
        if recentdate_dict:
            return recentdate_dict['table'], recentdate_dict['column']
        
        # else recent date dict was not provided we need to figure it out.
        parsed_tname = tc.parse_tablename(tablename, logger=logger)
        print(parsed_tname)
        # if the table turns out to be misc we won't know the phase. 
        # Therefore we won't know the most recent date.
        if 'type' in parsed_tname:
            # ignore the type
            if parsed_tname['type'] == 'misc':
                raise we.type_is_misc_warning('parse(%s) -> type:misc'%tablename)

        # if there is a phase we might know it
        if 'phase' in parsed_tname:
            if parsed_tname['phase'] in self.settings['known_phases']:
                recent_table = self.settings['known_phases'][parsed_tname['phase']]['table']
                recent_date_col = self.settings['known_phases'][parsed_tname['phase']]['column']
                return recent_table, recent_date_col
            else:# if phase is unrecognized
                raise we.phase_unrecognized_err('parse(%s)->phase:%s. Unrecognized'%(tablename,parsed_tname['phase']))

        # if type isn't present / not misc
        # AND  phase either isn't present or unrecognized
        # I'm not sure what to do with it.
        raise we.parse_err('Table %s got parsed very strangely, unsure what to do with it. Parsing:%s'%(tablename, u.prettify_str(parsed_tname)))

    # appends date column to table
    def append_date_col_to_table(self, 
        basetable=None, 
        from_table=None, 
        date_col=None, 
        equalities = [['familyid','familyid']], 
        con=None, 
        logger = logging):
        '''
            This function appends a date column to a table. matching records based on equalities.
            basetable -> table to be appended to
            from_table -> table to get column from
            date_col -> column of dates to be appended
            equalities -> list of columns to match upon
                e.g. [familyid,familyid],[twin,twin]
                would be extrapllated to.
                UPDATE TABLE ADD COLUMN from_table.date_col;
    
                UPDATE basetable  SET date_col = (
                SELECT date_col FROM from_table  
                WHERE basetable.familyid = from_table.familyid
                )
                
    
                returns [columns added], [(columns failed to be created, explanations)], [(columns failed to be filled, explanations)], [columns failed to be dropped]

                THIS should not raise any errors under normal operation.
                except for if basetable= from_table just use the date_col passed in. no need to add anything more.
                    throws we.no_need_warn
        '''
        if from_table == basetable:
            # no need to add a new column if the from column already exists in the table.
            raise we.no_need_warn('Just use the date_col passed in! no need to add a new column')
        
        cols_failed_to_be_created = [] # a list of columns that could not be created for some reason
        cols_failed_to_be_filled = [] # a list of columns that could not be filled for some reason
        cols_filled_successfully = [] # a list of columns succesfully created and filled. presumably
        cols_failed_to_be_dropped =[] # a list of columns not successfully dropped.

        # if table doesn't already have a from_table.date_col column.
        try:
            # sometimes there will be empty in the date column
            new_col = '%s_%s'%(from_table,date_col.replace(" ", "_")) # define new column name. usually from_table_date_column
            logger.info('Ensuring the table %s has column %s'%(basetable, new_col))

            # try to drop duplicate column
            try:
                logger.info('columns in table: %s'%str(db.get_columns(con,basetable)))

                if new_col in db.get_columns(con, basetable):

                    logger.info('Column already exists. dropping dupicate column %s'%new_col)
                    drop_col_sql = 'ALTER TABLE "%s" DROP COLUMN "%s"'%(basetable,new_col)
                    db.dosql(con, drop_col_sql) # throws an error if it doesn't work. but it should.
            except Exception as e:
                raise we.fail_to_drp_col_err('Failed to drop Column. error -> %s'%str(e))
            logger.info('table %s does not seem to have column %s'%(basetable, str(new_col)))


            try:
                # add the date column
                logger.info('adding column %s'%new_col)

                #add_col_sql = 'ALTER TABLE "%s" ADD COLUMN (%s varchar(50))'%(basetable,new_col)
                add_col_sql = db.create_add_column_stmt_for_string_column(con, basetable, new_col)
                db.dosql(con, add_col_sql) # this should be fine. but if shit fucks up. this can throw an error.
            except Exception as e:
                # I'm using different errors to mean different things. if a column is failed to be created We can't go on. 
                raise we.fail_to_create_col_err('Failed to add column. error->%s'%str(e))

            # table definately has column

            try:
                join_sql = 'UPDATE %s SET %s = ( SELECT "%s" FROM %s WHERE '%(basetable, new_col, date_col, from_table)
                for e in equalities:
                    join_sql  += ' %s.%s = %s.%s '%(basetable,e[0],from_table,e[1])
                    join_sql += 'AND'
                join_sql =join_sql[:-3] # remove the last AND i.e. last 3 chars.
                join_sql += ')' 
        
                #   join_sql =
                #   UPDATE basetable SET from_table_date_col = 
                #       (
                #       Select date_col FROM from_table
                #           WHERE   basetable.familyID = from_table.familyID
                #           AND     basetable.twinID = from_table.twinid
                #       )
                logger.info(' Updating column %s. Running sql %s'%(new_col, join_sql))
                db.dosql(con, join_sql)  # update the table with the recent dates. 

                cols_filled_successfully.append(new_col)
            except ValueError as e:
                # I'm using arbitrary unique exceptions to mean different things. If a column was failed to be set. we need to know about it
                logger.error('Column was failed to be filled. error-> %s'%str(e))
                raise we.fail_to_fill_col_err('Column was failed to be filled. error-> %s'%str(e))
            
        except we.fail_to_create_col_err as e: # column was not created successfully i.e. column was not ADDED.
            cols_failed_to_be_created.append((new_col, str(e)))
        except we.fail_to_fill_col_err as e: # Column was created, but was not filled. i.e. updated.
            cols_failed_to_be_filled.append((new_col, str(e)))
        except we.fail_to_drp_col_err as e:
            cols_failed_to_be_dropped.append((new_col,str(e)))    

        return cols_filled_successfully, cols_failed_to_be_created, cols_failed_to_be_filled, cols_failed_to_be_dropped
        # END append_date_col_to_table

    # determines which kind of people are represented in this table. 
    def people_from_table(self, con, tablename, logger=logging):
        '''
            This returns a dictionary representing useful information about each
            type of person represented within this table.
            i.e. if there is a twin column and familyid we can determine twin 
            age, 
            if. theres familyid then we can determine father and mother age.
    
            returns
            {
                'father':
                {
                    'table':'',
                    'column':'',
                    'equalities':[]     # equalities array is defined elsewhere.
    
                },
                'mother':{},
                'twin':{},
                'sibling :{}       
            } 

            this shouldn't raise any exceptions
        '''
        dob_keys = self.settings['dob_keys']
    
        # let the users know what we have info on.
        info_string_introduction = 'Settings file has info on these columns: ' \
                 + '%s'%str(list(self.settings['dob_keys'].keys())) 
        # let them know what we've found. 
        info_string_cols_found = 'We know about information for these columns: ' 
        people = {}

        # It doesn't use primary key. Thus, I can just use the column in my temporary table
        for col in db.get_columns(con, tablename):
            if col in dob_keys:
                # debug string: we know about information for these columns: 
                    # twin, familyid, sibling etc...
                info_string_cols_found += '%s ,'%col  
                for person in dob_keys[col]:
                    people[person] = dob_keys[col][person] 
                    # this is a dict of things about each person type. 
    
        #logger.info(info_string_introduction + info_string_cols_found)
        return people

    # add ages to a table
    def append_ages_for_table(self, con, tablename,
        dest_table = None,
        recent_col = None, 
        dob_col = None, 
        new_col_name= None, 
        primary_keys=None, 
        override_columns = True, 
        statuses = True,  
        logger = logging):
        '''
            This function is responsible for creating the new column and 
            filling it with ages. 
            it needs to know which table, and which columns  to use. 
            it also needs to know what to call it.
            Additionally it needs to know what the primary_keys are for 
                the table.  
            returns a dict of primary id's 
    
            basically it should run this command. 
            Update tablename set new_col_name = age where 
            primary_key1 = primary_key1 for this row 
            and primary_key2 = primary_key2 for this row,...
            on each record.
    
            returns  
                list of successful_updates identified by the primary keys of 
                the table as passed into this function.
                list of failed_updates identified tthe same way.
                if (statuses) -> a zip of failed & successfull rows. 
                in the following form : 
                    [(T/F, Keys identifying row, age calc_explanation),...]



        '''
        ## __ GET THE ROWS TO CALCULATE AGES WITH __
        # get a list of rows in this table  
        rows = db.get_rows(con, tablename, logger=logger)   
        # get a list of columns in this table
        cols = db.get_columns(con,tablename) 
    
        # all these lists should be the same length. one for each row. 
        # they may be None. all None's should be 9998(wtp_convention)
        dob_rows = [r[cols.index(dob_col)] for r in rows]
        recent_rows = [r[cols.index(recent_col)] for r in rows]
        
        pk_rows = {}
        for pk in primary_keys: 
            # this produces a dictionary with the key being the column 
            # name and the value a list of rows
            pk_rows[pk] = [r[cols.index(pk)] for r in rows]
    
    
        ### __ CALCULATE THE AGES __
        ages, stats = self.calc_ages(dob_rows, recent_rows)
            # stats is a list of strings either okay. or an error string
            # but we'd like to replace the error string with the actual 
            # table and column we're pulling from
        recent_rep_str = 'recent_row {recent is from column %s}'%recent_col
        dob_rep_str = 'dob_row {dob is from column %s}'%dob_col
        for i,stat in enumerate(stats):
            stat = stat.replace('recent_row', recent_rep_str)
            stat = stat.replace('dob_row', dob_rep_str)
            stats[i] = stat

        ### __ APPEND THE AGES TO THE ORIGIN TABLE __
        # make sure theres a place to put the ages in the table
        failed_updates = None 
        successful_updates = None
        try:
            # add the column
            # check if new _col_name already exists
           #  if new_col_name in cols:
           #     db.drop_columns(con, tablename, new_col_name, logger=logger)
            db.add_column(con, dest_table,
                        new_col_name, 
                        'INT', 
                        logger=logger)
        except Exception as e:
            logger.error('Something went wrong creating the column to '\
                 + 'append the ages into. %s' %e)
            # if this triggers. all values will return in the failed category.            
        # fill it with values
        successful_updates = []
        failed_updates = []
        bitmap  = []
        try:
            successful_updates,     \
            failed_updates,         \
            bitmap = db.update_column(  con,
                                        tablename = dest_table,
                                        values_to_set = ages,
                                        col_to_update = new_col_name,
                                        pk_rows = pk_rows,
                                        override_columns = override_columns,
                                        logger=logger)

        except we.empty_list_warn as e: # gets thrown is ages is empty
            logger.info('No ages provided for table %s. Error: %s'%(tablename,
                str(e)))
        
        if statuses: # then we want verbose return saying why things didn't work 
            # copy a tmp version of the mutable object   
            tmp_success = c.deepcopy(successful_updates) 
            tmp_fail = c.deepcopy(failed_updates)
            updated_rows = []
            explanation = []
            # for i,bit in enumerate(bitmap):
            #     if bit: # then things was a successfull row.
            #         updated_rows.append(tmp_success.pop(0))
            #     else: # Then this was a failed row.
            #         #   We want to say why.
            #         #   it probably has something to do with the age calc
            #         updated_rows.append(tmp_fail.pop(0))
            #     explanation.append(stats.pop(0))
    
            explanationzip = zip(bitmap, updated_rows, explanation)
                            # (T/F, keys, age calculation explanation)
            return successful_updates, failed_updates, explanationzip
    
        else: return successful_updates, failed_updates
    
    # calculates the ages for a bunch of rows.
    def calc_ages(self,dob_rows, recent_rows, statuses=True):
        '''
            Creates a set of rows matched to the provided rows
            filled with the ages calculated between them.
            returns ages and statuses if statuses is true
            else just the list of ages. 
    
            statuses is a string describing the problem if any while 
            calculating the ages.
            possibilities are :
            okay, recent_row/dob_row is missing/not in the correct format
            if theres a problem age is 9998 missing data in wtp_convention
        '''
        if len(dob_rows) != len(recent_rows):
            raise RuntimeError('dob_rows(%d)'%len(dob_rows)     \
                + ' and recent rows(%d)'%len(recent_rows)       \
                +'do not have the same length.')
    
        ages = []
        status = []
        for i in range(len(recent_rows)): 
            # get the index for either of the row sets
            age = None
            try:
                age = self.calc_age(recent_rows[i], dob_rows[i])
                status.append('okay')
            except (we.date_empty_err, we.date_wrong_format_err) as e:
                age = 9998
                status.append(str(e).replace('fdate',
                                            'recent_row').replace('tdate',
                                                                'dob_row'))
            
            # append the age calculated here. it accounts for missing pieces. 
            ages.append(age) 
        if statuses:
            return ages, status
        else:
            return ages
    
    # calculate the difference in months of two dates 
    def calc_age(self, tdate,fdate, format_string='%m/%d/%Y'):
        '''
            calculates the difference in months betweet two dates given either 
            as datetime objects of as a string of the form 
            format_string using standard satetime conventions.

            throws we.date_empty_err
            or we.date_wrong_format_err

            you should probably replace fdate and tdate with the column names 
            they come from. this function isn't aware of that.
        '''
        if tdate in self.settings['empty_equivs']:
            raise we.date_empty_err('tdate is empty!')
        if fdate in self.settings['empty_equivs']:
            raise we.date_empty_err('fdate is empty!')
    
        
        # make sure the dates are datetimeobjects
        try:
            if type(tdate) is not datetime:
                tdate = datetime.strptime(tdate, format_string)
        except ValueError:
            # if the data is in string format then we check for %m/%d/%Y. If the date is not in that format then we check for another date format : '%Y-%m-%d %H:%M:%S'
            # This has to be extended as a seperate case as the code has been modified and not in the initial design.
            try:
                if type(tdate) is not datetime:
                    tdate = datetime.strptime(tdate, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise we.date_wrong_format_err('tdate(%s) '%tdate           \
                    + 'is not in the correct format(%s)'%format_string)
        
        try:
            if type(fdate) is not datetime:
                fdate = datetime.strptime(fdate, format_string)
        except ValueError:
            try:
                if type(fdate) is not datetime:
                    fdate = datetime.strptime(fdate, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise we.date_wrong_format_err('fdate(%s)'%tdate            \
                    + ' is not in the correct format(%s)'%format_string)
        
        return (tdate.year - fdate.year)*12 + tdate.month - fdate.month


def get_db_exporter():
    global exporter
    if exporter is None:
        exporter = db_exporter()
        exporter.load_settings()
    return exporter
