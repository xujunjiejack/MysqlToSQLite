from connection_coordinator import DataSource, get_coordinator
from migrator_kit.migrate_data import Migrater
from db_exporter_kit.export_db import db_exporter, get_db_exporter
from util.fetch_all_table_name import read_table_names_without_quote
from db_exporter_kit.age_appender import AgeAppender
from db_exporter_kit.data_sanitizor import DataSanitizer
import migrator_kit.table_creator as table_creator
from migrator_kit.checksum_table import ChecksumTable
from util.table_process_helper import TableProcessQueue
from os.path import join, splitext
from loggers import logging
from os import rename
from os.path import exists
import sys
import json
import datetime
import functools
import argparse
from typing import *

logger = logging.getLogger("sql2sqlite.main")
logger.setLevel(logging.INFO)


# Then I can test everything, and move on to implementation of the back arrow.
# configure connection

db_directory = "" # type: str
db_name = "" # type: str
db_archive = "" # type: str
checksum_path = "" # type: str

# The code should be dedicated to the copy. Not mutate. We need a separated function for this
#
trackers = ["data_1_tr", "data_3_tr", "data_4_tr", "data_5_tr", "data_5_tr", "data_6_tr", "data_r1_tr", "data_r1_tr",
            "data_c3_tr", "data_mr_tr", "data_r1_tr", "data_s_tr", "data_at_dates", "data_sd_tr", "data_w14_tr"]
# type: List[str]


def put_trackers_to_the_front(data_tables : List[str]) -> List[str]:
    return_lists = [] # type : List[str]
    for table in data_tables:
        if table not in trackers:
            return_lists.append(table)
        else:
            return_lists.insert(0, table)
    return return_lists


def build_new_db_from_all_tables(cc):

    m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter(), cc=cc)
    age_appender = AgeAppender(cc, get_db_exporter())
    sanitzer = DataSanitizer(cc, get_db_exporter())

    cc.connect()

    table_creator.create_new_table(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())

    cc.connect()
    all_tables = read_table_names_without_quote(cc.sql_cur)
    # all_tables = ["calc_mr_pb_t","data_4_zy_e2","data_3_le_m","user_5_dps_p_2010_0210","data_4_cortid","data_4_au_m"]
    # all_tables = ['data_1_tr','arch_interview 1 response tracker', 'arch_interview 3 response tracker', 'arch_interview 4 response tracker', 'arch_zyg_follow up', 'trash_hhg_sd interventions', 'user_cla_complete sd screener data', 'user_cla_extended milw zips', 'user_cla_sd mpq by twin', 'user_cla_sd tracker dates spc flag']

    all_tables_queue = TableProcessQueue(all_tables)

    all_tables_queue.process_by(m.migrate_one_table)\
                    .process_by(age_appender.append_age_to_one_table)\
                    .process_by(sanitzer.sanitize_one_table)

    checksum_table = ChecksumTable()
    cc.connect()
    checksum_table.create_new_checksum_for_tables(all_tables_queue.success_tables(), cc)
    logger.critical("Success tables are: {0}".format(all_tables_queue.success_tables()))

    logger.critical("Failure tables are: {0}".format(all_tables_queue.failure_tables()))

    return


    # the best way is to create a new table. At the end of the process, the response towards success should
    # be deleting the origin table, and rename it. If the response is failure, then delete this temporary table.

    #   truncate the table, or delete the table
    #   So that I can get around with the winded commit system with my own .
    #   create new one

    #   migrator
    #   append age to it
    #   do data cleaning
    # I need a transaction and rollback system

def update_the_db_development(cc):

    dob_updated_info_tables = ["gen_twins", "gen_parentdates"]
    not_imported_tables = get_db_exporter().unwanted_tables()

    checksum_table = ChecksumTable(ignore_tables=dob_updated_info_tables + not_imported_tables)
    new_tables, updated_tables = checksum_table.tables_for_migration(cc)

    # the update of the tracker should be put before the computation of the age for other tables
    new_tables_with_trackers_front = put_trackers_to_the_front(new_tables)
    updated_tables_with_trackers_front = put_trackers_to_the_front(updated_tables)

    print(new_tables, updated_tables)

    m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter(), cc=cc)
    age_appender = AgeAppender(cc, get_db_exporter())
    sanitzer = DataSanitizer(cc, get_db_exporter())

    test_new_tables = ["data_4_au_t", "data_5_au_t", "data_rd_au_t"]

    # The dob tables should not show up in either the update table and new table
    # It will be imported everyday.

    dob_tables_process_queue = TableProcessQueue(dob_updated_info_tables)

    updated_tables_process_queue = TableProcessQueue(updated_tables_with_trackers_front)
    new_tables_process_queue = TableProcessQueue(new_tables_with_trackers_front)

    # Set up the partial function for later use
    rename_table_to_temp_table_with_origin_temp = \
        functools.partial(table_creator.rename_one_to_temp_table,temp_table_postfix="_origin_temp", cc=cc)

    delete_temp_table_with_origin_temp = functools.partial(table_creator.delete_the_temp_table,
                                                           temp_table_postfix="_origin_temp",
                                                           cc=cc)
    rename_temp_table_with_origin_temp_back_to_origin = functools.partial(table_creator.rename_the_temp_table_back_to_table,
                                                                        temp_table_postfix="_origin_temp", cc=cc)
    create_new_table_from_wtp_data = functools.partial(table_creator._create_one_table_in_sqlite_ , cc=cc, exporter = get_db_exporter()
                                                       )

    # copy all of the table to its corresponding temp table
    # I assume that the dob_tables will always be there
    updated_tables_process_queue.process_by(rename_table_to_temp_table_with_origin_temp)
    dob_tables_process_queue.process_by(rename_table_to_temp_table_with_origin_temp)

    # import ipdb;  ipdb.set_trace()
    # This will delete the failure tables to prevent the tables get processed later
    updated_tables_process_queue.treat_fail_tables_as_error(rename_temp_table_with_origin_temp_back_to_origin)
    dob_tables_process_queue.treat_fail_tables_as_error(rename_temp_table_with_origin_temp_back_to_origin)

    # dob tables can't get sanitized before used by other for computation, because the dob fields are all sensitive data
    roadmap_for_dob_tables = [create_new_table_from_wtp_data,  m.migrate_one_table]
    roadmap = [create_new_table_from_wtp_data, m.migrate_one_table,
               age_appender.append_age_to_one_table, sanitzer.sanitize_one_table, delete_temp_table_with_origin_temp]

    # load in the dob tables
    dob_tables_process_queue.process_by_functions_chain_(roadmap_for_dob_tables)

    updated_tables_process_queue.process_by_functions_chain_(roadmap)
    new_tables_process_queue.process_by_functions_chain_(roadmap)

    dob_tables_process_queue.process_by_functions_chain_([sanitzer.sanitize_one_table, delete_temp_table_with_origin_temp])

    checksum_table.update_the_checksum_of_successful_tables(updated_tables_process_queue.success_tables() +
                                                            new_tables_process_queue.success_tables())

    # detail with failure tables
    def restore_origin_table_from_temp(table):
        return  table_creator.delete_the_table(table , cc) and \
                table_creator.rename_the_temp_table_back_to_table(table, "_origin_temp", cc)

    updated_tables_process_queue.treat_fail_tables_as_error(restore_origin_table_from_temp)

    logger.critical("Success tables are: {0}, {1}".format(updated_tables_process_queue.success_tables(),
                                                          new_tables_process_queue.success_tables()))

    logger.critical("Failure tables are: {0}, {1}".format(updated_tables_process_queue.failure_tables(),
                                                          new_tables_process_queue.failure_tables()))
    new_tables_process_queue.treat_fail_tables_as_error(restore_origin_table_from_temp)
    dob_tables_process_queue.treat_fail_tables_as_error(restore_origin_table_from_temp)

def load_path():
    with open("file_path.json", "r") as f:
        setting = json.load(f)
        global db_name
        global db_archive
        global db_directory
        global checksum_path
        db_name = setting["db_name"]
        db_directory = setting["db_directory"]
        db_archive = setting["archive_dir"]
        checksum_path = setting["checksum_name"]
        return

def generate_archive_file_name(origin_filename):
    file, extension = splitext(origin_filename)
    new_file_name = file + "_" + str(datetime.datetime.now()).replace(" ","_").replace(":", "_") + extension
    return new_file_name


def import_all_of_the_data():
    temp_db_path = join(db_directory, "temp.db")
    #
    #   # Create a temporary db, in case the migration fails.
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = temp_db_path
    cc.connect()
    build_new_db_from_all_tables(cc)
    # my goal is to create a temporary data file, and move the origin one to the test folder, and then rename it

    cc.logger.info("backing up the origin db")
    # Rename the origin db file, put it into an archive folder, and then rename the temporary db to the db.

    origin_file_path = join(db_directory, db_name)

    if not exists(origin_file_path):
        rename(temp_db_path, origin_file_path)
        return

    archive_file_name = generate_archive_file_name(db_name)
    archive_file_path = join(db_archive, archive_file_name)
    rename(origin_file_path, archive_file_path)
    rename(temp_db_path, origin_file_path)


def increment_update_wtp_collab():
    db_path = join(db_directory, db_name)
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = db_path
    update_the_db_development(cc)


if __name__ == "__main__":
    load_path()
    parser = argparse.ArgumentParser(description="Import Data to SQLite")
    parser.add_argument('-a', help="update all db data at once", action='store_true')
    parser.add_argument('-u', help="update data incrementally", action='store_true')
    args = parser.parse_args()

    logger.info("Program starts")
    if args.a and args.u:
        logger.critical("You can't enter '-a' and '-u' as arguments at the same time")

    elif args.a:
        import_all_of_the_data()

    elif args.u:
        # it's the incremental update
        increment_update_wtp_collab()

    # assume that the cc.sqlite_conn here has closed
    logger.info("Program ends")

