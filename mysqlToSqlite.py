from connection_coordinator import DataSource, get_coordinator
from migrator_kit.migrate_data import Migrater
from db_exporter_kit.export_db import db_exporter, get_db_exporter
from util.fetch_all_table_name import read_table_names_without_quote
from db_exporter_kit.age_appender import AgeAppender
from db_exporter_kit.data_sanitizor import DataSanitizer
import migrator_kit.table_creator as table_creator
from migrator_kit.checksum_table import ChecksumTable
from util.table_process_helper import TableProcessQueue
from connection_coordinator import ConnectionCoordinator
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
file_handler = logging.FileHandler("main.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
logger.addHandler(file_handler)

# One huge problem for the design of this program is its verbose and hard to remember.
# Even though I wrote it, I still have to acknowledge that I don't how to find a way to
# make the code intuitive and less error prone. More effort should be put on the thought
# of the design but I don't have time, and it probably needs a major change in thinking.
#
#               JJ 9/29/2017

db_directory = "" # type: str
db_name = "" # type: str
db_archive = "" # type: str
checksum_path = "" # type: str

UpdateTables = List[str]
NewTables = List[str]

# The code should be dedicated to the copy. Not mutate. We need a separated function for this
#
trackers = ["data_1_tr", "data_3_tr", "data_4_tr", "data_5_tr", "data_5_tr", "data_6_tr", "data_r1_tr", "data_r1_tr",
            "data_c3_tr", "data_mr_tr", "data_r1_tr", "data_s_tr", "data_at_dates", "data_sd_tr", "data_w14_tr"]
# type: List[str]

dob_updated_info_tables = ["gen_twins", "gen_parentdates"]

def put_trackers_to_the_front(data_tables : List[str]) -> List[str]:
    return_lists = [] # type : List[str]
    for table in data_tables:
        if table not in trackers:
            return_lists.append(table)
        else:
            return_lists.insert(0, table)
    return return_lists


def build_new_db_from_all_tables(cc : ConnectionCoordinator):

    m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter(), cc=cc)
    age_appender = AgeAppender(cc, get_db_exporter())
    sanitzer = DataSanitizer(cc, get_db_exporter())

    cc.connect()

    s_t, f_t = table_creator.create_new_table(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
    #print(s_t)
    #print(f_t)

    cc.connect()
    all_tables = read_table_names_without_quote(cc.sql_cur)
    # all_tables = ["calc_mr_pb_t","data_4_zy_e2","data_3_le_m","user_5_dps_p_2010_0210","data_4_cortid","data_4_au_m"]
    # all_tables = ['data_1_tr','arch_interview 1 response tracker', 'arch_interview 3 response tracker', 'arch_interview 4 response tracker', 'arch_zyg_follow up', 'trash_hhg_sd interventions', 'user_cla_complete sd screener data', 'user_cla_extended milw zips', 'user_cla_sd mpq by twin', 'user_cla_sd tracker dates spc flag']

    all_tables_queue = TableProcessQueue(all_tables)

    all_tables_queue.process_by(m.migrate_one_table)\
                    .process_by(age_appender.append_age_to_one_table)\
                    .process_by(sanitzer.sanitize_one_table)

    checksum_table = ChecksumTable(checksum_file_name=checksum_path)
    cc.connect()
    checksum_table.connect_to_db(cc.sql_conn)
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


def default_checksum_table():
    not_imported_tables = get_db_exporter().unwanted_tables()
    return ChecksumTable(ignore_tables=dob_updated_info_tables + not_imported_tables, checksum_file_name=checksum_path)


def update_the_db_development_based_on_checksum_table(cc: ConnectionCoordinator):
    checksum_table = default_checksum_table()
    checksum_table.connect_to_db(cc.sql_conn)
    checksum_table.load_in_file()
    new_tables, updated_tables = checksum_table.tables_for_migration(cc)
    update_the_db_development(cc, new_tables=new_tables, updated_tables=updated_tables, checksum_table = checksum_table)


def update_the_db_development(cc: ConnectionCoordinator,
                              new_tables:Optional[List[str]] = None,
                              updated_tables:Optional[List[str]] = None,
                              checksum_table: ChecksumTable = default_checksum_table()):
    cc.connect()
    checksum_table.connect_to_db(cc.sql_conn)

    if checksum_table.cur_checksum is None:
        checksum_table.load_in_file()

    # if the function called with updated_tables,
    if new_tables is None:
        new_tables = []
    if updated_tables is None:
        updated_tables = []

    # the update of the tracker should be put before the computation of the age for other tables
    new_tables_with_trackers_front = put_trackers_to_the_front(new_tables)
    updated_tables_with_trackers_front = put_trackers_to_the_front(updated_tables)

   # print(new_tables, updated_tables)

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


def show_changed_table(cc: ConnectionCoordinator) -> None:
    # get the checksum_table
    # call a function to return the table
    checksum_table = default_checksum_table()
    checksum_table.connect_to_db(cc.sql_conn)
    checksum_table.load_in_file()
    new_tables, updated_tables = checksum_table.tables_for_migration(cc)
    print("New Tables: {0}".format(new_tables))
    print("Updatged Tables: {0}".format(updated_tables))

    return

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
    cc.connect()
    update_the_db_development_based_on_checksum_table(cc)

#updated = ['arch_demographics', 'arch_ibq_respondent1', 'arch_ibq_respondent2', 'arch_ibq_respondent3', 'arch_int1_bci', 'arch_int1_bfi_respondent1', 'arch_int1_bfi_respondent2', 'arch_int1_bfi_respondent3', 'arch_int1_cbq_respondent1', 'arch_int1_cbq_respondent2', 'arch_int1_ccare', 'arch_int1_d_cbq', 'arch_int1_d_icq', 'arch_int1_d_pcq', 'arch_int1_d_tbaq', 'arch_int1_feq_respondent1', 'arch_int1_feq_respondent2', 'arch_int1_feq_respondent3', 'arch_int1_icq', 'arch_int1_paq_respondent1', 'arch_int1_paq_respondent2', 'arch_int1_paq_respondent3', 'arch_int1_pcq', 'arch_int1_psi', 'arch_int1_s_bci', 'arch_int1_s_bfi', 'arch_int1_s_ccare', 'arch_int1_s_feq', 'arch_int1_s_paq', 'arch_int1_s_scq', 'arch_int1_s_ses', 'arch_int1_s_zyg', 'arch_int1_scq', 'arch_int1_tbaq_respondent1', 'arch_int1_tbaq_respondent2', 'arch_interview 1 response tracker', 'arch_interview 3 response tracker', 'arch_interview 4 response tracker', 'arch_zyg', 'arch_zyg_follow up', 'calc_1_tb_99_m', 'calc_4_ad_c', 'calc_4_ad_t', 'data_1_cv_f', 'data_1_cv_m', 'data_1_tb_99_f', 'data_1_tb_99_m', 'data_3_bd_f', 'data_3_bd_m', 'data_3_bl_t', 'data_3_bp_pp_t', 'data_3_bp_sy_t', 'data_3_ch_m', 'data_3_le_m', 'data_3_mp_f', 'data_3_mp_m', 'data_3_pp_t', 'data_3_ps_m', 'data_3_sc_t', 'data_3_sp_m', 'data_3_span_mp_m', 'data_3_tp', 'data_4_ap_t', 'data_4_bb_f', 'data_4_bb_m', 'data_4_bb_t', 'data_4_bl_t', 'data_4_di_t', 'data_4_ei_t', 'data_4_ha_pb_t', 'data_4_ha_sp_c', 'data_4_ha_sp_t', 'data_4_hs_t', 'data_4_hu_t', 'data_4_le_m', 'data_4_mq_f', 'data_4_mq_m', 'data_4_nr_t', 'data_4_pp_t', 'data_4_rs_t', 'data_4_se_t', 'data_4_sp_m', 'data_4_sr_t', 'data_4_st_m', 'data_5_ap_t', 'data_5_bb_t', 'data_5_ha_pb_t', 'data_5_hu_t', 'data_5_rs_t', 'data_c3_ap_t', 'data_c3_bb_t', 'data_c3_ha_pb_t', 'data_c3_ha_sp_c', 'data_c3_ha_sp_t', 'data_c3_hu_t', 'data_c3_rs_t', 'data_mr_ps_f', 'data_mr_ps_m', 'data_rd_ap_t', 'data_rd_bb_t', 'data_rd_ha_pb_t', 'data_rd_ha_sp_c', 'data_rd_ha_sp_t', 'data_rd_hs_t', 'data_rd_hu_t', 'data_rd_rs_t', 'data_rd_se_t', 'data_rd_se_t_test', 'data_sd_mp_m', 'data_sd_sa_td_c', 'gen_staff', 'trash_mkv_geiq_10', 'trash_mkv_kathy6', 'trash_mkv_sb20', 'trash_mkv_sw39', 'user_jj_rdoc_ppt_info']
updated = [] # type: List[str]

def update_some_tables():
    db_path = join(db_directory, db_name)
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = db_path
    update_the_db_development(cc, updated_tables=updated)


def show_tables():
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.connect()
    show_changed_table(cc)

if __name__ == "__main__":
    load_path()
    parser = argparse.ArgumentParser(description="Import Data to SQLite")
    parser.add_argument('-a', help="update all db data at once", action='store_true')
    parser.add_argument('-u', help="update data incrementally", action='store_true')
    parser.add_argument('-t', help="update some tables, tables hard coded right now", action='store_true')
    parser.add_argument('-changed', help="see the tables that get changed", action='store_true')
    args = parser.parse_args()

    logger.info("Program starts")
    if args.a and args.u and args.t:
        logger.critical("You can't enter '-a' and '-u' and '-t' as arguments at the same time")

    elif args.a:
        import_all_of_the_data()

    elif args.u:
        # it's the incremental update
        increment_update_wtp_collab()

    elif args.t:
        update_some_tables()

    elif args.changed:
        show_tables()

    # assume that the cc.sqlite_conn here has closed
    logger.info("Program ends")

