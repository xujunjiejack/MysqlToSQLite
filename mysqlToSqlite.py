from connection_coordinator import DataSource, get_coordinator
from migrator_kit.migrate_data import Migrater
from db_exporter_kit.export_db import db_exporter, get_db_exporter
from db_exporter_kit.age_appender import AgeAppender
from db_exporter_kit.data_sanitizor import DataSanitizer
import migrator_kit.table_creator as table_creator
from migrator_kit.checksum_table import ChecksumTable
from os.path import join, splitext
from loggers import logging
from os import rename
import sys
import json
import datetime

logger = logging.getLogger("sql2sqlite.main")
logger.setLevel(logging.INFO)


# Then I can test everything, and move on to implementation of the back arrow.
# configure connection

db_directory = ""
db_name = ""
db_archive = ""

# The code should be dedicated to the copy. Not mutate. We need a seperated function for this
#

def build_new_db_from_all_tables(cc):

    # Now we need to truncate
    table_creator.create_new_table(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())

    m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
    # For test purpose
    # m.migrate_some_tables(["`calc_mr_pb_t`","`data_4_zy_e2`","`data_3_le_m`","`user_5_dps_p_2010_0210`","`data_4_cortid`","`data_4_au_m`"])
    m.migrate_all_tables()

    ageAppender = AgeAppender(cc, get_db_exporter())
    ageAppender.append_age()

    sanitzer = DataSanitizer(cc, get_db_exporter())
    sanitzer.sanitizer_collab()

    checksum_table = ChecksumTable()
    # Based on the succesful tables
    #

    return

def update_the_db(cc):
    # If only update, then I should care less about the performance, because the number of tables needed updated should be
    # relatively small.

    checksum_table = ChecksumTable()
    tables = checksum_table.tables_for_migration(cc)

    m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter(), cc=cc)
    age_appender = AgeAppender(cc, get_db_exporter())
    sanitzer = DataSanitizer(cc, get_db_exporter())

    successful_table = []
    failure_table = []
    print(tables)
    # for each table needed for proceeded

    # next step. Make sure the exception will be thrown to this level
    for table in tables:
        temp_table = table + "_origin_as_temp"
        # The temporary table holds the original table.
        try:
            table_creator.rename_one_to_another_table(table, temp_table, cc)
        except Exception as e:
            # seperate this exception from the rest of the process.
            # If the program encounters an exception at this point, then the origin table should have its origin data,
            # because it hasn't got changed into the temp_table

            # Since the origin data is still in the table, then I can't drop the table, or I will lose the data in wtp_collab
            # This lose of data is the least thing I want to happen.
            failure_table.append(table)
            logger.critical(e)
            continue

        try:
            table_creator.create_new_table(table, data_source=DataSource.WTP_DATA, exporter=get_db_exporter())

            m.migrate_some_tables(table)

            age_appender.append_age(table)

            sanitzer.sanitizer_collab(table)

            table_creator.delete_the_table(temp_table, cc)

            successful_table.append(table)
        except Exception as e:

            failure_table.append(table)
            # delete the temporary table
            table_creator.delete_the_table(table, cc)
            table_creator.rename_one_to_another_table(temp_table, table, cc)
            logger.critical(e)

    checksum_table.update_the_checksum_of_successful_tables(successful_table)

    # the best way is to create a new table. At the end of the process, the response towards success should
    # be deleting the origin table, and rename it. If the response is failure, then delete this temporary table.

    #   truncate the table, or delete the table
    #   So that I can get around with the winded commit system with my own .
    #   create new one

    #   migrator
    #   append age to it
    #   do data cleaning
    # I need a transaction and rollback system

def load_path():
    with open("file_path.json", "r") as f:
        setting = json.load(f)
        global db_name
        global db_archive
        global db_directory
        db_name = setting["db_name"]
        db_directory = setting["db_directory"]
        db_archive = setting["archive_dir"]
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
    archive_file_name = generate_archive_file_name(db_name)
    archive_file_path = join(db_archive, archive_file_name)
    rename(origin_file_path, archive_file_path)
    rename(temp_db_path, origin_file_path)


def increment_update_wtp_collab():
    db_path = join(db_directory, db_name)
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = db_path
    update_the_db(cc)


def main(arg):
    logger.info("Program starts")

    if arg[1] == "-a":
        import_all_of_the_data()

    elif arg[1] == "-u":
        # it's the incremental update
        increment_update_wtp_collab()

    # assume that the cc.sqlite_conn here has closed
    logger.info("Program ends")

if __name__ == "__main__":
    load_path()
    main(sys.argv)
