from connection_coordinator import DataSource, get_coordinator
from migrator_kit.migrate_data import Migrater
from db_exporter_kit.export_db import db_exporter, get_db_exporter
from db_exporter_kit.age_appender import AgeAppender
from db_exporter_kit.data_sanitizor import DataSanitizer
import migrator_kit.table_creator as table_creator
from os.path import join, splitext
from os import rename
import json
import sqlite3
import argparse
import datetime

# Then I can test everything, and move on to implementation of the back arrow.
# configure connection

db_directory = ""
db_name = ""
db_archive = ""

def run_whole_process(cc):
    table_creator.create_new_table(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
    # m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
    # m.migrate_all_tables()

    # ageAppender = AgeAppender(cc, get_db_exporter())
    # ageAppender.append_age()

    # sanitzer = DataSanitizer(cc, get_db_exporter())
    # sanitzer.sanitizer_collab()
    return


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

def main():

    load_path()
    temp_db_path = join(db_directory, "temp.db")

    # Create a temporary db, in case the migration fails.
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = temp_db_path
    cc.connect()
    run_whole_process(cc)
    # my goal is to create a temporary data file, and move the origin one to the test folder, and then rename it

    cc.logger.info("backing up the origin db")
    # Rename the origin db file, put it into an archive folder, and then rename the temporary db to the db.
    origin_file_path = join(db_directory, db_name)
    archive_file_name = generate_archive_file_name(db_name)
    archive_file_path = join(db_archive, archive_file_name)
    rename(origin_file_path, archive_file_path)
    rename(temp_db_path, origin_file_path)

    # assume that the cc.sqlite_conn here has closed
    cc.logger.info("Program ends")

if __name__ == "__main__":
    main()
