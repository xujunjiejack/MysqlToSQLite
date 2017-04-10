from connection_coordinator import DataSource, get_coordinator
from migrator_kit.migrate_data import Migrater
from db_exporter_kit.export_db import db_exporter, get_db_exporter
from db_exporter_kit.age_appender import AgeAppender
from db_exporter_kit.data_sanitizor import DataSanitizer
import migrator_kit.table_creator as table_creator
import sqlite3

# Then I can test everything, and move on to implementation of the back arrow.
# TODO: Don't append age to user, or I don't need worry about it?

# configure connection
cc = get_coordinator(data_souce=DataSource.WTP_DATA)
cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test2.db'
cc.connect()

# whole process to migrate data from WTP_DATA to sqlite3
# table_creator.create_new_table(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
# m = Migrater(data_source=DataSource.WTP_DATA, exporter=get_db_exporter())
# m.migrate_all_tables()


# assume that the cc.sqlite_conn here has closed
# Append age
#ageAppender = AgeAppender(cc, get_db_exporter())
#ageAppender.append_age()

sanitzer = DataSanitizer(cc, get_db_exporter())
sanitzer.sanitizer_collab()

#exporter.add_ages_to_table(cc.sqlite_conn, "data_rdmr_hdt_t", verb=False)

cc.logger.info("Program ends")