import logging
from db_exporter_kit.export_db import db_exporter
import db_exporter_kit.waisman_utils.db_utils as db
from util.fetch_all_table_name import read_sqlite_tables_from_sqlite_cursor
from db_exporter_kit.waisman_utils.waisman_errors import AgeAppendingError, type_is_misc_warning
from connection_coordinator import get_coordinator, DataSource


class AgeAppender:
    """
        This class is a wrapper for the function add_ages_to_tables of db_exporter class.
        It travers each table in db (or tables list user define), and appends the age
        for mother, father, twin. The decision of which of three columns will be appended
        is based on whether the table has key "family", and "twin".
        Age columns will not get appended for those tables in unknown phases(known phases include 4,5,rd,c3)

    """

    # This class itself maintains a connection and db_exporter singleton
    def __init__(self, cc, exporter):
        self.cc = cc
        self.dest_db_con = cc.sqlite_conn
        self.collab_tables = None # saved for remembering the collab_tables

        # prepare logger
        self.logger = logging.getLogger("sql2sqlite.ageAppender")
        self.logger.setLevel(logging.INFO)
        fileHandler = logging.FileHandler("logs/ageAppender.logs")
        fileHandler.setLevel(logging.CRITICAL)
        fileHandler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
        self.logger.addHandler(fileHandler)
        self.exporter = exporter

    def append_age(self, tables=None):
        self.cc.connect()
        self.dest_db_con = self.cc.sqlite_conn
        table_names = None

        if tables is None and self.dest_db_con is not None:
            table_names = read_sqlite_tables_from_sqlite_cursor(self.dest_db_con.cursor())

        elif type(tables) is str:
            table_names = [tables]

        else:
            table_names = tables

        # filter out the table that is created by collaborator. We don't need them. Still testing
        # table_names = filter(lambda table_name: not self.is_collab_table(table_name), table_names)
        self.append_ages_to_tables(table_names)
        self.cc.close_all_connection()

    def append_ages_to_tables(self, table_names):
        for table_name in table_names:
            self.logger.info("------ Appending age for {0}------".format(table_name))
            try:
                self.exporter.add_ages_to_table(self.dest_db_con, table_name, verb=False)
            except AgeAppendingError as e:
                self.logger.critical("Appending age fail: table {table_name} ,msg: {msg}".format(table_name=table_name,
                                                                                              msg=e))
                continue

    def is_collab_table(self, tablename):
        if self.collab_tables is None:
            rows = db.get_rows(self.cc.sql_con, "USER_TABLE_TRACKER")
            self.collab_tables = [row["TABLE_NAME"] for row in rows]

        return tablename in self.collab_tables



def test():
    cc = get_coordinator(data_souce=DataSource.WTP_DATA)
    cc.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test.db'
    cc.connect()
    ageAppender = AgeAppender(cc.sqlite_conn)
    ageAppender.append_age()
    cc.close_all_connection()