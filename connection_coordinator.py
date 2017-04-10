import pyodbc
import sqlite3
import logging
from enum import Enum

highest_logger = logging.getLogger("sql2sqlite")
formatter = logging.Formatter('%(asctime)s: %(name)s: %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
highest_logger.addHandler(ch)

class DataSource(Enum):
    WTP_DATA = 1
    WTP_COLLAB = 2


class ConnectionCoordinator:

    # data source is used to specify which database we are using to get the data. Default is the wtp_collab mysql
    # however, I'm trying to support main database and try to integrate disensitized process/
    def __init__(self, data_source=DataSource.WTP_COLLAB):
        self.logger = logging.getLogger("sql2sqlite.coordinator")
        self.sqlite_filepath = 'C:/Users/jxu259/Desktop/sqlite/wtp_collab_test.db'
        self.logger.setLevel(logging.INFO)
        self.data_source = data_source
        self.sql_conn = None
        self.sqlite_conn = None
        self.sql_cur = None
        self.sqlite_cur = None
        self.connection_DSN = {DataSource.WTP_DATA : "wtp_data", DataSource.WTP_COLLAB : "wtp_collab"}

    # It can throw both sqlite connection exception and sql connection exception
    def connect(self):
        if self.sqlite_conn is None:
            self.sqlite_conn = sqlite3.connect(self.sqlite_filepath)
            #self.sqlite_conn = sqlite3.connect('C:/Users/jxu259/Desktop/sqlite/example.db')
            #self.sqlite_conn = sqlite3.connect('O:/wtp_collab.db')
            self.sqlite_cur = self.sqlite_conn.cursor()

        self.logger.info("Sqlite connection success")

        if self.sql_conn is None:
            self.sql_conn = pyodbc.connect(DSN=self.connection_DSN[self.data_source])
            self.logger.info("wtp_collab sql connection success")
            self.sql_cur = self.sql_conn.cursor()

    def commit(self):
        self.sqlite_conn.commit()
        self.sql_conn.commit()


    def close_all_connection(self):
        # reset everything

        if self.sqlite_conn is not None:
            self.sqlite_conn.close()
            self.sqlite_conn = None

        if self.sql_conn is not None:
            self.sql_conn.close()
            self.sql_conn = None

        self.sql_cur = None
        self.sqlite_cur = None


def get_coordinator(data_souce=DataSource.WTP_COLLAB):
    global c_cordinater
    if c_cordinater is None:
        c_cordinater = ConnectionCoordinator(data_source=data_souce)

    return c_cordinater


c_cordinater = None






